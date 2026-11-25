package store

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// createNotificationQuery, tekil bildirim ekler.
//
// status ve attempt_count veritabanı varsayılanlarına bırakılmıyor, açıkça yazılıyor:
// kaydın hangi durumda oluştuğu çağıran tarafın kararı ve bu karar domain'de verildi.
//
// RETURNING ile zaman damgaları veritabanından okunuyor; uygulama saati ile DB saati
// arasındaki fark yanıta sızmasın ve istemci gerçekten kaydedilen değeri görsün.
//
// available_at COALESCE ile yazılıyor: zamanlanmamış bildirimde argüman NULL geçer ve
// damgayı DB basar (batch yolundaki kuralla aynı — tek saat kaynağı DB). Daha önce
// uygulama saati yazılıyordu ve claim sorgusu DB saatine baktığı için API ile Postgres
// farklı saatlerdeyse satır bir süre görünmez kalıyordu. Zamanlanmış bildirimde ise
// argüman doludur; o değer bir saat OKUMASI değil, istemcinin verdiği mutlak an.
const createNotificationQuery = `
	INSERT INTO notifications
		(id, batch_id, channel, recipient, content, priority, status,
		 attempt_count, max_attempts, available_at, correlation_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, COALESCE($10::timestamptz, now()), $11)
	RETURNING available_at, created_at, updated_at`

// batchColumns, CopyFrom ile yazılan kolonlar.
//
// available_at, created_at ve updated_at bilerek YOK: veritabanı varsayılanı (now())
// uygulanır, böylece toplu yazımda tek saat kaynağı DB olur. priority_rank ise
// GENERATED ALWAYS olduğu için verilemez.
//
// Zamanlanmış öğeler bu listeye available_at EKLENEREK çözülmedi: COPY satır başına
// "varsayılanı uygula" diyemiyor (NULL geçmek NOT NULL kısıtını ihlal eder), yani
// karışık bir batch'te zamanlanmamış satırlara uygulama saatini yazmak zorunda
// kalırdık. Bunun yerine COPY'den sonra aynı transaction'da yalnızca zamanlanmış
// satırlar tek UPDATE ile güncelleniyor (scheduleBatchQuery).
var batchColumns = []string{
	"id", "batch_id", "channel", "recipient", "content", "priority",
	"status", "attempt_count", "max_attempts", "correlation_id",
}

// CreateNotification, tekil bildirimi idempotency koruması altında yazar.
//
// renderResponse bir fonksiyon değeri: store API'nin DTO tipini bilmez, httpapi de pgx'i
// bilmez. Zaman damgaları RETURNING ile dolduktan SONRA çağrılır, böylece saklanan cevap
// istemciye dönenle birebir aynı olur.
func (s *Store) CreateNotification(
	ctx context.Context,
	n *domain.Notification,
	idem IdempotencyRequest,
	renderResponse func() any,
) (Outcome, error) {
	return s.runIdempotent(ctx, idem, func(ctx context.Context, tx pgx.Tx) error {
		err := tx.QueryRow(ctx, createNotificationQuery,
			n.ID,
			n.BatchID,
			string(n.Channel),
			n.Recipient,
			n.Content,
			string(n.Priority),
			string(n.Status),
			n.AttemptCount,
			n.MaxAttempts,
			nullableTime(n.AvailableAt),
			n.CorrelationID,
		).Scan(&n.AvailableAt, &n.CreatedAt, &n.UpdatedAt)
		if err != nil {
			return fmt.Errorf("bildirim eklenemedi: %w", err)
		}

		return nil
	}, renderResponse)
}

// CreateBatch, bildirimleri tek transaction'da toplu yazar.
//
// CopyFrom (PostgreSQL COPY protokolü) seçildi: 1000 satır için en hızlı yol ve tek ağ
// geçişi. RETURNING desteklemiyor ama batch yanıtı özet olduğu için zaman damgasına
// ihtiyaç yok. Bir satır kısıt ihlal ederse tüm COPY düşer — zaten hep ya hiç istiyoruz.
func (s *Store) CreateBatch(
	ctx context.Context,
	notifications []*domain.Notification,
	idem IdempotencyRequest,
	renderResponse func() any,
) (Outcome, error) {
	return s.runIdempotent(ctx, idem, func(ctx context.Context, tx pgx.Tx) error {
		source := pgx.CopyFromSlice(len(notifications), func(i int) ([]any, error) {
			n := notifications[i]

			return []any{
				n.ID,
				n.BatchID,
				string(n.Channel),
				n.Recipient,
				n.Content,
				string(n.Priority),
				string(n.Status),
				n.AttemptCount,
				n.MaxAttempts,
				n.CorrelationID,
			}, nil
		})

		copied, err := tx.CopyFrom(ctx, pgx.Identifier{"notifications"}, batchColumns, source)
		if err != nil {
			return fmt.Errorf("batch yazılamadı: %w", err)
		}

		if copied != int64(len(notifications)) {
			return fmt.Errorf("batch eksik yazıldı: %d/%d", copied, len(notifications))
		}

		return scheduleBatch(ctx, tx, notifications)
	}, renderResponse)
}

// scheduleBatchQuery, zamanlanmış batch satırlarının available_at'ini düzeltir.
//
// Tek statement, iki dizi: unnest ile kimlik–zaman çiftleri satıra çevrilip tek
// UPDATE ile yazılıyor. Satır başına bir UPDATE, 1000 öğeli bir batch'te COPY'nin
// kazandırdığı her şeyi geri verirdi.
const scheduleBatchQuery = `
	UPDATE notifications AS n
	SET available_at = s.send_at
	FROM unnest($1::uuid[], $2::timestamptz[]) AS s(id, send_at)
	WHERE n.id = s.id`

// scheduleBatch, batch içindeki zamanlanmış satırların available_at'ini yazar.
//
// Hiç zamanlanmış satır yoksa sorgu HİÇ çalışmaz: zamanlanmış bildirim istisnai durum,
// olağan batch yolunun maliyeti artmasın.
func scheduleBatch(ctx context.Context, tx pgx.Tx, notifications []*domain.Notification) error {
	ids := make([]uuid.UUID, 0, len(notifications))
	times := make([]time.Time, 0, len(notifications))

	for _, n := range notifications {
		if n.AvailableAt.IsZero() {
			continue
		}

		ids = append(ids, n.ID)
		times = append(times, n.AvailableAt)
	}

	if len(ids) == 0 {
		return nil
	}

	tag, err := tx.Exec(ctx, scheduleBatchQuery, ids, times)
	if err != nil {
		return fmt.Errorf("zamanlanmış batch satırları yazılamadı: %w", err)
	}

	// Satır sayısı tutmuyorsa COPY ile UPDATE arasında bir şey tutarsız: sessizce
	// devam etmek, istemciye "kabul edildi" deyip bazı mesajları yanlış zamanda
	// göndermek olurdu.
	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("zamanlanmış satır sayısı tutmuyor: %d/%d",
			tag.RowsAffected(), len(ids))
	}

	return nil
}

// nullableTime, sıfır zamanı NULL'a çevirir.
//
// Sıfır değer "veritabanı damgalasın" anlamına geliyor (bkz. createNotificationQuery).
func nullableTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}

	return &value
}

// runIdempotent, verilen işi idempotency koruması altında tek transaction'da çalıştırır.
//
// Sıra bilinçli — anahtar İŞTEN ÖNCE talep edilir: aksi hâlde 1000 satır yazılıp
// tekrar gönderim fark edildiğinde çöpe atılırdı. Yer tutucu cevap aynı transaction
// içinde güncellendiği için yarı yazılmış bir kayıt kalamaz; rollback her şeyi siler.
func (s *Store) runIdempotent(
	ctx context.Context,
	idem IdempotencyRequest,
	work func(ctx context.Context, tx pgx.Tx) error,
	renderResponse func() any,
) (Outcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Outcome{}, fmt.Errorf("transaction başlatılamadı: %w", err)
	}

	// Commit edilmiş bir transaction'da Rollback hata vermez (ErrTxClosed yutulur),
	// bu yüzden defer güvenli bir ağ olarak bırakılıyor.
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if idem.enabled() {
		claimed, claimErr := claimIdempotencyKey(ctx, tx, idem)
		if claimErr != nil {
			return Outcome{}, claimErr
		}

		// Taze bir kayıt var: iş hiç yapılmaz, saklanan cevap döner (ya da 409).
		if !claimed {
			return readStoredResponse(ctx, tx, idem)
		}
	}

	if err := work(ctx, tx); err != nil {
		return Outcome{}, err
	}

	if idem.enabled() {
		// Cevap iş tamamlandıktan sonra üretilir: tekil uçta zaman damgaları artık dolu.
		if err := completeIdempotencyKey(
			ctx, tx, idem, http.StatusAccepted, renderResponse(),
		); err != nil {
			return Outcome{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Outcome{}, fmt.Errorf("transaction commit edilemedi: %w", err)
	}

	return Outcome{}, nil
}
