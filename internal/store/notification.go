package store

import (
	"context"
	"fmt"
	"net/http"

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
const createNotificationQuery = `
	INSERT INTO notifications
		(id, batch_id, channel, recipient, content, priority, status,
		 attempt_count, max_attempts, available_at, correlation_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	RETURNING created_at, updated_at`

// batchColumns, CopyFrom ile yazılan kolonlar.
//
// available_at, created_at ve updated_at bilerek YOK: veritabanı varsayılanı (now())
// uygulanır, böylece toplu yazımda tek saat kaynağı DB olur. priority_rank ise
// GENERATED ALWAYS olduğu için verilemez.
// (Zamanlanmış bildirim bonusu geldiğinde available_at bu listeye eklenecek.)
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
			n.AvailableAt,
			n.CorrelationID,
		).Scan(&n.CreatedAt, &n.UpdatedAt)
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

		return nil
	}, renderResponse)
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
