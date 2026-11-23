package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// ErrNotFound, istenen kaydın bulunamadığını bildirir. Çağıran bunu 404'e çevirir.
var ErrNotFound = errors.New("kayıt bulunamadı")

// NotificationWithAttempts, bir bildirim ve deneme geçmişi.
//
// GET {id} ve listeleme aynı şekli kullanır, böylece handler tek bir dönüştürücüyle
// çalışır ve iki kod yolu ayrışmaz.
type NotificationWithAttempts struct {
	Notification *domain.Notification
	Attempts     []domain.Attempt
}

// notificationColumns, okuma sorgularının seçtiği kolonlar.
// Tek yerde tutuluyor ki scan sırası ile SELECT sırası ayrışmasın.
const notificationColumns = `
	id, batch_id, channel, recipient, content, priority, status,
	attempt_count, max_attempts, available_at, lease_token, lease_expires_at,
	provider_message_id, last_error, correlation_id, template_id,
	created_at, updated_at`

// scanNotification, bir satırı domain modeline okur.
func scanNotification(row pgx.Row) (*domain.Notification, error) {
	var (
		n       domain.Notification
		channel string
		status  string
		prio    string
	)

	err := row.Scan(
		&n.ID, &n.BatchID, &channel, &n.Recipient, &n.Content, &prio, &status,
		&n.AttemptCount, &n.MaxAttempts, &n.AvailableAt, &n.LeaseToken, &n.LeaseExpiresAt,
		&n.ProviderMessageID, &n.LastError, &n.CorrelationID, &n.TemplateID,
		&n.CreatedAt, &n.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	n.Channel = domain.Channel(channel)
	n.Status = domain.Status(status)
	n.Priority = domain.Priority(prio)

	return &n, nil
}

// GetNotification, tek bir bildirimi deneme geçmişiyle döner.
func (s *Store) GetNotification(
	ctx context.Context, id uuid.UUID,
) (NotificationWithAttempts, error) {
	query := `SELECT ` + notificationColumns + ` FROM notifications WHERE id = $1`

	notification, err := scanNotification(s.pool.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return NotificationWithAttempts{}, ErrNotFound
	}
	if err != nil {
		return NotificationWithAttempts{}, fmt.Errorf("bildirim okunamadı: %w", err)
	}

	// Tek elemanlı dilimle aynı yardımcı kullanılıyor: listeleme ile tek kayıt okuma
	// farklı kod yolları olmasın.
	attempts, err := s.attemptsFor(ctx, []uuid.UUID{id})
	if err != nil {
		return NotificationWithAttempts{}, err
	}

	return NotificationWithAttempts{
		Notification: notification,
		Attempts:     attempts[id],
	}, nil
}

// attemptsByNotificationsQuery, birden çok bildirimin denemelerini TEK sorguyla çeker.
//
// N+1 sorgu yerine tek sorgu: 20 kayıtlık bir sayfa için 20 değil 1 gidiş-dönüş.
// ORDER BY, notification_attempts üzerindeki UNIQUE (notification_id, attempt_no)
// index'inin sırasıyla birebir aynı → Sort adımı gerekmez, ek index de gerekmez.
const attemptsByNotificationsQuery = `
	SELECT notification_id, attempt_no, status_code, error, duration_ms, created_at
	FROM notification_attempts
	WHERE notification_id = ANY($1)
	ORDER BY notification_id, attempt_no`

// attemptsFor, verilen bildirimlerin denemelerini kimliğe göre gruplanmış döner.
//
// Dönen haritada yalnızca denemesi olan kimlikler bulunur; çağıran eksik kimlik için
// boş dilim görür (nil map erişimi sıfır değer döner).
func (s *Store) attemptsFor(
	ctx context.Context, ids []uuid.UUID,
) (map[uuid.UUID][]domain.Attempt, error) {
	grouped := make(map[uuid.UUID][]domain.Attempt, len(ids))

	if len(ids) == 0 {
		return grouped, nil
	}

	rows, err := s.pool.Query(ctx, attemptsByNotificationsQuery, ids)
	if err != nil {
		return nil, fmt.Errorf("deneme geçmişi okunamadı: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			notificationID uuid.UUID
			attempt        domain.Attempt
		)

		if err := rows.Scan(
			&notificationID, &attempt.AttemptNo, &attempt.StatusCode,
			&attempt.Error, &attempt.DurationMS, &attempt.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("deneme kaydı okunamadı: %w", err)
		}

		grouped[notificationID] = append(grouped[notificationID], attempt)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("deneme geçmişi okunamadı: %w", err)
	}

	return grouped, nil
}

// buildListQuery, filtreye göre listeleme sorgusunu ve argümanlarını üretir.
//
// Neden dinamik? "$n IS NULL OR kolon = $n" kalıbı kullanılmıyor: pgx hazır ifadeleri
// önbelleğe alıyor ve PostgreSQL birkaç çalıştırmadan sonra generic plan'a geçiyor.
// Generic plan'da parametrenin NULL olup olmadığı bilinmediği için koşul index aralığı
// (Index Cond) olarak kullanılamaz, Filter'a düşer. Cursor koşulu için bu yıkıcı:
// derin sayfalarda atlanması gereken tüm satırlar okunmaya başlar.
//
// Yalnızca placeholder NUMARALARI biçimlendiriliyor; koşul şablonları kod içinde sabit
// ve değerler her zaman parametre olarak gidiyor, yani SQL'e dışarıdan metin girmiyor.
func buildListQuery(filter domain.ListFilter) (string, []any) {
	var (
		conditions []string
		args       []any
	)

	if len(filter.Statuses) > 0 {
		values := make([]string, 0, len(filter.Statuses))
		for _, status := range filter.Statuses {
			values = append(values, string(status))
		}

		args = append(args, values)
		conditions = append(conditions, fmt.Sprintf("status = ANY($%d)", len(args)))
	}

	if len(filter.Channels) > 0 {
		values := make([]string, 0, len(filter.Channels))
		for _, channel := range filter.Channels {
			values = append(values, string(channel))
		}

		args = append(args, values)
		conditions = append(conditions, fmt.Sprintf("channel = ANY($%d)", len(args)))
	}

	// Yarı açık aralık [from, to): ardışık zaman pencereleri sınırdaki kaydı iki kez saymaz.
	if filter.From != nil {
		args = append(args, *filter.From)
		conditions = append(conditions, fmt.Sprintf("created_at >= $%d", len(args)))
	}

	if filter.To != nil {
		args = append(args, *filter.To)
		conditions = append(conditions, fmt.Sprintf("created_at < $%d", len(args)))
	}

	// Keyset sayfalama: OFFSET yerine satır karşılaştırması, çünkü OFFSET atlanan
	// satırları da okur ve derin sayfalarda doğrusal yavaşlar.
	if filter.Cursor != nil {
		args = append(args, filter.Cursor.CreatedAt, filter.Cursor.ID)
		conditions = append(conditions,
			fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}

	// limit+1: fazlalık gelirse sonraki sayfa var demektir.
	args = append(args, filter.Limit+1)

	var where string
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}

	// ORDER BY tam olarak notifications_cursor_idx'in sırası → Sort adımı yok.
	query := `SELECT ` + notificationColumns + ` FROM notifications` + where +
		fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))

	return query, args
}

// ListNotifications, filtreye uyan bildirimleri deneme geçmişleriyle döner.
//
// hasMore, istenen limitten fazla kayıt bulunduğunu söyler; fazlalık kırpılır ve
// çağıran buna göre cursor üretir.
func (s *Store) ListNotifications(
	ctx context.Context, filter domain.ListFilter,
) ([]NotificationWithAttempts, bool, error) {
	query, args := buildListQuery(filter)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("bildirimler listelenemedi: %w", err)
	}
	defer rows.Close()

	notifications := make([]*domain.Notification, 0, filter.Limit+1)

	for rows.Next() {
		notification, scanErr := scanNotification(rows)
		if scanErr != nil {
			return nil, false, fmt.Errorf("bildirim kaydı okunamadı: %w", scanErr)
		}

		notifications = append(notifications, notification)
	}

	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("bildirimler listelenemedi: %w", err)
	}

	hasMore := len(notifications) > filter.Limit
	if hasMore {
		notifications = notifications[:filter.Limit]
	}

	ids := make([]uuid.UUID, 0, len(notifications))
	for _, notification := range notifications {
		ids = append(ids, notification.ID)
	}

	// Sayfadaki tüm denemeler tek sorguyla: listedeki attempts alanı boş bırakılamaz,
	// aksi hâlde "denenmedi" anlamına gelip yanıltırdı.
	attempts, err := s.attemptsFor(ctx, ids)
	if err != nil {
		return nil, false, err
	}

	result := make([]NotificationWithAttempts, 0, len(notifications))
	for _, notification := range notifications {
		result = append(result, NotificationWithAttempts{
			Notification: notification,
			Attempts:     attempts[notification.ID],
		})
	}

	return result, hasMore, nil
}

// batchSummaryQuery, batch'in durum bazında sayımını ve ilk oluşturma zamanını döner.
//
// notifications_batch_idx (batch_id, status) satırları bulmak için kullanılıyor
// (EXPLAIN: Bitmap Index Scan). min(created_at) index'te olmayan bir kolon olduğu için
// plan ayrıca heap'e gidiyor — 1000 satırlık bir batch için önemsiz bir maliyet ve
// karşılığında batch'in oluşma zamanını ikinci bir sorgu atmadan alıyoruz.
// created_at'i index'e eklemek sayımı index-only yapardı ama index'i her batch için
// büyütürdü; bu takas bilinçli.
const batchSummaryQuery = `
	SELECT status, count(*), min(created_at)
	FROM notifications
	WHERE batch_id = $1
	GROUP BY status`

// GetBatchSummary, bir batch'in durum bazında özetini döner.
//
// Batch'te hiç bildirim yoksa ErrNotFound: var olmayan bir batch için sıfırlardan oluşan
// bir özet dönmek, istemciye "batch var ama boş" demek olurdu.
func (s *Store) GetBatchSummary(
	ctx context.Context, batchID uuid.UUID,
) (domain.BatchSummary, error) {
	rows, err := s.pool.Query(ctx, batchSummaryQuery, batchID)
	if err != nil {
		return domain.BatchSummary{}, fmt.Errorf("batch özeti okunamadı: %w", err)
	}
	defer rows.Close()

	var (
		counts    = make(map[domain.Status]int)
		createdAt time.Time
	)

	for rows.Next() {
		var (
			status      string
			count       int
			groupOldest time.Time
		)

		if err := rows.Scan(&status, &count, &groupOldest); err != nil {
			return domain.BatchSummary{}, fmt.Errorf("batch sayımı okunamadı: %w", err)
		}

		counts[domain.Status(status)] = count

		// Batch'in oluşma zamanı = en eski satırın zamanı.
		if createdAt.IsZero() || groupOldest.Before(createdAt) {
			createdAt = groupOldest
		}
	}

	if err := rows.Err(); err != nil {
		return domain.BatchSummary{}, fmt.Errorf("batch özeti okunamadı: %w", err)
	}

	if len(counts) == 0 {
		return domain.BatchSummary{}, ErrNotFound
	}

	// Sıfır doldurma ve Completed hesabı domain'de: durum listesi burada tekrarlanmaz.
	return domain.NewBatchSummary(batchID, counts, createdAt), nil
}
