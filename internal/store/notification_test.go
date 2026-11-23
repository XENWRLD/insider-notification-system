package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// newTestNotification, domain kurallarından geçmiş geçerli bir bildirim üretir.
func newTestNotification(t *testing.T, channel, priority string) *domain.Notification {
	t.Helper()

	recipients := map[string]string{
		"sms":   "+905551234567",
		"email": "ali@example.com",
		"push":  "dGVzdC10b2tlbg",
	}

	notification, err := domain.NewNotification(domain.NewNotificationInput{
		Channel:   channel,
		Recipient: recipients[channel],
		Content:   "Merhaba",
		Priority:  priority,
	})
	require.NoError(t, err)

	notification.CorrelationID = uuid.NewString()

	return notification
}

// TestCreateNotificationPersistsAllFields, yazılan satırın tamamını geri okur.
//
// Amaç yalnızca "hata dönmedi" demek değil: kolonların doğru sıraya bağlandığını
// doğrulamak. Parametre sırası kaysa (örn. recipient ile content yer değiştirse) test
// bunu yakalar, oysa yalnızca hata kontrolü yapan bir test fark etmezdi.
func TestCreateNotificationPersistsAllFields(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	notification := newTestNotification(t, "sms", "high")

	before := time.Now().UTC()
	require.NoError(t, store.CreateNotification(ctx, notification))

	// RETURNING ile zaman damgaları veritabanından dolduruldu.
	require.False(t, notification.CreatedAt.IsZero(), "created_at doldurulmalıydı")
	require.False(t, notification.UpdatedAt.IsZero(), "updated_at doldurulmalıydı")
	require.WithinDuration(t, before, notification.CreatedAt, time.Minute)

	var got struct {
		channel       string
		recipient     string
		content       string
		priority      string
		priorityRank  int16
		status        string
		attemptCount  int
		maxAttempts   int
		correlationID string
		availableAt   time.Time
		batchID       *uuid.UUID
		leaseToken    *uuid.UUID
	}

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT channel, recipient, content, priority, priority_rank, status,
		       attempt_count, max_attempts, correlation_id, available_at,
		       batch_id, lease_token
		FROM notifications WHERE id = $1`, notification.ID,
	).Scan(
		&got.channel, &got.recipient, &got.content, &got.priority, &got.priorityRank,
		&got.status, &got.attemptCount, &got.maxAttempts, &got.correlationID,
		&got.availableAt, &got.batchID, &got.leaseToken,
	))

	require.Equal(t, "sms", got.channel)
	require.Equal(t, "+905551234567", got.recipient)
	require.Equal(t, "Merhaba", got.content)
	require.Equal(t, "high", got.priority)
	require.EqualValues(t, 1, got.priorityRank, "high önceliğin rank'i 1 olmalı")
	require.Equal(t, "pending", got.status)
	require.Zero(t, got.attemptCount)
	require.Equal(t, domain.DefaultMaxAttempts, got.maxAttempts)

	// correlation_id kayda yazılmalı: API ve worker ayrı process, kimlik DB üzerinden taşınır.
	require.Equal(t, notification.CorrelationID, got.correlationID)

	// Tekil oluşturmada batch yok, iş henüz çekilmediği için lease de yok.
	require.Nil(t, got.batchID)
	require.Nil(t, got.leaseToken)
}

// TestCreateNotificationAllChannels, üç kanalın da yazılabildiğini doğrular.
func TestCreateNotificationAllChannels(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	for _, channel := range []string{"sms", "email", "push"} {
		notification := newTestNotification(t, channel, "")
		require.NoError(t, store.CreateNotification(ctx, notification))
	}

	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM notifications WHERE status = 'pending'`).Scan(&count))
	require.Equal(t, 3, count)
}

// TestCreateNotificationIsImmediatelyClaimable, kaydın INSERT sonrası worker tarafından
// çekilebilir olduğunu doğrular.
//
// Kayıt ile iş kuyruğu aynı tablo olduğu için ayrı bir "kuyruğa ekle" adımı yok;
// bu test o tasarım kararının gerçekten işlediğini gösterir.
func TestCreateNotificationIsImmediatelyClaimable(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	// Öncelikler karışık sırada yazılıyor; claim sorgusu high'ı önce getirmeli.
	for _, priority := range []string{"low", "normal", "high"} {
		require.NoError(t, store.CreateNotification(ctx, newTestNotification(t, "sms", priority)))
	}

	// Adım 6'daki claim sorgusunun şekli (henüz kilit alınmıyor).
	rows, err := pool.Query(ctx, `
		SELECT priority FROM notifications
		WHERE channel = 'sms'
		  AND status IN ('pending', 'retrying')
		  AND available_at <= now()
		ORDER BY priority_rank, available_at, id`)
	require.NoError(t, err)
	defer rows.Close()

	var order []string
	for rows.Next() {
		var priority string
		require.NoError(t, rows.Scan(&priority))
		order = append(order, priority)
	}
	require.NoError(t, rows.Err())

	require.Equal(t, []string{"high", "normal", "low"}, order)
}

// TestCreateNotificationRejectsDuplicateID, birincil anahtar çakışmasının hata olarak
// sarılıp döndüğünü doğrular.
func TestCreateNotificationRejectsDuplicateID(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	notification := newTestNotification(t, "sms", "normal")

	require.NoError(t, store.CreateNotification(ctx, notification))

	err := store.CreateNotification(ctx, notification)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bildirim eklenemedi")
}

// TestCreateNotificationHonoursContextCancellation, iptal edilmiş context ile yazım
// denenmediğini doğrular — her dış çağrı context'e saygı duymalı.
func TestCreateNotificationHonoursContextCancellation(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.CreateNotification(ctx, newTestNotification(t, "sms", "normal"))
	require.ErrorIs(t, err, context.Canceled)
}
