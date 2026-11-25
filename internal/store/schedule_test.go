package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// newScheduledNotification, verilen an için zamanlanmış bir bildirim üretir.
func newScheduledNotification(t *testing.T, sendAt time.Time) *domain.Notification {
	t.Helper()

	notification, err := domain.NewNotification(domain.NewNotificationInput{
		Channel:   "sms",
		Recipient: "+905551234567",
		Content:   "Zamanlanmis mesaj",
		SendAt:    sendAt.Format(time.RFC3339Nano),
	})
	require.NoError(t, err)

	notification.CorrelationID = fmt.Sprintf("sched-%d", sendAt.UnixNano())

	return notification
}

// TestCreateNotificationStampsAvailableAtFromDatabase, zamanlanmamış bildirimde
// available_at'i veritabanının bastığını doğrular.
//
// Bu, F12'nin regresyon testi: önce uygulama saati yazılıyordu ve claim sorgusu DB
// saatine baktığı için API ile Postgres farklı saatlerdeyse satır bir süre görünmez
// kalıyordu. İki saatin aynı makinede olduğu bir testte fark doğrudan gözlenemez, bu
// yüzden ölçüt DEĞERİN KAYNAĞI: damga created_at ile aynı transaction saatinden
// gelmeli, yani ikisi birbirine eşit olmalı.
func TestCreateNotificationStampsAvailableAtFromDatabase(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.True(t, notification.AvailableAt.IsZero(), "domain hemen gönderimi sıfır bırakır")

	require.NoError(t, createPlain(t, store, ctx, notification))

	// RETURNING ile dolduruldu: çağıran gerçekten kaydedilen değeri görüyor.
	require.False(t, notification.AvailableAt.IsZero())
	require.Equal(t, notification.CreatedAt, notification.AvailableAt,
		"zamanlanmamış kayıtta available_at = created_at (tek saat kaynağı DB)")
}

// TestCreateNotificationScheduledIsNotClaimableUntilDue, zamanı gelmemiş bildirimin
// çekilmediğini doğrular (docs/PLAN.md §9 test listesi: "Scheduled").
func TestCreateNotificationScheduledIsNotClaimableUntilDue(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	scheduled := newScheduledNotification(t, time.Now().Add(time.Hour))
	require.NoError(t, createPlain(t, store, ctx, scheduled))

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 10, time.Minute)
	require.NoError(t, err)
	require.Empty(t, claimed, "zamanı gelmemiş bildirim çekilmemeli")

	// Zaman geldiğinde aynı satır çekilebilir olmalı: bekleme kalıcı bir engel değil.
	_, err = store.pool.Exec(ctx,
		`UPDATE notifications SET available_at = now() - interval '1 second' WHERE id = $1`,
		scheduled.ID)
	require.NoError(t, err)

	claimed, err = store.ClaimNotifications(ctx, domain.ChannelSMS, 10, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, scheduled.ID, claimed[0].ID)
}

// TestCreateNotificationScheduledKeepsRequestedInstant, yazılan anın istemcinin
// verdiği an olduğunu doğrular.
func TestCreateNotificationScheduledKeepsRequestedInstant(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	sendAt := time.Now().Add(90 * time.Minute).UTC().Truncate(time.Millisecond)

	scheduled := newScheduledNotification(t, sendAt)
	require.NoError(t, createPlain(t, store, ctx, scheduled))

	var stored time.Time
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT available_at FROM notifications WHERE id = $1`, scheduled.ID).Scan(&stored))

	require.True(t, stored.Equal(sendAt), "saklanan %s, istenen %s", stored, sendAt)
}

// TestCreateBatchWritesScheduledRowsOnly, karışık bir batch'te yalnızca zamanlanmış
// satırların available_at'inin yazıldığını doğrular.
//
// Karışık batch asıl zor durum: COPY satır başına "varsayılanı uygula" diyemediği için
// zamanlanmış öğeler ayrı bir UPDATE ile düzeltiliyor ve zamanlanmamış öğeler DB
// damgasında kalmalı.
func TestCreateBatchWritesScheduledRowsOnly(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	sendAt := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Millisecond)

	notifications, batchID := newTestBatch(t, 3)
	// Ortadaki öğe zamanlanmış: sıranın da doğru eşlendiğini görmek için uçta değil.
	notifications[1].AvailableAt = sendAt

	_, err := store.CreateBatch(ctx, notifications, noIdem, func() any { return nil })
	require.NoError(t, err)

	rows, err := store.pool.Query(ctx,
		`SELECT id, available_at, created_at FROM notifications WHERE batch_id = $1`, batchID)
	require.NoError(t, err)
	defer rows.Close()

	stamps := make(map[string]struct{ availableAt, createdAt time.Time })

	for rows.Next() {
		var (
			id      string
			stamp   time.Time
			created time.Time
		)
		require.NoError(t, rows.Scan(&id, &stamp, &created))
		stamps[id] = struct{ availableAt, createdAt time.Time }{stamp, created}
	}

	require.NoError(t, rows.Err())
	require.Len(t, stamps, 3)

	scheduled := stamps[notifications[1].ID.String()]
	require.True(t, scheduled.availableAt.Equal(sendAt),
		"zamanlanmış satır: %s, istenen %s", scheduled.availableAt, sendAt)

	for _, index := range []int{0, 2} {
		immediate := stamps[notifications[index].ID.String()]
		require.Equal(t, immediate.createdAt, immediate.availableAt,
			"zamanlanmamış satır DB damgasında kalmalı (öğe %d)", index)
	}
}

// TestQueueDepthSeparatesDeferredRows, zamanı gelmemiş satırların ayrı sayıldığını
// doğrular.
//
// Ayrım olmasa gelecek haftaya kurulmuş 10.000 bildirim, operatöre tıkanmış bir kuyruk
// gibi görünürdü; aynı sorun retry backoff'u bekleyen satırlar için de geçerli.
func TestQueueDepthSeparatesDeferredRows(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	seedClaimable(t, store.pool, "sms", 4)

	for i := range 2 {
		scheduled := newScheduledNotification(t, time.Now().Add(time.Duration(i+1)*time.Hour))
		require.NoError(t, createPlain(t, store, ctx, scheduled))
	}

	depth, err := store.QueueDepth(ctx)
	require.NoError(t, err)

	ready, deferredCount := 0, 0

	for _, count := range depth {
		if count.Deferred {
			deferredCount += int(count.Count)

			continue
		}

		ready += int(count.Count)
	}

	require.Equal(t, 4, ready, "yalnızca zamanı gelmiş satırlar kuyruk derinliği")
	require.Equal(t, 2, deferredCount, "zamanlanmış satırlar ayrı sayılmalı")
}
