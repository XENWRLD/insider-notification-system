package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// newTestBatch, n adet geçerli bildirim ve ortak batch kimliği üretir.
func newTestBatch(t *testing.T, n int) ([]*domain.Notification, uuid.UUID) {
	t.Helper()

	inputs := make([]domain.NewNotificationInput, 0, n)
	for i := range n {
		inputs = append(inputs, domain.NewNotificationInput{
			Channel:   "sms",
			Recipient: fmt.Sprintf("+9055500%05d", i),
			Content:   "Kampanya",
		})
	}

	notifications, batchID, err := domain.NewNotificationBatch(inputs)
	require.NoError(t, err)

	correlationID := uuid.NewString()
	for _, notification := range notifications {
		notification.CorrelationID = correlationID
	}

	return notifications, batchID
}

// renderBatch, batch özet cevabı üretir.
func renderBatch(batchID uuid.UUID, count int) func() any {
	return func() any {
		return map[string]any{"batch_id": batchID.String(), "accepted_count": count}
	}
}

// TestCreateBatchWritesAllRows, 1000 satırın CopyFrom ile yazıldığını doğrular.
func TestCreateBatchWritesAllRows(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	notifications, batchID := newTestBatch(t, domain.BatchMaxSize)

	outcome, err := store.CreateBatch(ctx, notifications,
		idemFor("b-1", `{"n":1000}`), renderBatch(batchID, len(notifications)))
	require.NoError(t, err)
	require.False(t, outcome.Replayed)

	require.Equal(t, domain.BatchMaxSize, countNotifications(t, pool))

	// Hepsi aynı batch'e ait ve kuyruğa hazır.
	var (
		distinctBatches int
		pending         int
	)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(DISTINCT batch_id), count(*) FILTER (WHERE status = 'pending')
		 FROM notifications`).Scan(&distinctBatches, &pending))

	require.Equal(t, 1, distinctBatches)
	require.Equal(t, domain.BatchMaxSize, pending)
}

// TestCreateBatchAppliesDatabaseDefaults, zaman kolonlarının DB tarafından
// doldurulduğunu doğrular.
//
// available_at / created_at / updated_at COPY listesinde YOK; toplu yazımda tek saat
// kaynağı veritabanı olsun diye bilerek varsayılana bırakıldı.
func TestCreateBatchAppliesDatabaseDefaults(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	notifications, batchID := newTestBatch(t, 3)

	_, err := store.CreateBatch(ctx, notifications,
		idemFor("b-def", `{}`), renderBatch(batchID, 3))
	require.NoError(t, err)

	var nullTimestamps int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM notifications
		WHERE available_at IS NULL OR created_at IS NULL OR updated_at IS NULL`,
	).Scan(&nullTimestamps))
	require.Zero(t, nullTimestamps, "zaman kolonları DB varsayılanıyla dolmalıydı")

	// Satırlar hemen çekilebilir olmalı: available_at = now().
	var claimable int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM notifications WHERE available_at <= now()`).Scan(&claimable))
	require.Equal(t, 3, claimable)

	// priority_rank generated kolonu da dolmuş olmalı.
	var rank int16
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT DISTINCT priority_rank FROM notifications`).Scan(&rank))
	require.EqualValues(t, 2, rank, "normal önceliğin rank'i 2 olmalı")
}

// TestCreateBatchRollsBackEverythingOnConstraintViolation, hep ya hiç davranışını
// doğrular — bu adımın en önemli testi.
//
// Domain doğrulaması atlanıp elle bozuk bir satır kuruluyor (channel = "fax", veritabanı
// CHECK kısıtını ihlal eder). Beklenen: COPY düşer, transaction geri alınır, NE bildirim
// NE de idempotency kaydı kalır. Aksi hâlde istemci 500 alır ama anahtarı "kullanılmış"
// sayıldığı için bir daha hiç deneyemezdi.
func TestCreateBatchRollsBackEverythingOnConstraintViolation(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	notifications, batchID := newTestBatch(t, 10)

	// 4. satır veritabanı kısıtını ihlal edecek.
	notifications[4].Channel = "fax"

	_, err := store.CreateBatch(ctx, notifications,
		idemFor("b-bad", `{}`), renderBatch(batchID, len(notifications)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "batch yazılamadı")

	// Geçerli dokuz satır bile yazılmamalı.
	require.Zero(t, countNotifications(t, pool), "hiçbir satır yazılmamalıydı")

	// Anahtar da serbest kalmalı: istemci düzeltip aynı anahtarla tekrar deneyebilsin.
	require.Zero(t, countIdempotencyKeys(t, pool), "idempotency kaydı oluşmamalıydı")
}

// TestCreateBatchReleasesKeyForRetry, başarısız bir batch'ten sonra aynı anahtarın
// yeniden kullanılabildiğini doğrular.
func TestCreateBatchReleasesKeyForRetry(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	broken, brokenID := newTestBatch(t, 3)
	broken[1].Channel = "fax"

	_, err := store.CreateBatch(ctx, broken,
		idemFor("b-retry", `{}`), renderBatch(brokenID, 3))
	require.Error(t, err)

	// Aynı anahtarla düzeltilmiş batch gönderiliyor.
	fixed, fixedID := newTestBatch(t, 3)
	outcome, err := store.CreateBatch(ctx, fixed,
		idemFor("b-retry", `{}`), renderBatch(fixedID, 3))
	require.NoError(t, err, "başarısız denemeden sonra anahtar serbest olmalıydı")
	require.False(t, outcome.Replayed)
	require.Equal(t, 3, countNotifications(t, pool))
}

// TestCreateBatchReplaysSummary, batch tekrar gönderiminde özet cevabın aynen döndüğünü
// doğrular.
func TestCreateBatchReplaysSummary(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	const body = `{"notifications":[...]}`

	first, firstID := newTestBatch(t, 5)
	idem := IdempotencyRequest{
		Scope: ScopeNotificationsBatch, Key: "b-replay", Hash: hashOf(body), TTL: testTTL,
	}

	_, err := store.CreateBatch(ctx, first, idem, renderBatch(firstID, 5))
	require.NoError(t, err)
	require.Equal(t, 5, countNotifications(t, pool))

	second, secondID := newTestBatch(t, 5)
	outcome, err := store.CreateBatch(ctx, second, idem, renderBatch(secondID, 5))
	require.NoError(t, err)

	require.True(t, outcome.Replayed)
	require.Equal(t, 5, countNotifications(t, pool), "tekrar gönderim yeni satır yazmamalı")
	require.Contains(t, string(outcome.Body), firstID.String(),
		"ilk batch'in kimliği dönmeliydi")
	require.NotContains(t, string(outcome.Body), secondID.String())
}

// TestCreateBatchMixedChannels, üç kanalın tek batch'te yazılabildiğini doğrular.
func TestCreateBatchMixedChannels(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notifications, batchID, err := domain.NewNotificationBatch([]domain.NewNotificationInput{
		{Channel: "sms", Recipient: "+905551234567", Content: "Merhaba"},
		{Channel: "email", Recipient: "ali@example.com", Content: "Merhaba"},
		{Channel: "push", Recipient: "dGVzdC10b2tlbg", Content: "Merhaba", Priority: "high"},
	})
	require.NoError(t, err)

	for _, notification := range notifications {
		notification.CorrelationID = uuid.NewString()
	}

	_, err = store.CreateBatch(ctx, notifications,
		idemFor("b-mixed", `{}`), renderBatch(batchID, 3))
	require.NoError(t, err)

	rows, err := pool.Query(ctx,
		`SELECT channel, priority_rank FROM notifications ORDER BY priority_rank, channel`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		channel string
		rank    int16
	}

	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.channel, &r.rank))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())

	// high öncelikli push önce gelmeli.
	require.Equal(t, []row{
		{channel: "push", rank: 1},
		{channel: "email", rank: 2},
		{channel: "sms", rank: 2},
	}, got)
}
