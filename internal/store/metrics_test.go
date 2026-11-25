package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// seedTerminal, verilen durumda ve kanalda bitmiş satırlar üretir.
//
// latency = created_at ile updated_at arası (uçtan uca gecikme),
// age = updated_at'in ne kadar geçmişte olduğu (pencere testleri için).
//
// Damgalar INSERT'te veriliyor, sonradan UPDATE ile DEĞİL: notifications_set_updated_at
// trigger'ı BEFORE UPDATE çalışıp updated_at'i now() yapıyor ve testin kaydırmasını
// eziyor. Bu, testler yazılırken bulundu.
func seedTerminal(
	t *testing.T,
	pool *pgxpool.Pool,
	status, channel string,
	count int,
	latency, age time.Duration,
) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		INSERT INTO notifications
			(id, channel, recipient, content, correlation_id, status, created_at, updated_at)
		SELECT gen_random_uuid(), $2, '+90555' || lpad(i::text, 7, '0'), 'metrik',
		       gen_random_uuid()::text, $3,
		       now() - $4::interval - $5::interval, now() - $5::interval
		FROM generate_series(1, $1) AS i`,
		count, channel, status, latency, age)
	require.NoError(t, err)
}

// TestQueueDepthCountsOnlyActiveStatuses, kuyruk derinliğinin yalnızca bekleyen ve
// işlenen işleri saydığını doğrular.
//
// Terminal satırları saymak "ne kadar iş bekliyor" sorusunu cevaplamıyor ve milyonlarca
// sent satırıyla scrape'i pahalılaştırırdı.
func TestQueueDepthCountsOnlyActiveStatuses(t *testing.T) {
	// Paralel DEĞİL: sharedPool her testin sonunda tabloları TRUNCATE ediyor.
	store := New(sharedPool(t))
	ctx := context.Background()

	seedClaimable(t, store.pool, "sms", 7)
	seedTerminal(t, store.pool, "sent", "email", 5, time.Second, 0)
	seedTerminal(t, store.pool, "failed", "push", 3, time.Second, 0)

	depth, err := store.QueueDepth(ctx)
	require.NoError(t, err)

	total := 0

	for _, count := range depth {
		require.NotContains(t, []string{"sent", "failed", "cancelled"}, count.Status,
			"terminal durum kuyruk derinliğinde görünmemeli: %+v", count)

		total += int(count.Count)
	}

	require.Equal(t, 7, total, "yalnızca aktif satırlar sayılmalı")
}

// TestQueueDepthQueryAvoidsSeqScan, kuyruk derinliği sorgusunun maliyetinin TABLO
// BOYUTUNDAN değil aktif satır sayısından geldiğini doğrular.
//
// 15 saniyede bir koşan bir scrape sorgusunun Seq Scan yapması, tablo büyüdükçe
// doğrusal kötüleşen bir yük demek. Ölçüm uygulamanın ÜRETTİĞİ metinle yapılıyor
// (Adım 2/5'teki kural).
func TestQueueDepthQueryAvoidsSeqScan(t *testing.T) {
	store := New(sharedPool(t))

	// Gerçekçi oran: çok sayıda bitmiş satır, az sayıda aktif.
	seedTerminal(t, store.pool, "sent", "sms", 20000, 2*time.Second, 0)
	seedClaimable(t, store.pool, "sms", 50)

	_, err := store.pool.Exec(context.Background(), "ANALYZE notifications")
	require.NoError(t, err)

	plan := explainQuery(t, store.pool, queueDepthQuery, []any{activeStatuses})

	require.NotContains(t, plan, "Seq Scan",
		"aktif küme küçükken kısmi index'ler kullanılmalı:\n%s", plan)
	require.Contains(t, plan, "Index",
		"plan bir index taraması içermeli:\n%s", plan)
}

// TestTerminalCountsQueryUsesMetricsIndex, pencereli sayımın 000005'teki index ile
// koştuğunu doğrular: maliyet tablo boyutundan değil pencerenin hacminden geliyor.
//
// Kümülatif sayım ölçümde Seq Scan üretiyordu (WHERE koşulu index'in kısmi yordamıyla
// birebir aynı olduğu için planlayıcı doğrudan tabloyu tarıyor) — pencere bu yüzden
// zorunlu oldu.
func TestTerminalCountsQueryUsesMetricsIndex(t *testing.T) {
	store := New(sharedPool(t))

	// Çoğu satır pencerenin DIŞINDA: aralık taraması ölçülebilsin.
	seedTerminal(t, store.pool, "sent", "sms", 15000, 2*time.Second, 6*time.Hour)
	seedTerminal(t, store.pool, "sent", "sms", 300, 2*time.Second, 0)
	seedTerminal(t, store.pool, "failed", "email", 50, time.Second, 0)

	_, err := store.pool.Exec(context.Background(), "ANALYZE notifications")
	require.NoError(t, err)

	plan := explainQuery(t, store.pool, terminalCountsQuery, []any{15 * time.Minute})

	require.NotContains(t, plan, "Seq Scan", "plan:\n%s", plan)
	require.Contains(t, plan, "notifications_metrics_idx",
		"000005'teki metrik index'i kullanılmalı:\n%s", plan)
}

// TestTerminalCountsRespectWindowAndExcludeActive, sayımın PENCERELİ olduğunu ve aktif
// satırları içermediğini doğrular.
//
// Pencerenin bedeli değerin monoton olmaması (rate() uygulanamaz); metrik adı
// (notifications_terminal_recent) ve help metni bunu söylüyor.
func TestTerminalCountsRespectWindowAndExcludeActive(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	seedTerminal(t, store.pool, "sent", "sms", 4, time.Second, 48*time.Hour) // pencere dışı
	seedTerminal(t, store.pool, "sent", "sms", 6, time.Second, 0)
	seedTerminal(t, store.pool, "failed", "sms", 2, time.Second, 0)
	seedClaimable(t, store.pool, "sms", 3)

	counts, err := store.TerminalCounts(ctx, 15*time.Minute)
	require.NoError(t, err)

	byStatus := map[string]int64{}
	for _, count := range counts {
		byStatus[count.Status] += count.Count
	}

	require.Equal(t, int64(6), byStatus["sent"], "pencere dışı satırlar sayılmamalı")
	require.Equal(t, int64(2), byStatus["failed"])
	require.NotContains(t, byStatus, "pending", "aktif satırlar terminal sayımda olmamalı")
}

// TestEndToEndLatencyMeasuresQueueTimeToo, uçtan uca gecikmenin kuyrukta beklemeyi de
// içerdiğini doğrular: kullanıcının yaşadığı gecikme bu.
func TestEndToEndLatencyMeasuresQueueTimeToo(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	seedTerminal(t, store.pool, "sent", "sms", 20, 4*time.Second, 0)

	latency, err := store.EndToEndLatency(ctx, 15*time.Minute)
	require.NoError(t, err)

	require.Equal(t, int64(20), latency.Count)
	require.InDelta(t, 4*time.Second, latency.P50, float64(2*time.Second),
		"created_at → updated_at farkı ölçülmeli")
	require.Positive(t, latency.P95)
	require.Positive(t, latency.P99)
}

// TestEndToEndLatencyRespectsWindow, pencere dışındaki satırların hesaba girmediğini
// doğrular. Tüm zamanların yüzdeliği bir olay sırasında kıpırdamaz ve ölçmek
// istediğimiz şeyi gizler.
func TestEndToEndLatencyRespectsWindow(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	// Pencerenin DIŞINDA tohumlanıyor; UPDATE ile kaydırmak işe yaramıyor (trigger
	// updated_at'i now() yapar — bkz. seedTerminal).
	seedTerminal(t, store.pool, "sent", "sms", 5, 2*time.Second, 2*time.Hour)

	latency, err := store.EndToEndLatency(ctx, 15*time.Minute)
	require.NoError(t, err)

	require.Zero(t, latency.Count, "pencere dışı satır sayılmamalı")
	require.Zero(t, latency.P50, "boş pencerede yüzdelik üretilmemeli")
}

// TestLatencyReturnsEmptyOnNoRows, boş pencerede hata değil sıfır sayım döndüğünü
// doğrular: çağıran metriği hiç yayınlamıyor, 0 ms gecikme yayınlamıyor.
func TestLatencyReturnsEmptyOnNoRows(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	endToEnd, err := store.EndToEndLatency(ctx, time.Minute)
	require.NoError(t, err)
	require.Zero(t, endToEnd.Count)

	providerLatency, err := store.ProviderLatency(ctx, time.Minute)
	require.NoError(t, err)
	require.Zero(t, providerLatency.Count)
}

// TestProviderLatencyMeasuresAttemptDuration, sağlayıcı süresinin denemelerden
// okunduğunu doğrular.
//
// Uçtan uca gecikmenin yanında ayrıca gerekiyor: ikisi birlikte "yavaşlık kuyrukta mı
// sağlayıcıda mı" sorusunu cevaplıyor. Uçtan uca 30 sn ama sağlayıcı 50 ms ise sorun
// hız sınırı/kuyruk; ikisi birlikte yükseldiyse sorun sağlayıcıda.
func TestProviderLatencyMeasuresAttemptDuration(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	notification := claimOne(t, store)

	require.NoError(t, store.CompleteNotification(ctx, NotificationResult{
		ID:         notification.ID,
		LeaseToken: *notification.LeaseToken,
		Status:     "sent",
		StatusCode: ptr(202),
		DurationMS: 250,
	}))

	latency, err := store.ProviderLatency(ctx, 15*time.Minute)
	require.NoError(t, err)

	require.Equal(t, int64(1), latency.Count)
	require.Equal(t, 250*time.Millisecond, latency.P50)
}

// TestProviderLatencyQueryUsesMetricsIndex, deneme süresi sorgusunun 000005'teki
// index ile koştuğunu doğrular.
func TestProviderLatencyQueryUsesMetricsIndex(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	seedClaimable(t, store.pool, "sms", 1)

	claimed, err := store.ClaimNotifications(ctx, "sms", 1, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	// Çok sayıda deneme kaydı, çoğu pencerenin dışında: aralık taraması ölçülebilsin.
	_, err = store.pool.Exec(ctx, `
		INSERT INTO notification_attempts (notification_id, attempt_no, duration_ms, created_at)
		SELECT $1, i, (i % 500), now() - (i || ' seconds')::interval
		FROM generate_series(1, 5000) AS i`, claimed[0].ID)
	require.NoError(t, err)

	_, err = store.pool.Exec(ctx, "ANALYZE notification_attempts")
	require.NoError(t, err)

	plan := explainQuery(t, store.pool, providerLatencyQuery, []any{15 * time.Minute})

	require.NotContains(t, plan, "Seq Scan", "plan:\n%s", plan)
	require.Contains(t, plan, "notification_attempts_metrics_idx",
		"000005'teki deneme index'i kullanılmalı:\n%s", plan)
}
