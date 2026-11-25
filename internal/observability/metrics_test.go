package observability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/breakerstate"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// stubMetricsStore, sorgu sayısını sayan ve istenen sorguyu düşürebilen store.
type stubMetricsStore struct {
	rounds atomic.Int64

	// delay, singleflight testinin eşzamanlı scrape'leri yakalayabilmesi için.
	delay time.Duration

	queueErr    error
	terminalErr error
	latencyErr  error

	percentiles store.Percentiles
}

func (s *stubMetricsStore) QueueDepth(context.Context) ([]store.LabelledCount, error) {
	// Tur sayacı yalnızca ilk sorguda artıyor: bir "tur" dört sorgunun tamamı.
	s.rounds.Add(1)

	if s.delay > 0 {
		time.Sleep(s.delay)
	}

	if s.queueErr != nil {
		return nil, s.queueErr
	}

	return []store.LabelledCount{
		{Status: "pending", Channel: "sms", Count: 12},
		{Status: "processing", Channel: "email", Count: 3},
	}, nil
}

func (s *stubMetricsStore) TerminalCounts(
	context.Context, time.Duration,
) ([]store.LabelledCount, error) {
	if s.terminalErr != nil {
		return nil, s.terminalErr
	}

	return []store.LabelledCount{{Status: "sent", Channel: "sms", Count: 40}}, nil
}

func (s *stubMetricsStore) EndToEndLatency(
	context.Context, time.Duration,
) (store.Percentiles, error) {
	if s.latencyErr != nil {
		return store.Percentiles{}, s.latencyErr
	}

	return s.percentiles, nil
}

func (s *stubMetricsStore) ProviderLatency(
	context.Context, time.Duration,
) (store.Percentiles, error) {
	return s.percentiles, nil
}

// stubBreakerReader, devre durumlarını testin elinde tutar.
type stubBreakerReader struct {
	entries []breakerstate.Entry
	err     error
}

func (s stubBreakerReader) Read(context.Context) ([]breakerstate.Entry, error) {
	return s.entries, s.err
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// gather, collector'ı bir registry'ye kaydedip Prometheus metin çıktısını döner.
//
// testutil.CollectAndFormat kullanılmıyor: metrik adı filtresi boş bırakıldığında
// HİÇBİR metrik döndürmüyor (testler yazılırken bulundu) ve burada ölçmek istediğimiz
// şey tam olarak "ne yayınlandı, ne yayınlanmadı".
func gather(t *testing.T, collector prometheus.Collector) string {
	t.Helper()

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(collector))

	families, err := registry.Gather()
	require.NoError(t, err)

	var buffer bytes.Buffer

	encoder := expfmt.NewEncoder(&buffer, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		require.NoError(t, encoder.Encode(family))
	}

	return buffer.String()
}

// TestCollectorPublishesAllMetrics, dört sorgunun ve devre durumunun çıktıya
// dönüştüğünü doğrular.
func TestCollectorPublishesAllMetrics(t *testing.T) {
	t.Parallel()

	metrics := &stubMetricsStore{
		percentiles: store.Percentiles{
			P50: 250 * time.Millisecond, P95: time.Second, P99: 2 * time.Second, Count: 9,
		},
	}
	breakers := stubBreakerReader{entries: []breakerstate.Entry{
		{Channel: "sms", Worker: "worker-1", State: breaker.StateOpen},
		{Channel: "email", Worker: "worker-1", State: breaker.StateClosed},
	}}

	output := gather(t, NewCollector(metrics, breakers, discardLogger()))

	require.Contains(t, output, `notifications_queue_depth{channel="sms",status="pending"} 12`)
	require.Contains(t, output, `notifications_terminal_recent{channel="sms",status="sent"} 40`)
	require.Contains(t, output, `notification_end_to_end_duration_seconds{quantile="0.5"} 0.25`)
	require.Contains(t, output, `notification_provider_duration_seconds{quantile="0.99"} 2`)
	require.Contains(t, output, `notifications_breaker_state{channel="sms",worker="worker-1"} 2`)
	require.Contains(t, output, `notifications_breaker_state{channel="email",worker="worker-1"} 0`)
	require.Contains(t, output, "notifications_metrics_collect_errors 0")
}

// TestCollectorCachesResults, 5 saniyelik önbelleğin ikinci scrape'te sorgu
// koşturmadığını doğrular.
//
// /metrics kimliksiz ve değerleri veritabanından türetiyor: her istek sorgu
// koşturursa tek bir istemci havuzu tüketip API'yi hazır olmayan duruma düşürebilir.
func TestCollectorCachesResults(t *testing.T) {
	t.Parallel()

	metrics := &stubMetricsStore{}
	collector := NewCollector(metrics, nil, discardLogger())

	gather(t, collector)
	gather(t, collector)
	gather(t, collector)

	require.Equal(t, int64(1), metrics.rounds.Load(),
		"önbellek süresi içindeki scrape'ler tek sorgu turu paylaşmalı")
}

// TestCollectorSharesConcurrentScrapes, aynı anda gelen scrape'lerin tek sorgu turu
// paylaştığını doğrular (singleflight).
//
// Önbellek tek başına yetmiyor: soğuk önbellekte aynı anda gelen N istek N tur
// koştururdu ve havuzu tam o anda tüketirdi.
func TestCollectorSharesConcurrentScrapes(t *testing.T) {
	t.Parallel()

	metrics := &stubMetricsStore{delay: 80 * time.Millisecond}
	collector := NewCollector(metrics, nil, discardLogger())

	var waiting sync.WaitGroup

	const scrapes = 8

	waiting.Add(scrapes)

	for range scrapes {
		go func() {
			defer waiting.Done()

			// testutil yerine doğrudan Collect: paralel çağrı registry gerektirmiyor.
			ch := make(chan prometheus.Metric, 64)
			collector.Collect(ch)
			close(ch)
		}()
	}

	waiting.Wait()

	require.Equal(t, int64(1), metrics.rounds.Load(),
		"eşzamanlı scrape'ler tek tur paylaşmalı")
}

// TestCollectorSurvivesPartialFailure, bir sorgu düştüğünde /metrics'in yine yanıt
// verdiğini ve yalnızca o metriğin eksik kaldığını doğrular.
//
// Aksi hâlde tek bir yavaş sorgu bütün izlemeyi kör ederdi — ve izlemenin kör olduğu
// an, tam da ona en çok ihtiyaç duyulan andır.
func TestCollectorSurvivesPartialFailure(t *testing.T) {
	t.Parallel()

	metrics := &stubMetricsStore{
		terminalErr: errors.New("bağlantı koptu"),
		percentiles: store.Percentiles{P50: time.Second, P95: time.Second, P99: time.Second, Count: 1},
	}

	output := gather(t, NewCollector(metrics, nil, discardLogger()))

	require.Contains(t, output, "notifications_queue_depth",
		"düşen sorgu diğerlerini engellememeli")
	require.NotContains(t, output, "notifications_terminal_recent",
		"düşen sorgunun metriği yayınlanmamalı")
	require.Contains(t, output, "notifications_metrics_collect_errors 1",
		"hata sayısı görünür olmalı")
}

// TestCollectorOmitsPercentilesWhenWindowEmpty, boş pencerede yüzdelik
// yayınlanmadığını doğrular: 0 saniye gecikme "her şey hızlı" izlenimi verirdi, oysa
// doğru bilgi "veri yok".
func TestCollectorOmitsPercentilesWhenWindowEmpty(t *testing.T) {
	t.Parallel()

	metrics := &stubMetricsStore{percentiles: store.Percentiles{Count: 0}}

	output := gather(t, NewCollector(metrics, nil, discardLogger()))

	require.NotContains(t, output, "notification_end_to_end_duration_seconds{")
	require.NotContains(t, output, "notification_provider_duration_seconds{")
	require.Contains(t, output, "notifications_queue_depth")
}

// TestCollectorStillPublishesWhenRedisUnavailable, Redis okunamadığında /metrics'in
// yine döndüğünü ve yalnızca devre durumunun eksik kaldığını doğrular.
func TestCollectorStillPublishesWhenRedisUnavailable(t *testing.T) {
	t.Parallel()

	metrics := &stubMetricsStore{}
	breakers := stubBreakerReader{err: errors.New("redis erişilemez")}

	output := gather(t, NewCollector(metrics, breakers, discardLogger()))

	require.Contains(t, output, "notifications_queue_depth")
	require.NotContains(t, output, "notifications_breaker_state{")
	require.Contains(t, output, "notifications_metrics_collect_errors 1")
}

// TestCollectorSkipsBreakerMetricWithoutReader, okuyucu hiç verilmediğinde metriğin
// yayınlanmadığını doğrular (nil = özellik kapalı).
func TestCollectorSkipsBreakerMetricWithoutReader(t *testing.T) {
	t.Parallel()

	output := gather(t, NewCollector(&stubMetricsStore{}, nil, discardLogger()))

	require.NotContains(t, output, "notifications_breaker_state{")
	require.Contains(t, output, "notifications_metrics_collect_errors 0",
		"okuyucu yoksa bu bir hata değil")
}

// TestBreakerStateValueOrdersSeverity, sayısal değerlerin şiddet sırasına göre
// arttığını doğrular: tek bir alarm kuralı (> 0) hem yarı açığı hem açığı yakalıyor.
func TestBreakerStateValueOrdersSeverity(t *testing.T) {
	t.Parallel()

	require.Equal(t, float64(0), breakerStateValue(breaker.StateClosed))
	require.Equal(t, float64(1), breakerStateValue(breaker.StateHalfOpen))
	require.Equal(t, float64(2), breakerStateValue(breaker.StateOpen))
	require.Equal(t, float64(2), breakerStateValue(breaker.State("bilinmeyen")),
		"tanınmayan durum açık sayılmalı: alarm üretmesi güvenli taraf")
}

// TestCollectorOutputIsValidPrometheusText, çıktının biçim ve adlandırma kurallarına
// uyduğunu doğrular (help metni, tip satırı, etiket kaçışı).
func TestCollectorOutputIsValidPrometheusText(t *testing.T) {
	t.Parallel()

	output := gather(t, NewCollector(&stubMetricsStore{}, nil, discardLogger()))

	require.Contains(t, output, "# HELP notifications_queue_depth")
	require.Contains(t, output, "# TYPE notifications_queue_depth gauge")

	for _, line := range strings.Split(output, "\n") {
		require.NotContains(t, line, "\t", "Prometheus metni sekme içermemeli")
	}
}
