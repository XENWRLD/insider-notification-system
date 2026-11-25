package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/ratelimit"
)

// Bu dosya KAPANIŞ GÜRÜLTÜSÜNÜ ölçüyor: normal bir kapanış, gerçek bir arızayla aynı
// ERROR satırlarını üretmemeli. Doğruluk sorunu yoktu (satırlar kuyruğa dönüyor), ama
// operatör kapanış loglarında "izin alınamadı" / "lease'ler kurtarılamadı" görüp
// Redis'i veya Postgres'i aramaya çıkıyordu (review bulguları F19, F42).

// recordingHandler, yayılan log kayıtlarını seviyeleriyle toplar.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, record.Clone())

	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// levelCount, verilen seviyedeki kayıt sayısını döner.
func (h *recordingHandler) levelCount(level slog.Level) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	count := 0

	for _, record := range h.records {
		if record.Level == level {
			count++
		}
	}

	return count
}

// messages, toplanan mesajları döner.
func (h *recordingHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := make([]string, 0, len(h.records))
	for _, record := range h.records {
		result = append(result, record.Message)
	}

	return result
}

// newRecordingEngine, logu kaydeden bir motor kurar.
func newRecordingEngine(
	t *testing.T, cfg Config, queue Queue, sender Sender, limiter ratelimit.Limiter,
) (*Engine, *recordingHandler) {
	t.Helper()

	handler := &recordingHandler{}

	engine, err := New(cfg, queue, sender, limiter, nil, nil, slog.New(handler))
	require.NoError(t, err)

	return engine, handler
}

// TestTickLoopSkipsWorkWhenContextAlreadyDone, kapanışta ölü context'le iş
// yapılmadığını doğrular.
//
// Yarış DETERMİNİST kuruluyor: tick kanalı önceden doldurulup ctx önceden iptal
// ediliyor, yani select'te iki dal da hazır ve Go aralarından rastgele seçiyor. 200 tur:
// tek bir koşu, rastgeleliğin tick dalını seçmesini kaçırabilir. tickLoop'un tick
// kanalını dışarıdan almasının sebebi tam olarak bu — gerçek bir time.Ticker ile tick,
// select'e varılmadan önce hazır olmuyor ve test yarışı hiç görmüyordu.
func TestTickLoopSkipsWorkWhenContextAlreadyDone(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t, testConfig(), &stubQueue{},
		&stubSender{resp: acceptedResponse()})

	deadContextCalls := 0

	for range 200 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Tamponlu kanal: tick dalı da hazır.
		ticks := make(chan time.Time, 1)
		ticks <- time.Now()

		engine.tickLoop(ctx, ticks, "test", func(workCtx context.Context) {
			if workCtx.Err() != nil {
				deadContextCalls++
			}
		})
	}

	require.Zero(t, deadContextCalls,
		"iptal edilmiş context ile iş yapılmamalı (kapanışta boşa sorgu + yanıltıcı ERROR)")
}

// TestReaperAndJanitorUseTheSharedLoop, iki döngünün de ortak tickLoop üzerinden
// koştuğunu ve kapanışta sorgu atmadığını doğrular.
func TestReaperAndJanitorUseTheSharedLoop(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}

	cfg := testConfig()
	cfg.ReaperInterval = time.Nanosecond
	cfg.IdempotencyCleanupInterval = time.Nanosecond

	engine := newTestEngine(t, cfg, queue, &stubSender{resp: acceptedResponse()})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	engine.runReaper(ctx)
	engine.runJanitor(ctx)

	require.Zero(t, queue.reclaimCalls.Load())
	require.Zero(t, queue.cleanupCalls.Load())
}

// TestPermitErrorOnShutdownIsNotLoggedAsError, kapanışta kesilen izin beklemesinin
// ERROR olarak loglanmadığını doğrular.
//
// Gerçek bir Redis arızası (ctx canlıyken) ERROR kalmalı — ayrımın tamamı bu.
func TestPermitErrorOnShutdownIsNotLoggedAsError(t *testing.T) {
	t.Parallel()

	limiter := failingLimiter(errors.New("redis erişilemez"))

	engine, logs := newRecordingEngine(t, testConfig(), &stubQueue{},
		&stubSender{resp: acceptedResponse()}, limiter)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	granted := engine.awaitPermit(ctx, engine.logger, newTestNotification(domain.ChannelSMS, 1))

	require.False(t, granted, "izin alınmadı, satır kuyruğa dönmeli")
	require.Zero(t, logs.levelCount(slog.LevelError),
		"kapanış gerçek bir arıza gibi loglanmamalı: %v", logs.messages())
	require.Contains(t, logs.messages(), "kapanış sırasında izin beklemesi kesildi")
}

// TestPermitErrorWhileRunningIsLoggedAsError, ctx canlıyken hatanın ERROR kaldığını
// doğrular: F19 düzeltmesi gerçek arızayı sessizleştirmemeli.
func TestPermitErrorWhileRunningIsLoggedAsError(t *testing.T) {
	t.Parallel()

	limiter := failingLimiter(errors.New("redis erişilemez"))

	cfg := testConfig()
	// Bekleme bütçesi kısa: test gerçek arızada tek ERROR görmek için bekliyor.
	cfg.RateWaitMax = 20 * time.Millisecond

	engine, logs := newRecordingEngine(t, cfg, &stubQueue{},
		&stubSender{resp: acceptedResponse()}, limiter)

	granted := engine.awaitPermit(context.Background(), engine.logger,
		newTestNotification(domain.ChannelSMS, 1))

	require.False(t, granted)
	require.Equal(t, 1, logs.levelCount(slog.LevelError),
		"gerçek arıza tam bir kez ERROR loglanmalı: %v", logs.messages())
}
