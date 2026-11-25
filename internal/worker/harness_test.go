package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
	"github.com/XENWRLD/insider-notification-system/internal/ratelimit"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// Bu dosyadaki stub'lar sayesinde havuz mantığı (jeton muhasebesi, kapanış, backoff,
// hata dayanıklılığı) Docker'sız test edilebiliyor. Veritabanı gerçeğinin testleri
// internal/store içinde, SQL'in yanında duruyor.

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testConfig, geçerli ve hızlı bir yapılandırma döner.
func testConfig() Config {
	return Config{
		Channels:        []domain.Channel{domain.ChannelSMS},
		Concurrency:     4,
		ClaimBatchSize:  20,
		LeaseDuration:   time.Minute,
		PollMinInterval: time.Millisecond,
		PollMaxInterval: 5 * time.Millisecond,
		ProviderTimeout: time.Second,
		ResultTimeout:   time.Second,
		DrainTimeout:    5 * time.Second,
		FixedRetryDelay: 5 * time.Second,
		RateWaitMax:     300 * time.Millisecond,

		ReaperInterval:  time.Millisecond,
		ReaperBatchSize: 100,

		IdempotencyTTL:             time.Hour,
		IdempotencyCleanupInterval: time.Millisecond,
		IdempotencyCleanupBatch:    1000,
	}
}

// newTestNotification, claim edilmiş (lease'i olan) bir bildirim üretir.
func newTestNotification(channel domain.Channel, attemptCount int) *domain.Notification {
	token := uuid.New()

	return &domain.Notification{
		ID:            uuid.New(),
		Channel:       channel,
		Recipient:     "+905551234567",
		Content:       "Merhaba",
		Priority:      domain.PriorityNormal,
		Status:        domain.StatusProcessing,
		AttemptCount:  attemptCount,
		MaxAttempts:   domain.DefaultMaxAttempts,
		LeaseToken:    &token,
		CorrelationID: uuid.NewString(),
		AvailableAt:   time.Now(),
	}
}

// claimCall, stub'a gelen bir claim çağrısının cevabı.
type claimCall struct {
	notifications []*domain.Notification
	err           error
}

// stubQueue, Queue arayüzünün test karşılığı.
//
// Claim cevapları sırayla verilir; liste tükenince sürekli boş döner (worker boşta
// dönmeye devam eder, bu da kapanış testlerinin ihtiyacı).
type stubQueue struct {
	mu sync.Mutex

	claims    []claimCall
	claimIdx  int
	claimArgs []int

	// alwaysFull: kuyruk tükenmez, her claim limit kadar taze iş döner. Lease
	// sınırını ölçen testler sürekli dolu bir kuyruğa ihtiyaç duyuyor.
	alwaysFull bool

	// claimedChannel: alwaysFull üretirken satırları İSTENEN kanalda üret.
	// Kanal izolasyonu testleri hangi satırın hangi kanaldan geldiğini bilmek zorunda.
	claimedChannel bool

	// channels, üretilen her satırın kanalını tutar (channelOf ile okunur).
	channels map[uuid.UUID]domain.Channel

	completed   []store.NotificationResult
	completeErr error
	// completeCtxDone, sonuç yazımına gelen context'in iptal edilmiş olup olmadığını
	// kaydeder: kapanışa dayanıklılığın kanıtı.
	completeCtxDone []bool
	onComplete      func()

	released [][]store.LeaseHold
	deferred []deferredHold

	reclaimCalls atomic.Int64
	cleanupCalls atomic.Int64

	// outstanding, claim edilip henüz tamamlanmamış satır sayısı (tepe değerle birlikte).
	outstanding atomic.Int64
	peak        atomic.Int64
}

func (q *stubQueue) ClaimNotifications(
	_ context.Context, channel domain.Channel, limit int, _ time.Duration,
) ([]*domain.Notification, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.claimArgs = append(q.claimArgs, limit)

	var claimed []*domain.Notification

	switch {
	case q.claimIdx < len(q.claims):
		call := q.claims[q.claimIdx]
		q.claimIdx++

		if call.err != nil {
			return nil, call.err
		}

		// Gerçek sorgu gibi davran: limitten fazla satır dönmez.
		claimed = call.notifications
		if len(claimed) > limit {
			claimed = claimed[:limit]
		}

	case q.alwaysFull:
		generated := domain.ChannelSMS
		if q.claimedChannel {
			generated = channel
		}

		claimed = make([]*domain.Notification, limit)
		for i := range claimed {
			claimed[i] = newTestNotification(generated, 1)
		}

	default:
		return nil, nil
	}

	if q.channels == nil {
		q.channels = make(map[uuid.UUID]domain.Channel)
	}

	for _, notification := range claimed {
		q.channels[notification.ID] = notification.Channel
	}

	current := q.outstanding.Add(int64(len(claimed)))
	for {
		peak := q.peak.Load()
		if current <= peak || q.peak.CompareAndSwap(peak, current) {
			break
		}
	}

	return claimed, nil
}

func (q *stubQueue) CompleteNotification(ctx context.Context, res store.NotificationResult) error {
	if q.onComplete != nil {
		q.onComplete()
	}

	q.mu.Lock()
	q.completed = append(q.completed, res)
	q.completeCtxDone = append(q.completeCtxDone, ctx.Err() != nil)
	err := q.completeErr
	q.mu.Unlock()

	q.outstanding.Add(-1)

	return err
}

func (q *stubQueue) ReleaseLeases(_ context.Context, holds []store.LeaseHold) (int64, error) {
	q.mu.Lock()
	q.released = append(q.released, holds)
	q.mu.Unlock()

	q.outstanding.Add(-int64(len(holds)))

	return int64(len(holds)), nil
}

// deferredHold, DeferLeases ile geri verilen bir partinin kaydı: testler hem
// satırları hem uygulanan gecikmeyi/sebebi okuyor.
type deferredHold struct {
	holds  []store.LeaseHold
	delay  time.Duration
	reason string
}

func (q *stubQueue) DeferLeases(
	_ context.Context, holds []store.LeaseHold, delay time.Duration, reason string,
) (int64, error) {
	q.mu.Lock()
	q.deferred = append(q.deferred, deferredHold{holds: holds, delay: delay, reason: reason})
	q.mu.Unlock()

	q.outstanding.Add(-int64(len(holds)))

	return int64(len(holds)), nil
}

func (q *stubQueue) ReclaimExpiredLeases(
	_ context.Context, _ int,
) ([]store.ReclaimedNotification, error) {
	q.reclaimCalls.Add(1)

	return nil, nil
}

func (q *stubQueue) DeleteExpiredIdempotencyKeys(
	_ context.Context, _ time.Duration, _ int,
) (int64, error) {
	q.cleanupCalls.Add(1)

	return 0, nil
}

// completedResults, yazılan sonuçların kopyasını verir.
func (q *stubQueue) completedResults() []store.NotificationResult {
	q.mu.Lock()
	defer q.mu.Unlock()

	return append([]store.NotificationResult(nil), q.completed...)
}

// claimLimits, claim sorgusuna geçilen limitlerin kopyasını verir.
func (q *stubQueue) claimLimits() []int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return append([]int(nil), q.claimArgs...)
}

// channelOf, claim edilmiş bir satırın kanalını verir.
func (q *stubQueue) channelOf(id uuid.UUID) domain.Channel {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.channels[id]
}

// releasedHolds, geri verilen lease'lerin kopyasını verir.
func (q *stubQueue) releasedHolds() [][]store.LeaseHold {
	q.mu.Lock()
	defer q.mu.Unlock()

	return append([][]store.LeaseHold(nil), q.released...)
}

// stubSender, Sender arayüzünün test karşılığı.
type stubSender struct {
	mu sync.Mutex

	resp provider.SendResponse
	err  error

	delay time.Duration
	calls int

	// onSend, gönderim anında çalışır (eşzamanlılık testleri için).
	onSend func()

	// onSendCtx, gönderime geçen context'i inceler (zaman aşımı testleri için).
	onSendCtx func(ctx context.Context)
}

func (s *stubSender) Send(
	ctx context.Context, _ provider.SendRequest,
) (provider.SendResponse, error) {
	s.mu.Lock()
	s.calls++
	delay := s.delay
	resp, err := s.resp, s.err
	onSend := s.onSend
	onSendCtx := s.onSendCtx
	s.mu.Unlock()

	if onSendCtx != nil {
		onSendCtx(ctx)
	}

	if onSend != nil {
		onSend()
	}

	if delay > 0 {
		select {
		case <-ctx.Done():
			return provider.SendResponse{}, ctx.Err()
		case <-time.After(delay):
		}
	}

	return resp, err
}

func (s *stubSender) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

// stubLimiter, ratelimit.Limiter arayüzünün test karşılığı.
//
// decisions sırayla tüketilir; liste tükenince son karar tekrarlanır. Böylece
// "önce reddet sonra izin ver" ve "sürekli reddet" senaryoları aynı stub'la kurulur.
type stubLimiter struct {
	mu sync.Mutex

	decisions []ratelimit.Decision
	index     int
	err       error

	calls     atomic.Int64
	byChannel map[domain.Channel]int
}

// newStubLimiter, verilen kararları sırayla veren bir limiter kurar.
func newStubLimiter(decisions ...ratelimit.Decision) *stubLimiter {
	return &stubLimiter{
		decisions: decisions,
		byChannel: make(map[domain.Channel]int),
	}
}

// denyAllLimiter, hiç izin vermeyen limiter.
func denyAllLimiter() *stubLimiter {
	return newStubLimiter(ratelimit.Decision{OK: false, RetryAfter: time.Millisecond})
}

// failingLimiter, her çağrıda hata döndüren limiter (Redis erişilemez).
func failingLimiter(err error) *stubLimiter {
	limiter := newStubLimiter()
	limiter.err = err

	return limiter
}

func (l *stubLimiter) Allow(
	_ context.Context, channel domain.Channel,
) (ratelimit.Decision, error) {
	l.calls.Add(1)

	l.mu.Lock()
	defer l.mu.Unlock()

	l.byChannel[channel]++

	if l.err != nil {
		return ratelimit.Decision{}, l.err
	}

	if len(l.decisions) == 0 {
		return ratelimit.Decision{OK: true}, nil
	}

	decision := l.decisions[min(l.index, len(l.decisions)-1)]
	l.index++

	return decision, nil
}

// channelCalls, bir kanal için yapılan izin talebi sayısını verir.
func (l *stubLimiter) channelCalls(channel domain.Channel) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.byChannel[channel]
}

// newTestEngine, stub'larla ve sınırsız limiter'la bir motor kurar.
//
// Rate limit dışındaki davranışı ölçen testler Redis kurmak zorunda kalmasın.
func newTestEngine(t *testing.T, cfg Config, queue Queue, sender Sender) *Engine {
	t.Helper()

	return newTestEngineWithLimiter(t, cfg, queue, sender, ratelimit.Unlimited())
}

// newTestEngineWithLimiter, hız sınırı davranışını ölçen testler için.
func newTestEngineWithLimiter(
	t *testing.T, cfg Config, queue Queue, sender Sender, limiter ratelimit.Limiter,
) *Engine {
	t.Helper()

	engine, err := New(cfg, queue, sender, limiter, discardLogger())
	if err != nil {
		t.Fatalf("motor kurulamadı: %v", err)
	}

	return engine
}

// waitFor, koşul sağlanana kadar kısa aralıklarla bekler.
//
// Sabit bir sleep yerine koşul yoklanıyor: test hem hızlı hem yavaş makinede
// güvenilir olsun.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}

		time.Sleep(time.Millisecond)
	}

	return condition()
}
