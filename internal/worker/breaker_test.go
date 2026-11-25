package worker

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
	"github.com/XENWRLD/insider-notification-system/internal/ratelimit"
)

// stubBreaker, durumu testin elinde olan devre kesici.
//
// Gerçek gobreaker ile açık/yarı açık kurmak eşik doldurup zaman beklemek demek;
// motor düzeyi testler durumun KENDİSİNİ sabitleyip davranışı ölçüyor.
type stubBreaker struct {
	mu    sync.Mutex
	state breaker.State

	// refuse, Do'nun çağrıyı yapmadan ErrOpen döndürmesini sağlar (yarı açık sonda
	// kotasının dolması ya da izin beklerken devrenin açılması).
	refuse bool

	doCalls atomic.Int64
}

func newStubBreaker(state breaker.State) *stubBreaker {
	return &stubBreaker{state: state}
}

func (b *stubBreaker) Do(fn func() error) error {
	b.mu.Lock()
	refuse := b.refuse
	b.mu.Unlock()

	if refuse {
		return breaker.ErrOpen
	}

	b.doCalls.Add(1)

	return fn()
}

func (b *stubBreaker) State() breaker.State {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.state
}

func (b *stubBreaker) setState(state breaker.State) {
	b.mu.Lock()
	b.state = state
	b.mu.Unlock()
}

func (b *stubBreaker) setRefuse(refuse bool) {
	b.mu.Lock()
	b.refuse = refuse
	b.mu.Unlock()
}

// breakerFactory, her kanal için aynı stub'ı veren fabrika.
func breakerFactory(brk breaker.Breaker) func(domain.Channel) (breaker.Breaker, error) {
	return func(domain.Channel) (breaker.Breaker, error) { return brk, nil }
}

// TestOpenBreakerConsumesNoAttemptsOrPermits, bu adımın EN ÖNEMLİ KURALINI sabitler.
//
// Devre açıkken: sağlayıcıya gidilmez, rate limit izni HARCANMAZ, deneme kaydı
// yazılmaz ve satır DeferLeases ile (denemesi sayılmadan) kuyruğa döner. İzin kapısı
// erken kapının ARKASINDA olduğu için paylaşılan Redis bütçesi boşa yanmıyor.
func TestOpenBreakerConsumesNoAttemptsOrPermits(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := &stubLimiter{byChannel: map[domain.Channel]int{}}
	brk := newStubBreaker(breaker.StateOpen)

	engine := newTestEngineWithBreaker(t, cfg, queue, sender, limiter, breakerFactory(brk))

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))

	require.Zero(t, sender.callCount(), "devre açıkken sağlayıcıya gidilmemeli")
	require.Zero(t, limiter.calls.Load(), "devre açıkken rate limit izni harcanmamalı")
	require.Empty(t, queue.completedResults(), "deneme kaydı yazılmamalı")
	require.Zero(t, brk.doCalls.Load(), "çağrı devre kesicinin içine bile girmemeli")

	deferred := queue.deferredHolds()
	require.Len(t, deferred, 1, "satır DeferLeases ile geri verilmeli")
	require.Equal(t, engine.retryPolicy().RequeueDelay, deferred[0].delay)
	require.NotEmpty(t, deferred[0].reason, "last_error tek tutanak")
	require.Empty(t, queue.releasedHolds(), "ReleaseLeases değil DeferLeases kullanılmalı")
}

// TestHandleRequeuesWhenBreakerOpensDuringPermitWait, izin beklerken devrenin açılması
// yarışını doğrular: ikinci kapı (brk.Do → ErrOpen) satırı kurtarıyor.
func TestHandleRequeuesWhenBreakerOpensDuringPermitWait(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := &stubLimiter{byChannel: map[domain.Channel]int{}}

	// Erken kapı geçilir (kapalı görünüyor) ama Do çağrıyı reddediyor.
	brk := newStubBreaker(breaker.StateClosed)
	brk.setRefuse(true)

	engine := newTestEngineWithBreaker(t, testConfig(), queue, sender, limiter, breakerFactory(brk))

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))

	require.Zero(t, sender.callCount(), "gönderim yapılmamalı")
	require.Equal(t, int64(1), limiter.calls.Load(), "izin alınmıştı: yandı ama satır kaybolmadı")
	require.Len(t, queue.deferredHolds(), 1)
	require.Empty(t, queue.completedResults(), "deneme sayılmamalı")
}

// TestSustainedRateLimitNeverFailsMessage, sürekli 429'un mesajı öldürmediğini
// doğrular: deneme harcanmadığı için max_attempts'a hiç ulaşılmıyor.
func TestSustainedRateLimitNeverFailsMessage(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	// Tavan rehberin üstünde: bu test kırpmayı değil "rehbere uyma"yı ölçüyor
	// (kırpma TestClassifyCapsRetryAfterAtMaxDelay'de sabitlendi).
	cfg.RetryMaxDelay = 30 * time.Second

	queue := &stubQueue{}
	sender := &stubSender{err: &provider.StatusError{
		StatusCode: http.StatusTooManyRequests,
		RetryAfter: 15 * time.Second,
	}}
	limiter := &stubLimiter{byChannel: map[domain.Channel]int{}}
	brk := newStubBreaker(breaker.StateClosed)

	engine := newTestEngineWithBreaker(t, cfg, queue, sender, limiter, breakerFactory(brk))

	// Aynı satır, deneme hakkının tamamı kadar tur.
	for range domain.DefaultMaxAttempts + 2 {
		engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))
	}

	require.Empty(t, queue.completedResults(),
		"429 sonuç YAZMAMALI: ne retrying ne failed, satır denenmeden döner")

	deferred := queue.deferredHolds()
	require.Len(t, deferred, domain.DefaultMaxAttempts+2)

	for _, hold := range deferred {
		require.Equal(t, 15*time.Second, hold.delay, "sağlayıcının Retry-After'ına uyulmalı")
	}
}

// TestBreakerIsPerChannel, bir kanalın devresi açıkken diğerinin gönderime devam
// ettiğini doğrular. İzolasyon yapısal: her kanalın kendi durum makinesi var.
func TestBreakerIsPerChannel(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Channels = []domain.Channel{domain.ChannelSMS, domain.ChannelEmail}

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := &stubLimiter{byChannel: map[domain.Channel]int{}}

	smsBreaker := newStubBreaker(breaker.StateOpen)
	emailBreaker := newStubBreaker(breaker.StateClosed)

	engine := newTestEngineWithBreaker(t, cfg, queue, sender, limiter,
		func(channel domain.Channel) (breaker.Breaker, error) {
			if channel == domain.ChannelSMS {
				return smsBreaker, nil
			}

			return emailBreaker, nil
		})

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))
	engine.handle(context.Background(), newTestNotification(domain.ChannelEmail, 1))

	require.Equal(t, 1, sender.callCount(), "yalnızca e-posta gönderilmeli")
	require.Zero(t, limiter.channelCalls(domain.ChannelSMS), "SMS izni harcanmamalı")
	require.Equal(t, 1, limiter.channelCalls(domain.ChannelEmail))
	require.Len(t, queue.completedResults(), 1, "e-postanın sonucu yazılmalı")
	require.Len(t, queue.deferredHolds(), 1, "SMS denenmeden geri verilmeli")
}

// TestClaimerStopsClaimingWhenBreakerOpen, devre açıkken claimer'ın hiç iş
// çekmediğini doğrular — satırlar kuyrukta kalıyor, yani iptal edilebilir durumda.
func TestClaimerStopsClaimingWhenBreakerOpen(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	queue := &stubQueue{alwaysFull: true}
	sender := &stubSender{resp: acceptedResponse()}
	brk := newStubBreaker(breaker.StateOpen)

	engine := newTestEngineWithBreaker(t, cfg, queue, sender, ratelimit.Unlimited(),
		breakerFactory(brk))

	pool := newChannelPool(engine, domain.ChannelSMS)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		pool.run(ctx)
	}()

	// Birkaç poll turu geçsin.
	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("havuz kapanmadı")
	}

	require.Empty(t, queue.claimLimits(), "devre açıkken claim sorgusu atılmamalı")
	require.Zero(t, sender.callCount())
	require.Len(t, pool.sem, cfg.Concurrency, "jetonlar geri verilmeli")
}

// TestClaimerClaimsSingleRowWhenHalfOpen, yarı açıkta tek satır çekildiğini doğrular:
// sonda TEK olmak zorunda, yoksa sağlayıcı hâlâ bozukken ona birden fazla istek gider.
func TestClaimerClaimsSingleRowWhenHalfOpen(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 4
	cfg.ClaimBatchSize = 20

	queue := &stubQueue{alwaysFull: true}
	sender := &stubSender{resp: acceptedResponse()}
	brk := newStubBreaker(breaker.StateHalfOpen)

	engine := newTestEngineWithBreaker(t, cfg, queue, sender, ratelimit.Unlimited(),
		breakerFactory(brk))

	runPoolUntil(t, engine, func() bool { return len(queue.claimLimits()) > 0 })

	for _, limit := range queue.claimLimits() {
		require.Equal(t, 1, limit, "yarı açıkta claim limiti 1 olmalı")
	}
}

// TestHalfOpenDoesNotClaimWhileProbeInFlight, sonda uçuşurken yeni iş çekilmediğini
// doğrular.
//
// Uçuş kontrolü olmasa: claim başarılı olunca döngü beklemeden dönüyor, her turda bir
// satır daha çekiliyor, sender erken kapıyı geçip (durum Open değil) izin alıyor ve
// brk.Do'da ErrOpen yiyor. Tur milisaniyeler sürdüğü için PAYLAŞILAN Redis bütçesi
// saniyeler içinde tükenir ve yüzlerce gereksiz UPDATE atılır.
func TestHalfOpenDoesNotClaimWhileProbeInFlight(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 4
	cfg.ClaimBatchSize = 20

	queue := &stubQueue{alwaysFull: true}
	// Yavaş sonda: uçuş penceresi ölçülebilir olsun.
	sender := &stubSender{resp: acceptedResponse(), delay: 150 * time.Millisecond}
	limiter := &stubLimiter{byChannel: map[domain.Channel]int{}}
	brk := newStubBreaker(breaker.StateHalfOpen)

	engine := newTestEngineWithBreaker(t, cfg, queue, sender, limiter, breakerFactory(brk))

	pool := newChannelPool(engine, domain.ChannelSMS)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		pool.run(ctx)
	}()

	// Sonda uçuşurken birçok poll turu geçsin.
	time.Sleep(100 * time.Millisecond)

	// Uçuş sürerken ölç: tek sonda dışında ne gönderim ne izin olmalı.
	require.Equal(t, 1, sender.callCount(), "yarı açıkta tek sonda uçmalı")
	require.Equal(t, int64(1), limiter.calls.Load(), "tek izin harcanmalı")

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("havuz kapanmadı")
	}

	require.Len(t, pool.sem, cfg.Concurrency)
}

// TestClaimerResumesWhenBreakerCloses, devre kapanınca claim limitinin yeniden
// boştaki sender sayısına çıktığını doğrular.
func TestClaimerResumesWhenBreakerCloses(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 4
	cfg.ClaimBatchSize = 20

	queue := &stubQueue{alwaysFull: true}
	sender := &stubSender{resp: acceptedResponse()}
	brk := newStubBreaker(breaker.StateOpen)

	engine := newTestEngineWithBreaker(t, cfg, queue, sender, ratelimit.Unlimited(),
		breakerFactory(brk))

	pool := newChannelPool(engine, domain.ChannelSMS)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)

		pool.run(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	require.Empty(t, queue.claimLimits(), "açıkken claim olmamalı")

	brk.setState(breaker.StateClosed)

	require.True(t, waitFor(t, 2*time.Second, func() bool {
		return len(queue.claimLimits()) > 0
	}), "devre kapanınca claim yeniden başlamalı")

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("havuz kapanmadı")
	}

	require.Len(t, pool.sem, cfg.Concurrency)
}

// TestDecodeErrorProbeClosesBreaker, 2xx + çözülemeyen gövdenin GERÇEK devre kesiciyi
// kapattığını doğrular.
//
// Closure "gerçek hatayı döndür" biçiminde yazılsaydı DecodeError de hataya çevrilir,
// IsFailure onu false döndürdüğü için sonuç "excluded" olurdu: mesaj gönderilmiş
// olmasına rağmen yarı açık sonda devreyi KAPATMAZ ve gövdesi JSON olmayan bir
// sağlayıcıda (webhook.site HTML dönebiliyor) devre süresiz yarı açık kalırdı.
func TestDecodeErrorProbeClosesBreaker(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	queue := &stubQueue{}
	sender := &stubSender{
		resp: provider.SendResponse{StatusCode: http.StatusAccepted},
		err:  &provider.DecodeError{Err: context.Canceled, Body: "<html>"},
	}

	// GERÇEK devre kesici: eşik 1, yani tek hata açıyor.
	// OpenTimeout cömert: breaker.State() SAF BİR OKUMA DEĞİL — açık süresi dolmuşsa
	// geçişi kendisi yapıyor. Kısa bir pencerede "açıldı mı" iddiası, assert'e gelene
	// kadar yarı açığa geçtiği için kırılgan olurdu.
	const openTimeout = 300 * time.Millisecond

	real, err := breaker.New(breaker.Config{
		Name:             "sms",
		FailureThreshold: 1,
		OpenTimeout:      openTimeout,
		IsFailure:        BreakerIsFailure,
	})
	require.NoError(t, err)

	engine := newTestEngineWithBreaker(t, cfg, queue, sender, ratelimit.Unlimited(),
		breakerFactory(real))

	// Önce devreyi aç: gerçek bir kanal arızası (503).
	sender.mu.Lock()
	sender.err = &provider.StatusError{StatusCode: http.StatusServiceUnavailable}
	sender.mu.Unlock()

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))
	require.Equal(t, breaker.StateOpen, real.State(), "503 devreyi açmalı")

	// Yarı açığa geçmesini bekle, sonra sonda DecodeError alsın.
	time.Sleep(openTimeout + 50*time.Millisecond)
	require.Equal(t, breaker.StateHalfOpen, real.State(), "süre dolunca yarı açığa geçmeli")

	sender.mu.Lock()
	sender.err = &provider.DecodeError{Err: context.Canceled, Body: "<html>"}
	sender.mu.Unlock()

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))

	require.Equal(t, breaker.StateClosed, real.State(),
		"2xx + çözülemeyen gövde kanalın sağlıklı olduğunun kanıtı: devre KAPANMALI")

	// Mesaj da gönderildi sayılmalı.
	results := queue.completedResults()
	require.NotEmpty(t, results)
	require.Equal(t, domain.StatusSent, results[len(results)-1].Status)
}
