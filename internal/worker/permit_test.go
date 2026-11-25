package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/ratelimit"
)

// Bu dosya hız sınırının worker tarafındaki davranışını ölçüyor: izin alınmadan
// gönderim yapılmaması, fail-closed, kapanış ve kanal izolasyonu. Gerçek Redis'e
// karşı testler internal/ratelimit içinde.

// allow, izin veren bir karar.
func allow() ratelimit.Decision {
	return ratelimit.Decision{OK: true}
}

// deny, reddeden bir karar.
func deny(retryAfter time.Duration) ratelimit.Decision {
	return ratelimit.Decision{OK: false, RetryAfter: retryAfter}
}

// TestHandleWaitsForPermitBeforeSending, izin alınmadan sağlayıcıya gidilmediğini
// doğrular.
//
// Limiter ilk iki turda reddediyor, üçüncüde izin veriyor: gönderim tam bir kez ve
// izinden SONRA yapılmalı.
func TestHandleWaitsForPermitBeforeSending(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := newStubLimiter(
		deny(time.Millisecond),
		deny(time.Millisecond),
		allow(),
	)

	engine := newTestEngineWithLimiter(t, testConfig(), queue, sender, limiter)

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))

	require.Equal(t, 1, sender.callCount(), "gönderim tam bir kez yapılmalı")
	require.EqualValues(t, 3, limiter.calls.Load(), "iki red + bir izin")

	results := queue.completedResults()
	require.Len(t, results, 1)
	require.Equal(t, domain.StatusSent, results[0].Status)
	require.Empty(t, queue.releasedHolds(), "izin alındı, lease geri verilmemeli")
}

// TestHandleConsumesOnePermitPerSend, izin israfı olmadığını doğrular.
//
// İzin gönderimden hemen önce alınıyor, bu yüzden 1 izin = 1 gerçek gönderim.
// İzin claim sırasında alınsaydı, claim beklenenden az satır döndürdüğünde fark
// boşa yanardı ve kova gerçek trafikten hızlı tükenirdi.
func TestHandleConsumesOnePermitPerSend(t *testing.T) {
	t.Parallel()

	const sends = 7

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := newStubLimiter(allow())

	engine := newTestEngineWithLimiter(t, testConfig(), queue, sender, limiter)

	for range sends {
		engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))
	}

	require.Equal(t, sends, sender.callCount())
	require.EqualValues(t, sends, limiter.calls.Load(), "gönderim başına tam bir izin")
}

// TestPermitWaitBacksOffInsteadOfHammeringLimiter, bekleyen sender'ın limiter'ı
// dövmediğini doğrular.
//
// GCRA'nın RetryAfter'ı "bir sonraki izne kadar" süredir (~10 ms) ve BEKLEYEN HERKES
// AYNI DEĞERİ ALIR. Her turda onu doğrudan beklemek, kova boşken onlarca sender'ın
// saniyede binlerce istek atması demek olurdu. Katlanan pencere (taban 10 ms, tavan
// 250 ms) bunu iki mertebe aşağı çekiyor.
//
// İddia yalnızca ÜST SINIR: yavaş bir makinede daha az çağrı olması testi kırmaz.
func TestPermitWaitBacksOffInsteadOfHammeringLimiter(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.RateWaitMax = 300 * time.Millisecond

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := denyAllLimiter()

	engine := newTestEngineWithLimiter(t, cfg, queue, sender, limiter)

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))

	// Katlanan pencere: 10, 20, 40, 80, 160, 250... → 300 ms'de ~5-6 tur.
	// Jitter beklemeleri kısaltabileceği için üst sınır cömert tutuluyor.
	calls := limiter.calls.Load()
	require.LessOrEqual(t, calls, int64(12),
		"bekleme katlanmalı; RetryAfter'ı doğrudan beklemek yüzlerce çağrı üretirdi")
	require.Positive(t, calls)

	require.Zero(t, sender.callCount(), "izin alınamadı, gönderim yapılmamalı")
}

// TestPermitWaitIsNotCountedInProviderTimeout, izin beklemesinin sağlayıcı zaman
// aşımından düşülmediğini doğrular.
//
// Aynı context kullanılsaydı uzun bir izin beklemesi, sağlayıcıya tanınan süreyi
// yiyip gönderimi boşa düşürürdü.
func TestPermitWaitIsNotCountedInProviderTimeout(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.ProviderTimeout = 5 * time.Second
	cfg.RateWaitMax = time.Second
	// validate: drain >= provider + result, lease >= 2 × (rate + provider + result).
	cfg.DrainTimeout = 10 * time.Second
	cfg.LeaseDuration = time.Minute

	var deadline time.Time

	sender := &stubSender{resp: acceptedResponse()}
	sender.onSendCtx = func(ctx context.Context) {
		if d, ok := ctx.Deadline(); ok {
			deadline = d
		}
	}

	// Birkaç tur beklendikten sonra izin veriliyor.
	limiter := newStubLimiter(
		deny(20*time.Millisecond),
		deny(20*time.Millisecond),
		allow(),
	)

	engine := newTestEngineWithLimiter(t, cfg, &stubQueue{}, sender, limiter)

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))

	require.Equal(t, 1, sender.callCount())
	require.False(t, deadline.IsZero(), "gönderim context'i zaman aşımlı olmalı")
	require.WithinDuration(t, time.Now().Add(cfg.ProviderTimeout), deadline, time.Second,
		"sağlayıcı zaman aşımı izin beklemesiyle kısalmamalı")
}

// TestHandleHandsBackLeaseWhenPermitWaitExceeded, izin alınamayınca satırın kuyruğa
// geri verildiğini doğrular.
//
// Gönderim YAPILMAZ ve sonuç YAZILMAZ: satır hiç denenmemiş sayılıyor, bu yüzden
// ReleaseLeases attempt_count'u da geri alıyor.
func TestHandleHandsBackLeaseWhenPermitWaitExceeded(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.RateWaitMax = 50 * time.Millisecond

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}

	engine := newTestEngineWithLimiter(t, cfg, queue, sender, denyAllLimiter())

	notification := newTestNotification(domain.ChannelSMS, 1)

	engine.handle(context.Background(), notification)

	require.Zero(t, sender.callCount(), "izin yok, sağlayıcıya gidilmemeli")
	require.Empty(t, queue.completedResults(), "deneme yapılmadı, sonuç yazılmamalı")

	released := queue.releasedHolds()
	require.Len(t, released, 1)
	require.Len(t, released[0], 1)
	require.Equal(t, notification.ID, released[0][0].ID)
	require.Equal(t, *notification.LeaseToken, released[0][0].Token)
}

// TestHandleFailsClosedWhenLimiterErrors, Redis'e ulaşılamadığında gönderim
// yapılmadığını doğrular — fail closed.
//
// Sınırın aşılmadığından emin olunamadığı için göndermek doğru olmazdı. Hata
// reddedilmiş izinle aynı muameleyi görüyor: sınırlı süre yeniden denenir, sonra
// satır kuyruğa döner.
func TestHandleFailsClosedWhenLimiterErrors(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.RateWaitMax = 50 * time.Millisecond

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := failingLimiter(errors.New("redis erişilemez"))

	engine := newTestEngineWithLimiter(t, cfg, queue, sender, limiter)

	notification := newTestNotification(domain.ChannelSMS, 1)

	engine.handle(context.Background(), notification)

	require.Zero(t, sender.callCount(), "limiter hatasında gönderim yapılmamalı")
	require.Empty(t, queue.completedResults())

	released := queue.releasedHolds()
	require.Len(t, released, 1)
	require.Equal(t, notification.ID, released[0][0].ID)

	// Hata yolunda da katlanan bekleme uygulanıyor: Redis düştüğünde sıcak döngü yok.
	require.LessOrEqual(t, limiter.calls.Load(), int64(12))
}

// TestHandleStopsWaitingOnShutdown, kapanışta izin beklemesinin kesildiğini doğrular.
//
// Bekleme gönderimin aksine İPTAL EDİLEBİLİR: hiçbir yan etkisi yok, dolayısıyla
// kapanış RateWaitMax kadar beklemek zorunda değil. Satır kuyruğa dönüyor ve
// DrainTimeout'un RateWaitMax'ı kapsaması gerekmiyor.
func TestHandleStopsWaitingOnShutdown(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.RateWaitMax = 30 * time.Second // kapanış bunu beklemeyecek
	// Lease penceresi RateWaitMax'ı kapsamak zorunda: 2 × (30s + 1s + 1s) = 64s.
	cfg.LeaseDuration = 2 * time.Minute

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}

	engine := newTestEngineWithLimiter(t, cfg, queue, sender, denyAllLimiter())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		defer close(done)

		engine.handle(ctx, newTestNotification(domain.ChannelSMS, 1))
	}()

	// Bekleme başladıktan sonra kapanış sinyali.
	require.True(t, waitFor(t, 5*time.Second, func() bool {
		return limiterCalled(engine)
	}), "izin talebi yapılmalıydı")

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("kapanış izin beklemesini kesmedi")
	}

	require.Zero(t, sender.callCount())
	require.Len(t, queue.releasedHolds(), 1, "satır kuyruğa geri verilmeli")
}

// limiterCalled, motorun limiter'ına en az bir çağrı yapıldığını söyler.
func limiterCalled(engine *Engine) bool {
	stub, ok := engine.limiter.(*stubLimiter)

	return ok && stub.calls.Load() > 0
}

// TestRetryConsumesAnotherPermit, yeniden denenen bir bildirimin de izin harcadığını
// doğrular: retry gerçek bir gönderimdir, sınırın dışında tutulamaz.
func TestRetryConsumesAnotherPermit(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := newStubLimiter(allow())

	engine := newTestEngineWithLimiter(t, testConfig(), queue, sender, limiter)

	// attempt_count 2: bu satır daha önce denenmiş ve retry'a alınmış.
	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 2))

	require.Equal(t, 1, sender.callCount())
	require.EqualValues(t, 1, limiter.calls.Load(), "retry de izin harcamalı")
}

// TestPermitIsRequestedPerChannel, izin talebinin kanal bazında yapıldığını doğrular.
//
// Tek bir kovaya sorulsaydı SMS yoğunluğu e-posta gönderimini durdururdu.
func TestPermitIsRequestedPerChannel(t *testing.T) {
	t.Parallel()

	limiter := newStubLimiter(allow())
	engine := newTestEngineWithLimiter(
		t, testConfig(), &stubQueue{}, &stubSender{resp: acceptedResponse()}, limiter)

	ctx := context.Background()

	engine.handle(ctx, newTestNotification(domain.ChannelSMS, 1))
	engine.handle(ctx, newTestNotification(domain.ChannelEmail, 1))
	engine.handle(ctx, newTestNotification(domain.ChannelEmail, 1))

	require.Equal(t, 1, limiter.channelCalls(domain.ChannelSMS))
	require.Equal(t, 2, limiter.channelCalls(domain.ChannelEmail))
	require.Zero(t, limiter.channelCalls(domain.ChannelPush))
}

// channelLimiter, kanal başına farklı davranan bir limiter.
type channelLimiter struct {
	denied map[domain.Channel]bool
}

func (l channelLimiter) Allow(
	_ context.Context, channel domain.Channel,
) (ratelimit.Decision, error) {
	if l.denied[channel] {
		return deny(time.Millisecond), nil
	}

	return allow(), nil
}

// TestRunKeepsSendingOtherChannelsWhenOneIsThrottled, bir kanalın kovası boşken
// diğer kanalların gönderime devam ettiğini doğrular.
//
// PLAN §9 "Kanal izolasyonu: SMS yoğunluğu email gecikmesine yol açmaz" satırının
// motor tarafındaki kanıtı. Kanallar ayrı havuzlarda ve ayrı kovalarda olduğu için
// SMS'in tıkanması e-posta sender'larını etkilemiyor.
func TestRunKeepsSendingOtherChannelsWhenOneIsThrottled(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Channels = []domain.Channel{domain.ChannelSMS, domain.ChannelEmail}
	cfg.Concurrency = 2
	cfg.ClaimBatchSize = 2
	cfg.RateWaitMax = 50 * time.Millisecond

	// Kuyruk hep dolu; claim edilen satırların kanalı istenen kanal olur.
	queue := &stubQueue{alwaysFull: true, claimedChannel: true}
	sender := &stubSender{resp: acceptedResponse()}

	limiter := channelLimiter{denied: map[domain.Channel]bool{domain.ChannelSMS: true}}

	engine := newTestEngineWithLimiter(t, cfg, queue, sender, limiter)

	ctx, cancel := context.WithCancel(context.Background())

	runErr := make(chan error, 1)

	go func() { runErr <- engine.Run(ctx) }()

	// E-posta tarafı ilerlemeli.
	require.True(t, waitFor(t, 10*time.Second, func() bool {
		return len(queue.completedResults()) >= 10
	}), "e-posta gönderimleri SMS tıkanıklığına rağmen ilerlemeliydi")

	cancel()

	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run kapanmadı")
	}

	// Tamamlanan her sonuç e-posta kanalından gelmiş olmalı: SMS hiç gönderilmedi.
	for _, result := range queue.completedResults() {
		require.Equal(t, domain.ChannelEmail, queue.channelOf(result.ID),
			"SMS kovası boşken SMS gönderilmemeliydi")
	}

	// SMS satırları kuyruğa geri verilmiş olmalı.
	require.NotEmpty(t, queue.releasedHolds(), "izin alamayan SMS satırları geri verilmeli")
}

// blockingLimiter, çağrıyı context iptal edilene kadar bekleten limiter.
//
// Takılı bir Redis'in taklidi: container durdurulduğunda go-redis çağrısı istemci
// içinde bloklanıyor.
type blockingLimiter struct {
	calls atomic.Int64
}

func (l *blockingLimiter) Allow(
	ctx context.Context, _ domain.Channel,
) (ratelimit.Decision, error) {
	l.calls.Add(1)

	<-ctx.Done()

	return ratelimit.Decision{}, ctx.Err()
}

// TestPermitWaitBoundsEachLimiterCall, takılı bir limiter'ın tek çağrıda bütün
// bekleme bütçesini yemediğini doğrular.
//
// Bu, canlı ölçümde yakalanan bir boşluğun regresyon testi: Redis container'ı
// durdurulduğunda çağrı bloklanıyor, zaman aşımı olmadan RateWaitMax tek çağrıda
// tükeniyordu — ne hata loglanıyor ne yeniden deneniyor, satır da bütün süre
// boyunca lease altında kalıyordu.
//
// rateAllowTimeout (1 sn) sayesinde bütçe birden çok denemeye bölünüyor.
func TestPermitWaitBoundsEachLimiterCall(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	// Bütçe tek çağrının zaman aşımından belirgin şekilde büyük: birden çok
	// deneme sığmalı.
	cfg.RateWaitMax = 3 * rateAllowTimeout
	cfg.LeaseDuration = 5 * time.Minute // validate: lease >= 2 × pencere

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := &blockingLimiter{}

	engine := newTestEngineWithLimiter(t, cfg, queue, sender, limiter)

	started := time.Now()

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))

	elapsed := time.Since(started)

	require.GreaterOrEqual(t, limiter.calls.Load(), int64(2),
		"bütçe tek çağrıda tükenmemeli, birden çok deneme yapılmalı")
	require.Zero(t, sender.callCount(), "izin alınamadı, gönderim yapılmamalı")
	require.Len(t, queue.releasedHolds(), 1, "satır kuyruğa geri verilmeli")

	// Toplam süre bütçeyi aşmamalı. Pay TEK bir çağrının zaman aşımından küçük:
	// bütçe sona ererken başlayan sorgu artık kalan süreyle sınırlanıyor (bkz.
	// TestAllowOnceCapsTimeoutByRemainingBudget), yani RateWaitMax gerçek bir üst sınır.
	require.Less(t, elapsed, cfg.RateWaitMax+rateAllowTimeout)
}

// TestAllowOnceCapsTimeoutByRemainingBudget, tek izin sorgusunun kalan bekleme
// bütçesini aşmadığını doğrular.
//
// Regresyon: allowOnce her zaman rateAllowTimeout (1 sn) kullanıyordu ve bütçe
// kontrolü sorgudan SONRA yapılıyordu. Bütçenin bitmesine milisaniyeler kalmışken
// başlayan bir sorgu bu yüzden RateWaitMax'ı ~1 sn aşabiliyordu; buna uykunun payı da
// eklenince aşım ~1.25 sn'ye çıkıyordu. leaseWindow() süreyi RateWaitMax ile
// hesapladığı için validate'in zorladığı lease alt sınırı eksik kalıyor ve ayarı tam o
// değere çeken bir operatör lease'in gönderim ortasında dolmasına, yani reaper'ın
// devralıp mesajı İKİNCİ KEZ göndermesine açık hâle geliyordu.
func TestAllowOnceCapsTimeoutByRemainingBudget(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	limiter := &blockingLimiter{}

	engine := newTestEngineWithLimiter(t, cfg, queue, sender, limiter)

	// Bütçe tek çağrının zaman aşımından çok küçük: sorgu bütçe kadar sürmeli.
	const budget = 50 * time.Millisecond

	started := time.Now()

	_, err := engine.allowOnce(context.Background(), domain.ChannelSMS, budget)

	elapsed := time.Since(started)

	require.Error(t, err, "takılı limiter bütçe dolunca hata döndürmeli")
	require.Less(t, elapsed, rateAllowTimeout/2,
		"sorgu kalan bütçeyle sınırlanmalı, rateAllowTimeout'u beklememeli")
}
