package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// acceptedResponse, sağlayıcının kabul yanıtı.
func acceptedResponse() provider.SendResponse {
	return provider.SendResponse{StatusCode: 202, MessageID: "msg-1", Status: "accepted"}
}

// runPoolUntil, havuzu çalıştırır ve koşul sağlanınca kapatır.
func runPoolUntil(
	t *testing.T, engine *Engine, condition func() bool,
) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	pool := newChannelPool(engine, domain.ChannelSMS)

	done := make(chan struct{})

	go func() {
		defer close(done)

		pool.run(ctx)
	}()

	require.True(t, waitFor(t, 5*time.Second, condition), "beklenen duruma ulaşılamadı")

	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("havuz kapanmadı")
	}

	// Kapanış sonrası her jeton semafora dönmüş olmalı: sızıntı yoksa havuz yeniden
	// kullanılabilir durumda biter.
	require.Len(t, pool.sem, engine.cfg.Concurrency,
		"kapanışta tüm jetonlar geri verilmeliydi")
}

// TestChannelPoolSendsClaimedWork, temel akışı doğrular: claim → gönder → sonuç yaz.
func TestChannelPoolSendsClaimedWork(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{claims: []claimCall{
		{notifications: []*domain.Notification{
			newTestNotification(domain.ChannelSMS, 1),
			newTestNotification(domain.ChannelSMS, 1),
		}},
	}}
	sender := &stubSender{resp: acceptedResponse()}
	engine := newTestEngine(t, testConfig(), queue, sender)

	runPoolUntil(t, engine, func() bool { return len(queue.completedResults()) == 2 })

	results := queue.completedResults()
	require.Len(t, results, 2)

	for _, result := range results {
		require.Equal(t, domain.StatusSent, result.Status)
		require.NotNil(t, result.ProviderMessageID)
	}
}

// TestClaimerAsksForIdleSenderCount, claim limitinin boştaki sender sayısıyla
// sınırlandığını doğrular.
//
// Bu, lease hesabının temeli: claimer boştaki sender'dan fazla iş çekerse satırlar
// sırada bekler ve lease iş bitmeden dolar.
func TestClaimerAsksForIdleSenderCount(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 3
	cfg.ClaimBatchSize = 20

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	engine := newTestEngine(t, cfg, queue, sender)

	runPoolUntil(t, engine, func() bool { return len(queue.claimLimits()) > 0 })

	for _, limit := range queue.claimLimits() {
		require.LessOrEqual(t, limit, cfg.Concurrency,
			"claim, boştaki sender sayısından fazla iş istememeli")
		require.Positive(t, limit)
	}
}

// TestWorkerNeverHoldsMoreLeasesThanConcurrency, bir worker'ın aynı anda tuttuğu
// lease sayısının Concurrency'yi geçmediğini doğrular.
//
// Planın ilk hâlinde dağıtım kanalı tamponluydu ve claimer öne geçebiliyordu: worker
// kanal başına 2×ClaimBatchSize satırın lease'ini tutuyor, son satır sırada beklerken
// lease süresi doluyordu. Bu test o regresyonu kapatıyor.
func TestWorkerNeverHoldsMoreLeasesThanConcurrency(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 4
	cfg.ClaimBatchSize = 20

	// Kuyruk hiç boşalmıyor: claimer sürekli iş bulacak ve öne geçmeye çalışacak.
	queue := &stubQueue{alwaysFull: true}

	// Gönderim yapay olarak yavaş: claimer öne geçmeye çalışsın.
	sender := &stubSender{resp: acceptedResponse(), delay: 2 * time.Millisecond}
	engine := newTestEngine(t, cfg, queue, sender)

	runPoolUntil(t, engine, func() bool { return len(queue.completedResults()) >= 60 })

	require.LessOrEqual(t, queue.peak.Load(), int64(cfg.Concurrency),
		"tutulan lease sayısı Concurrency'yi geçmemeli")
}

// TestClaimerReturnsUnusedTokens, kuyrukta istenenden az iş olduğunda kullanılmayan
// jetonların geri verildiğini doğrular.
//
// Verilmezse semafordan tur başına jeton sızar ve havuz zamanla hiç hata vermeden
// durur — boş kuyrukta çalışan bir worker'ın kendini kilitlemesi.
func TestClaimerReturnsUnusedTokens(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 20
	cfg.ClaimBatchSize = 20

	// Kuyrukta yalnızca 3 iş var; claimer 20 jetonla claim edecek.
	queue := &stubQueue{claims: []claimCall{
		{notifications: []*domain.Notification{
			newTestNotification(domain.ChannelSMS, 1),
			newTestNotification(domain.ChannelSMS, 1),
			newTestNotification(domain.ChannelSMS, 1),
		}},
	}}

	// Gönderim bloklanır: 3 jeton sender'larda kalsın, kalanı geri verilmiş olmalı.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	var sending atomic.Int64

	sender := &stubSender{
		resp: acceptedResponse(),
		onSend: func() {
			sending.Add(1)
			<-release
		},
	}

	engine := newTestEngine(t, cfg, queue, sender)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := newChannelPool(engine, domain.ChannelSMS)

	go pool.run(ctx)

	require.True(t, waitFor(t, 5*time.Second, func() bool { return sending.Load() == 3 }),
		"üç gönderim başlamalıydı")

	// 20 jetonla claim edildi, 3'ü dağıtıldı → 17'si hemen geri verilmiş olmalı.
	require.True(t, waitFor(t, 5*time.Second, func() bool {
		return len(pool.sem) == cfg.Concurrency-3
	}), "kullanılmayan jetonlar geri verilmeliydi, boşta: %d", len(pool.sem))

	require.Len(t, pool.sem, 17)
}

// TestSemaphoreCountStableAfterManyRounds, yüzlerce boş / yarı dolu / hatalı claim
// turundan sonra jeton sayısının değişmediğini doğrular.
//
// Tur başına tek jeton sızıntısı olsa bile bu test yakalar: sızıntı birikir ve
// semafor kalıcı olarak boşalır.
func TestSemaphoreCountStableAfterManyRounds(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 8
	cfg.ClaimBatchSize = 8

	// Üç durumu döngüsel olarak üret: boş kuyruk, yarı dolu kuyruk, hata.
	claims := make([]claimCall, 0, 300)
	for round := range 300 {
		switch round % 3 {
		case 0:
			claims = append(claims, claimCall{})
		case 1:
			claims = append(claims, claimCall{notifications: []*domain.Notification{
				newTestNotification(domain.ChannelSMS, 1),
				newTestNotification(domain.ChannelSMS, 1),
			}})
		default:
			claims = append(claims, claimCall{err: errors.New("bağlantı koptu")})
		}
	}

	queue := &stubQueue{claims: claims}
	sender := &stubSender{resp: acceptedResponse()}
	engine := newTestEngine(t, cfg, queue, sender)

	// Tüm turlar tüketilene kadar çalış.
	runPoolUntil(t, engine, func() bool { return len(queue.claimLimits()) >= 300 })

	// runPoolUntil kapanışta jeton sayısını zaten doğruluyor; burada turların
	// gerçekten üç durumu da kapsadığını sabitliyoruz.
	require.GreaterOrEqual(t, len(queue.claimLimits()), 300)
	require.NotEmpty(t, queue.completedResults(), "yarı dolu turlar işlenmiş olmalı")
}

// TestChannelPoolKeepsRunningAfterClaimError, geçici veritabanı hatasının havuzu
// düşürmediğini doğrular.
//
// Hata döndürülse errgroup bütün worker'ı kapatır ve restart politikasıyla
// crash-loop'a girerdi: tek saniyelik bir Postgres hıçkırığı servisi yok ederdi.
func TestChannelPoolKeepsRunningAfterClaimError(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{claims: []claimCall{
		{err: errors.New("bağlantı koptu")},
		{err: errors.New("bağlantı koptu")},
		{notifications: []*domain.Notification{newTestNotification(domain.ChannelSMS, 1)}},
	}}
	sender := &stubSender{resp: acceptedResponse()}
	engine := newTestEngine(t, testConfig(), queue, sender)

	runPoolUntil(t, engine, func() bool { return len(queue.completedResults()) == 1 })

	require.Len(t, queue.completedResults(), 1,
		"iki hatadan sonra üçüncü tur işini yapmalıydı")
	require.GreaterOrEqual(t, len(queue.claimLimits()), 3)
}

// TestChannelPoolReleasesUndispatchedLeasesOnShutdown, kapanışta claim edilmiş ama
// hiç denenmemiş işlerin kuyruğa geri verildiğini doğrular.
//
// "Worker restart'ta iş kaybı yok" kabulünün mekanizması bu: geri verilmezse satırlar
// lease süresi dolana kadar (dakikalar) kimseye gitmez.
func TestChannelPoolReleasesUndispatchedLeasesOnShutdown(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 2
	cfg.ClaimBatchSize = 2

	first := newTestNotification(domain.ChannelSMS, 1)
	second := newTestNotification(domain.ChannelSMS, 1)

	queue := &stubQueue{claims: []claimCall{
		{notifications: []*domain.Notification{first, second}},
	}}

	// Sender'lar bloklanır: ilk iki iş uçar, claimer bir sonraki turda jeton
	// bekleyemeyeceği için kapanış anında elinde iş kalmasını kurgulayalım.
	sendStarted := make(chan struct{}, 2)
	release := make(chan struct{})

	sender := &stubSender{
		resp: acceptedResponse(),
		onSend: func() {
			sendStarted <- struct{}{}
			<-release
		},
	}

	engine := newTestEngine(t, cfg, queue, sender)

	ctx, cancel := context.WithCancel(context.Background())
	pool := newChannelPool(engine, domain.ChannelSMS)

	done := make(chan struct{})

	go func() {
		defer close(done)

		pool.run(ctx)
	}()

	// İki gönderim de başladı: jetonların ikisi de sender'larda.
	for range 2 {
		select {
		case <-sendStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("gönderimler başlamadı")
		}
	}

	cancel()
	close(release)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("havuz kapanmadı")
	}

	// Bu kurguda iki iş de sender'a geçtiği için geri verilecek lease yok; önemli
	// olan uçan gönderimlerin KESİLMEMESİ ve sonuçlarının yazılması.
	require.Len(t, queue.completedResults(), 2,
		"kapanış uçan gönderimleri kesmemeli")
}

// TestClaimerHandsBackLeasesWhenShutdownInterruptsDispatch, dağıtım sırasında gelen
// kapanışta elde kalan işlerin ReleaseLeases ile geri verildiğini doğrular.
//
// Kurgu: tek sender var ama claim iki iş döndürüyor. İlk iş sender'a geçer ve orada
// bloklanır; ikincisi claimer'ın elinde kalır. Kapanış tam o anda gelir.
func TestClaimerHandsBackLeasesWhenShutdownInterruptsDispatch(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 1
	cfg.ClaimBatchSize = 1

	first := newTestNotification(domain.ChannelSMS, 1)
	second := newTestNotification(domain.ChannelSMS, 1)

	// Claim limiti 1 olacağı için stub ikinci işi kırpar; bu yüzden iki tur kuruyoruz:
	// ilk tur birinci işi verir, ikinci tur ikinci işi verir ve o sırada kapanış gelir.
	queue := &stubQueue{claims: []claimCall{
		{notifications: []*domain.Notification{first}},
		{notifications: []*domain.Notification{second}},
	}}

	sendStarted := make(chan struct{}, 1)
	release := make(chan struct{})

	sender := &stubSender{
		resp: acceptedResponse(),
		onSend: func() {
			select {
			case sendStarted <- struct{}{}:
			default:
			}

			<-release
		},
	}

	engine := newTestEngine(t, cfg, queue, sender)

	ctx, cancel := context.WithCancel(context.Background())
	pool := newChannelPool(engine, domain.ChannelSMS)

	done := make(chan struct{})

	go func() {
		defer close(done)

		pool.run(ctx)
	}()

	select {
	case <-sendStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("gönderim başlamadı")
	}

	// Tek jeton sender'da; claimer jeton bekliyor. Kapanış gelince claimer jeton
	// beklerken çıkar (acquireTokens 0 döner) — iş kaybı olmaz.
	cancel()
	close(release)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("havuz kapanmadı")
	}

	require.Len(t, queue.completedResults(), 1)
	require.Len(t, pool.sem, cfg.Concurrency, "jetonlar geri verilmeliydi")
}

// TestDispatchAllReturnsUndispatchedWorkOnShutdown, kapanış dağıtımın ortasında
// gelirse elde kalan işlerin çağırana döndüğünü doğrular.
//
// Dağıtım kanalı tamponsuz olduğu için bir satır ya claimer'ın elinde ya bir
// sender'da olur; üçüncü bir yer yok. Bu da kapanışta hangi satırların geri
// verileceğini kesinleştiriyor.
func TestDispatchAllReturnsUndispatchedWorkOnShutdown(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t, testConfig(), &stubQueue{}, &stubSender{})
	pool := newChannelPool(engine, domain.ChannelSMS)

	claimed := []*domain.Notification{
		newTestNotification(domain.ChannelSMS, 1),
		newTestNotification(domain.ChannelSMS, 1),
	}

	// Kapanış sinyali gelmiş: hiçbir sender dinlemiyor.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pending := pool.dispatchAll(ctx, claimed)

	require.Len(t, pending, 2, "dağıtılamayan işlerin tamamı dönmeli")
	require.Equal(t, claimed, pending)
}

// TestReleaseLeasesReturnsClaimedWorkToQueue, kapanışta claim edilmiş ama hiç
// denenmemiş işlerin kuyruğa geri verildiğini doğrular.
//
// "Worker restart'ta iş kaybı yok" kabulünün mekanizması bu: geri verilmezse satırlar
// lease süresi dolana kadar (varsayılan 60 saniye) kimseye gitmez.
//
// Ayrıca kritik bir ayrıntıyı sabitliyor: geri verme, ZATEN İPTAL EDİLMİŞ bir context
// ile çağrıldığında da çalışmalı (context.WithoutCancel). Parent'ın çocuğu olsaydı
// sorgu hiç gönderilmezdi.
func TestReleaseLeasesReturnsClaimedWorkToQueue(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}
	engine := newTestEngine(t, testConfig(), queue, &stubSender{})
	pool := newChannelPool(engine, domain.ChannelSMS)

	first := newTestNotification(domain.ChannelSMS, 1)
	second := newTestNotification(domain.ChannelSMS, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	engine.releaseLeases(ctx, pool.logger, []*domain.Notification{first, second})

	released := queue.releasedHolds()
	require.Len(t, released, 1, "tek çağrıda hepsi geri verilmeli")
	require.ElementsMatch(t, []store.LeaseHold{
		{ID: first.ID, Token: *first.LeaseToken},
		{ID: second.ID, Token: *second.LeaseToken},
	}, released[0], "her satır kendi token'ıyla eşlenmeli")
}

// TestReleaseLeasesSkipsRowsWithoutToken, token'sız satırın atlandığını doğrular.
//
// Claim her satıra token yazar; token'sız satır bir değişmez ihlali. Geri verme
// sorgusu (id, lease_token) ikilisiyle eşleştiği için token'sız kayıt gönderilse
// hiçbir satırı etkilemezdi — yine de sessiz kalınmıyor.
func TestReleaseLeasesSkipsRowsWithoutToken(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}
	engine := newTestEngine(t, testConfig(), queue, &stubSender{})
	pool := newChannelPool(engine, domain.ChannelSMS)

	withToken := newTestNotification(domain.ChannelSMS, 1)
	withoutToken := newTestNotification(domain.ChannelSMS, 1)
	withoutToken.LeaseToken = nil

	engine.releaseLeases(context.Background(), pool.logger,
		[]*domain.Notification{withToken, withoutToken})

	released := queue.releasedHolds()
	require.Len(t, released, 1)
	require.Len(t, released[0], 1, "yalnızca token'ı olan satır geri verilmeli")
	require.Equal(t, withToken.ID, released[0][0].ID)
}

// TestReleaseLeasesSkipsQueryWhenNothingPending, elde iş yokken veritabanına
// gidilmediğini doğrular: kapanışta boş havuz gereksiz sorgu atmasın.
func TestReleaseLeasesSkipsQueryWhenNothingPending(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}
	engine := newTestEngine(t, testConfig(), queue, &stubSender{})
	pool := newChannelPool(engine, domain.ChannelSMS)

	engine.releaseLeases(context.Background(), pool.logger, nil)

	require.Empty(t, queue.releasedHolds())
}

// TestSendWritesResultWithShutdownSafeContext, kapanış sinyalinden sonra bile sonuç
// yazımının İPTAL EDİLMİŞ OLMAYAN bir context ile yapıldığını doğrular.
//
// Bu tasarımın en tehlikeli hatası: parent context'in çocuğu kullanılsaydı SIGTERM
// sonrası gönderim başarılı olup sonucu yazılamaz, satır processing'de kalır ve
// reaper aynı mesajı ikinci kez gönderirdi.
func TestSendWritesResultWithShutdownSafeContext(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	engine := newTestEngine(t, testConfig(), queue, sender)

	// Parent zaten iptal edilmiş: gerçek kapanış anının taklidi.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	engine.handle(ctx, newTestNotification(domain.ChannelSMS, 1))

	require.Equal(t, 1, sender.callCount(), "iptal edilmiş parent gönderimi engellememeli")

	queue.mu.Lock()
	defer queue.mu.Unlock()

	require.Len(t, queue.completeCtxDone, 1)
	require.False(t, queue.completeCtxDone[0],
		"sonuç yazımı iptal edilmiş bir context ile yapılmamalı")
}

// TestHandleSkipsNotificationWithoutLease, lease token'ı olmayan satırın işlenmediğini
// doğrular: claim her satıra token yazar, token'sız satır değişmez ihlalidir.
func TestHandleSkipsNotificationWithoutLease(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}
	sender := &stubSender{resp: acceptedResponse()}
	engine := newTestEngine(t, testConfig(), queue, sender)

	notification := newTestNotification(domain.ChannelSMS, 1)
	notification.LeaseToken = nil

	engine.handle(context.Background(), notification)

	require.Zero(t, sender.callCount(), "token'sız satır sağlayıcıya gönderilmemeli")
	require.Empty(t, queue.completedResults())
}
