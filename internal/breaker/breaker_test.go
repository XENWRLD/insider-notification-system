package breaker

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Bu testler dış altyapı gerektirmez ve -short altında da koşar.
//
// Zamanlama: OpenTimeout testlerde çok kısa (50 ms) ve iddialar YÖN üzerine
// (açıldı/kapandı), tam süre üzerine değil — yavaş bir makinede kalmasınlar.

const (
	testThreshold   = 3
	testOpenTimeout = 50 * time.Millisecond
)

// errBoom, devreyi tetikleyen örnek hata.
var errBoom = errors.New("sağlayıcı patladı")

// errIgnored, devreyi ilgilendirmeyen örnek hata (429'un taklidi).
var errIgnored = errors.New("hız sınırı")

// newTestBreaker, errIgnored dışındaki her hatayı arıza sayan bir devre kesici kurar.
func newTestBreaker(t *testing.T) Breaker {
	t.Helper()

	breaker, err := New(Config{
		Name:             "test",
		FailureThreshold: testThreshold,
		OpenTimeout:      testOpenTimeout,
		IsFailure: func(err error) bool {
			return !errors.Is(err, errIgnored)
		},
	})
	require.NoError(t, err)

	return breaker
}

// fail, devreye bir arıza bildirir.
func fail(t *testing.T, breaker Breaker) error {
	t.Helper()

	return breaker.Do(func() error { return errBoom })
}

// trip, devreyi eşiğe kadar hata vererek açar.
func trip(t *testing.T, breaker Breaker) {
	t.Helper()

	for range testThreshold {
		require.ErrorIs(t, fail(t, breaker), errBoom)
	}

	require.Equal(t, StateOpen, breaker.State(), "eşik dolunca devre açılmalıydı")
}

// awaitHalfOpen, devrenin yarı açığa geçmesini bekler.
//
// gobreaker durumu tembel değerlendiriyor: State() çağrısı geçişi tetikliyor.
func awaitHalfOpen(t *testing.T, breaker Breaker) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if breaker.State() == StateHalfOpen {
			return
		}

		time.Sleep(testOpenTimeout / 5)
	}

	require.Equal(t, StateHalfOpen, breaker.State(), "devre yarı açığa geçmeliydi")
}

// TestClosedBreakerRunsRequest, kapalı devrenin çağrıyı geçirdiğini doğrular.
func TestClosedBreakerRunsRequest(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)

	called := false
	require.NoError(t, breaker.Do(func() error {
		called = true

		return nil
	}))

	require.True(t, called)
	require.Equal(t, StateClosed, breaker.State())
}

// TestDoReturnsCallersError, çağrının hatasının olduğu gibi döndüğünü doğrular:
// devre kesici hatayı yutmuyor ya da sarmıyor.
func TestDoReturnsCallersError(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)

	require.ErrorIs(t, fail(t, breaker), errBoom)
}

// TestBreakerOpensAfterThresholdFailures, ardışık hata eşiğinde devrenin açıldığını
// doğrular.
func TestBreakerOpensAfterThresholdFailures(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)

	for i := range testThreshold - 1 {
		require.ErrorIs(t, fail(t, breaker), errBoom)
		require.Equal(t, StateClosed, breaker.State(), "eşik dolmadan açılmamalı (hata %d)", i+1)
	}

	require.ErrorIs(t, fail(t, breaker), errBoom)
	require.Equal(t, StateOpen, breaker.State())
}

// TestOpenBreakerDoesNotCallRequest, açık devrede çağrının HİÇ yapılmadığını doğrular.
//
// Asıl kazanç bu: boşa gidecek istek sağlayıcıya ulaşmıyor, dolayısıyla worker
// tarafında deneme hakkı ve rate limit izni de harcanmıyor.
func TestOpenBreakerDoesNotCallRequest(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)
	trip(t, breaker)

	called := false
	err := breaker.Do(func() error {
		called = true

		return nil
	})

	require.ErrorIs(t, err, ErrOpen)
	require.False(t, called, "açık devrede sağlayıcıya gidilmemeli")
}

// TestSuccessResetsConsecutiveFailures, araya giren başarının sayacı sıfırladığını
// doğrular.
//
// Bu davranış "zehirli mesaj" senaryosunun kaynağı: tek bir mesaj hep hata verse de
// diğerleri başarılı oldukça devre açılmıyor. Worker bunu bilerek sınıflandırma
// yapıyor (bkz. internal/worker/classify.go).
func TestSuccessResetsConsecutiveFailures(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)

	for range testThreshold - 1 {
		require.ErrorIs(t, fail(t, breaker), errBoom)
	}

	require.NoError(t, breaker.Do(func() error { return nil }))

	// Sayaç sıfırlandığı için eşik yeniden baştan dolmalı.
	for range testThreshold - 1 {
		require.ErrorIs(t, fail(t, breaker), errBoom)
		require.Equal(t, StateClosed, breaker.State())
	}
}

// TestExcludedErrorDoesNotResetFailureCounter, DIŞLANAN hatanın sayaca hiç
// dokunmadığını doğrular.
//
// Kütüphane davranışını okumaya değil teste bağlıyoruz. Dışlanan hata "başarı"
// sayılsaydı sayacı sıfırlardı ve kâh 503 kâh 429 dönen karışık bir kesintide devre
// çok geç açılırdı.
func TestExcludedErrorDoesNotResetFailureCounter(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)

	// Eşiğe bir kalana kadar arıza.
	for range testThreshold - 1 {
		require.ErrorIs(t, fail(t, breaker), errBoom)
	}

	// Araya dışlanan hata giriyor: sayaç DEĞİŞMEMELİ.
	require.ErrorIs(t, breaker.Do(func() error { return errIgnored }), errIgnored)
	require.Equal(t, StateClosed, breaker.State(), "dışlanan hata devreyi açmamalı")

	// Tek bir arıza daha eşiği doldurmalı — yani sayaç sıfırlanmadı.
	require.ErrorIs(t, fail(t, breaker), errBoom)
	require.Equal(t, StateOpen, breaker.State(),
		"dışlanan hata ardışık sayacı sıfırlamamalıydı")
}

// TestBreakerHalfOpensAfterTimeout, açık devrenin süre sonunda yarı açığa geçtiğini
// doğrular.
func TestBreakerHalfOpensAfterTimeout(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)
	trip(t, breaker)

	awaitHalfOpen(t, breaker)
}

// TestHalfOpenAllowsSingleProbe, yarı açıkta yalnızca TEK çağrının geçtiğini doğrular.
//
// İkinci çağrı sonda uçuştayken ErrOpen alıyor: sağlayıcı hâlâ bozuk olabilirken
// ona birden fazla istek gitmiyor.
func TestHalfOpenAllowsSingleProbe(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)
	trip(t, breaker)
	awaitHalfOpen(t, breaker)

	probeStarted := make(chan struct{})
	release := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		_ = breaker.Do(func() error {
			close(probeStarted)
			<-release

			return nil
		})
	}()

	<-probeStarted

	// Sonda uçuşta: ikinci çağrı reddedilmeli ve fn çağrılmamalı.
	called := false
	err := breaker.Do(func() error {
		called = true

		return nil
	})

	require.ErrorIs(t, err, ErrOpen)
	require.False(t, called, "yarı açıkta ikinci istek sağlayıcıya gitmemeli")

	close(release)
	wg.Wait()
}

// TestExcludedProbeDoesNotConsumeHalfOpenSlot, dışlanan bir sondanın yarı açık
// kotasını tüketmediğini doğrular.
//
// Tüketseydi: 429 dönen tek bir sonda devreyi yarı açıkta kilitlerdi — ne kapanır
// ne açılır, hiçbir sonda daha geçemezdi. Kütüphanenin validRequests() davranışını
// bu test sabitliyor.
func TestExcludedProbeDoesNotConsumeHalfOpenSlot(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)
	trip(t, breaker)
	awaitHalfOpen(t, breaker)

	// Birinci sonda dışlanan bir hata alıyor.
	require.ErrorIs(t, breaker.Do(func() error { return errIgnored }), errIgnored)

	// İkinci sonda HÂLÂ geçebilmeli.
	called := false
	err := breaker.Do(func() error {
		called = true

		return nil
	})

	require.NoError(t, err, "dışlanan sonda kotayı tüketmemeliydi")
	require.True(t, called)
}

// TestHalfOpenClosesOnSuccessfulProbe, başarılı sondanın devreyi kapattığını doğrular.
func TestHalfOpenClosesOnSuccessfulProbe(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)
	trip(t, breaker)
	awaitHalfOpen(t, breaker)

	require.NoError(t, breaker.Do(func() error { return nil }))
	require.Equal(t, StateClosed, breaker.State(), "başarılı sonda devreyi kapatmalı")
}

// TestHalfOpenReopensOnFailedProbe, başarısız sondanın devreyi tekrar açtığını
// doğrular — tek bir hata yeterli, eşiğin yeniden dolması gerekmiyor.
func TestHalfOpenReopensOnFailedProbe(t *testing.T) {
	t.Parallel()

	breaker := newTestBreaker(t)
	trip(t, breaker)
	awaitHalfOpen(t, breaker)

	require.ErrorIs(t, fail(t, breaker), errBoom)
	require.Equal(t, StateOpen, breaker.State(), "başarısız sonda devreyi tekrar açmalı")
}

// stateChange, OnStateChange callback'ine düşen bir geçiş.
type stateChange struct {
	name string
	from State
	to   State
}

// TestOnStateChangeReportsTransitions, geçişlerin callback'e bildirildiğini doğrular.
//
// Worker bunu loglama için kullanıyor: her yoklamada loglamak yerine geçiş başına
// bir satır.
func TestOnStateChangeReportsTransitions(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		changes []stateChange
	)

	breaker, err := New(Config{
		Name:             "sms",
		FailureThreshold: testThreshold,
		OpenTimeout:      testOpenTimeout,
		IsFailure:        func(error) bool { return true },
		OnStateChange: func(name string, from, to State) {
			mu.Lock()
			defer mu.Unlock()

			changes = append(changes, stateChange{name: name, from: from, to: to})
		},
	})
	require.NoError(t, err)

	trip(t, breaker)
	awaitHalfOpen(t, breaker)
	require.NoError(t, breaker.Do(func() error { return nil }))

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, []stateChange{
		{name: "sms", from: StateClosed, to: StateOpen},
		{name: "sms", from: StateOpen, to: StateHalfOpen},
		{name: "sms", from: StateHalfOpen, to: StateClosed},
	}, changes)
}

// TestAlwaysClosedNeverBlocks, test yardımcısının hiç engellemediğini doğrular.
func TestAlwaysClosedNeverBlocks(t *testing.T) {
	t.Parallel()

	breaker := AlwaysClosed()

	for range 100 {
		require.ErrorIs(t, breaker.Do(func() error { return errBoom }), errBoom)
		require.Equal(t, StateClosed, breaker.State())
	}

	require.NoError(t, breaker.Do(func() error { return nil }))
}

// TestNewRejectsInvalidConfig, hatalı kurulumun boot'ta yakalandığını ve bütün
// sorunların tek hatada toplandığını doğrular.
func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := New(Config{FailureThreshold: 1, OpenTimeout: time.Second})
	require.Error(t, err, "ad olmadan kurulmamalı")

	for _, threshold := range []int{0, -1} {
		_, err = New(Config{Name: "x", FailureThreshold: threshold, OpenTimeout: time.Second})
		require.Error(t, err, "eşik: %d", threshold)
	}

	_, err = New(Config{Name: "x", FailureThreshold: 1, OpenTimeout: 0})
	require.Error(t, err, "süre olmadan kurulmamalı")

	_, err = New(Config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Name")
	require.Contains(t, err.Error(), "FailureThreshold")
	require.Contains(t, err.Error(), "OpenTimeout")
}

// TestNilIsFailureTreatsEveryErrorAsFailure, varsayılan davranışı sabitler.
func TestNilIsFailureTreatsEveryErrorAsFailure(t *testing.T) {
	t.Parallel()

	breaker, err := New(Config{
		Name:             "test",
		FailureThreshold: testThreshold,
		OpenTimeout:      testOpenTimeout,
	})
	require.NoError(t, err)

	for range testThreshold {
		require.ErrorIs(t, breaker.Do(func() error { return errIgnored }), errIgnored)
	}

	require.Equal(t, StateOpen, breaker.State(),
		"IsFailure verilmediyse her hata arıza sayılmalı")
}
