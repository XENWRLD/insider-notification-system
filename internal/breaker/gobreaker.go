package breaker

import (
	"errors"
	"fmt"

	"github.com/sony/gobreaker/v2"
)

// halfOpenProbes, yarı açık durumda geçmesine izin verilen istek sayısı.
//
// Ayarlanabilir değil, tasarımın parçası: yarı açıkta TEK sonda atılıyor ve sonucu
// devrenin kapanıp kapanmayacağını belirliyor. Birden fazla sonda, sağlayıcı hâlâ
// bozukken ona birden fazla istek göndermek olurdu.
//
// (gobreaker'da 0 da "1" anlamına geliyor; açıkça yazmak niyeti belgeliyor.)
const halfOpenProbes = 1

// circuitBreaker, sony/gobreaker üzerine kurulu Breaker uygulaması.
type circuitBreaker struct {
	cb *gobreaker.CircuitBreaker[any]
}

// New, verilen ayarlarla bir devre kesici kurar.
func New(cfg Config) (Breaker, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	threshold := uint32(cfg.FailureThreshold) //nolint:gosec // validate 1'den küçük olmasını engelliyor

	isFailure := cfg.IsFailure
	if isFailure == nil {
		isFailure = func(error) bool { return true }
	}

	settings := gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: halfOpenProbes,

		// Interval 0: kapalı durumda sayaçları periyodik temizlemeye gerek yok.
		// ReadyToTrip ARDIŞIK hataya bakıyor ve tek bir başarı sayacı sıfırlıyor,
		// yani "eski hatalar unutulsun" işini başarının kendisi yapıyor.
		Interval: 0,

		Timeout: cfg.OpenTimeout,

		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= threshold
		},

		// Üç yollu sınıflandırma (bkz. Config.IsFailure):
		//   nil             → başarı
		//   IsFailure true  → hata
		//   IsFailure false → SAYILMAZ (IsExcluded)
		//
		// gobreaker dışlanan hatada onSuccess/onFailure'ın ikisini de atlıyor:
		// ardışık sayaçlar değişmiyor, durum geçişi olmuyor ve yarı açık kotası
		// validRequests() ile ölçüldüğü için sonda yuvası da tüketilmiyor.
		// İkisi de breaker_test.go'da sabitlendi.
		IsSuccessful: func(err error) bool { return err == nil },
		IsExcluded: func(err error) bool {
			return err != nil && !isFailure(err)
		},
	}

	if cfg.OnStateChange != nil {
		onStateChange := cfg.OnStateChange
		settings.OnStateChange = func(name string, from, to gobreaker.State) {
			onStateChange(name, translateState(from), translateState(to))
		}
	}

	return &circuitBreaker{cb: gobreaker.NewCircuitBreaker[any](settings)}, nil
}

// Do, fn'i devre kesicinin altında çalıştırır.
func (b *circuitBreaker) Do(fn func() error) error {
	_, err := b.cb.Execute(func() (any, error) {
		return nil, fn()
	})

	// Kütüphanenin iki ret sebebi tek sentinel'e indiriliyor: çağıran için ikisi de
	// "sağlayıcıya gidilmedi" demek. ErrTooManyRequests yarı açıkta sonda kotası
	// dolduğunda geliyor.
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return ErrOpen
	}

	return err
}

// State, devrenin o anki durumunu döner.
func (b *circuitBreaker) State() State {
	return translateState(b.cb.State())
}

// translateState, gobreaker durumunu paketin kendi tipine çevirir.
func translateState(state gobreaker.State) State {
	switch state {
	case gobreaker.StateClosed:
		return StateClosed
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	case gobreaker.StateOpen:
		return StateOpen
	default:
		// Kütüphane yeni bir durum eklerse sessizce "kapalı" demek yanlış olurdu:
		// en güvenli varsayım gönderimi durdurmak.
		return StateOpen
	}
}

// Derleme zamanında arayüzün karşılandığını doğrular.
var _ Breaker = (*circuitBreaker)(nil)

// gobreakerStateNames, hata mesajlarında kullanılabilecek tanı bilgisi.
func (b *circuitBreaker) String() string {
	counts := b.cb.Counts()

	return fmt.Sprintf("breaker(%s) durum=%s ardışık_hata=%d dışlanan=%d",
		b.cb.Name(), b.State(), counts.ConsecutiveFailures, counts.TotalExclusions)
}
