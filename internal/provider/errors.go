package provider

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Bu dosyadaki hata tipleri sağlayıcıdan dönen OLGUYU taşır, politikayı taşımaz.
//
// Client "bu hata kalıcı mı, yeniden denenmeli mi" sorusuna cevap vermez: o karar
// worker'ın sınıflandırma katmanında (Adım 8'de genişleyecek) verilir ve client
// yeniden yazılmadan değişebilir. Çağıran errors.As ile hangi olguyla karşılaştığını
// öğrenir.

// StatusError, sağlayıcıdan 2xx dışı bir yanıt geldiğini bildirir.
type StatusError struct {
	StatusCode int

	// RetryAfter, Retry-After header'ından çözülen bekleme süresi; header yoksa 0.
	// Ayrıştırma burada yapılıyor çünkü header biçimi bir transport ayrıntısı;
	// süreye uyup uymamak ise politika.
	RetryAfter time.Duration

	// Body, yanıt gövdesinin ilk baytları. Hata mesajına konur: sağlayıcının
	// "neden" dediğini görmek olmadan 400 ile 400 arasındaki fark anlaşılmaz.
	Body string
}

func (e *StatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("sağlayıcı %d döndü", e.StatusCode)
	}

	return fmt.Sprintf("sağlayıcı %d döndü: %s", e.StatusCode, e.Body)
}

// TransportError, isteğin hiç yanıt almadığını bildirir: timeout, DNS hatası,
// bağlantı reddi.
//
// Bu durumda mesajın sağlayıcıya ulaşıp ulaşmadığı BİLİNEMEZ; yeniden deneme
// duplicate üretebilir. At-least-once teslimatın kaynağı budur.
type TransportError struct {
	Err error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("sağlayıcıya ulaşılamadı: %v", e.Err)
}

// Unwrap, errors.Is(err, context.DeadlineExceeded) ve net.Error iddialarının
// sarmalayıcıdan geçebilmesi için.
func (e *TransportError) Unwrap() error { return e.Err }

// DecodeError, 2xx yanıtın beklenen zarfa çözülemediğini bildirir.
//
// Mesaj sağlayıcıya ulaşmış olabilir; bu yüzden "gönderilemedi" değil "sonucu
// okunamadı" durumudur.
type DecodeError struct {
	Err  error
	Body string
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("sağlayıcı yanıtı çözülemedi: %v (gövde: %s)", e.Err, e.Body)
}

func (e *DecodeError) Unwrap() error { return e.Err }

// parseRetryAfter, Retry-After header'ını süreye çevirir.
//
// RFC 9110 iki biçim tanımlıyor: saniye cinsinden tamsayı ("120") ve HTTP-date
// ("Wed, 21 Oct 2015 07:28:00 GMT"). İkisi de destekleniyor çünkü hangisinin
// geleceği sağlayıcıya bağlı ve yalnızca birini desteklemek sessizce 0 döndürürdü.
//
// now parametre olarak alınıyor ki HTTP-date dalı testte determinist olsun.
func parseRetryAfter(header string, now time.Time) time.Duration {
	if header == "" {
		return 0
	}

	if seconds, err := strconv.Atoi(header); err == nil {
		if seconds <= 0 {
			return 0
		}

		return time.Duration(seconds) * time.Second
	}

	when, err := http.ParseTime(header)
	if err != nil {
		return 0
	}

	// Geçmiş bir tarih "hemen deneyebilirsin" demektir.
	if delay := when.Sub(now); delay > 0 {
		return delay
	}

	return 0
}
