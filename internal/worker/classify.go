package worker

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// Bu dosya, sağlayıcı yanıtını yapılacak EYLEME çeviren tek yerdir.
//
// Üç eksen var ve bunlar BAĞIMSIZ: (a) sonuç nedir, (b) deneme hakkı harcanır mı,
// (c) devre kesici tetiklenir mi. Adım 6'nın kaba kuralı (2xx dışı her şey sabit
// gecikmeyle retrying) bu adımda yerini aşağıdaki tabloya bıraktı:
//
//	sağlayıcı sonucu                      sonuç        deneme  available_at                   devre
//	------------------------------------  -----------  ------  -----------------------------  --------
//	2xx (+ DecodeError)                   sent         evet    —                              başarı
//	400/422 ve tanınmayan 4xx             failed       evet    —                              başarı
//	429                                   kuyruğa dön  HAYIR   now + rateLimitDelay           sayılmaz
//	timeout — devre kapalı                retrying     EVET    now + backoff                  hata
//	timeout — devre devraldı               retrying     EVET    now + max(backoff, requeue)    hata
//	5xx/401/403/404/bağlantı — kapalı     retrying     EVET    now + backoff                  hata
//	5xx/401/403/404/bağlantı — devraldı   kuyruğa dön  HAYIR   now + requeueDelay             hata
//	ErrOpen (çağrı hiç yapılmadı)         kuyruğa dön  HAYIR   now + requeueDelay             —
//	max_attempts tükendi                  failed       —       —                              —
//
// Fonksiyon SAF tutuluyor (log yok, saat yok, veritabanı yok, devre durumu okunmuyor —
// parametre olarak alınıyor) ki her dalı tek satırlık bir birim testiyle sabitlenebilsin.

// action, bir gönderim sonucunun ardından yapılacak iş.
//
// İki eylem gerekiyor çünkü bazı yollarda veritabanına SONUÇ yazılmıyor: mesajın suçu
// olmayan reddedilmelerde (429) ve devre kesicinin devraldığı durumlarda satır,
// denemesi sayılmadan kuyruğa geri veriliyor.
type action int

const (
	// actionWriteResult, sonucu (sent | failed | retrying) ve deneme kaydını yazar.
	actionWriteResult action = iota

	// actionRequeue, denemeyi saymadan satırı kuyruğa döndürür (store.DeferLeases).
	actionRequeue
)

// decision, classify'ın verdiği karar.
type decision struct {
	action action

	// result yalnızca actionWriteResult için anlamlı.
	result store.NotificationResult

	// requeueDelay, actionRequeue için erteleme. 0 ise available_at KORUNUR.
	requeueDelay time.Duration

	// requeueReason, actionRequeue için last_error'a yazılacak sebep. Bu yol deneme
	// kaydı üretmediği için "satır neden bekliyor" sorusunun tek cevabı budur.
	requeueReason string
}

// retryPolicy, gecikme hesabının ayarları. Config'ten türetiliyor ki classify
// Config'in tamamını görmek zorunda kalmasın.
type retryPolicy struct {
	// BaseDelay/MaxDelay, exponential backoff penceresinin sınırları.
	BaseDelay time.Duration
	MaxDelay  time.Duration

	// RequeueDelay, devre kesici devraldığında satırın erteleneceği süre.
	//
	// 2 × BreakerOpenTimeout seçildi ve tek kat YETMİYOR; sebebi sıralama anahtarının
	// ilk terimi: claim sorgusu ORDER BY priority_rank, available_at, id kullanıyor,
	// yani öncelik available_at'ten ÖNCE geliyor. Tek katla satır bir sonraki yarı açık
	// pencerede çoktan çekilebilir olur ve yüksek öncelikliyse sondaya YİNE o çıkar —
	// sonda hep başarısız olur, devre hiç kapanmaz. İki kat, satırın o pencereyi kesin
	// kaçırmasını sağlıyor: sonda başka bir satırla yapılıp devre kapanıyor, ardından
	// zehirli mesaj KAPALI devrede sayılıp max_attempts'ta failed oluyor.
	RequeueDelay time.Duration
}

// statusKind, bir HTTP durum kodunun retry açısından sınıfı.
type statusKind int

const (
	// kindPermanent, mesajın kendisi hatalı: aynı baytları yeniden göndermek
	// sağlayıcıyı meşgul etmekten başka bir şey yapmaz.
	kindPermanent statusKind = iota

	// kindThrottle, anlık yoğunluk sinyali (429). Mesajın suçu değil.
	kindThrottle

	// kindChannel, kanalın bütününe dair arıza: 5xx (sağlayıcı bozuk) ve
	// 401/403/404 (yapılandırma — yanlış anahtar, IP izin listesi, yanlış URL).
	// İkisi davranış olarak aynı: "kesin ulaşmadı" + devreyi ilgilendirir.
	kindChannel
)

// statusClass, durum kodunu sınıfına eşler.
//
// 401/403/404 neden failed DEĞİL: bunlar mesajın değil YAPILANDIRMANIN hatası.
// failed deseydik anahtar rotasyonu sırasında kuyruktaki her mesaj kalıcı olarak
// ölürdü — mesajlar geçerliydi, sistem bozuktu, ve failed terminal olduğu için
// anahtar düzeltilince onları gönderen bir şey kalmazdı. Doğru cevap devre kesici:
// hata kanalın bütününe dair olduğu için devre açılır, claimer çekmeyi bırakır,
// deneme harcanmaz ve operatör düzeltince birikmiş kuyruk olduğu gibi akar.
//
// Tanınmayan diğer 4xx'ler kindPermanent: 4xx "isteğin yanlış" demek ve bilinmeyen
// bir kodu kanal arızası saymak tek bir tuhaf yanıtın bütün kanalı durdurmasına
// yol açardı.
func statusClass(code int) statusKind {
	switch {
	case code == 429:
		return kindThrottle

	case code == 401, code == 403, code == 404:
		return kindChannel

	case code >= 500:
		return kindChannel

	case code >= 400:
		return kindPermanent

	default:
		// 1xx/3xx beklenmiyor (client 2xx'i başarı sayıyor). Buraya düşen bir yanıt
		// yapılandırma hatasıdır; kanal arızası saymak güvenli taraf.
		return kindChannel
	}
}

// breakerOutcome, bir gönderim sonucunun DEVRE açısından ne ifade ettiği.
//
// Üç değer var, iki değil — "hata" ile "kanal arızalı" aynı şey değil.
type breakerOutcome int

const (
	// breakerHealthy, kanal sağlıklı: ardışık hata sayacı sıfırlanır ve yarı açık
	// sonda devreyi KAPATIR.
	breakerHealthy breakerOutcome = iota

	// breakerIgnored, kanala dair bilgi yok: sayaçlara hiç dokunulmaz.
	breakerIgnored

	// breakerFailed, kanal arızalı: ardışık hata sayacı artar, eşikte devre açılır.
	breakerFailed
)

// breakerOutcomeFor, gönderim hatasını devre sonucuna eşler.
//
//	nil (2xx)                              → healthy  (mesaj kabul edildi)
//	DecodeError (2xx, zarf okunamadı)      → healthy  (mesaj KABUL EDİLDİ, yalnızca
//	                                         yanıtı çözemedik — kanal sağlıklı)
//	400/422/tanınmayan 4xx                 → healthy  (sağlayıcı ayakta ve düzgün bir
//	                                         ret üretti: sağlığının kanıtı)
//	429                                    → ignored  (ne sağlık ne arıza kanıtı)
//	5xx/401/403/404/TransportError         → failed
//
// 429 neden BAŞARI değil: başarı sayılsaydı ardışık hata sayacını SIFIRLARDI ve karışık
// bir kesintide (sağlayıcı kâh 503 kâh 429 dönüyor) devre çok geç açılırdı — 429'lar
// araya girdikçe 5xx'ler hiç üst üste gelmezdi.
func breakerOutcomeFor(sendErr error) breakerOutcome {
	if sendErr == nil {
		return breakerHealthy
	}

	var decodeErr *provider.DecodeError
	if errors.As(sendErr, &decodeErr) {
		return breakerHealthy
	}

	var statusErr *provider.StatusError
	if errors.As(sendErr, &statusErr) {
		switch statusClass(statusErr.StatusCode) {
		case kindThrottle:
			return breakerIgnored

		case kindPermanent:
			return breakerHealthy

		case kindChannel:
			return breakerFailed
		}
	}

	// TransportError ve timeout: yanıt hiç gelmedi, kanala dair arıza.
	return breakerFailed
}

// BreakerIsFailure, devre kesici yapılandırmasının IsFailure fonksiyonu.
//
// Dışa açık çünkü devre kesici fabrikasını cmd/worker kuruyor ama politika worker
// paketine ait: hangi hatanın kanalı ilgilendirdiğini bilen yer burası.
func BreakerIsFailure(err error) bool {
	return breakerOutcomeFor(err) == breakerFailed
}

// isTimeout, hatanın zaman aşımı olup olmadığını söyler.
//
// BİLİNÇLİ MUHAFAZAKÂRLIK: dial/TLS zaman aşımları da buraya düşüyor, oysa orada
// istek hiç gönderilmedi (çift gönderim riski yok) ve aslında denemeyi saymamak
// gerekirdi. Ayrımı yapmak hata zincirinde *net.OpError{Op:"dial"} aramak demek;
// kazanç marjinal (devre zaten açılıyor, sonda başına bir deneme), karmaşıklık
// gerçek. Hata yönü güvenli tarafa düşüyor: fazla saymak mesajı durdurur, az saymak
// sonsuz döngü üretirdi.
func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error

	return errors.As(err, &netErr) && netErr.Timeout()
}

// backoffWindow, verilen deneme için gecikme penceresini verir:
// min(base × 2^(attempt-1), max). Saf ve tam değerle test edilir.
func backoffWindow(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	window := base

	for range attempt - 1 {
		// Taşma ve tavan birlikte kontrol ediliyor: çok büyük bir pencere ikiyle
		// çarpıldığında negatife düşebilir ve negatif gecikme "hemen" demek olurdu.
		if doubled := window * 2; doubled <= 0 || doubled >= maxDelay {
			return maxDelay
		}

		window *= 2
	}

	return window
}

// rateLimitDelay, 429'da uygulanacak gecikmeyi verir.
//
// Taban şart: attempt_count geri alındığı için backoffWindow BÜYÜMÜYOR (pencere her
// turda BaseDelay kalır) ve jitter onu sıfıra yakın bir değere indirebilir — 429
// fırtınasında satır neredeyse beklemeden dönerdi. Tavan da şart: sağlayıcı
// "Retry-After: 86400" derse satır bir gün kaybolmamalı.
func rateLimitDelay(retryAfter time.Duration, policy retryPolicy) time.Duration {
	if retryAfter > 0 {
		return min(retryAfter, policy.MaxDelay)
	}

	return policy.BaseDelay + jitter(policy.BaseDelay)
}

// classify, gönderim sonucunu yapılacak eyleme çevirir.
//
// breakerState, brk.Do'dan SONRA okunan devre durumudur ve "zehirli mesaj" açığını
// kapatan parametredir (bkz. requeueFor).
func classify(
	notification *domain.Notification,
	resp provider.SendResponse,
	sendErr error,
	policy retryPolicy,
	elapsed time.Duration,
	breakerState breaker.State,
) decision {
	result := store.NotificationResult{
		ID:         notification.ID,
		LeaseToken: *notification.LeaseToken,
		DurationMS: durationMS(elapsed),
	}

	// Devre kesici çağrıyı hiç yapmadı: ne deneme ne izin harcandı.
	if errors.Is(sendErr, breaker.ErrOpen) {
		return requeue(policy.RequeueDelay, sendErr)
	}

	var decodeErr *provider.DecodeError

	switch {
	case sendErr == nil:
		return decision{result: accepted(result, resp)}

	case errors.As(sendErr, &decodeErr):
		// 2xx geldi, yalnızca zarf okunamadı: sağlayıcı mesajı KABUL ETTİ. retrying
		// yapmak aynı mesajı tekrar göndermek olurdu — gövdesi JSON olmayan bir
		// sağlayıcıda her bildirim max_attempts kez giderdi.
		return decision{result: accepted(result, provider.SendResponse{StatusCode: resp.StatusCode})}
	}

	// Devre bu çağrıdan sonra açıldıysa (ya da yarı açık sonda başarısız olduysa)
	// devreyi "devraldı" sayıyoruz. İki durum da aynı koşula iniyor.
	breakerTookOver := breakerState != breaker.StateClosed

	var statusErr *provider.StatusError
	if errors.As(sendErr, &statusErr) {
		switch statusClass(statusErr.StatusCode) {
		case kindPermanent:
			// Mesajın kendisi hatalı: hak kalsa bile hemen failed.
			return decision{result: permanentFailure(result, resp, sendErr)}

		case kindThrottle:
			// 429 "kesin kabul edilmedi" demek (çift gönderim riski yok) ve sürekli
			// 429 görmek BİZİM ayar hatamız: RATE_LIMIT_PER_SECOND sağlayıcının
			// sınırından yüksek. Mesajın suçu olmadığı için deneme harcatmak yanlış
			// olurdu — harcasaydık mesajlar 5 kez 429 alıp kalıcı failed olurdu.
			return requeue(rateLimitDelay(statusErr.RetryAfter, policy), sendErr)

		case kindChannel:
			// Aşağıdaki ortak yola devam.
		}
	}

	if isTimeout(sendErr) {
		// TIMEOUT BELİRSİZDİR: mesajın gidip gitmediği bilinmiyor (yanıt gelmedi ama
		// SMS gönderilmiş olabilir). Bu yüzden deneme HER DURUMDA sayılır — saymazsak
		// aynı satır her yarı açık sondada yeniden seçilip sınırsız gönderilebilirdi
		// (30 sn'lik devre penceresiyle saatte ~120 kez). Deneme sayılınca satır
		// max_attempts'ta durur: at-least-once'ın üst sınırı korunur.
		delay := jitter(backoffWindow(notification.AttemptCount, policy.BaseDelay, policy.MaxDelay))

		// Devre devraldıysa gecikme yükseltilir: aksi hâlde yüksek öncelikli bir satır
		// 2 sn'lik backoff'la yarı açıkta YİNE sondaya çıkar ve 5 denemesi bitene kadar
		// kanalın tek sonda yuvasını tek başına tutar. Devre açıkken daha kısa bir
		// gecikmenin faydası yok: satır o sırada zaten çekilemiyor.
		if breakerTookOver {
			delay = max(delay, policy.RequeueDelay)
		}

		return decision{result: rejected(result, notification, resp, sendErr, delay)}
	}

	// "Kesin ulaşmadı" sınıfı: 5xx, 401/403/404, bağlantı hatası.
	//
	// Denemeyi koşulsuz SAYMAMAK zehirli mesaj açığı yaratıyordu: sağlayıcı yalnızca
	// TEK bir mesaja hep 500 dönerse (ör. alıcısını kendi doğrulayıcısı reddediyor ama
	// 400 yerine 500 veriyor), araya giren başarılar ardışık sayacı sıfırladığı için
	// devre HİÇ açılmaz; o mesaj attempt_count hiç artmadığı için max_attempts'a asla
	// ulaşmaz ve sonsuza kadar döner — her turda bir rate limit izni yiyerek. Bu yüzden
	// muafiyet tam olarak tasarlandığı duruma daraltıldı: deneme yalnızca BREAKER
	// DEVRALDIYSA sayılmaz; devre kapalıysa bu sıradan bir geçici hatadır.
	if breakerTookOver {
		return requeue(policy.RequeueDelay, sendErr)
	}

	delay := jitter(backoffWindow(notification.AttemptCount, policy.BaseDelay, policy.MaxDelay))

	return decision{result: rejected(result, notification, resp, sendErr, delay)}
}

// requeue, denemeyi saymadan kuyruğa dönme kararını kurar.
func requeue(delay time.Duration, sendErr error) decision {
	return decision{
		action:        actionRequeue,
		requeueDelay:  delay,
		requeueReason: sendErr.Error(),
	}
}

// accepted, sağlayıcının kabul ettiği gönderimin sonucunu kurar.
func accepted(
	result store.NotificationResult, resp provider.SendResponse,
) store.NotificationResult {
	result.Status = domain.StatusSent

	if resp.StatusCode != 0 {
		result.StatusCode = &resp.StatusCode
	}

	// Boş kimlik NULL olarak yazılır: boş string "kimlik aldık ama boş" izlenimi verirdi.
	if resp.MessageID != "" {
		messageID := resp.MessageID
		result.ProviderMessageID = &messageID
	}

	return result
}

// recordError, başarısız denemenin hata bilgisini sonuca işler.
func recordError(
	result store.NotificationResult,
	resp provider.SendResponse,
	sendErr error,
) store.NotificationResult {
	message := sendErr.Error()
	result.LastError = &message

	// Yanıt geldiyse durum kodu kaydedilir; timeout/bağlantı hatasında nil kalır.
	var statusErr *provider.StatusError
	if errors.As(sendErr, &statusErr) {
		result.StatusCode = &statusErr.StatusCode
	} else if resp.StatusCode != 0 {
		result.StatusCode = &resp.StatusCode
	}

	return result
}

// permanentFailure, mesajın kendisinden kaynaklanan kalıcı hatayı kurar: deneme hakkı
// kalsa bile failed.
func permanentFailure(
	result store.NotificationResult,
	resp provider.SendResponse,
	sendErr error,
) store.NotificationResult {
	result = recordError(result, resp, sendErr)
	result.Status = domain.StatusFailed

	return result
}

// rejected, hak varsa retrying, yoksa failed sonucunu kurar.
//
// attempt_count claim anında artırıldığı için buradaki karşılaştırma "bu denemeden
// sonra hak kaldı mı" sorusunu cevaplıyor.
func rejected(
	result store.NotificationResult,
	notification *domain.Notification,
	resp provider.SendResponse,
	sendErr error,
	delay time.Duration,
) store.NotificationResult {
	result = recordError(result, resp, sendErr)

	if notification.AttemptCount >= notification.MaxAttempts {
		result.Status = domain.StatusFailed

		return result
	}

	result.Status = domain.StatusRetrying
	result.RetryAfter = delay

	return result
}
