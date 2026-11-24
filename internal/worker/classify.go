package worker

import (
	"errors"
	"time"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// ADIM 8 DİKİŞİ.
//
// Bu dosya, sağlayıcı yanıtını veritabanına yazılacak sonuca çeviren TEK yerdir.
// Adım 6'daki kural bilinçli olarak kaba: 2xx dışı her sonuçta deneme sayılır,
// hak kalmışsa sabit gecikmeyle retrying, kalmamışsa failed.
//
// Adım 8'de bu fonksiyonun GÖVDESİ değişecek (400/422 → hemen failed; 429 →
// StatusError.RetryAfter; 5xx/timeout → min(2s×2^(n-1), 5dk) + full jitter; circuit
// breaker danışması). claim, reaper, havuz ve sağlayıcı client'ı değişmeyecek —
// dikişin doğru çizildiğinin testi bu.
//
// Fonksiyon saf tutuluyor (log yok, saat yok, veritabanı yok) ki her dalı tek satırlık
// bir birim testiyle sabitlenebilsin.

// classify, gönderim sonucunu veritabanına yazılacak hâle çevirir.
func classify(
	notification *domain.Notification,
	resp provider.SendResponse,
	sendErr error,
	retryDelay time.Duration,
	elapsed time.Duration,
) store.NotificationResult {
	result := store.NotificationResult{
		ID:         notification.ID,
		LeaseToken: *notification.LeaseToken,
		DurationMS: durationMS(elapsed),
	}

	var decodeErr *provider.DecodeError

	switch {
	case sendErr == nil:
		return accepted(result, resp)

	case errors.As(sendErr, &decodeErr):
		// 2xx geldi, yalnızca zarf okunamadı: sağlayıcı mesajı KABUL ETTİ.
		//
		// DecodeError yalnızca başarılı durum kodlarında üretilir, bu yüzden burayı
		// retrying yapmak aynı mesajı tekrar göndermek olurdu — gövdesi JSON olmayan
		// bir sağlayıcıda her bildirim max_attempts kez giderdi. Gövdesiz 2xx ile
		// aynı muamele: gönderildi sayılır, mesaj kimliği boş kalır.
		return accepted(result, provider.SendResponse{StatusCode: resp.StatusCode})

	default:
		return rejected(result, notification, resp, sendErr, retryDelay)
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

// rejected, başarısız gönderimin sonucunu kurar.
//
// attempt_count claim anında artırıldığı için buradaki karşılaştırma "bu denemeden
// sonra hak kaldı mı" sorusunu cevaplıyor.
func rejected(
	result store.NotificationResult,
	notification *domain.Notification,
	resp provider.SendResponse,
	sendErr error,
	retryDelay time.Duration,
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

	if notification.AttemptCount >= notification.MaxAttempts {
		result.Status = domain.StatusFailed

		return result
	}

	result.Status = domain.StatusRetrying
	result.RetryAfter = retryDelay

	return result
}
