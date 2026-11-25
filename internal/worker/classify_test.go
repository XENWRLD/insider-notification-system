package worker

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
)

// testPolicy, sınıflandırma testlerinin gecikme ayarları. Tam değerler seçildi ki
// iddialar aralık değil eşitlik olabilsin.
func testPolicy() retryPolicy {
	return retryPolicy{
		BaseDelay:    2 * time.Second,
		MaxDelay:     5 * time.Minute,
		RequeueDelay: 60 * time.Second, // 2 × 30 sn BreakerOpenTimeout
	}
}

// statusError, verilen kodla bir sağlayıcı hatası üretir.
func statusError(code int, retryAfter time.Duration) error {
	return &provider.StatusError{StatusCode: code, RetryAfter: retryAfter, Body: "gövde"}
}

// classifyWith, tek satırlık iddialar için sarmalayıcı.
func classifyWith(
	notification *domain.Notification, sendErr error, state breaker.State,
) decision {
	return classify(notification, provider.SendResponse{}, sendErr, testPolicy(), 12*time.Millisecond, state)
}

// TestClassifyMarksSentOnAcceptedResponse, kabul edilen gönderimin sent yazıldığını
// doğrular.
func TestClassifyMarksSentOnAcceptedResponse(t *testing.T) {
	t.Parallel()

	notification := newTestNotification(domain.ChannelSMS, 1)

	outcome := classify(notification, acceptedResponse(), nil, testPolicy(),
		25*time.Millisecond, breaker.StateClosed)

	require.Equal(t, actionWriteResult, outcome.action)
	require.Equal(t, domain.StatusSent, outcome.result.Status)
	require.Equal(t, notification.ID, outcome.result.ID)
	require.NotNil(t, outcome.result.ProviderMessageID)
	require.Equal(t, 25, outcome.result.DurationMS)
	require.Nil(t, outcome.result.LastError)
}

// TestClassifyLeavesMessageIDNilWhenProviderOmitsIt, gövdesiz 2xx'te mesaj kimliğinin
// NULL kaldığını doğrular: boş string "kimlik aldık ama boş" izlenimi verirdi.
func TestClassifyLeavesMessageIDNilWhenProviderOmitsIt(t *testing.T) {
	t.Parallel()

	outcome := classify(newTestNotification(domain.ChannelSMS, 1),
		provider.SendResponse{StatusCode: http.StatusOK}, nil, testPolicy(), 0, breaker.StateClosed)

	require.Equal(t, domain.StatusSent, outcome.result.Status)
	require.Nil(t, outcome.result.ProviderMessageID)
	require.NotNil(t, outcome.result.StatusCode)
	require.Equal(t, http.StatusOK, *outcome.result.StatusCode)
}

// TestClassifyTreatsDecodeErrorAsSent, 2xx + çözülemeyen gövdenin sent sayıldığını
// doğrular.
//
// DecodeError yalnızca başarılı durum kodlarında üretiliyor, yani sağlayıcı mesajı
// KABUL ETTİ. retrying yapmak aynı mesajı tekrar göndermek olurdu.
func TestClassifyTreatsDecodeErrorAsSent(t *testing.T) {
	t.Parallel()

	decodeErr := &provider.DecodeError{Err: errors.New("bozuk json"), Body: "<html>"}

	outcome := classify(newTestNotification(domain.ChannelSMS, 1),
		provider.SendResponse{StatusCode: http.StatusAccepted}, decodeErr, testPolicy(), 0,
		breaker.StateClosed)

	require.Equal(t, actionWriteResult, outcome.action)
	require.Equal(t, domain.StatusSent, outcome.result.Status)
	require.Nil(t, outcome.result.ProviderMessageID, "zarf okunamadı, kimlik yok")
	require.Nil(t, outcome.result.LastError, "hata değil: sağlayıcı kabul etti")
}

// TestClassifyFailsImmediatelyOnBadRequest, 400/422'nin hak kalsa bile hemen failed
// olduğunu doğrular: mesajın kendisi hatalı, aynı baytları 5 kez göndermek anlamsız.
func TestClassifyFailsImmediatelyOnBadRequest(t *testing.T) {
	t.Parallel()

	for _, code := range []int{http.StatusBadRequest, http.StatusUnprocessableEntity} {
		// attempt 1 / max 5: hak bol.
		outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1), statusError(code, 0),
			breaker.StateClosed)

		require.Equal(t, actionWriteResult, outcome.action, "kod %d", code)
		require.Equal(t, domain.StatusFailed, outcome.result.Status, "kod %d", code)
		require.Zero(t, outcome.result.RetryAfter, "kod %d: yeniden denenmemeli", code)
		require.Equal(t, code, *outcome.result.StatusCode)
		require.NotNil(t, outcome.result.LastError)
	}
}

// TestClassifyFailsOnUnrecognizedClientError, tanınmayan 4xx'lerin de failed olduğunu
// doğrular: bilinmeyen bir kodu kanal arızası saymak tek bir tuhaf yanıtın bütün kanalı
// durdurmasına yol açardı.
func TestClassifyFailsOnUnrecognizedClientError(t *testing.T) {
	t.Parallel()

	for _, code := range []int{http.StatusConflict, http.StatusRequestEntityTooLarge} {
		outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1), statusError(code, 0),
			breaker.StateClosed)

		require.Equal(t, domain.StatusFailed, outcome.result.Status, "kod %d", code)
	}
}

// TestClassifyCountsServerErrorWhileBreakerClosed, devre kapalıyken 5xx'in sıradan bir
// geçici hata gibi sayıldığını doğrular — ZEHİRLİ MESAJ açığının kapandığı yer.
//
// Denemeyi koşulsuz saymasaydık: sağlayıcı yalnızca tek bir mesaja hep 500 dönerse
// araya giren başarılar ardışık sayacı sıfırlar, devre hiç açılmaz ve o mesaj
// attempt_count hiç artmadığı için sonsuza kadar döner.
func TestClassifyCountsServerErrorWhileBreakerClosed(t *testing.T) {
	t.Parallel()

	for _, code := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable} {
		outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1), statusError(code, 0),
			breaker.StateClosed)

		require.Equal(t, actionWriteResult, outcome.action, "kod %d: deneme sayılmalı", code)
		require.Equal(t, domain.StatusRetrying, outcome.result.Status, "kod %d", code)
		require.Positive(t, outcome.result.RetryAfter, "kod %d: backoff uygulanmalı", code)
		require.LessOrEqual(t, outcome.result.RetryAfter, testPolicy().BaseDelay,
			"kod %d: attempt 1'de pencere BaseDelay", code)
	}
}

// TestClassifyRequeuesServerErrorWhenBreakerTookOver, devre devraldığında 5xx'in
// denemeyi harcamadığını ve 2 × BreakerOpenTimeout ertelendiğini doğrular.
func TestClassifyRequeuesServerErrorWhenBreakerTookOver(t *testing.T) {
	t.Parallel()

	for _, state := range []breaker.State{breaker.StateOpen, breaker.StateHalfOpen} {
		outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1),
			statusError(http.StatusServiceUnavailable, 0), state)

		require.Equal(t, actionRequeue, outcome.action, "durum %s", state)
		require.Equal(t, testPolicy().RequeueDelay, outcome.requeueDelay, "durum %s", state)
		require.NotEmpty(t, outcome.requeueReason, "durum %s: last_error tek tutanak", state)
	}
}

// TestClassifyCountsConfigErrorWhileBreakerClosed, 401/403/404'ün devre kapalıyken
// normal retry gördüğünü doğrular.
func TestClassifyCountsConfigErrorWhileBreakerClosed(t *testing.T) {
	t.Parallel()

	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1), statusError(code, 0),
			breaker.StateClosed)

		require.Equal(t, actionWriteResult, outcome.action, "kod %d", code)
		require.Equal(t, domain.StatusRetrying, outcome.result.Status,
			"kod %d: yapılandırma hatası failed OLMAMALI, mesajlar geçerli", code)
	}
}

// TestClassifyRequeuesConfigErrorWhenBreakerTookOver, 401/403/404'te devre
// devraldığında deneme harcanmadığını doğrular: anahtar rotasyonu sırasında kuyruktaki
// mesajlar ölmemeli.
func TestClassifyRequeuesConfigErrorWhenBreakerTookOver(t *testing.T) {
	t.Parallel()

	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1), statusError(code, 0),
			breaker.StateOpen)

		require.Equal(t, actionRequeue, outcome.action, "kod %d", code)
		require.Equal(t, testPolicy().RequeueDelay, outcome.requeueDelay, "kod %d", code)
	}
}

// TestClassifyRequeuesRateLimitWithRetryAfter, 429'un sağlayıcının rehberine uyduğunu
// ve denemeyi harcamadığını doğrular.
func TestClassifyRequeuesRateLimitWithRetryAfter(t *testing.T) {
	t.Parallel()

	outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1),
		statusError(http.StatusTooManyRequests, 30*time.Second), breaker.StateClosed)

	require.Equal(t, actionRequeue, outcome.action, "429 deneme harcamamalı")
	require.Equal(t, 30*time.Second, outcome.requeueDelay)
	require.Equal(t, breakerIgnored, breakerOutcomeFor(statusError(http.StatusTooManyRequests, 0)),
		"429 devrede sayılmamalı: başarı sayılsaydı ardışık hata sayacını sıfırlardı")
}

// TestClassifyFloorsRateLimitDelayWhenHeaderMissing, Retry-After yokken gecikmenin
// sıfıra yakın olamayacağını doğrular.
//
// attempt_count geri alındığı için backoffWindow BÜYÜMÜYOR ve jitter tek başına
// neredeyse sıfır bir değer üretebilirdi: 429 fırtınasında satır beklemeden dönerdi.
func TestClassifyFloorsRateLimitDelayWhenHeaderMissing(t *testing.T) {
	t.Parallel()

	policy := testPolicy()

	for range 50 {
		outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1),
			statusError(http.StatusTooManyRequests, 0), breaker.StateClosed)

		require.GreaterOrEqual(t, outcome.requeueDelay, policy.BaseDelay)
		require.LessOrEqual(t, outcome.requeueDelay, 2*policy.BaseDelay)
	}
}

// TestClassifyCapsRetryAfterAtMaxDelay, sağlayıcının aşırı uzun rehberinin
// tavanlandığını doğrular: "Retry-After: 24h" satırı bir gün kaybetmemeli.
func TestClassifyCapsRetryAfterAtMaxDelay(t *testing.T) {
	t.Parallel()

	outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1),
		statusError(http.StatusTooManyRequests, 24*time.Hour), breaker.StateClosed)

	require.Equal(t, testPolicy().MaxDelay, outcome.requeueDelay)
}

// TestClassifyConnectionErrorFollowsBreakerState, bağlantı hatasının devre durumuna
// göre davrandığını doğrular.
func TestClassifyConnectionErrorFollowsBreakerState(t *testing.T) {
	t.Parallel()

	connErr := &provider.TransportError{Err: errors.New("connection refused")}

	closed := classifyWith(newTestNotification(domain.ChannelSMS, 1), connErr, breaker.StateClosed)
	require.Equal(t, actionWriteResult, closed.action)
	require.Equal(t, domain.StatusRetrying, closed.result.Status)
	require.Nil(t, closed.result.StatusCode, "yanıt gelmedi, durum kodu olmamalı")

	open := classifyWith(newTestNotification(domain.ChannelSMS, 1), connErr, breaker.StateOpen)
	require.Equal(t, actionRequeue, open.action)
}

// TestClassifyCountsTimeoutRegardlessOfBreakerState, timeout'un her devre durumunda
// deneme harcadığını doğrular.
//
// Timeout BELİRSİZDİR: mesaj gitmiş olabilir. Saymazsak aynı satır her yarı açık
// sondada yeniden seçilip sınırsız gönderilebilirdi; sayınca max_attempts'ta durur ve
// at-least-once'ın üst sınırı korunur.
func TestClassifyCountsTimeoutRegardlessOfBreakerState(t *testing.T) {
	t.Parallel()

	timeoutErr := &provider.TransportError{Err: context.DeadlineExceeded}

	for _, state := range []breaker.State{breaker.StateClosed, breaker.StateHalfOpen, breaker.StateOpen} {
		outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1), timeoutErr, state)

		require.Equal(t, actionWriteResult, outcome.action,
			"durum %s: timeout'ta deneme SAYILMALI", state)
		require.Equal(t, domain.StatusRetrying, outcome.result.Status, "durum %s", state)
	}
}

// TestClassifyRaisesTimeoutDelayWhenBreakerTookOver, devre devraldığında timeout
// gecikmesinin yükseltildiğini doğrular.
//
// Yoksa: high öncelikli bir satır timeout alır, devre açılır, satıra 2 sn backoff
// yazılır; yarı açıkta önceliği yüzünden YİNE sondaya çıkar ve 5 denemesi bitene kadar
// kanalın tek sonda yuvasını tek başına tutar.
func TestClassifyRaisesTimeoutDelayWhenBreakerTookOver(t *testing.T) {
	t.Parallel()

	timeoutErr := &provider.TransportError{Err: context.DeadlineExceeded}
	policy := testPolicy()

	open := classifyWith(newTestNotification(domain.ChannelSMS, 1), timeoutErr, breaker.StateOpen)
	require.GreaterOrEqual(t, open.result.RetryAfter, policy.RequeueDelay)

	closed := classifyWith(newTestNotification(domain.ChannelSMS, 1), timeoutErr, breaker.StateClosed)
	require.LessOrEqual(t, closed.result.RetryAfter, policy.BaseDelay,
		"devre kapalıyken yalnızca backoff penceresi uygulanmalı")
}

// TestClassifyRequeuesWhenBreakerRefusedCall, devre kesicinin çağrıyı hiç yapmadığı
// durumda satırın denenmeden döndüğünü doğrular.
func TestClassifyRequeuesWhenBreakerRefusedCall(t *testing.T) {
	t.Parallel()

	outcome := classifyWith(newTestNotification(domain.ChannelSMS, 1),
		breaker.ErrOpen, breaker.StateOpen)

	require.Equal(t, actionRequeue, outcome.action)
	require.Equal(t, testPolicy().RequeueDelay, outcome.requeueDelay)
	require.Contains(t, outcome.requeueReason, "devre kesici")
}

// TestClassifyFailsWhenAttemptsExhausted, hakkı tükenen satırın failed olduğunu
// doğrular.
func TestClassifyFailsWhenAttemptsExhausted(t *testing.T) {
	t.Parallel()

	notification := newTestNotification(domain.ChannelSMS, domain.DefaultMaxAttempts)

	outcome := classifyWith(notification, &provider.TransportError{Err: context.DeadlineExceeded},
		breaker.StateClosed)

	require.Equal(t, domain.StatusFailed, outcome.result.Status)
	require.Zero(t, outcome.result.RetryAfter)
}

// TestBackoffWindowGrowsExponentiallyAndCaps, backoff penceresini TAM değerle
// doğrular (jitter uygulanmamış saf fonksiyon).
func TestBackoffWindowGrowsExponentiallyAndCaps(t *testing.T) {
	t.Parallel()

	const (
		base     = 2 * time.Second
		maxDelay = 5 * time.Minute
	)

	expected := map[int]time.Duration{
		0:  2 * time.Second, // savunma: attempt < 1 → 1 gibi davran
		1:  2 * time.Second,
		2:  4 * time.Second,
		3:  8 * time.Second,
		4:  16 * time.Second,
		5:  32 * time.Second,
		6:  64 * time.Second,
		7:  128 * time.Second,
		8:  256 * time.Second,
		9:  maxDelay, // 512 sn > 5 dk → tavan
		50: maxDelay,
	}

	for attempt, want := range expected {
		require.Equal(t, want, backoffWindow(attempt, base, maxDelay), "deneme %d", attempt)
	}
}

// TestBreakerOutcomeForMapsEachErrorKind, devre eşlemesini tablo hâlinde sabitler.
func TestBreakerOutcomeForMapsEachErrorKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want breakerOutcome
	}{
		{"2xx", nil, breakerHealthy},
		{"decode error (2xx, zarf bozuk)", &provider.DecodeError{Err: errors.New("x")}, breakerHealthy},
		{"400", statusError(http.StatusBadRequest, 0), breakerHealthy},
		{"422", statusError(http.StatusUnprocessableEntity, 0), breakerHealthy},
		{"429", statusError(http.StatusTooManyRequests, 0), breakerIgnored},
		{"401", statusError(http.StatusUnauthorized, 0), breakerFailed},
		{"403", statusError(http.StatusForbidden, 0), breakerFailed},
		{"404", statusError(http.StatusNotFound, 0), breakerFailed},
		{"500", statusError(http.StatusInternalServerError, 0), breakerFailed},
		{"503", statusError(http.StatusServiceUnavailable, 0), breakerFailed},
		{"transport", &provider.TransportError{Err: errors.New("refused")}, breakerFailed},
		{"timeout", &provider.TransportError{Err: context.DeadlineExceeded}, breakerFailed},
	}

	for _, test := range tests {
		require.Equal(t, test.want, breakerOutcomeFor(test.err), test.name)

		// BreakerIsFailure, fabrikaya geçen politika: yalnızca breakerFailed true.
		require.Equal(t, test.want == breakerFailed, BreakerIsFailure(test.err), test.name)
	}
}

// TestIsTimeoutRecognizesRealClientTimeout, timeout tanımasının VARSAYIMA değil
// ölçüme dayandığını doğrular: gerçek bir httptest sunucusu ve gerçek provider.Client
// ile üretilmiş hata üzerinde sınanıyor.
func TestIsTimeoutRecognizesRealClientTimeout(t *testing.T) {
	t.Parallel()

	// Handler SINIRLI bir gecikme taklit ediyor, ctx.Done() beklemiyor: httptest
	// Close() uçan istekleri bekliyor ve sunucunun istemci kopuşunu fark etmesi
	// platforma göre gecikebiliyor — testi askıda bırakırdı. Gecikme istemci zaman
	// aşımının (50 ms) belirgin üstünde.
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer server.Close()

	client, err := provider.New(provider.Config{
		URL:     server.URL,
		Timeout: 50 * time.Millisecond,
	}, discardLogger())
	require.NoError(t, err)

	_, sendErr := client.Send(context.Background(), provider.SendRequest{
		To: "+905551234567", Channel: "sms", Content: "merhaba",
	})

	require.Error(t, sendErr)
	require.True(t, isTimeout(sendErr), "gerçek client timeout'u tanınmalı: %v", sendErr)

	// Zaman aşımı OLMAYAN bir hata yanlışlıkla timeout sayılmamalı.
	require.False(t, isTimeout(statusError(http.StatusInternalServerError, 0)))
	require.False(t, isTimeout(&provider.TransportError{Err: errors.New("dns yok")}))
}

// TestIsTimeoutRecognizesNetError, net.Error arayüzünden gelen zaman aşımını da
// tanıdığını doğrular (context.DeadlineExceeded dışındaki yol).
func TestIsTimeoutRecognizesNetError(t *testing.T) {
	t.Parallel()

	var netErr net.Error = &net.OpError{Op: "dial", Err: timeoutStub{}}

	require.True(t, isTimeout(&provider.TransportError{Err: netErr}))
}

// timeoutStub, net.Error'ın Timeout() true dönen en küçük hâli.
type timeoutStub struct{}

func (timeoutStub) Error() string { return "i/o timeout" }
func (timeoutStub) Timeout() bool { return true }
func (timeoutStub) Temporary() bool {
	return true
}
