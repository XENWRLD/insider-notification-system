package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
)

const testRetryDelay = 5 * time.Second

// TestClassifyMarksSentOnAcceptedResponse, kabul edilen gönderimin sent yazıldığını
// doğrular.
func TestClassifyMarksSentOnAcceptedResponse(t *testing.T) {
	t.Parallel()

	notification := newTestNotification(domain.ChannelSMS, 1)

	result := classify(notification, acceptedResponse(), nil, testRetryDelay, 42*time.Millisecond)

	require.Equal(t, domain.StatusSent, result.Status)
	require.Equal(t, notification.ID, result.ID)
	require.Equal(t, *notification.LeaseToken, result.LeaseToken)
	require.Equal(t, ptr(202), result.StatusCode)
	require.Equal(t, ptr("msg-1"), result.ProviderMessageID)
	require.Nil(t, result.LastError)
	require.Equal(t, 42, result.DurationMS)
	require.Zero(t, result.RetryAfter)
}

// TestClassifyLeavesMessageIDNilWhenProviderOmitsIt, gövdesiz 2xx'in sent sayıldığını
// ve kimliğin NULL kaldığını doğrular.
//
// Boş string yazmak "kimlik aldık ama boş" izlenimi verirdi.
func TestClassifyLeavesMessageIDNilWhenProviderOmitsIt(t *testing.T) {
	t.Parallel()

	result := classify(
		newTestNotification(domain.ChannelSMS, 1),
		provider.SendResponse{StatusCode: http.StatusOK},
		nil, testRetryDelay, time.Millisecond)

	require.Equal(t, domain.StatusSent, result.Status)
	require.Nil(t, result.ProviderMessageID)
	require.Equal(t, ptr(http.StatusOK), result.StatusCode)
}

// TestClassifyTreatsDecodeErrorAsSent, çözülemeyen 2xx gövdesinin sent sayıldığını
// doğrular.
//
// DecodeError yalnızca BAŞARILI durum kodlarında üretilir: sağlayıcı mesajı kabul
// etti, biz yalnızca zarfı okuyamadık. Bunu retrying yapmak aynı mesajı tekrar
// göndermek olurdu — gövdesi JSON olmayan bir sağlayıcıda her bildirim max_attempts
// kez giderdi ve webhook.site kotası tek seferde biterdi.
func TestClassifyTreatsDecodeErrorAsSent(t *testing.T) {
	t.Parallel()

	decodeErr := &provider.DecodeError{
		Err:  errors.New("invalid character '<'"),
		Body: "<html>not json</html>",
	}

	result := classify(
		newTestNotification(domain.ChannelSMS, 1),
		provider.SendResponse{StatusCode: http.StatusOK},
		decodeErr, testRetryDelay, time.Millisecond)

	require.Equal(t, domain.StatusSent, result.Status,
		"sağlayıcı kabul etti; yeniden denemek çift gönderim olurdu")
	require.Nil(t, result.ProviderMessageID, "kimlik okunamadı, NULL kalmalı")
	require.Equal(t, ptr(http.StatusOK), result.StatusCode)
	require.Nil(t, result.LastError, "gönderim başarısız değil")
	require.Zero(t, result.RetryAfter)
}

// TestClassifyRetriesWhenAttemptsRemain, geçici hatada işin kuyruğa döndüğünü doğrular.
func TestClassifyRetriesWhenAttemptsRemain(t *testing.T) {
	t.Parallel()

	notification := newTestNotification(domain.ChannelSMS, 1)
	statusErr := &provider.StatusError{StatusCode: http.StatusInternalServerError}

	result := classify(notification, provider.SendResponse{}, statusErr, testRetryDelay, time.Second)

	require.Equal(t, domain.StatusRetrying, result.Status)
	require.Equal(t, testRetryDelay, result.RetryAfter)
	require.Equal(t, ptr(http.StatusInternalServerError), result.StatusCode)
	require.NotNil(t, result.LastError)
	require.Contains(t, *result.LastError, "500")
	require.Nil(t, result.ProviderMessageID)
}

// TestClassifyFailsWhenAttemptsExhausted, hak tükenince failed yazıldığını doğrular.
//
// attempt_count claim anında artırıldığı için karşılaştırma "bu denemeden sonra hak
// kaldı mı" sorusunu cevaplıyor.
func TestClassifyFailsWhenAttemptsExhausted(t *testing.T) {
	t.Parallel()

	notification := newTestNotification(domain.ChannelSMS, domain.DefaultMaxAttempts)
	statusErr := &provider.StatusError{StatusCode: http.StatusInternalServerError}

	result := classify(notification, provider.SendResponse{}, statusErr, testRetryDelay, time.Second)

	require.Equal(t, domain.StatusFailed, result.Status)
	require.Zero(t, result.RetryAfter)
	require.NotNil(t, result.LastError)
}

// TestClassifyRecordsNoStatusCodeOnTransportError, yanıt hiç gelmediğinde durum
// kodunun NULL kaldığını doğrular.
func TestClassifyRecordsNoStatusCodeOnTransportError(t *testing.T) {
	t.Parallel()

	transportErr := &provider.TransportError{Err: context.DeadlineExceeded}

	result := classify(
		newTestNotification(domain.ChannelSMS, 1),
		provider.SendResponse{}, transportErr, testRetryDelay, 10*time.Second)

	require.Equal(t, domain.StatusRetrying, result.Status)
	require.Nil(t, result.StatusCode, "yanıt gelmediyse durum kodu yazılmamalı")
	require.NotNil(t, result.LastError)
}

// TestClassifyAppliesStep6RuleToEveryFailureKind, Adım 6'nın geçici kuralının tüm
// hata türlerine aynı şekilde uygulandığını doğrular.
//
// Adım 8 bu davranışı ayrıştıracak (400 → hemen failed, 429 → Retry-After, 5xx →
// exponential backoff). Test o değişikliğin bilinçli olmasını sağlıyor: kural
// değişince bu test kırmızı olur.
func TestClassifyAppliesStep6RuleToEveryFailureKind(t *testing.T) {
	t.Parallel()

	failures := []error{
		&provider.StatusError{StatusCode: http.StatusBadRequest},
		&provider.StatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: time.Minute},
		&provider.StatusError{StatusCode: http.StatusInternalServerError},
		&provider.TransportError{Err: errors.New("bağlantı reddedildi")},
	}

	for _, sendErr := range failures {
		result := classify(
			newTestNotification(domain.ChannelSMS, 1),
			provider.SendResponse{}, sendErr, testRetryDelay, time.Millisecond)

		require.Equal(t, domain.StatusRetrying, result.Status, "hata: %v", sendErr)
		require.Equal(t, testRetryDelay, result.RetryAfter,
			"Adım 6'da gecikme sabit; Retry-After henüz dikkate alınmıyor")
	}
}

// TestSendTreatsUndecodableSuccessAsSentWithSingleRequest, gerçek sağlayıcı
// client'ıyla uçtan uca doğrular: 200 + JSON olmayan gövde → sent ve sağlayıcıya
// YALNIZCA BİR istek gider.
//
// İstek sayısı asıl iddia: bunu retrying yapmak aynı mesajı tekrar göndermek olurdu.
func TestSendTreatsUndecodableSuccessAsSentWithSingleRequest(t *testing.T) {
	t.Parallel()

	var requests int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	}))
	t.Cleanup(server.Close)

	client, err := provider.New(provider.Config{
		URL:     server.URL,
		Timeout: 2 * time.Second,
	}, discardLogger())
	require.NoError(t, err)

	queue := &stubQueue{}
	engine := newTestEngine(t, testConfig(), queue, client)

	engine.handle(context.Background(), newTestNotification(domain.ChannelSMS, 1))

	results := queue.completedResults()
	require.Len(t, results, 1)
	require.Equal(t, domain.StatusSent, results[0].Status,
		"2xx gelmişse sağlayıcı kabul etmiştir")
	require.Nil(t, results[0].ProviderMessageID)
	require.Equal(t, 1, requests, "sağlayıcıya yalnızca bir istek gitmeliydi")
}

// ptr, test içinde işaretçi kurmak için.
func ptr[T any](v T) *T { return &v }
