package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// Bu testler dış altyapı gerektirmez: httptest ile sahte bir sağlayıcı kurulur,
// bu yüzden -short altında da koşarlar.

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestClient, verilen handler'a bağlı bir client döner.
func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client, err := New(Config{
		URL:                 server.URL,
		Timeout:             2 * time.Second,
		MaxIdleConnsPerHost: 4,
	}, discardLogger())
	require.NoError(t, err)

	return client
}

// newBlockingServer, isteklere hiç yanıt vermeyen bir sunucu kurar: zaman aşımı ve
// iptal yollarını test etmek için.
//
// Handler'ı serbest bırakan kanal, server.Close'dan ÖNCE kapanmak zorunda: Close
// süren handler'ları bekler, handler da kanalı beklerse test kilitlenir. t.Cleanup
// LIFO çalıştığı için serbest bırakma ikinci sırada kaydediliyor.
func newBlockingServer(t *testing.T) (*httptest.Server, chan struct{}) {
	t.Helper()

	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))

	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	return server, release
}

// testRequest, testlerde kullanılan örnek istek.
func testRequest() SendRequest {
	return SendRequest{
		To:             "+905551234567",
		Channel:        "sms",
		Content:        "Merhaba",
		IdempotencyKey: "11111111-1111-1111-1111-111111111111",
		CorrelationID:  "corr-1",
	}
}

// acceptedBody, sağlayıcının başarılı yanıt gövdesi.
func acceptedBody(messageID string) string {
	return `{"messageId":"` + messageID + `","status":"accepted","timestamp":"2026-11-24T10:00:00Z"}`
}

// TestNewRejectsInvalidConfig, eksik ayarın boot'ta yakalandığını doğrular.
func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := New(Config{URL: "   ", Timeout: time.Second}, discardLogger())
	require.Error(t, err)

	_, err = New(Config{URL: "http://localhost", Timeout: 0}, discardLogger())
	require.Error(t, err, "zaman aşımı verilmeden client kurulmamalı")
}

// TestSendAcceptsProviderAccepted, 202 yanıtının zarfla birlikte çözüldüğünü doğrular.
func TestSendAcceptsProviderAccepted(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(acceptedBody("msg-1")))
	})

	got, err := client.Send(context.Background(), testRequest())

	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, got.StatusCode)
	require.Equal(t, "msg-1", got.MessageID)
	require.Equal(t, "accepted", got.Status)
	require.Equal(t, 2026, got.Timestamp.Year())
}

// TestSendAcceptsAnySuccessStatus, başarının 202'ye kilitlenmediğini doğrular.
//
// webhook.site 200 döner; katı bir 202 kuralı gerçek sağlayıcı demosunun tamamını
// retrying yapardı.
func TestSendAcceptsAnySuccessStatus(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusNoContent} {
		client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		})

		got, err := client.Send(context.Background(), testRequest())

		require.NoError(t, err, "durum %d başarı sayılmalı", status)
		require.Equal(t, status, got.StatusCode)
	}
}

// TestSendTreatsEmptyBodyAsAccepted, gövdesiz 2xx'in hata olmadığını doğrular:
// mesaj kabul edildi, yalnızca kimliği öğrenemedik.
func TestSendTreatsEmptyBodyAsAccepted(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	got, err := client.Send(context.Background(), testRequest())

	require.NoError(t, err)
	require.Empty(t, got.MessageID, "kimlik yok ama gönderim başarılı")
}

// TestSendSendsExactContractBody, gövdenin docs/PLAN.md §5 sözleşmesine birebir
// uyduğunu doğrular: üç alan, fazlası yok.
func TestSendSendsExactContractBody(t *testing.T) {
	t.Parallel()

	var received map[string]any

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&received))

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(acceptedBody("msg-1")))
	})

	_, err := client.Send(context.Background(), testRequest())
	require.NoError(t, err)

	require.Equal(t, map[string]any{
		"to":      "+905551234567",
		"channel": "sms",
		"content": "Merhaba",
	}, received, "gövde sözleşmesi tam olarak üç alan olmalı")
}

// TestSendSendsIdempotencyAndCorrelationHeaders, kimliklerin gövde yerine header'da
// gittiğini doğrular. Tekrar denemeler aynı anahtarı taşır → sağlayıcı tekilleştirebilir.
func TestSendSendsIdempotencyAndCorrelationHeaders(t *testing.T) {
	t.Parallel()

	var idempotencyKey, correlationID string

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		idempotencyKey = r.Header.Get("Idempotency-Key")
		correlationID = r.Header.Get("X-Correlation-ID")

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(acceptedBody("msg-1")))
	})

	_, err := client.Send(context.Background(), testRequest())
	require.NoError(t, err)

	require.Equal(t, "11111111-1111-1111-1111-111111111111", idempotencyKey)
	require.Equal(t, "corr-1", correlationID)
}

// TestSendReturnsStatusErrorForClientError, 4xx'in olgu olarak döndüğünü doğrular.
//
// Client "bu kalıcı hata" demiyor; yalnızca kodu bildiriyor. Kalıcılık kararı Adım 8'de.
func TestSendReturnsStatusErrorForClientError(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid recipient"}`))
	})

	_, err := client.Send(context.Background(), testRequest())

	var statusErr *StatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, http.StatusBadRequest, statusErr.StatusCode)
	require.Contains(t, statusErr.Body, "invalid recipient")
	require.Zero(t, statusErr.RetryAfter)
	require.Contains(t, err.Error(), "400")
}

// TestSendReturnsStatusErrorForServerError, 5xx'in de aynı tipte döndüğünü doğrular.
func TestSendReturnsStatusErrorForServerError(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	_, err := client.Send(context.Background(), testRequest())

	var statusErr *StatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, http.StatusInternalServerError, statusErr.StatusCode)
}

// TestSendParsesRetryAfterSeconds, 429'daki saniye biçimli Retry-After'ın
// çözüldüğünü doğrular.
func TestSendParsesRetryAfterSeconds(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	_, err := client.Send(context.Background(), testRequest())

	var statusErr *StatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, http.StatusTooManyRequests, statusErr.StatusCode)
	require.Equal(t, 30*time.Second, statusErr.RetryAfter)
}

// TestSendParsesRetryAfterHTTPDate, HTTP-date biçiminin de desteklendiğini doğrular.
//
// RFC 9110 iki biçim tanımlıyor; yalnızca birini desteklemek diğerinde sessizce 0
// döndürür ve sağlayıcının söylediği bekleme yok sayılırdı.
func TestSendParsesRetryAfterHTTPDate(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", time.Now().Add(time.Minute).UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusTooManyRequests)
	})

	_, err := client.Send(context.Background(), testRequest())

	var statusErr *StatusError
	require.ErrorAs(t, err, &statusErr)
	require.InDelta(t, time.Minute, statusErr.RetryAfter, float64(2*time.Second))
}

// TestParseRetryAfterHandlesEdgeCases, header ayrıştırmanın uç durumlarını sabitler.
func TestParseRetryAfterHandlesEdgeCases(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{name: "boş", header: "", want: 0},
		{name: "saniye", header: "120", want: 2 * time.Minute},
		{name: "sıfır", header: "0", want: 0},
		{name: "negatif", header: "-5", want: 0},
		{name: "anlamsız", header: "yarın", want: 0},
		{name: "geçmiş tarih", header: "Wed, 21 Oct 2015 07:28:00 GMT", want: 0},
		{
			name:   "gelecek tarih",
			header: now.Add(90 * time.Second).Format(http.TimeFormat),
			want:   90 * time.Second,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.want, parseRetryAfter(test.header, now))
		})
	}
}

// TestSendReturnsTransportErrorOnTimeout, yanıt gelmediğinde TransportError
// döndüğünü ve sarmalayıcıdan deadline hatasının geçtiğini doğrular.
func TestSendReturnsTransportErrorOnTimeout(t *testing.T) {
	t.Parallel()

	server, _ := newBlockingServer(t)

	client, err := New(Config{URL: server.URL, Timeout: 100 * time.Millisecond}, discardLogger())
	require.NoError(t, err)

	_, err = client.Send(context.Background(), testRequest())

	var transportErr *TransportError
	require.ErrorAs(t, err, &transportErr)
	require.Contains(t, err.Error(), "ulaşılamadı")
}

// TestSendReturnsTransportErrorOnUnreachableHost, bağlantı kurulamadığında da aynı
// tipin döndüğünü doğrular.
func TestSendReturnsTransportErrorOnUnreachableHost(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close() // adres artık dinlenmiyor

	client, err := New(Config{URL: url, Timeout: time.Second}, discardLogger())
	require.NoError(t, err)

	_, err = client.Send(context.Background(), testRequest())

	var transportErr *TransportError
	require.ErrorAs(t, err, &transportErr)
}

// TestSendHonorsContextCancellation, çağıranın context'inin isteği kestiğini doğrular.
func TestSendHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	server, _ := newBlockingServer(t)

	client, err := New(Config{URL: server.URL, Timeout: time.Minute}, discardLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = client.Send(ctx, testRequest())

	require.ErrorIs(t, err, context.DeadlineExceeded,
		"Unwrap sayesinde deadline hatası sarmalayıcıdan geçmeli")
}

// TestSendReturnsDecodeErrorOnGarbageBody, 2xx ama bozuk zarfın ayrı bir tip
// döndürdüğünü doğrular: mesaj gitmiş olabilir, "gönderilemedi" demek yanlış olurdu.
func TestSendReturnsDecodeErrorOnGarbageBody(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("<html>bu JSON değil</html>"))
	})

	_, err := client.Send(context.Background(), testRequest())

	var decodeErr *DecodeError
	require.ErrorAs(t, err, &decodeErr)
	require.Contains(t, decodeErr.Body, "JSON değil")
}

// TestSendTruncatesHugeErrorBody, dev bir hata gövdesinin hata mesajını şişirmediğini
// doğrular.
func TestSendTruncatesHugeErrorBody(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)

		for range 1000 {
			_, _ = w.Write([]byte("0123456789"))
		}
	})

	_, err := client.Send(context.Background(), testRequest())

	var statusErr *StatusError
	require.ErrorAs(t, err, &statusErr)
	require.LessOrEqual(t, len(statusErr.Body), errorBodyExcerpt+len("…"))
}

// TestSendReusesConnections, keep-alive'ın çalıştığını doğrular: 100/s hedefinde her
// gönderimde yeni TCP el sıkışması yapmak hedefi kaçırırdı.
func TestSendReusesConnections(t *testing.T) {
	t.Parallel()

	var (
		mu          sync.Mutex
		remoteAddrs []string
	)

	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remoteAddrs = append(remoteAddrs, r.RemoteAddr)
		mu.Unlock()

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(acceptedBody("msg-1")))
	})

	for range 5 {
		_, err := client.Send(context.Background(), testRequest())
		require.NoError(t, err)
	}

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, remoteAddrs, 5)
	for _, addr := range remoteAddrs {
		require.Equal(t, remoteAddrs[0], addr, "aynı bağlantı yeniden kullanılmalıydı")
	}
}

// TestErrorTypesUnwrapToCause, sarmalayıcıların errors.Is zincirini kırmadığını
// doğrular: Adım 8 sınıflandırması buna dayanacak.
func TestErrorTypesUnwrapToCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("kök sebep")

	require.ErrorIs(t, &TransportError{Err: cause}, cause)
	require.ErrorIs(t, &DecodeError{Err: cause}, cause)
}

// TestSendKeepsErrorBodyWritableToDatabase, hata gövdesi kesitinin VERİTABANINA
// yazılabilir kaldığını doğrular.
//
// Regresyon: kesit `trimmed[:512]` ile BAYT olarak kesiliyordu ve 512. bayt bir rune
// sınırına denk gelmediğinde çok baytlı karakter ortadan bölünüp geçersiz UTF-8
// üretiyordu. O metin StatusError.Error() üzerinden worker'a, oradan
// notifications.last_error ve notification_attempts.error kolonlarına gidiyor; Postgres
// geçersiz bayt dizisini reddettiği için sonuç yazımının TAMAMI düşüyor, satır lease
// süresi boyunca processing'de kalıp reaper tarafından yeniden gönderiliyordu.
func TestSendKeepsErrorBodyWritableToDatabase(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)

		// "ş" 2 bayt: 512. bayt rune'un ortasına düşer. Araya NUL da konuyor —
		// Postgres onu da text kolonunda kabul etmiyor.
		_, _ = w.Write([]byte("x\x00"))
		for range 400 {
			_, _ = w.Write([]byte("ş"))
		}
	})

	_, err := client.Send(context.Background(), testRequest())

	var statusErr *StatusError
	require.ErrorAs(t, err, &statusErr)

	require.True(t, utf8.ValidString(statusErr.Body),
		"kesit geçerli UTF-8 olmalı, yoksa last_error yazımı düşer")
	require.NotContains(t, statusErr.Body, "\x00",
		"NUL baytı ayıklanmalı: Postgres text kolonunda kabul etmiyor")
	require.True(t, utf8.ValidString(statusErr.Error()),
		"hata mesajının tamamı da geçerli olmalı: veritabanına giden metin bu")
	require.LessOrEqual(t, len(statusErr.Body), errorBodyExcerpt+len("…"))
}

// TestSendDoesNotFollowRedirects, yönlendirmenin TAKİP EDİLMEDİĞİNİ doğrular.
//
// Regresyon: Go'nun varsayılanı 10 hop'a kadar takip etmek ve 301/302/303'te isteği
// GÖVDESİZ GET'e dönüştürmek. Sonuç: PROVIDER_URL yönlendirme dönerse mesaj hiç
// iletilmiyor ama hedefteki 2xx başarı sayılıp satır `sent` yazılıyordu — classify
// bunu göremiyor, çünkü Send başarı bildiriyor. Ayrıca hop hedefi keyfi ve özel
// header'lar (istemci kontrolündeki X-Correlation-ID dahil) oraya da taşınıyordu.
func TestSendDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	var targetHits atomic.Int64

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"messageId":"hedeften"}`))
	}))
	defer target.Close()

	client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL+"/ic-servis")
		w.WriteHeader(http.StatusFound)
	})

	resp, err := client.Send(context.Background(), testRequest())

	require.Zero(t, targetHits.Load(), "yönlendirme hedefine İSTEK GİTMEMELİ")
	require.Empty(t, resp.MessageID, "hedefin mesaj kimliği kabul edilmemeli")

	var statusErr *StatusError
	require.ErrorAs(t, err, &statusErr, "3xx hata olarak dönmeli: %v", err)
	require.Equal(t, http.StatusFound, statusErr.StatusCode)
}
