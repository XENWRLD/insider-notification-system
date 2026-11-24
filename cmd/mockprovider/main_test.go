package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/config"
)

// newTestProvider, gecikmesiz bir sahte sağlayıcı kurar (testler beklemesin).
func newTestProvider(t *testing.T, cfg config.MockProviderConfig) *mockProvider {
	t.Helper()

	return newMockProvider(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// postSend, /send ucuna bir gönderim isteği atar.
func postSend(t *testing.T, provider *mockProvider, channel string) *httptest.ResponseRecorder {
	t.Helper()

	body := `{"to":"+905551234567","channel":"` + channel + `","content":"Merhaba"}`
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/send", strings.NewReader(body))
	rec := httptest.NewRecorder()

	provider.send(rec, req)

	return rec
}

// TestSendAcceptsByDefault, varsayılan ayarlarda her isteğin kabul edildiğini
// doğrular: hata oranları 0 olduğunda zar hiçbir dala düşmemeli.
func TestSendAcceptsByDefault(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{})

	for range 20 {
		rec := postSend(t, provider, "sms")
		require.Equal(t, http.StatusAccepted, rec.Code)
	}

	var payload map[string]string
	require.NoError(t, json.NewDecoder(postSend(t, provider, "sms").Body).Decode(&payload))

	require.NotEmpty(t, payload["messageId"], "sağlayıcı mesaj kimliği dönmeli")
	require.Equal(t, "accepted", payload["status"])
	_, err := time.Parse(time.RFC3339, payload["timestamp"])
	require.NoError(t, err, "timestamp RFC3339 olmalı")
}

// TestSendAlwaysFailsWhenRateIsOne, oran 1 olduğunda ilgili dalın her zaman
// seçildiğini doğrular. Zarın eşik mantığı böylece rastgeleliğe bağlı olmadan ölçülür.
func TestSendAlwaysFailsWhenRateIsOne(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  config.MockProviderConfig
		want int
	}{
		{
			name: "sunucu hatası",
			cfg:  config.MockProviderConfig{FailureRate: 1},
			want: http.StatusInternalServerError,
		},
		{
			name: "rate limit",
			cfg:  config.MockProviderConfig{RateLimitRate: 1},
			want: http.StatusTooManyRequests,
		},
		{
			name: "kalıcı hata",
			cfg:  config.MockProviderConfig{BadRequestRate: 1},
			want: http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			provider := newTestProvider(t, test.cfg)

			for range 10 {
				require.Equal(t, test.want, postSend(t, provider, "sms").Code)
			}
		})
	}
}

// TestSendSetsRetryAfterOnRateLimit, 429 yanıtının Retry-After taşıdığını doğrular:
// worker'ın bu header'a uyduğunu göstermek demo senaryosunun parçası.
func TestSendSetsRetryAfterOnRateLimit(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{RateLimitRate: 1})

	rec := postSend(t, provider, "sms")

	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
}

// TestSendRejectsGarbageBody, bozuk gövdenin 400 aldığını doğrular.
func TestSendRejectsGarbageBody(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/send", strings.NewReader("bu JSON değil"))
	rec := httptest.NewRecorder()

	provider.send(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestStatsCountsByStatusAndChannel, sayaçların "%30 hata oranında retry görülüyor"
// iddiasını ölçülebilir kıldığını doğrular.
func TestStatsCountsByStatusAndChannel(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{FailureRate: 1})

	postSend(t, provider, "sms")
	postSend(t, provider, "sms")
	postSend(t, provider, "email")

	rec := httptest.NewRecorder()
	provider.stats(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/stats", nil))

	require.Equal(t, http.StatusOK, rec.Code)

	var payload struct {
		ByStatus  map[string]int `json:"by_status"`
		ByChannel map[string]int `json:"by_channel"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))

	require.Equal(t, 3, payload.ByStatus["500"])
	require.Equal(t, 2, payload.ByChannel["sms"])
	require.Equal(t, 1, payload.ByChannel["email"])
}

// TestUpdateConfigChangesBehaviourAtRuntime, davranışın restart olmadan
// değiştiğini doğrular: demo adımı "hata oranını artır" tek curl olmalı.
func TestUpdateConfigChangesBehaviourAtRuntime(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{})

	require.Equal(t, http.StatusAccepted, postSend(t, provider, "sms").Code)

	rec := httptest.NewRecorder()
	provider.updateConfig(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/config",
		strings.NewReader(`{"failure_rate":1,"latency":"0s"}`)))
	require.Equal(t, http.StatusOK, rec.Code)

	require.Equal(t, http.StatusInternalServerError, postSend(t, provider, "sms").Code)
}

// TestUpdateConfigLeavesUnsetFieldsAlone, verilmeyen alanın değişmediğini doğrular:
// yalnızca gecikmeyi değiştirmek hata oranını sıfırlamamalı.
func TestUpdateConfigLeavesUnsetFieldsAlone(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{FailureRate: 1})

	rec := httptest.NewRecorder()
	provider.updateConfig(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/config",
		strings.NewReader(`{"latency":"0s"}`)))
	require.Equal(t, http.StatusOK, rec.Code)

	require.Equal(t, http.StatusInternalServerError, postSend(t, provider, "sms").Code,
		"verilmeyen hata oranı korunmalıydı")
}

// TestUpdateConfigRejectsInvalidValuesAtomically, geçersiz ayarın hiç uygulanmadığını
// doğrular: yarı uygulanmış ayar seti yük testinin ortasında anlamsız davranış üretir.
func TestUpdateConfigRejectsInvalidValuesAtomically(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{})

	for _, body := range []string{
		`{"failure_rate":1.5}`,
		`{"failure_rate":-1}`,
		`{"latency":"yarım saat"}`,
		`{"latency_jitter":"-5s"}`,
		`bu JSON değil`,
	} {
		rec := httptest.NewRecorder()
		provider.updateConfig(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/config",
			strings.NewReader(body)))

		require.Equal(t, http.StatusBadRequest, rec.Code, "gövde: %s", body)
	}

	// Geçerli ayarla birlikte gelen geçersiz alan da her şeyi reddetmeli.
	rec := httptest.NewRecorder()
	provider.updateConfig(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/config",
		strings.NewReader(`{"failure_rate":1,"latency":"bozuk"}`)))
	require.Equal(t, http.StatusBadRequest, rec.Code)

	require.Equal(t, http.StatusAccepted, postSend(t, provider, "sms").Code,
		"reddedilen ayar uygulanmamalıydı")
}

// TestUpdateConfigResetsStats, sayaçların sıfırlanabildiğini doğrular: her demo
// adımı temiz sayılarla başlasın.
func TestUpdateConfigResetsStats(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{})

	postSend(t, provider, "sms")

	rec := httptest.NewRecorder()
	provider.updateConfig(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/config",
		strings.NewReader(`{"reset_stats":true}`)))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	provider.stats(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/stats", nil))

	var payload struct {
		ByStatus map[string]int `json:"by_status"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
	require.Empty(t, payload.ByStatus, "sayaçlar sıfırlanmalıydı")
}

// TestConcurrentSendsAreRaceFree, eşzamanlı gönderim + ayar değişikliği + sayaç
// okumasının yarış üretmediğini doğrular (go test -race ile anlamlı).
//
// Yük testi sırasında /config ve /stats çağrılacak; kilitleme hatası tam o anda
// ortaya çıkardı.
func TestConcurrentSendsAreRaceFree(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{FailureRate: 0.5})

	var wg sync.WaitGroup

	wg.Add(30)

	for i := range 30 {
		go func() {
			defer wg.Done()

			switch i % 3 {
			case 0:
				postSend(t, provider, "sms")
			case 1:
				provider.stats(httptest.NewRecorder(),
					httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/stats", nil))
			default:
				provider.updateConfig(httptest.NewRecorder(),
					httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/config",
						strings.NewReader(`{"failure_rate":0.3}`)))
			}
		}()
	}

	wg.Wait()
}

// TestLiveReportsAlive, compose healthcheck'inin beklediği yanıtı doğrular.
func TestLiveReportsAlive(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{})

	rec := httptest.NewRecorder()
	provider.live(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/live", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "alive")
}

// TestDelayAddsJitterWithinBounds, jitter'ın sınırlar içinde kaldığını doğrular.
func TestDelayAddsJitterWithinBounds(t *testing.T) {
	t.Parallel()

	withoutJitter := behaviour{Latency: 10 * time.Millisecond}
	require.Equal(t, 10*time.Millisecond, withoutJitter.delay())

	withJitter := behaviour{Latency: 10 * time.Millisecond, LatencyJitter: 5 * time.Millisecond}

	for range 50 {
		delay := withJitter.delay()
		require.GreaterOrEqual(t, delay, 10*time.Millisecond)
		require.Less(t, delay, 15*time.Millisecond)
	}
}

// TestBehaviourJSONRoundTripsThroughConfig, /stats çıktısının doğrudan /config'e
// geri verilebildiğini doğrular: iki uç aynı sözleşmeyi kullanıyor.
func TestBehaviourJSONRoundTripsThroughConfig(t *testing.T) {
	t.Parallel()

	provider := newTestProvider(t, config.MockProviderConfig{
		FailureRate: 0.25, Latency: 50 * time.Millisecond, LatencyJitter: 5 * time.Millisecond,
	})

	rec := httptest.NewRecorder()
	provider.stats(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/stats", nil))

	var payload struct {
		Behaviour json.RawMessage `json:"behaviour"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))

	// Alan adları ve süre biçimi /config'in beklediğiyle aynı olmalı.
	require.Contains(t, string(payload.Behaviour), `"failure_rate":0.25`)
	require.Contains(t, string(payload.Behaviour), `"latency":"50ms"`)

	applied := httptest.NewRecorder()
	provider.updateConfig(applied, httptest.NewRequestWithContext(t.Context(),
		http.MethodPost, "/config", strings.NewReader(string(payload.Behaviour))))

	require.Equal(t, http.StatusOK, applied.Code,
		"/stats çıktısı /config'e geri verilebilmeli")
}
