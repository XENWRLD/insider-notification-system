package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// stubPinger, testlerde gerçek altyapı yerine kullanılan bağımlılık.
type stubPinger struct {
	err error
}

func (s stubPinger) Ping(context.Context) error { return s.err }

// discardLogger, test çıktısını kirletmeyen logger.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func newTestRouter(health map[string]Pinger) http.Handler {
	return NewRouter(Dependencies{Logger: discardLogger(), Health: health})
}

func TestLiveAlwaysOK(t *testing.T) {
	t.Parallel()

	// Bağımlılıklar bozuk olsa bile liveness 200 dönmeli: process ayakta ve
	// yeniden başlatmak veritabanı arızasını çözmez.
	router := newTestRouter(map[string]Pinger{"postgres": stubPinger{err: errors.New("down")}})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/live", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"status":"ok"}`, recorder.Body.String())
}

func TestReadyWhenAllDependenciesHealthy(t *testing.T) {
	t.Parallel()

	router := newTestRouter(map[string]Pinger{
		"postgres": stubPinger{},
		"redis":    stubPinger{},
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/ready", nil))

	require.Equal(t, http.StatusOK, recorder.Code)

	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, "ready", body.Status)
	require.Equal(t, map[string]string{"postgres": "ok", "redis": "ok"}, body.Checks)
}

func TestReadyReturns503AndNamesBrokenDependency(t *testing.T) {
	t.Parallel()

	router := newTestRouter(map[string]Pinger{
		"postgres": stubPinger{},
		"redis":    stubPinger{err: errors.New("connection refused")},
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/ready", nil))

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)

	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, "unavailable", body.Status)
	require.Equal(t, "ok", body.Checks["postgres"])

	// Hangi bağımlılığın bozuk olduğu ANAHTAR ADINDAN anlaşılıyor; hata METNİ
	// yanıta konmuyor (bkz. TestReadyDoesNotLeakDependencyDetails). Teşhis için
	// gereken bu kadarı, ayrıntı log'da.
	require.Equal(t, "error", body.Checks["redis"])
}

func TestCorrelationIDIsEchoedWhenProvided(t *testing.T) {
	t.Parallel()

	router := newTestRouter(map[string]Pinger{})

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/live", nil)
	request.Header.Set(CorrelationIDHeader, "test-123")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	// İstemcinin gönderdiği kimliğe saygı duyulur: dağıtık izleme zinciri kopmaz.
	require.Equal(t, "test-123", recorder.Header().Get(CorrelationIDHeader))
}

func TestCorrelationIDIsGeneratedWhenMissing(t *testing.T) {
	t.Parallel()

	router := newTestRouter(map[string]Pinger{})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/live", nil))

	require.Len(t, recorder.Header().Get(CorrelationIDHeader), 36, "UUID biçiminde üretilmeli")
}

// TestHealthEndpointsAnswerHead, sağlık uçlarının HEAD'e de yanıt verdiğini doğrular.
//
// Regresyon: rotalar yalnızca router.Get ile kayıtlıydı. chi, net/http'nin
// ServeMux'ının aksine GET rotası için HEAD'i otomatik kaydetmiyor; istek
// MethodNotAllowed'a düşüp 405 alıyordu. Compose healthcheck'i `wget --spider`
// kullanıyor ve Dockerfile runtime aşamasında GNU wget kuruyor — GNU wget --spider
// HEAD atar, 405 alır ve 8 ile çıkar. Sonuç: api servisi kalıcı olarak
// `Up (unhealthy)` kalıyordu (restart politikası sağlık hatasında devreye girmiyor).
func TestHealthEndpointsAnswerHead(t *testing.T) {
	t.Parallel()

	router := newTestRouter(map[string]Pinger{"postgres": stubPinger{}})

	for _, path := range []string{"/health/live", "/health/ready"} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder,
			httptest.NewRequestWithContext(t.Context(), http.MethodHead, path, nil))

		require.Equal(t, http.StatusOK, recorder.Code,
			"%s HEAD'e 200 dönmeli; 405 compose healthcheck'ini kalıcı olarak bozar", path)
	}
}

// stubCollector, /metrics ucunun rota testleri için sabit bir metrik yayınlar.
type stubCollector struct {
	desc *prometheus.Desc
}

func newStubCollector() *stubCollector {
	return &stubCollector{
		desc: prometheus.NewDesc("test_metric", "Rota testi için.", nil, nil),
	}
}

func (s *stubCollector) Describe(ch chan<- *prometheus.Desc) { ch <- s.desc }

func (s *stubCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(s.desc, prometheus.GaugeValue, 42)
}

// TestMetricsEndpointServesPrometheusText, /metrics ucunun Prometheus metni
// döndürdüğünü ve Go runtime metriklerini de içerdiğini doğrular.
func TestMetricsEndpointServesPrometheusText(t *testing.T) {
	t.Parallel()

	router := NewRouter(Dependencies{
		Logger:           discardLogger(),
		Health:           map[string]Pinger{"postgres": stubPinger{}},
		MetricsCollector: newStubCollector(),
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), "test_metric 42")
	require.Contains(t, recorder.Body.String(), "go_goroutines",
		"Go runtime koleksiyoncusu da kayıtlı olmalı: olay anında sızıntı/yavaşlık ayrımı")
}

// TestMetricsEndpointAnswersHead, scrape öncesi yoklamaların 405 almadığını doğrular
// (sağlık uçlarıyla aynı chi tuzağı).
func TestMetricsEndpointAnswersHead(t *testing.T) {
	t.Parallel()

	router := NewRouter(Dependencies{
		Logger:           discardLogger(),
		MetricsCollector: newStubCollector(),
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodHead, "/metrics", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
}

// TestMetricsEndpointAbsentWithoutCollector, toplayıcı verilmediğinde ucun hiç
// kaydedilmediğini doğrular: boş bir /metrics yayınlamak yerine 404 dönmek dürüst.
func TestMetricsEndpointAbsentWithoutCollector(t *testing.T) {
	t.Parallel()

	router := NewRouter(Dependencies{Logger: discardLogger()})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))

	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Contains(t, recorder.Body.String(), "correlation_id",
		"404 de hata zarfını kullanmalı")
}

// TestReadyDoesNotLeakDependencyDetails, 503 gövdesinin iç ayrıntı sızdırmadığını
// doğrular.
//
// Regresyon: ham hata metni gövdeye konuyordu. pgx şifreyi maskeliyor ama kullanıcı
// adını, veritabanı adını, iç DNS adını, çözülmüş IP'yi ve portu maskelemiyor — ve bu,
// geçerli bir gövde gerektirmeden erişilebilen tek uç. Hangi bağımlılığın bozuk olduğu
// anahtar adından anlaşılıyor; ayrıntı log'da kalmalı.
func TestReadyDoesNotLeakDependencyDetails(t *testing.T) {
	t.Parallel()

	leak := "failed to connect to `user=insider database=notifications`: " +
		"10.1.2.3:5432 (db-prod-internal.svc.cluster.local): dial error"

	router := newTestRouter(map[string]Pinger{
		"postgres": stubPinger{err: errors.New(leak)},
		"redis":    stubPinger{},
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/ready", nil))

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)

	body := recorder.Body.String()

	require.NotContains(t, body, "user=insider")
	require.NotContains(t, body, "db-prod-internal")
	require.NotContains(t, body, "10.1.2.3")
	require.NotContains(t, body, "dial error")

	// Hangi bağımlılığın bozuk olduğu yine görünmeli.
	require.JSONEq(t,
		`{"status":"unavailable","checks":{"postgres":"error","redis":"ok"}}`, body)
}

// TestMethodNotAllowedCarriesAllowHeader, 405 yanıtının Allow header'ı taşıdığını
// doğrular (RFC 9110 §15.5.6).
//
// Regresyon: chi'nin kendi handler'ı Allow yazıyordu ama JSON hata zarfı için
// değiştirdiğimizde header kayboluyordu; istemci yolun hangi metodu kabul ettiğini
// makine-okunur biçimde keşfedemiyordu.
func TestMethodNotAllowedCarriesAllowHeader(t *testing.T) {
	t.Parallel()

	router := newTestRouter(map[string]Pinger{"postgres": stubPinger{}})

	tests := []struct {
		method string
		path   string
		expect []string
	}{
		{http.MethodDelete, "/api/v1/notifications", []string{"GET", "POST"}},
		{http.MethodPut, "/api/v1/notifications", []string{"GET", "POST"}},
		{http.MethodPost, "/health/live", []string{"GET", "HEAD"}},
	}

	for _, test := range tests {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder,
			httptest.NewRequestWithContext(t.Context(), test.method, test.path, nil))

		require.Equal(t, http.StatusMethodNotAllowed, recorder.Code, test.path)

		allow := recorder.Header().Get("Allow")
		require.NotEmpty(t, allow, "%s %s için Allow header'ı olmalı", test.method, test.path)

		for _, method := range test.expect {
			require.Contains(t, allow, method, "%s %s", test.method, test.path)
		}
	}
}
