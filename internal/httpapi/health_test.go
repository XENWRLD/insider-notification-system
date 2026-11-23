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
	require.Contains(t, body.Checks["redis"], "connection refused")
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
