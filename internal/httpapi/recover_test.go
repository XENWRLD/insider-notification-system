package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordingHandler, panik logunun gerçekten yapılandırılmış olduğunu görebilmek için
// slog kayıtlarını toplar.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, record.Clone())

	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// find, verilen mesajlı ilk kaydı döner.
func (h *recordingHandler) find(message string) (slog.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, record := range h.records {
		if record.Message == message {
			return record, true
		}
	}

	return slog.Record{}, false
}

// messages, toplanan mesajları döner (hata çıktısında işe yarıyor).
func (h *recordingHandler) messages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := make([]string, 0, len(h.records))
	for _, record := range h.records {
		result = append(result, record.Message)
	}

	return result
}

func newRecordingLogger(handler *recordingHandler) *slog.Logger {
	return slog.New(handler)
}

// attrsOf, kaydın alanlarını ad → metin haritasına çevirir.
func attrsOf(record slog.Record) map[string]string {
	attrs := make(map[string]string, record.NumAttrs())

	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()

		return true
	})

	return attrs
}

// TestRecovererWritesErrorEnvelope, paniğin hata zarfıyla yanıtlandığını doğrular.
//
// chi'nin hazır Recoverer'ı istemciye GÖVDESİZ bir 500 dönüyordu: error kodu yok,
// mesaj yok, gövdede correlation_id yok — yani "hata zarfı tüm uçlarda" ve
// "correlation_id her hata yanıtında" sözleşmeleri paniklerde geçerli değildi (F08).
func TestRecovererWritesErrorEnvelope(t *testing.T) {
	t.Parallel()

	panicking := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("test paniği")
	})

	handler := correlationIDMiddleware(recoverer(discardLogger())(panicking))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/notifications", nil))

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Equal(t, "application/json; charset=utf-8", recorder.Header().Get("Content-Type"))

	// decodeError, correlation_id'nin varlığını da doğruluyor.
	body := decodeError(t, recorder)
	require.Equal(t, CodeInternalError, body.Error)
	require.NotEmpty(t, body.Message)

	// Panik metni istemciye SIZMAZ: ayrıntı log'da kalır.
	require.NotContains(t, recorder.Body.String(), "test paniği")
}

// TestRecovererLogsPanicWithStack, paniğin yapılandırılmış log olarak basıldığını
// doğrular.
//
// Önemi: chi'nin Recoverer'ı yığını ANSI renkli ve çok satırlı olarak doğrudan
// os.Stderr'e basıyordu; JSON log hattına ayrıştırılamaz bir blob düşüyordu.
func TestRecovererLogsPanicWithStack(t *testing.T) {
	t.Parallel()

	panicking := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("kayıt altına alınacak panik")
	})

	logs := &recordingHandler{}
	handler := correlationIDMiddleware(recoverer(newRecordingLogger(logs))(panicking))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))

	require.Equal(t, http.StatusInternalServerError, recorder.Code)

	record, found := logs.find("handler panikledi")
	require.True(t, found, "panik ERROR olarak loglanmalı: %v", logs.messages())

	attrs := attrsOf(record)
	require.Contains(t, attrs, "stack")
	require.Contains(t, attrs, "panic")
	require.NotEmpty(t, attrs["correlation_id"], "panik logu correlation_id taşımalı")
}

// TestRecovererRepanicsOnAbortHandler, http.ErrAbortHandler'ın yeniden
// paniklediğini doğrular.
//
// Bu değer bilinçli bir iptal sözleşmesi (istemci/proxy bağlantıyı kesti); net/http
// onu kendisi sessizce işliyor. Yakalayıp 500 yazmak, kapanmış bir bağlantıya yanıt
// yazmaya çalışmak ve her kesilen istek için ERROR loglamak olurdu.
func TestRecovererRepanicsOnAbortHandler(t *testing.T) {
	t.Parallel()

	aborting := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	})

	handler := recoverer(discardLogger())(aborting)

	require.PanicsWithError(t, http.ErrAbortHandler.Error(), func() {
		handler.ServeHTTP(httptest.NewRecorder(),
			httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))
	})
}
