package httpapi

import (
	"errors"
	"runtime/debug"

	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

// CorrelationIDHeader, istemcinin kendi izleme kimliğini taşıyabileceği header.
const CorrelationIDHeader = "X-Correlation-ID"

// correlationIDKey, context içinde correlation ID'yi saklamak için kullanılan özel anahtar tipi.
// Dışa açılmaz; erişim yalnızca CorrelationID ile olur.
type correlationIDKey struct{}

// CorrelationID, context'teki correlation ID'yi döner; yoksa boş string.
//
// API ve worker ayrı process olduğu için bu kimlik, bildirim kaydıyla birlikte
// veritabanına yazılacak (Adım 3) ve worker logları da aynı kimliği taşıyacak.
func CorrelationID(ctx context.Context) string {
	if value, ok := ctx.Value(correlationIDKey{}).(string); ok {
		return value
	}
	return ""
}

// ContextWithCorrelationID, verilen kimliği context'e ekler.
func ContextWithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

// correlationIDMiddleware, her isteğe bir correlation ID bağlar.
//
// İstemci X-Correlation-ID gönderdiyse ona saygı duyulur (dağıtık izleme zinciri korunur),
// göndermediyse yeni bir UUID üretilir. Kimlik hem context'e hem yanıt header'ına konur,
// böylece istemci bir hatayı bildirirken log'u bulmamızı sağlayan kimliği elinde tutar.
func correlationIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(CorrelationIDHeader)
		if id == "" {
			id = uuid.NewString()
		}

		w.Header().Set(CorrelationIDHeader, id)
		next.ServeHTTP(w, r.WithContext(ContextWithCorrelationID(r.Context(), id)))
	})
}

// requestLogger, her isteği tamamlandıktan sonra tek satır JSON olarak loglar.
//
// chi'nin hazır logger'ı metin yazdığı için kullanılmıyor; burada slog ile yapılandırılmış
// alanlar (correlation_id, status, duration_ms) üretilir.
func requestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()
			wrapped := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(wrapped, r)

			// Health uçları container healthcheck'i tarafından saniyeler arayla çağrılır;
			// info seviyesinde loglanırsa gerçek istekleri bastırırlar.
			level := slog.LevelInfo
			if strings.HasPrefix(r.URL.Path, "/health") {
				level = slog.LevelDebug
			}

			logger.LogAttrs(r.Context(), level, "http request",
				slog.String("correlation_id", CorrelationID(r.Context())),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", wrapped.Status()),
				slog.Int("bytes", wrapped.BytesWritten()),
				slog.Int64("duration_ms", time.Since(started).Milliseconds()),
				slog.String("remote_addr", r.RemoteAddr),
			)
		})
	}
}

// recoverer, handler paniğini yakalar: hata zarfını yazar ve yığını slog'a basar.
//
// chi'nin hazır Recoverer'ı yerine kendi sürümü kullanılıyor. O, chi'nin kendi
// logger'ı kurulu olmadığında (bu projede bilinçli olarak kurulu değil) yığını ANSI
// renkli ve çok satırlı olarak doğrudan os.Stderr'e basıyor — JSON log hattına
// ayrıştırılamaz bir blob düşüyor — ve istemciye GÖVDESİZ bir 500 dönüyordu:
// error kodu yok, mesaj yok, gövdede correlation_id yok. İkisi de projenin iki
// belgelenmiş değişmezini kırıyordu (hata zarfı TÜM uçlarda, log yalnızca slog/JSON).
func recoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				recovered := recover()
				if recovered == nil {
					return
				}

				// http.ErrAbortHandler bilinçli bir iptal sözleşmesi (örn. proxy
				// bağlantıyı kesti): net/http'nin kendisi bunu sessizce işliyor ve
				// yeniden panikletmek doğru davranış.
				// (Tip kontrollü: panik değeri her zaman error olmak zorunda değil.)
				if err, ok := recovered.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(recovered)
				}

				logger.ErrorContext(r.Context(), "handler panikledi",
					slog.String("correlation_id", CorrelationID(r.Context())),
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Any("panic", recovered),
					// Yığın tek bir string alanı olarak: çok satırlı da olsa JSON
					// kaçışıyla tek log kaydı kalıyor ve log toplayıcı onu bölmüyor.
					slog.String("stack", string(debug.Stack())),
				)

				writeError(w, r, http.StatusInternalServerError, CodeInternalError,
					"istek işlenemedi")
			}()

			next.ServeHTTP(w, r)
		})
	}
}
