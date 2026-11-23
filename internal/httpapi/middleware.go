package httpapi

import (
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
