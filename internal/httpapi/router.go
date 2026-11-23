// Package httpapi, HTTP katmanını (router, middleware, handler) barındırır.
package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Dependencies, router'ın ihtiyaç duyduğu dış bağımlılıkları taşır.
// Sonraki adımlarda rate limiter ve metrik kayıt defteri buraya eklenir.
type Dependencies struct {
	Logger *slog.Logger
	// Health, readiness kontrolünde ping'lenecek bağımlılıklar (ad → Pinger).
	Health map[string]Pinger
	// Notifications, bildirim uçlarının kullandığı veri erişimi.
	Notifications NotificationStore

	// BodyLimitSingle / BodyLimitBatch, uç başına gövde sınırı (bayt).
	// Batch'in sınırı çok daha büyük: 1000 e-posta en kötü durumda ~10 MB tutar.
	BodyLimitSingle int64
	BodyLimitBatch  int64

	// IdempotencyTTL, saklanan idempotency cevaplarının geçerlilik süresi.
	IdempotencyTTL time.Duration
}

// NewRouter, middleware zinciri kurulmuş HTTP handler'ını döner.
func NewRouter(deps Dependencies) http.Handler {
	health := healthHandler{logger: deps.Logger, dependencies: deps.Health}

	notifications := notificationHandler{
		logger: deps.Logger,
		store:  deps.Notifications,
		singleLimit: bodyLimit{
			bytes: deps.BodyLimitSingle,
		},
		batchLimit: bodyLimit{
			bytes:  deps.BodyLimitBatch,
			advice: "batch'i daha küçük parçalara bölün",
		},
		idempotencyTTL: deps.IdempotencyTTL,
	}

	router := chi.NewRouter()

	// Sıra önemli: correlation ID, loglanacak alanları hazırlamak için logger'dan
	// önce; Recoverer, panik anında yanıtı 500'e çevirip logger'ın isteği yine
	// kaydedebilmesi için logger'dan sonra çalışır.
	//
	// chi'nin RealIP middleware'i bilinçli olarak kullanılmıyor: X-Forwarded-For
	// başlığına güvenip r.RemoteAddr'i değiştiriyor ve ters proxy arkasında
	// olmadığımız sürece istemci IP'si taklit edilebiliyor (GHSA-3fxj-6jh8-hvhx).
	router.Use(correlationIDMiddleware)
	router.Use(requestLogger(deps.Logger))
	router.Use(middleware.Recoverer)

	// chi'nin varsayılan 404/405 handler'ları düz metin döner; API'nin geri kalanıyla
	// aynı hata zarfını kullanmaları için değiştiriliyor.
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotFound, CodeNotFound, "kaynak bulunamadı")
	})
	router.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusMethodNotAllowed, CodeMethodNotAllowed,
			"bu yol için HTTP method'u desteklenmiyor")
	})

	router.Get("/health/live", health.live)
	router.Get("/health/ready", health.ready)

	// Sürümlü API tabanı: okuma ve iptal uçları (Adım 5) buraya eklenecek.
	router.Route("/api/v1", func(api chi.Router) {
		api.Post("/notifications", notifications.create)
		api.Post("/notifications/batch", notifications.createBatch)
	})

	return router
}
