// Package httpapi, HTTP katmanını (router, middleware, handler) barındırır.
package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Dependencies, router'ın ihtiyaç duyduğu dış bağımlılıkları taşır.
// Sonraki adımlarda store, rate limiter ve metrik kayıt defteri buraya eklenir.
type Dependencies struct {
	Logger *slog.Logger
	// Health, readiness kontrolünde ping'lenecek bağımlılıklar (ad → Pinger).
	Health map[string]Pinger
}

// NewRouter, middleware zinciri kurulmuş HTTP handler'ını döner.
func NewRouter(deps Dependencies) http.Handler {
	health := healthHandler{logger: deps.Logger, dependencies: deps.Health}

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

	router.Get("/health/live", health.live)
	router.Get("/health/ready", health.ready)

	// Sürümlü API tabanı: uçlar Adım 3'ten itibaren eklenir.
	router.Route("/api/v1", func(chi.Router) {})

	return router
}
