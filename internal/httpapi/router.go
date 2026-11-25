// Package httpapi, HTTP katmanını (router, middleware, handler) barındırır.
package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	// MetricsCollector, /metrics ucunun kaynağı. nil ise uç hiç kaydedilmiyor:
	// metrik toplayıcısı olmayan bir kurulumda 404 dönmek, boş bir /metrics
	// yayınlamaktan dürüst.
	MetricsCollector prometheus.Collector
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

	// HEAD de kaydediliyor. Gerekçe ölçümle bulundu: compose healthcheck'i
	// `wget --spider` ile çağırıyor, Dockerfile runtime aşamasında `apk add wget`
	// yaptığı için container'da GNU wget var ve `--spider` HEAD atıyor. chi,
	// net/http'nin ServeMux'ının aksine GET rotası için HEAD'i OTOMATİK kaydetmez:
	// istek MethodNotAllowed'a düşüp 405 alıyor, wget 8 ile çıkıyor ve `api` servisi
	// kalıcı olarak `Up (unhealthy)` kalıyordu (restart politikası sağlık hatasında
	// yeniden başlatmıyor). mockprovider bu tuzağa düşmüyor çünkü net/http ServeMux
	// kullanıyor ve o, GET pattern'ini HEAD'e de eşliyor.
	router.Get("/health/live", health.live)
	router.Head("/health/live", health.live)
	router.Get("/health/ready", health.ready)
	router.Head("/health/ready", health.ready)

	// /metrics kendi registry'siyle kuruluyor, default registry ile DEĞİL: global
	// registry'ye başka bir paketin sessizce metrik eklemesi mümkün olurdu ve ne
	// yayınladığımız açık kalmalı. Go runtime koleksiyoncusu (GC, goroutine sayısı,
	// bellek) ayrıca ekleniyor — bir olay anında "sızıntı mı yavaşlık mı" sorusunu
	// cevaplayan şey o.
	if deps.MetricsCollector != nil {
		registry := prometheus.NewRegistry()
		registry.MustRegister(collectors.NewGoCollector())
		registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		registry.MustRegister(deps.MetricsCollector)

		metricsHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
			// Toplama hatası yanıtı düşürmesin: Collector hata döndürmüyor ama
			// kayıt düzeyinde bir sorun olursa elimizdekini yayınlamak kör kalmaktan
			// iyi.
			ErrorHandling: promhttp.ContinueOnError,

			// Eşzamanlı scrape sınırı: /metrics kimliksiz ve her scrape toplama
			// turunu tetikleyebiliyor. Collector'ın kendi önbelleği + singleflight'ı
			// asıl korumayı sağlıyor, bu ikinci savunma hattı.
			MaxRequestsInFlight: 4,
		})

		router.Method(http.MethodGet, "/metrics", metricsHandler)
		router.Method(http.MethodHead, "/metrics", metricsHandler)
	}

	// Sürümlü API tabanı. /ws (bonus) buraya eklenecek.
	//
	// Rota sırası: chi en belirgin kalıbı seçer, bu yüzden "/notifications" ile
	// "/notifications/batch" çakışmaz.
	router.Route("/api/v1", func(api chi.Router) {
		api.Post("/notifications", notifications.create)
		api.Post("/notifications/batch", notifications.createBatch)
		api.Get("/notifications", notifications.list)
		api.Get("/notifications/{id}", notifications.get)
		api.Post("/notifications/{id}/cancel", notifications.cancel)
		api.Get("/batches/{id}", notifications.batchSummary)
	})

	return router
}
