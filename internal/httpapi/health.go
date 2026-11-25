package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// readinessTimeout, bağımlılık kontrollerine tanınan süre.
// Kısa tutulur: healthcheck'in kendisi takılı kalıp orchestrator'ı yanıltmamalı.
const readinessTimeout = 2 * time.Second

// Pinger, sağlık kontrolü yapılabilen bir bağımlılığı temsil eder.
// pgxpool.Pool ve redis.Client bu imzayı doğrudan karşılar; arayüz üzerinden
// alınması handler'ın gerçek altyapı olmadan test edilebilmesini sağlar.
type Pinger interface {
	Ping(ctx context.Context) error
}

// healthHandler, liveness ve readiness uçlarını sunar.
type healthHandler struct {
	logger       *slog.Logger
	dependencies map[string]Pinger
}

// live, process'in ayakta olduğunu bildirir.
//
// Bilerek hiçbir bağımlılığa dokunmaz: liveness başarısız olduğunda orchestrator
// container'ı yeniden başlatır. Veritabanı geçici olarak düştüğü için API'yi
// yeniden başlatmak sorunu çözmez, yalnızca kesintiyi büyütür.
func (h healthHandler) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ready, trafik alınabilir mi sorusunu yanıtlar.
//
// Bağımlılıklardan biri erişilemezse 503 döner ve load balancer bu kopyaya istek
// yöneltmeyi bırakır. Yanıt gövdesinde hangi bağımlılığın bozuk olduğu yazar;
// teşhis için log'a bakmaya gerek kalmaz.
func (h healthHandler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	checks := make(map[string]string, len(h.dependencies))
	healthy := true

	for name, dependency := range h.dependencies {
		if err := dependency.Ping(ctx); err != nil {
			healthy = false

			// Hata METNI yanıta KONMUYOR, yalnızca log'a. pgx şifreyi maskeliyor ama
			// kullanıcı adını, veritabanı adını, iç DNS adını, çözülmüş IP'yi ve
			// portu maskelemiyor; bu uç ise geçerli bir gövde gerektirmeden
			// erişilebilen tek uç. Hangi bağımlılığın bozuk olduğu anahtar adından
			// zaten anlaşılıyor — teşhis için gereken bu, ayrıntı log'da.
			checks[name] = "error"

			h.logger.WarnContext(ctx, "readiness kontrolü başarısız",
				slog.String("correlation_id", CorrelationID(ctx)),
				slog.String("dependency", name),
				slog.String("error", err.Error()),
			)

			continue
		}

		checks[name] = "ok"
	}

	status := http.StatusOK
	body := map[string]any{"status": "ready", "checks": checks}

	if !healthy {
		status = http.StatusServiceUnavailable
		body["status"] = "unavailable"
	}

	writeJSON(w, status, body)
}

// writeJSON, gövdeyi JSON olarak yazar.
// Encode hatası yanıt zaten yazılmaya başlandığı için istemciye iletilemez; yalnızca loglanır.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("JSON yanıt yazılamadı", slog.String("error", err.Error()))
	}
}
