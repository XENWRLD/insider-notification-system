// Command worker, bildirimleri kuyruktan çekip sağlayıcıya gönderir.
//
// API'den ayrı bir process: sağlayıcı yavaşladığında API yanıt süreleri etkilenmez ve
// gönderim kapasitesi API'den bağımsız ölçeklenebilir (docker compose --scale worker=3).
//
// HTTP dinlemez — dışa açık bir yüzeyi yok. Prometheus /metrics kararı Adım 9'da.
//
// Redis'e bağlanır: kanal başına hız sınırı orada paylaşılan bir kovayla uygulanıyor,
// böylece --scale worker=3 ile üç kopya da aynı sınırı tüketir.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/XENWRLD/insider-notification-system/internal/bootstrap"
	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/breakerstate"
	"github.com/XENWRLD/insider-notification-system/internal/config"
	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/observability"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
	"github.com/XENWRLD/insider-notification-system/internal/ratelimit"
	"github.com/XENWRLD/insider-notification-system/internal/store"
	"github.com/XENWRLD/insider-notification-system/internal/worker"
)

// serviceName, logların kaynağını ayırt etmek için kullanılır.
const serviceName = "worker"

func main() {
	if err := run(); err != nil {
		// Logger henüz kurulmamış olabileceği için doğrudan stderr'e yazılır.
		fmt.Fprintf(os.Stderr, "worker başlatılamadı: %v\n", err)
		os.Exit(1)
	}
}

// run, main'in hata dönebilen gövdesi.
// defer'lerin os.Exit tarafından atlanmaması için tüm iş burada yapılır.
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := observability.NewLogger(serviceName, cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// int32 dönüşümü güvenli: config.envInt değeri (0, MaxCountValue] aralığında
	// zorluyor ve MaxCountValue int32'ye rahat sığıyor. Sınır olmadan dönüşüm sessizce
	// taşıyordu (F14) — yanlış ayar artık açılışta hata veriyor.
	pool, err := bootstrap.OpenPostgres(ctx, logger, cfg.DatabaseURL, int32(cfg.WorkerDBMaxConns)) //nolint:gosec // config.MaxCountValue ile sınırlı
	if err != nil {
		return err
	}

	// Havuz, motor tamamen durduktan SONRA kapanır: uçan sonuç yazımları
	// kapanış sırasında bağlantısız kalmasın (defer sırası buna bağlı).
	defer pool.Close()

	redisClient, err := bootstrap.OpenRedis(ctx, logger, cfg.RedisURL)
	if err != nil {
		return err
	}

	// Aynı gerekçe: uçan izin talepleri bağlantısız kalmasın.
	defer func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			logger.Error("redis bağlantısı kapatılamadı", slog.String("error", closeErr.Error()))
		}
	}()

	limiter, err := ratelimit.New(
		redisClient, cfg.RateLimitPerSecond, cfg.RateLimitBurst, ratelimit.DefaultKeyPrefix)
	if err != nil {
		return err
	}

	providerClient, err := provider.New(provider.Config{
		URL:     cfg.ProviderURL,
		Timeout: cfg.WorkerProviderTimeout,

		// Bağlantı havuzu eşzamanlı gönderim sayısına eşitlenir: Go'nun varsayılanı
		// host başına 2 idle bağlantı ve saniyede yüzlerce gönderimde neredeyse her
		// istek yeni bir TCP (ve TLS) el sıkışmasına girerdi.
		//
		// Çarpan ZORUNLU: her kanal kendi havuzunda WORKER_CONCURRENCY sender
		// çalıştırıyor ve üç havuz AYNI provider.Client üzerinden aynı host'a gidiyor
		// → uçan istek sayısı kanal × concurrency. Yalnızca concurrency verilirse
		// yanıtların 2/3'ü idle havuzu dolu bulup bağlantıyı kapatıyor ve bu alanın
		// kaldırmak için eklendiği el sıkışma maliyeti geri geliyordu (F06; Adım 6-7
		// ölçümleri tek kanalla yapıldığı için görünmemişti).
		MaxIdleConnsPerHost: len(domain.AllChannels()) * cfg.WorkerConcurrency,
	}, logger)
	if err != nil {
		return err
	}

	// Devre kesici fabrikası: kanal başına bir durum makinesi. Devre YEREL (process
	// içi) — rate limiter'ın aksine paylaşılmıyor. Sayaç değişmeli (commutative) bir
	// değer olduğu için tek bir Redis anahtarında paylaşılabiliyor, ama "ardışık 5
	// hata" ifadesi kopyalar arası bir olay SIRALAMASI gerektiriyor ve bunu paylaşmak
	// dağıtık uzlaşı problemi. Karşılığında kazanç küçük: fazla korumak zararsız, az
	// korumak ise eşik × kopya ile sınırlı (3 kopyada ~12 mesaj birer deneme kaybeder).
	newBreaker := func(channel domain.Channel) (breaker.Breaker, error) {
		return breaker.New(breaker.Config{
			Name:             string(channel),
			FailureThreshold: cfg.WorkerBreakerFailureThreshold,
			OpenTimeout:      cfg.WorkerBreakerOpenTimeout,

			// Hangi hatanın devreyi ilgilendirdiği worker paketinin bilgisi:
			// 429 sayılmaz (excluded), 400/422 sağlığın kanıtı, 5xx/auth/bağlantı hata.
			IsFailure: worker.BreakerIsFailure,

			// Durum geçişi başına TEK log satırı: her yoklamada loglamak, boş kuyrukta
			// saniyede onlarca satır üretirdi.
			OnStateChange: func(name string, from, to breaker.State) {
				attrs := []any{
					slog.String("channel", name),
					slog.String("from", string(from)),
					slog.String("to", string(to)),
				}

				if to == breaker.StateClosed {
					logger.Info("devre kesici kapandı, kanal normale döndü", attrs...)

					return
				}

				logger.Warn("devre kesici durumu değişti", attrs...)
			},
		})
	}

	// Kopya kimliği: devre durumu metriği kopya başına yayınlandığı için etiket
	// gerekiyor. Compose'da container adı, K8s'te pod adı burayı dolduruyor.
	workerID, err := os.Hostname()
	if err != nil {
		// Kimlik alınamazsa metrik etiketi anlamsızlaşır ama gönderim etkilenmemeli.
		logger.Warn("hostname okunamadı, devre durumu metriği 'unknown' etiketiyle yayınlanacak",
			slog.String("error", err.Error()))

		workerID = "unknown"
	}

	// Devre durumu yayıncısı: worker HTTP dinlemediği için durum Redis üzerinden
	// API'nin /metrics ucuna taşınıyor. Kurulamazsa gönderim DURMUYOR — metrik
	// isteğe bağlı, hız sınırının aksine fail closed davranışı burada yanlış olurdu.
	var statePublisher worker.StatePublisher

	if publisher, stateErr := breakerstate.New(
		redisClient, breakerstate.DefaultKeyPrefix,
	); stateErr != nil {
		logger.Warn("devre durumu yayıncısı kurulamadı, metrik eksik kalacak",
			slog.String("error", stateErr.Error()))
	} else {
		statePublisher = publisher
	}

	engine, err := worker.New(worker.Config{
		Concurrency:     cfg.WorkerConcurrency,
		ClaimBatchSize:  cfg.WorkerClaimBatchSize,
		LeaseDuration:   cfg.WorkerLeaseDuration,
		PollMinInterval: cfg.WorkerPollMinInterval,
		PollMaxInterval: cfg.WorkerPollMaxInterval,
		ProviderTimeout: cfg.WorkerProviderTimeout,
		ResultTimeout:   cfg.WorkerResultTimeout,
		DrainTimeout:    cfg.WorkerDrainTimeout,
		ClaimTimeout:    cfg.WorkerClaimTimeout,
		WorkerID:        workerID,
		RateWaitMax:     cfg.WorkerRateWaitMax,
		ReaperInterval:  cfg.WorkerReaperInterval,
		ReaperBatchSize: cfg.WorkerReaperBatchSize,

		RetryBaseDelay:          cfg.WorkerRetryBaseDelay,
		RetryMaxDelay:           cfg.WorkerRetryMaxDelay,
		BreakerFailureThreshold: cfg.WorkerBreakerFailureThreshold,
		BreakerOpenTimeout:      cfg.WorkerBreakerOpenTimeout,
		BreakerStateInterval:    cfg.WorkerBreakerStateInterval,

		IdempotencyTTL:             cfg.IdempotencyTTL,
		IdempotencyCleanupInterval: cfg.IdempotencyCleanupInterval,
		IdempotencyCleanupBatch:    cfg.IdempotencyCleanupBatch,
	}, store.New(pool), providerClient, limiter, newBreaker, statePublisher, logger)
	if err != nil {
		return err
	}

	logger.Info("worker başlıyor",
		slog.String("env", cfg.AppEnv),
		slog.String("provider_url", cfg.ProviderURL),
		slog.Int("db_max_conns", cfg.WorkerDBMaxConns),
		slog.Int("rate_limit_per_second", cfg.RateLimitPerSecond),
		slog.Int("rate_limit_burst", cfg.RateLimitBurst),
		slog.Int("breaker_failure_threshold", cfg.WorkerBreakerFailureThreshold),
		slog.String("breaker_open_timeout", cfg.WorkerBreakerOpenTimeout.String()),
	)

	return engine.Run(ctx)
}
