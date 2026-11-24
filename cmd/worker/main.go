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
	"github.com/XENWRLD/insider-notification-system/internal/config"
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

	pool, err := bootstrap.OpenPostgres(ctx, logger, cfg.DatabaseURL, int32(cfg.WorkerDBMaxConns)) //nolint:gosec // yapılandırmadan gelen küçük pozitif sayı
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
		MaxIdleConnsPerHost: cfg.WorkerConcurrency,
	}, logger)
	if err != nil {
		return err
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
		FixedRetryDelay: cfg.WorkerRetryDelay,
		RateWaitMax:     cfg.WorkerRateWaitMax,
		ReaperInterval:  cfg.WorkerReaperInterval,
		ReaperBatchSize: cfg.WorkerReaperBatchSize,

		IdempotencyTTL:             cfg.IdempotencyTTL,
		IdempotencyCleanupInterval: cfg.IdempotencyCleanupInterval,
		IdempotencyCleanupBatch:    cfg.IdempotencyCleanupBatch,
	}, store.New(pool), providerClient, limiter, logger)
	if err != nil {
		return err
	}

	logger.Info("worker başlıyor",
		slog.String("env", cfg.AppEnv),
		slog.String("provider_url", cfg.ProviderURL),
		slog.Int("db_max_conns", cfg.WorkerDBMaxConns),
		slog.Int("rate_limit_per_second", cfg.RateLimitPerSecond),
		slog.Int("rate_limit_burst", cfg.RateLimitBurst),
	)

	return engine.Run(ctx)
}
