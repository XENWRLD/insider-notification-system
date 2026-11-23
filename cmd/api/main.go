// Command api, bildirim sisteminin HTTP arayüzünü çalıştırır.
//
// API yalnızca isteği doğrular ve veritabanına yazar; gönderim işini worker yapar.
// Bu ayrım sayesinde sağlayıcı yavaşladığında API yanıt süreleri etkilenmez.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/XENWRLD/insider-notification-system/internal/config"
	"github.com/XENWRLD/insider-notification-system/internal/httpapi"
	"github.com/XENWRLD/insider-notification-system/internal/observability"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

const (
	// serviceName, logların kaynağını ayırt etmek için kullanılır.
	serviceName = "api"

	// dependencyWaitAttempts/Delay: compose healthcheck'leri sırayı garanti etse de
	// ağ veya kurtarma anlarında bağımlılık birkaç saniye geç gelebilir. Sınırlı
	// sayıda deneme yapılır; süresiz beklemek hatalı yapılandırmayı gizler.
	dependencyWaitAttempts = 10
	dependencyWaitDelay    = 2 * time.Second

	// HTTP sunucu zaman aşımları açıkça verilir: Go'nun varsayılanı sınırsızdır ve
	// yavaş istemciler bağlantıları süresiz tutabilir.
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 60 * time.Second
)

func main() {
	if err := run(); err != nil {
		// Logger henüz kurulmamış olabileceği için doğrudan stderr'e yazılır.
		fmt.Fprintf(os.Stderr, "api başlatılamadı: %v\n", err)
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

	// SIGINT/SIGTERM geldiğinde bu context iptal olur; compose stop ve Kubernetes
	// her ikisinde de süreç SIGTERM ile sonlanır.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := openPostgres(ctx, logger, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	redisClient, err := openRedis(ctx, logger, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			logger.Error("redis bağlantısı kapatılamadı", slog.String("error", closeErr.Error()))
		}
	}()

	notificationStore := store.New(pool)

	handler := httpapi.NewRouter(httpapi.Dependencies{
		Logger: logger,
		Health: map[string]httpapi.Pinger{
			"postgres": pool,
			"redis":    redisPinger{client: redisClient},
		},
		Notifications: notificationStore,
	})

	server := &http.Server{
		Addr:              net.JoinHostPort("", cfg.HTTPPort),
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	return serve(ctx, logger, server, cfg)
}

// serve, sunucuyu ayağa kaldırır ve kapanış sinyalini bekler.
//
// Kapanışta önce yeni bağlantılar kesilir, sonra devam eden istekler için
// ShutdownTimeout kadar beklenir: cevap bekleyen istemciye bağlantı resetlenmez.
func serve(ctx context.Context, logger *slog.Logger, server *http.Server, cfg config.Config) error {
	serverErrors := make(chan error, 1)

	go func() {
		logger.Info("http sunucusu dinlemeye başladı",
			slog.String("addr", server.Addr),
			slog.String("env", cfg.AppEnv),
		)

		if listenErr := server.ListenAndServe(); listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
			serverErrors <- listenErr
			return
		}

		serverErrors <- nil
	}()

	select {
	case err := <-serverErrors:
		if err != nil {
			return fmt.Errorf("http sunucusu durdu: %w", err)
		}
		return nil

	case <-ctx.Done():
		logger.Info("kapanış sinyali alındı, graceful shutdown başlıyor",
			slog.String("timeout", cfg.ShutdownTimeout.String()),
		)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown tamamlanamadı: %w", err)
		}

		logger.Info("http sunucusu düzgün şekilde kapandı")

		return nil
	}
}

// openPostgres, bağlantı havuzunu kurar ve kullanılabilir olana kadar bekler.
func openPostgres(ctx context.Context, logger *slog.Logger, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres havuzu kurulamadı: %w", err)
	}

	if err := waitForDependency(ctx, logger, "postgres", pool.Ping); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

// openRedis, Redis istemcisini kurar ve kullanılabilir olana kadar bekler.
func openRedis(ctx context.Context, logger *slog.Logger, url string) (*redis.Client, error) {
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL çözümlenemedi: %w", err)
	}

	client := redis.NewClient(options)

	pinger := redisPinger{client: client}
	if err := waitForDependency(ctx, logger, "redis", pinger.Ping); err != nil {
		if closeErr := client.Close(); closeErr != nil {
			logger.Error("redis bağlantısı kapatılamadı", slog.String("error", closeErr.Error()))
		}
		return nil, err
	}

	return client, nil
}

// waitForDependency, ping başarılı olana veya deneme hakkı bitene kadar bekler.
func waitForDependency(ctx context.Context, logger *slog.Logger, name string, ping func(context.Context) error) error {
	var lastErr error

	for attempt := 1; attempt <= dependencyWaitAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, dependencyWaitDelay)
		lastErr = ping(attemptCtx)
		cancel()

		if lastErr == nil {
			logger.Info("bağımlılık hazır", slog.String("dependency", name), slog.Int("attempt", attempt))
			return nil
		}

		logger.Warn("bağımlılığa ulaşılamadı, yeniden denenecek",
			slog.String("dependency", name),
			slog.Int("attempt", attempt),
			slog.String("error", lastErr.Error()),
		)

		select {
		case <-ctx.Done():
			return fmt.Errorf("%s beklenirken kapanış sinyali alındı: %w", name, ctx.Err())
		case <-time.After(dependencyWaitDelay):
		}
	}

	return fmt.Errorf("%s %d denemede hazır olmadı: %w", name, dependencyWaitAttempts, lastErr)
}

// redisPinger, go-redis'in *StatusCmd dönen Ping'ini httpapi.Pinger imzasına uyarlar.
type redisPinger struct {
	client *redis.Client
}

func (p redisPinger) Ping(ctx context.Context) error {
	if err := p.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping başarısız: %w", err)
	}
	return nil
}
