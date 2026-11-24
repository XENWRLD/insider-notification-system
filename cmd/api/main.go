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

	"github.com/XENWRLD/insider-notification-system/internal/bootstrap"
	"github.com/XENWRLD/insider-notification-system/internal/config"
	"github.com/XENWRLD/insider-notification-system/internal/httpapi"
	"github.com/XENWRLD/insider-notification-system/internal/observability"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

const (
	// serviceName, logların kaynağını ayırt etmek için kullanılır.
	serviceName = "api"

	// apiDBMaxConns 0: API pgx'in varsayılan havuz boyutunu kullanır. Burada
	// ölçülmüş bir ihtiyaç yok — istekler kısa sorgular yapıyor ve gönderim işini
	// worker üstleniyor. Gerekirse Adım 9'da ölçülerek ayarlanır.
	apiDBMaxConns = 0

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

	pool, err := bootstrap.OpenPostgres(ctx, logger, cfg.DatabaseURL, apiDBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	redisClient, err := bootstrap.OpenRedis(ctx, logger, cfg.RedisURL)
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
			"redis":    bootstrap.RedisPinger{Client: redisClient},
		},
		Notifications:   notificationStore,
		BodyLimitSingle: cfg.BodyLimitSingle,
		BodyLimitBatch:  cfg.BodyLimitBatch,
		IdempotencyTTL:  cfg.IdempotencyTTL,
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
