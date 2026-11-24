// Package bootstrap, process'lerin ortak açılış işlerini barındırır: bağlantı
// havuzlarını kurar ve bağımlılıklar hazır olana kadar bekler.
//
// Bu kod daha önce cmd/api içinde package main'e hapsolmuştu. cmd/worker aynı
// bekleme mantığına ihtiyaç duyuyor ve kopyalamak, yalnızca soğuk açılışta veya
// kurtarma anında görünen bir hatanın iki kopyadan birinde kalmasına yol açardı.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	// dependencyWaitAttempts/Delay: compose healthcheck'leri sırayı garanti etse de
	// ağ veya kurtarma anlarında bağımlılık birkaç saniye geç gelebilir. Sınırlı
	// sayıda deneme yapılır; süresiz beklemek hatalı yapılandırmayı gizler.
	dependencyWaitAttempts = 10
	dependencyWaitDelay    = 2 * time.Second
)

// OpenPostgres, bağlantı havuzunu kurar ve kullanılabilir olana kadar bekler.
//
// maxConns 0 ise pgx'in varsayılanı (max(4, NumCPU)) korunur. Worker bu değeri
// açıkça verir: kanal başına onlarca sender kısa sonuç yazmaları yapıyor ve
// varsayılan havuz bu yazmaları sessizce sıraya sokardı.
func OpenPostgres(
	ctx context.Context, logger *slog.Logger, dsn string, maxConns int32,
) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres bağlantı adresi çözümlenemedi: %w", err)
	}

	if maxConns > 0 {
		poolConfig.MaxConns = maxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("postgres havuzu kurulamadı: %w", err)
	}

	if err := WaitForDependency(ctx, logger, "postgres", pool.Ping); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

// OpenRedis, Redis istemcisini kurar ve kullanılabilir olana kadar bekler.
func OpenRedis(ctx context.Context, logger *slog.Logger, url string) (*redis.Client, error) {
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL çözümlenemedi: %w", err)
	}

	client := redis.NewClient(options)

	pinger := RedisPinger{Client: client}
	if err := WaitForDependency(ctx, logger, "redis", pinger.Ping); err != nil {
		if closeErr := client.Close(); closeErr != nil {
			logger.Error("redis bağlantısı kapatılamadı", slog.String("error", closeErr.Error()))
		}
		return nil, err
	}

	return client, nil
}

// WaitForDependency, ping başarılı olana veya deneme hakkı bitene kadar bekler.
func WaitForDependency(ctx context.Context, logger *slog.Logger, name string, ping func(context.Context) error) error {
	return waitForDependency(ctx, logger, name, ping, dependencyWaitAttempts, dependencyWaitDelay)
}

// waitForDependency, deneme sayısı ve gecikmeyi parametre alır: test gerçek
// 10 × 2sn'yi beklemek zorunda kalmasın. Dışarıya açık sarmalayıcı sabitleri geçer.
func waitForDependency(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	ping func(context.Context) error,
	attempts int,
	delay time.Duration,
) error {
	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, delay)
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
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("%s %d denemede hazır olmadı: %w", name, attempts, lastErr)
}

// RedisPinger, go-redis'in *StatusCmd dönen Ping'ini tek hatalı dönüşe indirger.
//
// httpapi.Pinger arayüzünü yapısal olarak karşılar ama o paketi import etmez:
// bootstrap'ın HTTP katmanına bağlanması katman sırasını tersine çevirirdi.
type RedisPinger struct {
	Client *redis.Client
}

// Ping, bağlantının ayakta olup olmadığını söyler.
func (p RedisPinger) Ping(ctx context.Context) error {
	if err := p.Client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping başarısız: %w", err)
	}
	return nil
}
