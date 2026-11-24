package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// setRequiredEnv, zorunlu değişkenleri test süresince ayarlar.
func setRequiredEnv(t *testing.T) {
	t.Helper()

	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")
	t.Setenv("REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("PROVIDER_URL", "https://example.test/hook")
}

func TestLoadAppliesDefaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, "development", cfg.AppEnv)
	require.Equal(t, "info", cfg.LogLevel)
	require.Equal(t, "8080", cfg.HTTPPort)
	require.Equal(t, 15*time.Second, cfg.ShutdownTimeout)
}

func TestLoadReportsAllMissingRequiredValuesAtOnce(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")
	t.Setenv("PROVIDER_URL", "")

	_, err := Load()
	require.Error(t, err)

	// Tek seferde tüm eksikler bildirilir; kullanıcı her denemede bir hata görmez.
	require.Contains(t, err.Error(), "DATABASE_URL")
	require.Contains(t, err.Error(), "REDIS_URL")
	require.Contains(t, err.Error(), "PROVIDER_URL")
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("LOG_LEVEL", "verbose")
	t.Setenv("SHUTDOWN_TIMEOUT", "yarım saat")

	_, err := Load()
	require.Error(t, err)
	require.Contains(t, err.Error(), "LOG_LEVEL")
	require.Contains(t, err.Error(), "SHUTDOWN_TIMEOUT")
}

func TestLoadReadsOverrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("SHUTDOWN_TIMEOUT", "30s")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, "production", cfg.AppEnv)
	require.Equal(t, "debug", cfg.LogLevel)
	require.Equal(t, "9090", cfg.HTTPPort)
	require.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
}

func TestLoadRequestLimitDefaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := Load()
	require.NoError(t, err)

	require.EqualValues(t, 128<<10, cfg.BodyLimitSingle, "tekil uç varsayılanı 128 KiB olmalı")
	require.EqualValues(t, 16<<20, cfg.BodyLimitBatch, "batch varsayılanı 16 MiB olmalı")
	require.Equal(t, 24*time.Hour, cfg.IdempotencyTTL)
}

func TestLoadReadsRequestLimitOverrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("BODY_LIMIT_SINGLE", "4096")
	t.Setenv("BODY_LIMIT_BATCH", "1048576")
	t.Setenv("IDEMPOTENCY_TTL", "1h")

	cfg, err := Load()
	require.NoError(t, err)

	require.EqualValues(t, 4096, cfg.BodyLimitSingle)
	require.EqualValues(t, 1048576, cfg.BodyLimitBatch)
	require.Equal(t, time.Hour, cfg.IdempotencyTTL)
}

func TestLoadRejectsInvalidRequestLimits(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "sayı değil", key: "BODY_LIMIT_SINGLE", value: "128KB"},
		{name: "sıfır", key: "BODY_LIMIT_SINGLE", value: "0"},
		{name: "negatif", key: "BODY_LIMIT_BATCH", value: "-1"},
		{name: "geçersiz süre", key: "IDEMPOTENCY_TTL", value: "bir gün"},
		{name: "negatif süre", key: "IDEMPOTENCY_TTL", value: "-1h"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(test.key, test.value)

			_, err := Load()
			require.Error(t, err)
			require.Contains(t, err.Error(), test.key)
		})
	}
}

// TestLoadAppliesWorkerDefaults, worker ayarlarının varsayılanlarını sabitler.
//
// Varsayılanlar birbirine bağlı: WORKER_CONCURRENCY tutulan lease sayısının üst
// sınırı ve lease süresi (PROVIDER + RESULT) penceresinin iki katını kapsamalı.
// worker.New bu tutarlılığı ayrıca doğruluyor, buradaki test değerlerin kaymadığını
// garanti ediyor.
func TestLoadAppliesWorkerDefaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, 20, cfg.WorkerConcurrency)
	require.Equal(t, 20, cfg.WorkerClaimBatchSize)
	require.Equal(t, 60*time.Second, cfg.WorkerLeaseDuration)
	require.Equal(t, 50*time.Millisecond, cfg.WorkerPollMinInterval)
	require.Equal(t, 2*time.Second, cfg.WorkerPollMaxInterval)
	require.Equal(t, 10*time.Second, cfg.WorkerProviderTimeout)
	require.Equal(t, 3*time.Second, cfg.WorkerResultTimeout)
	require.Equal(t, 20*time.Second, cfg.WorkerDrainTimeout)
	require.Equal(t, 5*time.Second, cfg.WorkerRetryDelay)
	require.Equal(t, 10*time.Second, cfg.WorkerReaperInterval)
	require.Equal(t, 100, cfg.WorkerReaperBatchSize)
	require.Equal(t, 16, cfg.WorkerDBMaxConns)
	require.Equal(t, time.Hour, cfg.IdempotencyCleanupInterval)
	require.Equal(t, 1000, cfg.IdempotencyCleanupBatch)

	// Varsayılanlar worker.New'in şart koştuğu eşitsizlikleri sağlamalı.
	window := cfg.WorkerProviderTimeout + cfg.WorkerResultTimeout
	require.GreaterOrEqual(t, cfg.WorkerLeaseDuration, 2*window,
		"varsayılan lease, gönderim penceresinin iki katını kapsamalı")
	require.GreaterOrEqual(t, cfg.WorkerDrainTimeout, window,
		"varsayılan kapanış süresi gönderim penceresini kapsamalı")
}

// TestLoadReadsWorkerOverrides, worker değişkenlerinin okunduğunu doğrular.
func TestLoadReadsWorkerOverrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WORKER_CONCURRENCY", "8")
	t.Setenv("WORKER_CLAIM_BATCH_SIZE", "50")
	t.Setenv("WORKER_LEASE_DURATION", "90s")
	t.Setenv("WORKER_DRAIN_TIMEOUT", "45s")
	t.Setenv("WORKER_DB_MAX_CONNS", "32")
	t.Setenv("IDEMPOTENCY_CLEANUP_BATCH", "500")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, 8, cfg.WorkerConcurrency)
	require.Equal(t, 50, cfg.WorkerClaimBatchSize)
	require.Equal(t, 90*time.Second, cfg.WorkerLeaseDuration)
	require.Equal(t, 45*time.Second, cfg.WorkerDrainTimeout)
	require.Equal(t, 32, cfg.WorkerDBMaxConns)
	require.Equal(t, 500, cfg.IdempotencyCleanupBatch)
}

// TestLoadRejectsInvalidWorkerValues, geçersiz worker ayarının tek tek değil topluca
// yakalandığını doğrular.
func TestLoadRejectsInvalidWorkerValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "eşzamanlılık sayı değil", key: "WORKER_CONCURRENCY", value: "yirmi"},
		{name: "eşzamanlılık sıfır", key: "WORKER_CONCURRENCY", value: "0"},
		{name: "parti negatif", key: "WORKER_CLAIM_BATCH_SIZE", value: "-5"},
		{name: "lease geçersiz süre", key: "WORKER_LEASE_DURATION", value: "bir dakika"},
		{name: "drain negatif", key: "WORKER_DRAIN_TIMEOUT", value: "-20s"},
		{name: "temizlik partisi sıfır", key: "IDEMPOTENCY_CLEANUP_BATCH", value: "0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(test.key, test.value)

			_, err := Load()
			require.Error(t, err)
			require.Contains(t, err.Error(), test.key)
		})
	}
}

// TestLoadAppliesRateLimitDefaults, hız sınırı varsayılanlarını sabitler.
//
// Ayrıca kritik bir değişmezi doğruluyor: varsayılanlar worker.New'in şart koştuğu
// lease eşitsizliğini sağlamalı. İzin beklemesi satır lease altındayken geçtiği için
// lease penceresi üç terimli: rate + provider + result.
func TestLoadAppliesRateLimitDefaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, 100, cfg.RateLimitPerSecond)
	require.Equal(t, 10*time.Second, cfg.WorkerRateWaitMax)

	leaseWindow := cfg.WorkerRateWaitMax + cfg.WorkerProviderTimeout + cfg.WorkerResultTimeout
	require.Equal(t, 23*time.Second, leaseWindow)
	require.GreaterOrEqual(t, cfg.WorkerLeaseDuration, 2*leaseWindow,
		"varsayılan lease, izin beklemesi dâhil pencerenin iki katını kapsamalı")

	drainWindow := cfg.WorkerProviderTimeout + cfg.WorkerResultTimeout
	require.GreaterOrEqual(t, cfg.WorkerDrainTimeout, drainWindow,
		"kapanış penceresi gönderim + sonuç yazımını kapsamalı")
}

// TestLoadReadsRateLimitOverrides, hız sınırı değişkenlerinin okunduğunu doğrular.
func TestLoadReadsRateLimitOverrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RATE_LIMIT_PER_SECOND", "250")
	t.Setenv("WORKER_RATE_WAIT_MAX", "4s")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, 250, cfg.RateLimitPerSecond)
	require.Equal(t, 4*time.Second, cfg.WorkerRateWaitMax)
}

// TestLoadRejectsInvalidRateLimitValues, geçersiz hız sınırı ayarlarının
// yakalandığını doğrular.
//
// Sıfır da reddediliyor: "limitsiz" bir mod yok, çünkü limitsiz çalışmak gerçek bir
// sağlayıcıyı anında 429'a sokardı.
func TestLoadRejectsInvalidRateLimitValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "sınır sıfır", key: "RATE_LIMIT_PER_SECOND", value: "0"},
		{name: "sınır negatif", key: "RATE_LIMIT_PER_SECOND", value: "-100"},
		{name: "sınır sayı değil", key: "RATE_LIMIT_PER_SECOND", value: "yüz"},
		{name: "bekleme geçersiz", key: "WORKER_RATE_WAIT_MAX", value: "on saniye"},
		{name: "bekleme negatif", key: "WORKER_RATE_WAIT_MAX", value: "-10s"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(test.key, test.value)

			_, err := Load()
			require.Error(t, err)
			require.Contains(t, err.Error(), test.key)
		})
	}
}
