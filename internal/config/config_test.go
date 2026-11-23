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
