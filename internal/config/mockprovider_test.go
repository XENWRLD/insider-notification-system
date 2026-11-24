package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLoadMockProviderAppliesDefaults, hiçbir değişken verilmediğinde mock'un
// ayağa kalkabildiğini doğrular.
//
// Önemli olan şu: LoadMockProvider, Load'un zorunlu kıldığı DATABASE_URL /
// REDIS_URL / PROVIDER_URL'ü istemiyor — mock'un veritabanıyla işi yok.
func TestLoadMockProviderAppliesDefaults(t *testing.T) {
	cfg, err := LoadMockProvider()

	require.NoError(t, err, "mock sağlayıcı veritabanı değişkenleri olmadan açılmalı")
	require.Equal(t, "8081", cfg.Port)
	require.Equal(t, "info", cfg.LogLevel)
	require.Zero(t, cfg.FailureRate)
	require.Zero(t, cfg.RateLimitRate)
	require.Zero(t, cfg.BadRequestRate)
	require.Equal(t, 50*time.Millisecond, cfg.Latency)
	require.Zero(t, cfg.LatencyJitter)
}

// TestLoadMockProviderReadsOverrides, tüm değişkenlerin okunduğunu doğrular.
func TestLoadMockProviderReadsOverrides(t *testing.T) {
	t.Setenv("MOCK_PORT", "9090")
	t.Setenv("MOCK_FAILURE_RATE", "0.3")
	t.Setenv("MOCK_RATE_LIMIT_RATE", "0.1")
	t.Setenv("MOCK_BAD_REQUEST_RATE", "0.05")
	t.Setenv("MOCK_LATENCY", "250ms")
	t.Setenv("MOCK_LATENCY_JITTER", "20ms")

	cfg, err := LoadMockProvider()

	require.NoError(t, err)
	require.Equal(t, "9090", cfg.Port)
	require.InDelta(t, 0.3, cfg.FailureRate, 0.0001)
	require.InDelta(t, 0.1, cfg.RateLimitRate, 0.0001)
	require.InDelta(t, 0.05, cfg.BadRequestRate, 0.0001)
	require.Equal(t, 250*time.Millisecond, cfg.Latency)
	require.Equal(t, 20*time.Millisecond, cfg.LatencyJitter)
}

// TestLoadMockProviderAcceptsZeroRates, sıfırın geçerli bir oran olduğunu doğrular:
// "hata üretme" meşru bir ayar, envInt'in pozitiflik şartı burada geçerli değil.
func TestLoadMockProviderAcceptsZeroRates(t *testing.T) {
	t.Setenv("MOCK_FAILURE_RATE", "0")
	t.Setenv("MOCK_LATENCY_JITTER", "0s")

	cfg, err := LoadMockProvider()

	require.NoError(t, err)
	require.Zero(t, cfg.FailureRate)
	require.Zero(t, cfg.LatencyJitter)
}

// TestLoadMockProviderRejectsInvalidValues, aralık dışı oranın yakalandığını doğrular.
//
// 0.3 yerine 30 yazmak sessizce "her istek başarısız" demek olurdu ve yük testinin
// sonucu yanıltıcı çıkardı.
func TestLoadMockProviderRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "oran 1'den büyük", key: "MOCK_FAILURE_RATE", value: "30"},
		{name: "oran negatif", key: "MOCK_RATE_LIMIT_RATE", value: "-0.1"},
		{name: "oran sayı değil", key: "MOCK_BAD_REQUEST_RATE", value: "yüzde otuz"},
		{name: "gecikme geçersiz", key: "MOCK_LATENCY", value: "yarım saniye"},
		{name: "jitter negatif", key: "MOCK_LATENCY_JITTER", value: "-5ms"},
		{name: "jitter geçersiz", key: "MOCK_LATENCY_JITTER", value: "biraz"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.key, test.value)

			_, err := LoadMockProvider()

			require.Error(t, err)
			require.Contains(t, err.Error(), test.key)
		})
	}
}

// TestLoadMockProviderCollectsAllProblems, hataların tek yanıtta toplandığını
// doğrular (Load ile aynı kalıp).
func TestLoadMockProviderCollectsAllProblems(t *testing.T) {
	t.Setenv("MOCK_FAILURE_RATE", "5")
	t.Setenv("MOCK_LATENCY", "çok")
	t.Setenv("LOG_LEVEL", "verbose")

	_, err := LoadMockProvider()

	require.Error(t, err)
	require.Contains(t, err.Error(), "MOCK_FAILURE_RATE")
	require.Contains(t, err.Error(), "MOCK_LATENCY")
	require.Contains(t, err.Error(), "LOG_LEVEL")
}

// TestEnvIntRejectsInvalidValues, worker sayaçlarında kullanılacak yardımcıyı sabitler.
func TestEnvIntRejectsInvalidValues(t *testing.T) {
	t.Setenv("TEST_COUNT", "20")
	value, err := envInt("TEST_COUNT", 5)
	require.NoError(t, err)
	require.Equal(t, 20, value)

	for _, raw := range []string{"yirmi", "0", "-1", "1.5"} {
		t.Setenv("TEST_COUNT", raw)

		_, err := envInt("TEST_COUNT", 5)
		require.Error(t, err, "değer: %q", raw)
		require.Contains(t, err.Error(), "TEST_COUNT")
	}

	t.Setenv("TEST_COUNT", "")
	value, err = envInt("TEST_COUNT", 5)
	require.NoError(t, err)
	require.Equal(t, 5, value, "boş değişken varsayılana düşmeli")
}
