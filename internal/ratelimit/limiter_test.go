package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// Bu dosyadaki testler Redis gerektirmez ve -short altında da koşar.
// Gerçek Redis'e karşı testler redis_test.go içinde.

// TestUnlimitedAlwaysAllows, test limiter'ının her zaman izin verdiğini doğrular.
func TestUnlimitedAlwaysAllows(t *testing.T) {
	t.Parallel()

	limiter := Unlimited()

	for _, channel := range []domain.Channel{
		domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush,
	} {
		for range 1000 {
			decision, err := limiter.Allow(context.Background(), channel)

			require.NoError(t, err)
			require.True(t, decision.OK)
			require.Zero(t, decision.RetryAfter)
		}
	}
}

// TestNormalizeRetryAfterTreatsNonPositiveAsZero, kütüphanenin "geçerli değil"
// işaretinin temizlendiğini doğrular.
//
// redis_rate izin verdiğinde RetryAfter'ı -1 döndürüyor; negatif bir süre beklemeye
// çalışmak anlamsız olurdu. Çağıran tek kuralla çalışıyor: 0 = rehber yok.
func TestNormalizeRetryAfterTreatsNonPositiveAsZero(t *testing.T) {
	t.Parallel()

	require.Zero(t, normalizeRetryAfter(-1))
	require.Zero(t, normalizeRetryAfter(-time.Hour))
	require.Zero(t, normalizeRetryAfter(0))

	require.Equal(t, 10*time.Millisecond, normalizeRetryAfter(10*time.Millisecond))
}

// TestNewRejectsInvalidConfig, hatalı kurulumun boot'ta yakalandığını doğrular.
func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	_, err := New(nil, 100, DefaultKeyPrefix)
	require.Error(t, err, "istemci olmadan limiter kurulmamalı")

	for _, perSecond := range []int{0, -1} {
		_, err = New(client, perSecond, DefaultKeyPrefix)
		require.Error(t, err, "saniyedeki izin: %d", perSecond)
	}

	_, err = New(client, 100, "   ")
	require.Error(t, err, "boş önek bütün kanalları tek kovaya düşürürdü")
}

// TestNewUsesBurstOfOne, gereksinimin kodda sabitlendiğini doğrular.
//
// redis_rate.PerSecond(n) burst'ü n yapıyor ve ilk saniyede ~2n mesaj geçirirdi;
// "kanal başına en fazla 100/sn" bu yüzden ihlal olurdu. Bu test o regresyonu kapatır.
func TestNewUsesBurstOfOne(t *testing.T) {
	t.Parallel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	limiter, err := New(client, 100, DefaultKeyPrefix)
	require.NoError(t, err)

	require.Equal(t, 1, limiter.limit.Burst, "burst 1 olmalı: tepe asla rate'i aşmasın")
	require.Equal(t, 100, limiter.limit.Rate)
	require.Equal(t, time.Second, limiter.limit.Period)
}

// TestKeySeparatesChannels, her kanalın ayrı kovaya düştüğünü doğrular.
//
// Anahtarlar aynı olsaydı SMS yoğunluğu e-posta gönderimini durdururdu.
func TestKeySeparatesChannels(t *testing.T) {
	t.Parallel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	limiter, err := New(client, 100, "test-prefix")
	require.NoError(t, err)

	require.Equal(t, "test-prefix:sms", limiter.key(domain.ChannelSMS))
	require.Equal(t, "test-prefix:email", limiter.key(domain.ChannelEmail))
	require.Equal(t, "test-prefix:push", limiter.key(domain.ChannelPush))
}

// TestAllowReturnsErrorWhenRedisUnavailable, Redis'e ulaşılamadığında hata döndüğünü
// doğrular — fail-closed davranışının girdisi.
//
// Çağıran bu hatada GÖNDERMEMELİ: sınırın aşılmadığından emin olunamaz.
func TestAllowReturnsErrorWhenRedisUnavailable(t *testing.T) {
	t.Parallel()

	// Dinlemeyen bir adres: bağlantı kurulamaz.
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 100 * time.Millisecond,
	})
	t.Cleanup(func() { _ = client.Close() })

	limiter, err := New(client, 100, DefaultKeyPrefix)
	require.NoError(t, err)

	decision, err := limiter.Allow(context.Background(), domain.ChannelSMS)

	require.Error(t, err)
	require.False(t, decision.OK, "hata hâlinde izin verilmiş görünmemeli")
}
