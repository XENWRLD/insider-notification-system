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

	_, err := New(nil, 100, testBurst, DefaultKeyPrefix)
	require.Error(t, err, "istemci olmadan limiter kurulmamalı")

	for _, perSecond := range []int{0, -1} {
		_, err = New(client, perSecond, testBurst, DefaultKeyPrefix)
		require.Error(t, err, "saniyedeki izin: %d", perSecond)
	}

	// burst < 1 toleransi anlamsiz kilar; burst > rate ise kovanin bir saniyede
	// uretilenden fazlasini biriktirmesi demek olurdu.
	for _, burst := range []int{0, -1, 101} {
		_, err = New(client, 100, burst, DefaultKeyPrefix)
		require.Error(t, err, "burst: %d", burst)
	}

	_, err = New(client, 100, 100, DefaultKeyPrefix)
	require.NoError(t, err, "burst = rate sinirda kabul edilmeli")

	_, err = New(client, 100, testBurst, "   ")
	require.Error(t, err, "boş önek bütün kanalları tek kovaya düşürürdü")
}

// TestNewDerivesInternalRateFromBurst, "en fazla perSecond/sn" sözünün kodda
// sabitlendiğini doğrular.
//
// GCRA'da bir saniyelik pencerede en fazla Rate + (Burst-1) izin çıkar. perSecond'ı
// kütüphaneye doğrudan vermek sınırı burst-1 kadar AŞARDI (burst 5 ile 104). İç hız
// geri hesaplanıyor:
//
//	internalRate   = perSecond - (burst - 1)
//	en kötü pencere = internalRate + (burst - 1) = perSecond
func TestNewDerivesInternalRateFromBurst(t *testing.T) {
	t.Parallel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	const perSecond = 100

	for _, burst := range []int{1, 5, 20, perSecond} {
		limiter, err := New(client, perSecond, burst, DefaultKeyPrefix)
		require.NoError(t, err)

		require.Equal(t, burst, limiter.limit.Burst,
			"burst çağıranın verdiği değer olmalı, kütüphane varsayılanı değil")
		require.Equal(t, perSecond-(burst-1), limiter.limit.Rate,
			"iç hız burst kadar geri alınmalı")
		require.Equal(t, time.Second, limiter.limit.Period)

		// Asıl söz: en kötü bir saniyelik pencere sınırı aşmıyor.
		require.Equal(t, perSecond, limiter.limit.Rate+(limiter.limit.Burst-1),
			"en kötü pencere tam perSecond olmalı")

		require.Positive(t, limiter.limit.Rate, "iç hız pozitif kalmalı")
	}
}

// TestKeySeparatesChannels, her kanalın ayrı kovaya düştüğünü doğrular.
//
// Anahtarlar aynı olsaydı SMS yoğunluğu e-posta gönderimini durdururdu.
func TestKeySeparatesChannels(t *testing.T) {
	t.Parallel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	limiter, err := New(client, 100, testBurst, "test-prefix")
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

	limiter, err := New(client, 100, testBurst, DefaultKeyPrefix)
	require.NoError(t, err)

	decision, err := limiter.Allow(context.Background(), domain.ChannelSMS)

	require.Error(t, err)
	require.False(t, decision.OK, "hata hâlinde izin verilmiş görünmemeli")
}
