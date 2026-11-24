package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// Bu dosyadaki testler gerçek Redis'e karşı koşar (bkz. main_test.go).
//
// Zamanlamaya dayanmamaları için iki kural: (1) kova dolumunu beklemek yerine tek
// pencere içinde sayılır, (2) iddialar yalnızca ÜST SINIR'dır — yavaş bir makinede
// beklenenden az izin çıkması testi kırmaz.

// TestAllowGrantsAtMostBurstBeforeEmissionInterval, burst'ün tolerans penceresi
// olduğunu doğrular: boş kovadan ardı ardına EN ÇOK burst kadar izin çıkar.
//
// Burst kütüphane varsayılanına (= rate) bırakılsaydı yüzlerce izin üst üste
// verilirdi ve ilk saniyede ~2 x rate mesaj geçerdi.
func TestAllowGrantsAtMostBurstBeforeEmissionInterval(t *testing.T) {
	t.Parallel()

	// Hız bilinçli olarak DÜŞÜK: emisyon aralığı Period/internalRate olduğu için
	// 10/sn ve burst 5'te ~166 ms. Döngünün kendi süresi (birkaç Redis gidiş-dönüşü,
	// ~10 ms) bunun çok altında kalıyor, yani ölçüm sırasında yeni yuva AÇILMIYOR.
	//
	// 100/sn kullanılsaydı emisyon aralığı ~10 ms olur, döngü süresiyle aynı mertebeye
	// düşer ve test makine hızına göre ara sıra kalırdı (gözlendi: 7 izin).
	const perSecond = 10

	for _, burst := range []int{1, 5} {
		t.Run(fmt.Sprintf("burst-%d", burst), func(t *testing.T) {
			t.Parallel()

			limiter := newLimiterWithBurst(t, perSecond, burst)
			ctx := context.Background()

			granted := 0

			for range burst * 4 {
				decision, err := limiter.Allow(ctx, domain.ChannelSMS)
				require.NoError(t, err)

				if decision.OK {
					granted++
				}
			}

			require.Positive(t, granted)
			require.LessOrEqual(t, granted, burst+1,
				"tolerans burst kadar olmali, verilen: %d", granted)
		})
	}
}

// TestAllowStaysUnderRateInOneSecondWindow, asıl gereksinimi doğrular: boş kovayla
// bir saniyelik pencerede en çok RATE kadar izin verilir — burst ne olursa olsun.
//
// İç hız burst kadar geri alındığı için (internalRate = rate - burst + 1) en kötü
// pencere tam rate'e eşit. Bu hesap yapılmasa burst 5 ile pencere 104 çıkardı ve
// "kanal başına en fazla 100/sn" sözü tutulmamış olurdu.
//
// Yalnızca UST SINIR iddia ediliyor: yavaş bir makinede az izin çıkması testi kırmaz.
func TestAllowStaysUnderRateInOneSecondWindow(t *testing.T) {
	t.Parallel()

	// Düşük rate: emisyon aralığı 50 ms, pencere içinde ~20 izin beklenir.
	const (
		perSecond = 20
		burst     = 5
	)

	limiter := newLimiterWithBurst(t, perSecond, burst)
	ctx := context.Background()

	granted := 0
	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		decision, err := limiter.Allow(ctx, domain.ChannelSMS)
		require.NoError(t, err)

		if decision.OK {
			granted++
		}
	}

	require.LessOrEqual(t, granted, perSecond,
		"bir saniyede en cok rate izin verilmeli, verilen: %d", granted)
	require.Positive(t, granted, "hic izin verilmemesi de hata olurdu")
}

// TestAllowReportsRetryAfterWhenExhausted, reddedilen talebin bekleme rehberi
// döndürdüğünü doğrular.
//
// Tam değer değil aralık kontrol ediliyor: GCRA'nın hesabı mikrosaniye hassasiyetinde
// ve makine hızına göre oynar. Üst sınır emisyon aralığı (1/rate), çünkü testBurst = 1
// ile bir sonraki yuva en çok o kadar uzakta.
func TestAllowReportsRetryAfterWhenExhausted(t *testing.T) {
	t.Parallel()

	const perSecond = 100

	limiter := newLimiter(t, perSecond)
	ctx := context.Background()

	_, err := limiter.Allow(ctx, domain.ChannelSMS)
	require.NoError(t, err)

	denied, err := limiter.Allow(ctx, domain.ChannelSMS)
	require.NoError(t, err)
	require.False(t, denied.OK)

	require.Positive(t, denied.RetryAfter, "reddedilende bekleme rehberi dolmalı")
	require.LessOrEqual(t, denied.RetryAfter, rateFor(perSecond),
		"bekleme emisyon aralığını aşmamalı")
}

// TestAllowNormalizesRetryAfterWhenGranted, izin verildiğinde kütüphanenin -1
// işaretinin dışarı sızmadığını doğrular.
func TestAllowNormalizesRetryAfterWhenGranted(t *testing.T) {
	t.Parallel()

	limiter := newLimiter(t, 100)

	decision, err := limiter.Allow(context.Background(), domain.ChannelSMS)

	require.NoError(t, err)
	require.True(t, decision.OK)
	require.Zero(t, decision.RetryAfter, "izin verildiğinde bekleme rehberi 0 olmalı")
}

// TestAllowIsolatesChannels, kanal izolasyonunu doğrular: SMS kovası tükenmişken
// e-posta izin almaya devam eder.
//
// PLAN §9'daki "Kanal izolasyonu: SMS yoğunluğu email gecikmesine yol açmaz"
// satırının Redis tarafındaki kanıtı. Anahtarlar aynı olsaydı bu test kırılırdı.
func TestAllowIsolatesChannels(t *testing.T) {
	t.Parallel()

	limiter := newLimiter(t, 100)
	ctx := context.Background()

	// SMS kovası boşaltılıyor.
	granted := drainBucket(t, limiter, domain.ChannelSMS, 1000)
	require.Positive(t, granted)

	smsAgain, err := limiter.Allow(ctx, domain.ChannelSMS)
	require.NoError(t, err)
	require.False(t, smsAgain.OK, "SMS kovası boş olmalı")

	// E-posta ve push kovaları etkilenmemiş olmalı.
	for _, channel := range []domain.Channel{domain.ChannelEmail, domain.ChannelPush} {
		decision, err := limiter.Allow(ctx, channel)

		require.NoError(t, err)
		require.True(t, decision.OK, "%s kovası SMS'ten etkilenmemeli", channel)
	}
}

// TestAllowSharesBucketAcrossLimiterInstances, sınırın process'e değil Redis'teki
// kovaya ait olduğunu doğrular.
//
// `docker compose up -d --scale worker=3` semantiği tam olarak bu: üç ayrı worker
// kopyası üç ayrı limiter nesnesi kurar ama aynı anahtarı tüketir, dolayısıyla
// toplam hız değişmez. Yerel bir limiter bu testi geçemezdi.
func TestAllowSharesBucketAcrossLimiterInstances(t *testing.T) {
	t.Parallel()

	prefix := keyPrefixFor(t)
	client := sharedClient(t)
	ctx := context.Background()

	first, err := New(client, 100, testBurst, prefix)
	require.NoError(t, err)

	second, err := New(client, 100, testBurst, prefix)
	require.NoError(t, err)

	// Birinci kopya izni alıyor.
	decision, err := first.Allow(ctx, domain.ChannelSMS)
	require.NoError(t, err)
	require.True(t, decision.OK)

	// İkinci kopya aynı kovayı görüyor: izin kalmadı.
	decision, err = second.Allow(ctx, domain.ChannelSMS)
	require.NoError(t, err)
	require.False(t, decision.OK,
		"ikinci kopya aynı kovayı tüketmeli; sınır process'e değil Redis'e ait")
}

// TestAllowSeparatesBucketsByPrefix, farklı öneklerin birbirini etkilemediğini
// doğrular — testlerin paralel koşabilmesinin dayanağı.
func TestAllowSeparatesBucketsByPrefix(t *testing.T) {
	t.Parallel()

	client := sharedClient(t)
	ctx := context.Background()

	first, err := New(client, 100, testBurst, keyPrefixFor(t)+":a")
	require.NoError(t, err)

	second, err := New(client, 100, testBurst, keyPrefixFor(t)+":b")
	require.NoError(t, err)

	decision, err := first.Allow(ctx, domain.ChannelSMS)
	require.NoError(t, err)
	require.True(t, decision.OK)

	decision, err = second.Allow(ctx, domain.ChannelSMS)
	require.NoError(t, err)
	require.True(t, decision.OK, "ayrı önek ayrı kova demeli")
}
