package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// DefaultKeyPrefix, üretimde kullanılan Redis anahtar öneki.
//
// Anahtar `<önek>:<kanal>` biçiminde: kanallar ayrı kovalara düşer, böylece SMS
// kovası tükendiğinde e-posta gönderimi etkilenmez.
const DefaultKeyPrefix = "notifications:rate"

// RedisLimiter, Redis üzerinde paylaşılan bir kovayla hız sınırı uygular.
type RedisLimiter struct {
	limiter *redis_rate.Limiter
	limit   redis_rate.Limit
	prefix  string
}

// New, verilen Redis istemcisi üzerinde saniyede perSecond izin veren bir limiter kurar.
//
// burst, GCRA'nın tolerans penceresi: kovanın biriktirebileceği en fazla izin sayısı.
// redis_rate.PerSecond(n) kullanılmıyor çünkü o burst'ü n yapıyor; gerekçe ve doğru
// değerin nasıl seçileceği aşağıda.
//
// keyPrefix parametre olarak alınıyor: testler her test için ayrı bir önek kullanıp
// birbirinin kovasını tüketmesin. Üretimde DefaultKeyPrefix geçilir.
func New(
	client redis.UniversalClient, perSecond, burst int, keyPrefix string,
) (*RedisLimiter, error) {
	if client == nil {
		return nil, errors.New("rate limiter Redis istemcisi olmadan kurulamaz")
	}

	if perSecond <= 0 {
		return nil, fmt.Errorf("saniyedeki izin sayısı pozitif olmalı: %d", perSecond)
	}

	// burst > perSecond iç hızı sıfıra ya da negatife düşürürdü (aşağıdaki
	// internalRate hesabı). Sınırda (burst = perSecond) iç hız 1 olur: geçerli ama
	// kullanılabilir kapasite kalmaz, bu yüzden kabul edilip belgeleniyor.
	if burst < 1 || burst > perSecond {
		return nil, fmt.Errorf(
			"burst 1 ile saniyedeki izin sayısı (%d) arasında olmalı: %d", perSecond, burst)
	}

	if strings.TrimSpace(keyPrefix) == "" {
		return nil, errors.New("rate limiter anahtar öneki boş olamaz")
	}

	return &RedisLimiter{
		limiter: redis_rate.NewLimiter(client),

		// perSecond, "HERHANGİ BİR 1 SANİYELİK PENCEREDE EN FAZLA" demek.
		//
		// GCRA'da bir saniyelik pencerede en fazla Rate + (Burst-1) izin çıkar:
		// Rate düzgün akışı, Burst ise akıştan ne kadar sapmaya izin verildiğini
		// (tolerans penceresini) belirliyor. Dolayısıyla kütüphaneye perSecond'ı
		// doğrudan vermek en kötü durumda sınırı burst-1 kadar AŞARDI.
		//
		// Bu yüzden iç hız geri hesaplanıyor:
		//
		//     internalRate = perSecond - (burst - 1)
		//     en kötü pencere = internalRate + (burst - 1) = perSecond   ✓
		//
		// 100/sn ve burst 5 için iç hız 96, en kötü pencere tam 100.
		//
		// BURST NEDEN AYARLANABİLİR (ölçümle seçildi):
		//
		//   Burst = Rate (redis_rate.PerSecond'ın yaptığı): iç hız 1'e düşer,
		//     kullanılabilir kapasite yok olur.
		//
		//   Burst = 1: tolerans sıfır, iç hız = perSecond. Her 10 ms'de en çok bir
		//     izin, ama o 10 ms içinde talep edilmeyen yuva GERİ GELMİYOR. Sender'lar
		//     jitter'lı katlanan beklemeyle yoklama yaptığı için yuvaların bir kısmı
		//     kaçıyor: ölçümde 1000 mesaj 14 sn, gerçekleşen hız ~80/sn (%20 kayıp).
		//
		//   Burst = 5 (varsayılan): 40 ms tolerans, geç gelen sender kaçan yuvayı
		//     yakalayabiliyor. İç hız 96 olduğu için kapasitenin yalnızca %4'ü
		//     kullanılmıyor ve en kötü pencere sınırı AŞMIYOR.
		limit: redis_rate.Limit{
			Rate:   perSecond - (burst - 1),
			Burst:  burst,
			Period: time.Second,
		},

		prefix: keyPrefix,
	}, nil
}

// Allow, kanaldan bir gönderim izni ister.
//
// Hata döndüğünde çağıran GÖNDERMEMELİ: sınırın aşılmadığından emin olunamaz
// (fail closed). Çağıran bunu yeniden deneyip sonunda işi kuyruğa geri verir.
func (l *RedisLimiter) Allow(ctx context.Context, channel domain.Channel) (Decision, error) {
	// AllowN(1), AllowAtMost yerine: kısmi izin diye bir şey yok, tek izin ya verilir
	// ya verilmez. Reddedildiğinde RetryAfter doluyor.
	result, err := l.limiter.AllowN(ctx, l.key(channel), l.limit, 1)
	if err != nil {
		return Decision{}, fmt.Errorf("gönderim izni alınamadı (%s): %w", channel, err)
	}

	return Decision{
		OK:         result.Allowed > 0,
		RetryAfter: normalizeRetryAfter(result.RetryAfter),
	}, nil
}

// key, kanalın kova anahtarını üretir.
func (l *RedisLimiter) key(channel domain.Channel) string {
	return l.prefix + ":" + string(channel)
}
