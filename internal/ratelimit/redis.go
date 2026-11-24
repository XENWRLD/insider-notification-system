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
// keyPrefix parametre olarak alınıyor: testler her test için ayrı bir önek kullanıp
// birbirinin kovasını tüketmesin. Üretimde DefaultKeyPrefix geçilir.
func New(client redis.UniversalClient, perSecond int, keyPrefix string) (*RedisLimiter, error) {
	if client == nil {
		return nil, errors.New("rate limiter Redis istemcisi olmadan kurulamaz")
	}

	if perSecond <= 0 {
		return nil, fmt.Errorf("saniyedeki izin sayısı pozitif olmalı: %d", perSecond)
	}

	if strings.TrimSpace(keyPrefix) == "" {
		return nil, errors.New("rate limiter anahtar öneki boş olamaz")
	}

	return &RedisLimiter{
		limiter: redis_rate.NewLimiter(client),

		// Burst = 1, redis_rate.PerSecond(n) DEĞİL.
		//
		// PerSecond(n) burst'ü n yapıyor ve bu "kanal başına en fazla 100/sn"
		// gereksinimini ihlal ediyor: boşta kalmış bir kova 100 mesajı anında verir,
		// ardından aynı saniye içinde ~100 daha akar → ilk saniyede ~200 mesaj.
		//
		// Burst = 1 ile GCRA'nın emisyon aralığı Period/Rate = 10 ms ve tolerans
		// sıfır: her 10 ms'de en çok bir izin, düzgün akış. Toplam hız aynı kalıyor
		// (1000 mesaj ≈ 10 sn) ama tepe asla aşılmıyor.
		//
		// Bedeli: tolerans sıfır olduğu için kaçırılan bir 10 ms'lik yuva geri
		// gelmiyor, yani gerçekleşen hız 100/sn'nin bir tık altında kalır. Hata yönü
		// güvenli taraf.
		limit: redis_rate.Limit{
			Rate:   perSecond,
			Burst:  1,
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
