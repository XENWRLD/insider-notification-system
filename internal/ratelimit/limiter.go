// Package ratelimit, kanal başına gönderim hızını sınırlar.
//
// Sınır tek bir worker'ın değil, Redis'te paylaşılan bir kovanın özelliğidir:
// `docker compose up -d --scale worker=3` ile üç kopya çalışsa da hepsi aynı anahtarı
// tüketir, dolayısıyla kanal başına toplam hız değişmez. Yerel (process içi) bir
// limiter bunu yapamazdı — üç kopya üç ayrı limit uygulardı.
package ratelimit

import (
	"context"
	"time"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// Decision, tek bir izin talebinin sonucu.
type Decision struct {
	OK bool

	// RetryAfter yalnızca OK false iken anlamlı: bir iznin açılacağı ana kadarki süre.
	// 0 ise sağlayıcı rehberlik vermedi ve çağıran kendi tabanını kullanır.
	RetryAfter time.Duration
}

// Limiter, kanal başına tek bir gönderim izni verir.
//
// Tek izin veriliyor, toplu değil: izin mesaj başına, gönderimden hemen önce
// alınıyor (1 izin = 1 gerçek gönderim). Toplu bir API, kaç mesaj gönderileceği
// bilinmeden izin ayırmak demek olurdu ve kullanılmayan izinler boşa yanardı.
//
// Arayüz olarak alınıyor ki worker testleri Redis'siz koşabilsin.
type Limiter interface {
	Allow(ctx context.Context, channel domain.Channel) (Decision, error)
}

// unlimited, her zaman izin veren limiter.
type unlimited struct{}

// Unlimited, hız sınırı uygulamayan bir limiter döner.
//
// Testler için var: worker'ın rate limit dışındaki davranışını ölçen testler Redis
// kurmak zorunda kalmasın. Üretimde kullanılmaz — `RATE_LIMIT_PER_SECOND` zorunlu ve
// pozitif, yani limitsiz çalışmak yapılandırmayla ifade edilemiyor.
func Unlimited() Limiter {
	return unlimited{}
}

func (unlimited) Allow(context.Context, domain.Channel) (Decision, error) {
	return Decision{OK: true}, nil
}

// normalizeRetryAfter, kütüphaneden gelen süreyi çağıranın beklediği biçime çevirir.
//
// redis_rate izin verdiğinde RetryAfter'ı -1 döndürüyor ("geçerli değil" işareti).
// Negatif bir süreyi beklemeye çalışmak anlamsız olurdu; çağıranın tek bir kuralla
// çalışabilmesi için bütün geçersiz değerler 0'a indirilir: "rehber yok, kendi
// tabanını kullan".
func normalizeRetryAfter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}

	return d
}
