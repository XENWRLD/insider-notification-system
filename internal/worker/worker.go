// Package worker, bildirimleri kuyruktan çekip sağlayıcıya gönderen motordur.
//
// Yapı: kanal başına bir havuz (SMS tıkanınca e-posta etkilenmesin), havuz içinde bir
// claimer ve N sender goroutine'i, ayrıca tüm havuzlar için tek bir reaper (çöken
// worker'ın işini kurtarır) ve tek bir janitor (süresi dolmuş idempotency anahtarlarını
// temizler).
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
	"github.com/XENWRLD/insider-notification-system/internal/ratelimit"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// leaseSafetyFactor, lease süresinin en kötü lease penceresine oranı.
//
// Pencere (bkz. leaseWindow) kontrolümüzdeki üç zaman aşımıyla sınırlı: izin
// beklemesi, sağlayıcı çağrısı, sonuç yazımı. İki katı pay, GC duraklaması veya ağ
// gecikmesi gibi ölçmediğimiz gecikmelere yer bırakıyor. Lease dolarsa reaper işi
// yeniden dağıtır ve mesaj ikinci kez gider — bu yüzden pay cömert tutuluyor.
const leaseSafetyFactor = 2

// Queue, motorun veritabanından ihtiyaç duyduğu işlemler.
//
// Arayüz olarak alınıyor ki havuz mantığı (jeton muhasebesi, kapanış, backoff)
// Docker'sız, stub ile test edilebilsin. Üretimde *store.Store karşılıyor.
type Queue interface {
	ClaimNotifications(
		ctx context.Context, channel domain.Channel, limit int, leaseFor time.Duration,
	) ([]*domain.Notification, error)

	CompleteNotification(ctx context.Context, res store.NotificationResult) error

	ReleaseLeases(ctx context.Context, holds []store.LeaseHold) (int64, error)

	ReclaimExpiredLeases(ctx context.Context, limit int) ([]store.ReclaimedNotification, error)

	DeleteExpiredIdempotencyKeys(ctx context.Context, ttl time.Duration, batchSize int) (int64, error)
}

// Sender, dış sağlayıcıya gönderim yapan taraf. Üretimde *provider.Client.
type Sender interface {
	Send(ctx context.Context, req provider.SendRequest) (provider.SendResponse, error)
}

// Config, motorun ayarları.
type Config struct {
	// Channels, havuz açılacak kanallar. Boşsa üç kanalın hepsi.
	Channels []domain.Channel

	// Concurrency, kanal başına eşzamanlı gönderim sayısı.
	//
	// Aynı zamanda bir worker'ın aynı anda tutabileceği EN FAZLA lease sayısıdır:
	// claimer yalnızca boştaki sender kadar iş çeker.
	Concurrency int

	// ClaimBatchSize, tek claim sorgusunda istenen en fazla satır. Etkin değer
	// min(ClaimBatchSize, boştaki sender) olur.
	ClaimBatchSize int

	// LeaseDuration, claim edilen satırın sahiplik süresi.
	LeaseDuration time.Duration

	// PollMinInterval/PollMaxInterval, kuyruk boş veya hatalıyken beklenen süre
	// aralığı. Full jitter uygulanır ki worker kopyaları aynı anda sorgu atmasın.
	PollMinInterval time.Duration
	PollMaxInterval time.Duration

	// ProviderTimeout, tek bir sağlayıcı çağrısının üst sınırı.
	ProviderTimeout time.Duration

	// ResultTimeout, sonuç yazımının üst sınırı.
	ResultTimeout time.Duration

	// DrainTimeout, kapanışta uçan gönderimlerin bitmesi için beklenen süre.
	DrainTimeout time.Duration

	// FixedRetryDelay, Adım 6'nın geçici retry gecikmesi. Adım 8'de exponential
	// backoff + jitter bunun yerini alacak.
	FixedRetryDelay time.Duration

	// RateWaitMax, hız sınırından izin beklemenin üst sınırı.
	//
	// Bekleme satır lease altındayken geçiyor, bu yüzden lease hesabının üçüncü
	// terimi (bkz. leaseWindow). Süre aşılırsa satır kuyruğa geri verilir.
	RateWaitMax time.Duration

	ReaperInterval  time.Duration
	ReaperBatchSize int

	// IdempotencyTTL, saklanan cevapların geçerlilik süresi (temizlik için).
	IdempotencyTTL time.Duration

	IdempotencyCleanupInterval time.Duration
	IdempotencyCleanupBatch    int
}

// leaseWindow, bir satırın claim'den sonra lease altında geçirebileceği en kötü süre.
//
// Üç terim: hız sınırından izin beklemek, sağlayıcı çağrısı, sonuç yazımı. İzin
// beklemesi DAHİL çünkü satır o sırada claim edilmiş ve lease'i bizde.
//
// Jeton semaforu sayesinde bir satır claim edilir edilmez bir sender'a gider; yani
// pencere tek dalga. ClaimBatchSize bu hesaba GİRMEZ — tamponlu bir dağıtım kuyruğu
// olsaydı satırlar sıra bekleyip pencereyi katlardı.
func (c Config) leaseWindow() time.Duration {
	return c.RateWaitMax + c.ProviderTimeout + c.ResultTimeout
}

// drainWindow, kapanışta uçan işin bitmesi için beklenecek en kötü süre.
//
// İzin beklemesi DAHİL DEĞİL: o bekleme kapanışta iptal edilebiliyor (yan etkisi
// olmadığı için) ve satır kuyruğa dönüyor. Kesilemeyen tek şey başlamış bir
// gönderim ve onun sonuç yazımı.
func (c Config) drainWindow() time.Duration {
	return c.ProviderTimeout + c.ResultTimeout
}

// validate, ayarların kendi içinde tutarlı olduğunu doğrular.
//
// Bu kontrol config.Load'da değil burada: worker'a özel çapraz alan değişmezleri ve
// API bu yüzden açılmamalı. Hata mesajları sayıları içerir, çünkü "lease çok kısa"
// demek kullanıcıya hangi değeri değiştireceğini söylemiyor.
func (c Config) validate() error {
	var problems []string

	for _, positive := range []struct {
		name  string
		value time.Duration
	}{
		{"LeaseDuration", c.LeaseDuration},
		{"PollMinInterval", c.PollMinInterval},
		{"PollMaxInterval", c.PollMaxInterval},
		{"ProviderTimeout", c.ProviderTimeout},
		{"ResultTimeout", c.ResultTimeout},
		{"DrainTimeout", c.DrainTimeout},
		{"FixedRetryDelay", c.FixedRetryDelay},
		{"RateWaitMax", c.RateWaitMax},
		{"ReaperInterval", c.ReaperInterval},
		{"IdempotencyTTL", c.IdempotencyTTL},
		{"IdempotencyCleanupInterval", c.IdempotencyCleanupInterval},
	} {
		if positive.value <= 0 {
			problems = append(problems, positive.name+" pozitif olmalı")
		}
	}

	for _, positive := range []struct {
		name  string
		value int
	}{
		{"Concurrency", c.Concurrency},
		{"ClaimBatchSize", c.ClaimBatchSize},
		{"ReaperBatchSize", c.ReaperBatchSize},
		{"IdempotencyCleanupBatch", c.IdempotencyCleanupBatch},
	} {
		if positive.value <= 0 {
			problems = append(problems, positive.name+" pozitif olmalı")
		}
	}

	if c.PollMinInterval > c.PollMaxInterval {
		problems = append(problems, fmt.Sprintf(
			"PollMinInterval (%s) PollMaxInterval'den (%s) büyük olamaz",
			c.PollMinInterval, c.PollMaxInterval))
	}

	// Lease, satırın lease altında geçirebileceği en kötü süreyi güvenli payla
	// kapsamalı. Kapsamazsa lease iş bitmeden dolar, reaper satırı devralır ve aynı
	// mesaj ikinci kez gider.
	if window := c.leaseWindow(); window > 0 {
		required := leaseSafetyFactor * window
		if c.LeaseDuration < required {
			problems = append(problems, fmt.Sprintf(
				"LeaseDuration en az %s olmalı "+
					"(%d × (RateWaitMax %s + ProviderTimeout %s + ResultTimeout %s)), verilen: %s",
				required, leaseSafetyFactor,
				c.RateWaitMax, c.ProviderTimeout, c.ResultTimeout, c.LeaseDuration))
		}
	}

	// Kapanış penceresi uçan gönderim + sonuç yazımını kapsamalı. Kapsamazsa Run
	// sender'lar hâlâ çalışırken döner, main havuzu kapatır ve başarılı gönderimlerin
	// sonucu yazılamaz — satır reaper'a kadar processing'de kalır.
	//
	// İzin beklemesi bu pencereye girmiyor: kapanışta iptal edilip satır kuyruğa
	// döndüğü için drain'in onu beklemesi gerekmiyor.
	if window := c.drainWindow(); window > 0 && c.DrainTimeout < window {
		problems = append(problems, fmt.Sprintf(
			"DrainTimeout en az %s olmalı (ProviderTimeout %s + ResultTimeout %s), verilen: %s",
			window, c.ProviderTimeout, c.ResultTimeout, c.DrainTimeout))
	}

	for _, channel := range c.Channels {
		if !channel.Valid() {
			problems = append(problems, "geçersiz kanal: "+string(channel))
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("geçersiz worker yapılandırması: %s", joinProblems(problems))
	}

	return nil
}

// joinProblems, sorunları tek satırda birleştirir (config paketindeki kalıbın aynısı).
func joinProblems(problems []string) string {
	result := problems[0]
	for _, problem := range problems[1:] {
		result += "; " + problem
	}

	return result
}

// Engine, worker motoru.
type Engine struct {
	cfg     Config
	queue   Queue
	sender  Sender
	limiter ratelimit.Limiter
	logger  *slog.Logger
}

// New, ayarları doğrulayıp motoru kurar.
//
// Doğrulama boot'ta yapılıyor: yanlış ayarla çalışan bir worker saatler sonra
// "neden mesajlar iki kez gidiyor" sorusuna dönüşürdü.
func New(
	cfg Config, queue Queue, sender Sender, limiter ratelimit.Limiter, logger *slog.Logger,
) (*Engine, error) {
	if len(cfg.Channels) == 0 {
		cfg.Channels = []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	if queue == nil || sender == nil {
		return nil, errors.New("worker kuyruk ve gönderici olmadan çalışamaz")
	}

	// Limiter zorunlu: eksikse hız sınırı sessizce kapanırdı. Sınırsız çalışmak
	// isteyen test ratelimit.Unlimited() geçiyor — bilinçli ve görünür bir seçim.
	if limiter == nil {
		return nil, errors.New("worker hız sınırlayıcı olmadan çalışamaz")
	}

	return &Engine{
		cfg: cfg, queue: queue, sender: sender, limiter: limiter, logger: logger,
	}, nil
}

// Run, motoru çalıştırır ve context iptal edilene kadar bloklar.
//
// Kapanış: claimer'lar yeni iş almayı bırakır, elde kalan claim'ler kuyruğa geri
// verilir, uçan gönderimler biter. Geçici hatalar (DB kopması gibi) döndürülmez —
// loglanıp backoff ile yeniden denenir; aksi hâlde tek bir veritabanı hıçkırığı
// bütün worker'ı düşürür ve restart politikasıyla crash-loop'a girerdi.
func (e *Engine) Run(ctx context.Context) error {
	group, groupCtx := errgroup.WithContext(ctx)

	for _, channel := range e.cfg.Channels {
		pool := newChannelPool(e, channel)

		group.Go(func() error {
			pool.run(groupCtx)

			return nil
		})
	}

	group.Go(func() error {
		e.runReaper(groupCtx)

		return nil
	})

	group.Go(func() error {
		e.runJanitor(groupCtx)

		return nil
	})

	e.logger.InfoContext(ctx, "worker motoru başladı",
		slog.Int("channels", len(e.cfg.Channels)),
		slog.Int("concurrency", e.cfg.Concurrency),
		slog.Int("claim_batch_size", e.cfg.ClaimBatchSize),
		slog.String("lease_duration", e.cfg.LeaseDuration.String()),
		slog.String("rate_wait_max", e.cfg.RateWaitMax.String()),
	)

	if err := group.Wait(); err != nil {
		return fmt.Errorf("worker motoru durdu: %w", err)
	}

	e.logger.InfoContext(ctx, "worker motoru düzgün şekilde kapandı")

	return nil
}
