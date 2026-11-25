package observability

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"

	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/breakerstate"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

const (
	// cacheTTL, bir scrape sonucunun yeniden kullanılma süresi.
	//
	// /metrics KİMLİKSİZ bir uç ve değerleri veritabanından türetiyor: her istek
	// sorgu koşturursa tek bir istemci (ya da birkaç Prometheus kopyası) havuzu
	// tüketip API'yi kendi kendine hazır olmayan duruma düşürebilir. Önbellek +
	// singleflight bu yüzden güvenlik önlemi, optimizasyon değil.
	//
	// 5 saniye: tipik scrape aralığının (15 sn) altında olduğu için normal
	// izlemede her scrape taze veri görüyor; sel durumunda ise saniyede en fazla
	// bir sorgu turu koşuyor.
	cacheTTL = 5 * time.Second

	// queryTimeout, TEK bir metrik sorgusunun üst sınırı.
	//
	// Sorgu başına ayrı: biri yavaşlasa diğerleri yine toplanıyor ve hiçbir sorgu
	// bağlantıyı belirsiz süre tutamıyor.
	queryTimeout = 2 * time.Second

	// latencyWindow, gecikme yüzdeliklerinin hesaplandığı pencere.
	//
	// Tüm zamanların yüzdeliği bir olay sırasında kıpırdamaz ve ölçmek istediğimiz
	// şeyi gizler; 15 dakika hem yeterince taze hem 000005'teki index'in aralık
	// taramasıyla ucuz.
	latencyWindow = 15 * time.Minute
)

// MetricsStore, /metrics ucunun veritabanından ihtiyaç duyduğu sorgular.
//
// Arayüz: collector testleri Docker'sız koşuyor.
type MetricsStore interface {
	QueueDepth(ctx context.Context) ([]store.LabelledCount, error)
	TerminalCounts(ctx context.Context, window time.Duration) ([]store.LabelledCount, error)
	EndToEndLatency(ctx context.Context, window time.Duration) (store.Percentiles, error)
	ProviderLatency(ctx context.Context, window time.Duration) (store.Percentiles, error)
}

// BreakerStateReader, kopya başına devre durumunu okuyan taraf. nil olabilir.
type BreakerStateReader interface {
	Read(ctx context.Context) ([]breakerstate.Entry, error)
}

// snapshot, tek bir toplama turunun sonucu.
type snapshot struct {
	queue    []store.LabelledCount
	terminal []store.LabelledCount
	endToEnd store.Percentiles
	provider store.Percentiles
	breakers []breakerstate.Entry
	at       time.Time

	// failures/elapsed, turun KENDİ sağlığı. Snapshot'ta tutuluyor ki önbellek
	// isabetinde de gerçek değerler yayınlansın: 0 yayınlamak "toplama anlıktı" ve
	// "hiç hata yoktu" demek olurdu, oysa doğru bilgi son turun sonucu.
	failures int
	elapsed  time.Duration
}

// Collector, bildirim metriklerini veritabanından ve Redis'ten toplar.
//
// Metrikler worker process'inde sayaç tutmak yerine buradan türetiliyor: worker
// `--scale worker=3` ile çoğaltılabiliyor ve her kopyanın kendi /metrics portunu
// açması hem yüzey hem kopya toplama demekti. Bedeli, DB'den gelen değerlerin counter
// değil GAUGE olması; adlandırma bunu yansıtıyor.
type Collector struct {
	store    MetricsStore
	breakers BreakerStateReader
	logger   *slog.Logger

	// flight, aynı anda gelen scrape'lerin TEK sorgu turu paylaşmasını sağlar.
	flight singleflight.Group

	mu     sync.Mutex
	cached *snapshot

	queueDepth     *prometheus.Desc
	deferredDepth  *prometheus.Desc
	terminalTotal  *prometheus.Desc
	endToEnd       *prometheus.Desc
	providerCall   *prometheus.Desc
	breakerState   *prometheus.Desc
	collectErrors  *prometheus.Desc
	collectSeconds *prometheus.Desc
}

// NewCollector, metrik toplayıcısını kurar. breakers nil olabilir (devre durumu
// metriği o zaman hiç yayınlanmaz).
func NewCollector(
	metrics MetricsStore, breakers BreakerStateReader, logger *slog.Logger,
) *Collector {
	return &Collector{
		store:    metrics,
		breakers: breakers,
		logger:   logger,

		queueDepth: prometheus.NewDesc(
			"notifications_queue_depth",
			"ŞİMDİ gönderilebilir veya işlenmekte olan bildirim sayısı "+
				"(aktif durumlar, available_at <= now()). Alarm bu metriğe kurulur.",
			[]string{"status", "channel"}, nil),

		deferredDepth: prometheus.NewDesc(
			"notifications_deferred_depth",
			"Zamanı henüz gelmemiş bildirim sayısı (available_at > now()): zamanlanmış "+
				"gönderim veya retry backoff'u. Tıkanıklık DEĞİL; queue_depth ile toplamı "+
				"aktif satır sayısını verir.",
			[]string{"status", "channel"}, nil),

		terminalTotal: prometheus.NewDesc(
			"notifications_terminal_recent",
			"Son 15 dakikada biten bildirim sayısı. GAUGE: monoton DEĞİL, rate() uygulanmaz; "+
				"başarı oranı iki etiketin bölümü olarak okunur.",
			[]string{"status", "channel"}, nil),

		endToEnd: prometheus.NewDesc(
			"notification_end_to_end_duration_seconds",
			"Kabul edilmekten gönderilmeye kadar geçen süre (KUYRUKTA BEKLEME DÂHİL), son 15 dakika.",
			[]string{"quantile"}, nil),

		providerCall: prometheus.NewDesc(
			"notification_provider_duration_seconds",
			"Sağlayıcı çağrısının kendi süresi (kuyruk hariç), son 15 dakika.",
			[]string{"quantile"}, nil),

		breakerState: prometheus.NewDesc(
			"notifications_breaker_state",
			"Kanal başına devre kesici durumu, worker kopyası başına: 0=closed, 1=half-open, 2=open.",
			[]string{"channel", "worker"}, nil),

		collectErrors: prometheus.NewDesc(
			"notifications_metrics_collect_errors",
			"Son toplama turunda başarısız olan sorgu sayısı. 0 değilse bazı metrikler eksiktir.",
			nil, nil),

		collectSeconds: prometheus.NewDesc(
			"notifications_metrics_collect_duration_seconds",
			"Son toplama turunun süresi.",
			nil, nil),
	}
}

// Describe, Prometheus arayüzünün gerektirdiği tanım listesi.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.queueDepth
	ch <- c.deferredDepth
	ch <- c.terminalTotal
	ch <- c.endToEnd
	ch <- c.providerCall
	ch <- c.breakerState
	ch <- c.collectErrors
	ch <- c.collectSeconds
}

// Collect, metrikleri yayınlar.
//
// Toplama HATA DÖNDÜRMEZ: bir sorgu düşse bile /metrics yanıt veriyor ve yalnızca o
// metrik eksik kalıyor. Aksi hâlde tek bir yavaş sorgu bütün izlemeyi kör ederdi —
// ve izlemenin kör olduğu an, tam da ona en çok ihtiyaç duyulan andır.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	snap := c.load()

	for _, count := range snap.queue {
		desc := c.queueDepth
		if count.Deferred {
			desc = c.deferredDepth
		}

		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue,
			float64(count.Count), count.Status, count.Channel)
	}

	for _, count := range snap.terminal {
		ch <- prometheus.MustNewConstMetric(c.terminalTotal, prometheus.GaugeValue,
			float64(count.Count), count.Status, count.Channel)
	}

	// Pencere boşsa yüzdelik yayınlanmıyor: 0 saniye gecikme "her şey hızlı"
	// izlenimi verirdi, oysa doğru bilgi "veri yok".
	emitPercentiles(ch, c.endToEnd, snap.endToEnd)
	emitPercentiles(ch, c.providerCall, snap.provider)

	for _, entry := range snap.breakers {
		ch <- prometheus.MustNewConstMetric(c.breakerState, prometheus.GaugeValue,
			breakerStateValue(entry.State), string(entry.Channel), entry.Worker)
	}

	ch <- prometheus.MustNewConstMetric(c.collectErrors, prometheus.GaugeValue,
		float64(snap.failures))
	ch <- prometheus.MustNewConstMetric(c.collectSeconds, prometheus.GaugeValue,
		snap.elapsed.Seconds())
}

// load, önbellekten okur ya da tek bir toplama turu koşturur.
func (c *Collector) load() snapshot {
	c.mu.Lock()
	cached := c.cached
	c.mu.Unlock()

	if cached != nil && time.Since(cached.at) < cacheTTL {
		return *cached
	}

	// singleflight: aynı anda gelen scrape'ler tek tur paylaşıyor. Anahtar sabit
	// çünkü toplanacak tek bir küme var.
	value, _, _ := c.flight.Do("collect", func() (any, error) {
		started := time.Now()

		snap, failures := c.gather()

		snap.failures = failures
		snap.elapsed = time.Since(started)
		snap.at = time.Now()

		c.mu.Lock()
		c.cached = &snap
		c.mu.Unlock()

		return snap, nil
	})

	collected, ok := value.(snapshot)
	if !ok {
		// Ulaşılamaz: Do her zaman snapshot döndürüyor. Yine de sessiz kalmamak için
		// bir hata sayılıyor.
		return snapshot{failures: 1}
	}

	return collected
}

// gather, bütün sorguları koşturur ve başarısız olanların sayısını döner.
//
// Sorgular SIRAYLA koşuyor, paralel değil: dördü birden havuzdan bağlantı alsa
// kimliksiz bir uç dört bağlantıyı birden tutardı (F49 sınıfı risk). Her biri 2 sn ile
// sınırlı, yani en kötü tur ~8 sn ve bu da önbellek sayesinde 5 saniyede bir yaşanıyor.
func (c *Collector) gather() (snapshot, int) {
	var (
		snap     snapshot
		failures int
	)

	runQuery := func(name string, query func(context.Context) error) {
		ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
		defer cancel()

		if err := query(ctx); err != nil {
			failures++

			c.logger.Warn("metrik sorgusu başarısız, metrik eksik kalacak",
				slog.String("query", name),
				slog.String("error", err.Error()))
		}
	}

	runQuery("queue_depth", func(ctx context.Context) error {
		counts, err := c.store.QueueDepth(ctx)
		snap.queue = counts

		return err
	})

	runQuery("terminal_counts", func(ctx context.Context) error {
		counts, err := c.store.TerminalCounts(ctx, latencyWindow)
		snap.terminal = counts

		return err
	})

	runQuery("end_to_end_latency", func(ctx context.Context) error {
		percentiles, err := c.store.EndToEndLatency(ctx, latencyWindow)
		snap.endToEnd = percentiles

		return err
	})

	runQuery("provider_latency", func(ctx context.Context) error {
		percentiles, err := c.store.ProviderLatency(ctx, latencyWindow)
		snap.provider = percentiles

		return err
	})

	// Devre durumu isteğe bağlı: okuyucu yoksa ya da Redis'e ulaşılamazsa /metrics
	// yine dönüyor, yalnızca bu metrik eksik kalıyor.
	if c.breakers != nil {
		runQuery("breaker_state", func(ctx context.Context) error {
			entries, err := c.breakers.Read(ctx)
			snap.breakers = entries

			return err
		})
	}

	return snap, failures
}

// emitPercentiles, dolu bir dağılımı üç quantile etiketiyle yayınlar.
func emitPercentiles(
	ch chan<- prometheus.Metric, desc *prometheus.Desc, percentiles store.Percentiles,
) {
	if percentiles.Count == 0 {
		return
	}

	for label, value := range map[string]time.Duration{
		"0.5":  percentiles.P50,
		"0.95": percentiles.P95,
		"0.99": percentiles.P99,
	} {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value.Seconds(), label)
	}
}

// breakerStateValue, devre durumunu sayısal değere çevirir.
//
// Sayı sıralı: 0 kapalı (sağlıklı) → 2 açık (kanal durmuş). Böylece tek bir
// alarm kuralı (`notifications_breaker_state > 0`) hem yarı açığı hem açığı yakalıyor.
func breakerStateValue(state breaker.State) float64 {
	switch state {
	case breaker.StateClosed:
		return 0

	case breaker.StateHalfOpen:
		return 1

	case breaker.StateOpen:
		return 2

	default:
		// Tanınmayan durum: açık saymak güvenli taraf (alarm üretir).
		return 2
	}
}
