// Package config, tüm yapılandırmayı ortam değişkenlerinden okur.
//
// Yapılandırma yalnızca process başlangıcında bir kez okunur ve zorunlu bir alan
// eksikse process hemen hata ile durur. Böylece yarı yapılandırılmış bir servis
// trafik almaya başlamaz; hata, ilk istek yerine deploy anında görülür.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config, bir process'in çalışması için gereken tüm ayarları tutar.
type Config struct {
	// AppEnv: development | production. Log formatı ve hata ayrıntısı kararlarında kullanılır.
	AppEnv string
	// LogLevel: debug | info | warn | error.
	LogLevel string
	// HTTPPort, API'nin dinlediği port (container içi).
	HTTPPort string
	// DatabaseURL, PostgreSQL bağlantı adresi (pgx DSN veya URL).
	DatabaseURL string
	// RedisURL, Redis bağlantı adresi (redis://host:port/db).
	RedisURL string
	// ProviderURL, bildirimlerin POST edileceği dış sağlayıcı adresi.
	ProviderURL string
	// ShutdownTimeout, graceful shutdown için tanınan en uzun süre.
	ShutdownTimeout time.Duration

	// BodyLimitSingle, tekil oluşturma ucunun kabul ettiği en büyük gövde (bayt).
	// İçerik sınırı 10.000 karakter olduğu için ~40 KB yeterli; 128 KiB rahat üst sınır.
	BodyLimitSingle int64
	// BodyLimitBatch, batch ucunun kabul ettiği en büyük gövde (bayt).
	// 1000 e-posta × 10.000 karakter en kötü durumda ~10 MB tutar.
	BodyLimitBatch int64

	// IdempotencyTTL, saklanan idempotency cevaplarının geçerlilik süresi.
	// Süresi dolmuş bir anahtar yeniden kullanılabilir hâle gelir.
	IdempotencyTTL time.Duration

	// --- Worker (cmd/worker) ---
	//
	// API bu alanları okumaz. Tek Config'de duruyorlar ki "ortam değişkenleri tek
	// yerden okunur" kuralı bozulmasın; worker'a özel çapraz alan tutarlılığı
	// (lease penceresi gibi) worker.New içinde doğrulanıyor.

	// WorkerConcurrency, kanal başına eşzamanlı gönderim sayısı ve aynı zamanda
	// bir worker'ın tutabileceği en fazla lease sayısı.
	WorkerConcurrency int
	// WorkerClaimBatchSize, tek claim sorgusunda istenen en fazla satır.
	WorkerClaimBatchSize int
	// WorkerLeaseDuration, claim edilen satırın sahiplik süresi.
	WorkerLeaseDuration time.Duration
	// WorkerPollMinInterval/MaxInterval, boş kuyrukta bekleme aralığı.
	WorkerPollMinInterval time.Duration
	WorkerPollMaxInterval time.Duration
	// WorkerProviderTimeout, tek sağlayıcı çağrısının üst sınırı.
	WorkerProviderTimeout time.Duration
	// WorkerResultTimeout, sonuç yazımının üst sınırı.
	WorkerResultTimeout time.Duration
	// WorkerDrainTimeout, kapanışta uçan gönderimler için beklenen süre.
	WorkerDrainTimeout time.Duration
	// WorkerClaimTimeout, tek claim sorgusunun üst sınırı. Claim kapanıştan muaf
	// olduğu için süresini bu değer belirliyor.
	WorkerClaimTimeout time.Duration
	// WorkerRetryBaseDelay/WorkerRetryMaxDelay, exponential backoff penceresinin
	// sınırları: gecikme = jitter(min(base × 2^(deneme-1), max)).
	WorkerRetryBaseDelay time.Duration
	WorkerRetryMaxDelay  time.Duration

	// WorkerBreakerFailureThreshold, devre kesicinin açılması için gereken ARDIŞIK
	// hata sayısı (kanal başına).
	WorkerBreakerFailureThreshold int

	// WorkerBreakerOpenTimeout, açık devrenin yarı açığa geçmesi için beklenen süre.
	// Devre devraldığında satırlar bunun iki katı kadar ertelenir.
	WorkerBreakerOpenTimeout time.Duration
	// WorkerBreakerStateInterval, devre durumlarının /metrics için Redis'e yeniden
	// yazılma periyodu. TTL bunun üç katı: iki kaçırılmış yazıma tolerans var ama
	// ölen bir kopyanın durumu asılı kalmıyor.
	WorkerBreakerStateInterval time.Duration
	// WorkerReaperInterval/BatchSize, lease kurtarma döngüsü.
	WorkerReaperInterval  time.Duration
	WorkerReaperBatchSize int
	// WorkerDBMaxConns, worker'ın bağlantı havuzu boyutu.
	WorkerDBMaxConns int
	// WorkerRateWaitMax, hız sınırından izin beklemenin üst sınırı.
	// Bekleme satır lease altındayken geçtiği için lease hesabının bir terimi.
	WorkerRateWaitMax time.Duration

	// RateLimitPerSecond, KANAL BAŞINA saniyedeki gönderim sınırı.
	//
	// Sınır Redis'te paylaşılan bir kovada tutulur: --scale worker=3 ile üç kopya da
	// aynı kovayı tüketir, dolayısıyla toplam hız değişmez. Kapatma anahtarı yok —
	// limitsiz çalışmak gerçek bir sağlayıcıyı anında 429'a sokardı.
	RateLimitPerSecond int

	// RateLimitBurst, kovanın biriktirebileceği en fazla izin (GCRA tolerans penceresi).
	//
	// 1 olsaydı tolerans sıfır olur ve jitter'lı yoklama yüzünden kaçan yuvalar geri
	// gelmediği için gerçekleşen hız limitin %20 altında kalırdı (ölçüldü). Burst =
	// rate olsaydı boşta kalmış kova ilk saniyede ~2× rate geçirirdi. Varsayılan 5:
	// en kötü tek saniyelik pencere rate + 4, gerçekleşen hız ≈ rate.
	RateLimitBurst int
	// IdempotencyCleanupInterval/Batch, süresi dolmuş anahtarların temizliği.
	IdempotencyCleanupInterval time.Duration
	IdempotencyCleanupBatch    int
}

// Load, ortam değişkenlerini okur ve doğrular.
//
// Eksik veya geçersiz tüm alanlar tek bir hatada toplanır: kullanıcı her seferinde
// bir eksik değişken görüp yeniden başlatmak zorunda kalmaz.
func Load() (Config, error) {
	var problems []string

	cfg := Config{
		AppEnv:      envOrDefault("APP_ENV", "development"),
		LogLevel:    envOrDefault("LOG_LEVEL", "info"),
		HTTPPort:    envOrDefault("HTTP_PORT", "8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		RedisURL:    os.Getenv("REDIS_URL"),
		ProviderURL: os.Getenv("PROVIDER_URL"),
	}

	// PROVIDER_URL bu listede YOK ve bu bilinçli: değeri yalnızca worker kullanıyor,
	// API sağlayıcıya hiç gitmiyor. Kullanmadığı bir değişken yüzünden API'nin
	// açılmaması, ".env olmadan `docker compose up` çalışmıyor" demekti — oysa
	// teslim şartı tek komutla kurulum. Worker tarafında kontrol kaybolmuyor:
	// provider.New boş adresi açılışta reddediyor ("sağlayıcı adresi boş olamaz"),
	// yani "yanlış ayar process'i hemen durdurur" kuralı yerinde kalıyor.
	for _, required := range []struct {
		name  string
		value string
	}{
		{"DATABASE_URL", cfg.DatabaseURL},
		{"REDIS_URL", cfg.RedisURL},
	} {
		if strings.TrimSpace(required.value) == "" {
			problems = append(problems, required.name+" zorunlu")
		}
	}

	shutdownTimeout, err := envDuration("SHUTDOWN_TIMEOUT", 15*time.Second)
	if err != nil {
		problems = append(problems, err.Error())
	}
	cfg.ShutdownTimeout = shutdownTimeout

	idempotencyTTL, err := envDuration("IDEMPOTENCY_TTL", 24*time.Hour)
	if err != nil {
		problems = append(problems, err.Error())
	}
	cfg.IdempotencyTTL = idempotencyTTL

	bodyLimitSingle, err := envBytes("BODY_LIMIT_SINGLE", 128<<10)
	if err != nil {
		problems = append(problems, err.Error())
	}
	cfg.BodyLimitSingle = bodyLimitSingle

	bodyLimitBatch, err := envBytes("BODY_LIMIT_BATCH", 16<<20)
	if err != nil {
		problems = append(problems, err.Error())
	}
	cfg.BodyLimitBatch = bodyLimitBatch

	for _, field := range []struct {
		key      string
		target   *time.Duration
		fallback time.Duration
	}{
		{"WORKER_LEASE_DURATION", &cfg.WorkerLeaseDuration, 60 * time.Second},
		{"WORKER_POLL_MIN_INTERVAL", &cfg.WorkerPollMinInterval, 50 * time.Millisecond},
		{"WORKER_POLL_MAX_INTERVAL", &cfg.WorkerPollMaxInterval, 2 * time.Second},
		{"WORKER_PROVIDER_TIMEOUT", &cfg.WorkerProviderTimeout, 10 * time.Second},
		{"WORKER_RESULT_TIMEOUT", &cfg.WorkerResultTimeout, 3 * time.Second},
		{"WORKER_DRAIN_TIMEOUT", &cfg.WorkerDrainTimeout, 20 * time.Second},
		{"WORKER_CLAIM_TIMEOUT", &cfg.WorkerClaimTimeout, 3 * time.Second},
		{"WORKER_RETRY_BASE_DELAY", &cfg.WorkerRetryBaseDelay, 2 * time.Second},
		{"WORKER_RETRY_MAX_DELAY", &cfg.WorkerRetryMaxDelay, 5 * time.Minute},
		{"WORKER_BREAKER_OPEN_TIMEOUT", &cfg.WorkerBreakerOpenTimeout, 30 * time.Second},
		{"WORKER_BREAKER_STATE_INTERVAL", &cfg.WorkerBreakerStateInterval, 5 * time.Second},
		{"WORKER_REAPER_INTERVAL", &cfg.WorkerReaperInterval, 10 * time.Second},
		{"WORKER_RATE_WAIT_MAX", &cfg.WorkerRateWaitMax, 10 * time.Second},
		{"IDEMPOTENCY_CLEANUP_INTERVAL", &cfg.IdempotencyCleanupInterval, time.Hour},
	} {
		value, err := envDuration(field.key, field.fallback)
		if err != nil {
			problems = append(problems, err.Error())
		}

		*field.target = value
	}

	for _, field := range []struct {
		key      string
		target   *int
		fallback int
	}{
		{"WORKER_CONCURRENCY", &cfg.WorkerConcurrency, 20},
		{"WORKER_CLAIM_BATCH_SIZE", &cfg.WorkerClaimBatchSize, 20},
		{"WORKER_REAPER_BATCH_SIZE", &cfg.WorkerReaperBatchSize, 100},
		{"WORKER_BREAKER_FAILURE_THRESHOLD", &cfg.WorkerBreakerFailureThreshold, 5},
		{"WORKER_DB_MAX_CONNS", &cfg.WorkerDBMaxConns, 16},
		{"RATE_LIMIT_PER_SECOND", &cfg.RateLimitPerSecond, 100},
		{"RATE_LIMIT_BURST", &cfg.RateLimitBurst, 5},
		{"IDEMPOTENCY_CLEANUP_BATCH", &cfg.IdempotencyCleanupBatch, 1000},
	} {
		value, err := envInt(field.key, field.fallback)
		if err != nil {
			problems = append(problems, err.Error())
		}

		*field.target = value
	}

	if !isKnownLogLevel(cfg.LogLevel) {
		problems = append(problems, "LOG_LEVEL geçersiz (debug|info|warn|error): "+cfg.LogLevel)
	}

	if len(problems) > 0 {
		return Config{}, fmt.Errorf("geçersiz yapılandırma: %s", strings.Join(problems, "; "))
	}

	return cfg, nil
}

// envOrDefault, değişken tanımsız veya boşsa verilen varsayılanı döner.
func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

// envDuration, "30s" / "5m" biçimli süre değişkenini okur.
func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fallback, fmt.Errorf("%s süre olarak okunamadı (örn. 15s): %q", key, raw)
	}
	if parsed <= 0 {
		return fallback, fmt.Errorf("%s pozitif olmalı: %q", key, raw)
	}

	return parsed, nil
}

// envBytes, bayt cinsinden bir boyut değişkenini okur.
//
// Sade tamsayı kabul edilir ("16777216"); kısaltma (16MB) desteklenmez çünkü MB/MiB
// ayrımı belirsizlik yaratıyor. .env.example'da değerin yanına yorum düşülüyor.
func envBytes(key string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback, fmt.Errorf("%s bayt sayısı olarak okunamadı (örn. 131072): %q", key, raw)
	}
	if parsed <= 0 {
		return fallback, fmt.Errorf("%s pozitif olmalı: %q", key, raw)
	}

	return parsed, nil
}

// MaxCountValue, sayma amaçlı değişkenlerin üst sınırı.
//
// Üst sınır ZORUNLU, yalnızca "pozitif olmalı" demek yetmiyordu: bu değerler
// aşağı akışta int32'ye dönüştürülüyor (pgxpool.MaxConns) ve Go'da dönüşüm sessizce
// taşıyor. WORKER_DB_MAX_CONNS=4294967296 → int32(0) → havuz pgx varsayılanına
// sessizce düşerken açılış logu hâlâ 4294967296 yazıyordu; =4294967297 → int32(1) →
// bütün sender'lar tek bağlantı için yarışıyor ve gönderilmiş mesajların sonucu
// yazılamıyordu (review bulgusu F14). Sınır, paketin kendi kuralını da uyguluyor:
// yanlış ayar process'i AÇILIŞTA durdurur, sessizce bozulmaz.
//
// Değer bilinçli olarak cömert: buradaki hiçbir sayaç için makul üst sınır bu
// büyüklüğe yaklaşmıyor, yani sınır yalnızca saçma değerleri kesiyor.
const MaxCountValue = 1_000_000

// envInt, sayma amaçlı bir tamsayı değişkenini okur (eşzamanlılık, parti boyutu).
//
// envBytes'ın aynısı ama int döner ve hata mesajı "bayt" demez: yanlış mesaj,
// yanlış değişkeni aramaya yol açar.
func envInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback, fmt.Errorf("%s tam sayı olarak okunamadı (örn. 20): %q", key, raw)
	}
	if parsed <= 0 {
		return fallback, fmt.Errorf("%s pozitif olmalı: %q", key, raw)
	}
	if parsed > MaxCountValue {
		return fallback, fmt.Errorf("%s en fazla %d olabilir: %q", key, MaxCountValue, raw)
	}

	return parsed, nil
}

// envFloat01, 0 ile 1 arasında bir oran okur (mock sağlayıcının hata oranları).
//
// Aralık kontrolü burada: 0.3 yerine 30 yazmak sessizce "her istek başarısız"
// demek olurdu ve yük testinin sonucu yanıltıcı çıkardı. Sıfır geçerli bir değer
// (hata üretme), bu yüzden envInt gibi pozitiflik şartı yok.
func envFloat01(key string, fallback float64) (float64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback, fmt.Errorf("%s oran olarak okunamadı (örn. 0.3): %q", key, raw)
	}
	if parsed < 0 || parsed > 1 {
		return fallback, fmt.Errorf("%s 0 ile 1 arasında olmalı: %q", key, raw)
	}

	return parsed, nil
}

func isKnownLogLevel(level string) bool {
	switch strings.ToLower(level) {
	case "debug", "info", "warn", "error":
		return true
	default:
		return false
	}
}
