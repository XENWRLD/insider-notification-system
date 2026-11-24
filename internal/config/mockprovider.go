package config

import (
	"fmt"
	"strings"
	"time"
)

// MockProviderConfig, sahte sağlayıcının ayarları.
//
// Oranlar 0–1 arasında olasılıktır ve sırayla denenir: önce bad request, sonra rate
// limit, sonra sunucu hatası. Toplamı 1'i aşarsa sonraki dallar hiç çalışmaz —
// bilinçli: "hepsi 400 olsun" demek geçerli bir yük testi senaryosu.
type MockProviderConfig struct {
	LogLevel string

	// Port, sahte sağlayıcının dinlediği port (container içi).
	Port string

	// FailureRate, 500 dönme olasılığı. Retry davranışını görmek için.
	FailureRate float64

	// RateLimitRate, 429 + Retry-After dönme olasılığı.
	RateLimitRate float64

	// BadRequestRate, 400 dönme olasılığı (kalıcı hata yolu).
	BadRequestRate float64

	// Latency, yanıt vermeden önce beklenen süre. Gerçek sağlayıcı gecikmesini
	// taklit eder; worker'ın eşzamanlılık hesabı buna göre doğrulanır.
	Latency time.Duration

	// LatencyJitter, gecikmeye eklenen rastgele üst pay. 0 ise gecikme sabit.
	LatencyJitter time.Duration
}

// LoadMockProvider, sahte sağlayıcının yapılandırmasını okur.
//
// Load() çağrılmıyor: o DATABASE_URL / REDIS_URL / PROVIDER_URL'ü zorunlu kılıyor ve
// mock'un bunların hiçbiriyle işi yok. Aynı .env dosyası paylaşıldığı için bu ayrım
// mock'un veritabanı olmadan da ayağa kalkmasını sağlıyor.
func LoadMockProvider() (MockProviderConfig, error) {
	var problems []string

	cfg := MockProviderConfig{
		LogLevel: envOrDefault("LOG_LEVEL", "info"),
		Port:     envOrDefault("MOCK_PORT", "8081"),
	}

	for _, rate := range []struct {
		key      string
		target   *float64
		fallback float64
	}{
		{"MOCK_FAILURE_RATE", &cfg.FailureRate, 0},
		{"MOCK_RATE_LIMIT_RATE", &cfg.RateLimitRate, 0},
		{"MOCK_BAD_REQUEST_RATE", &cfg.BadRequestRate, 0},
	} {
		value, err := envFloat01(rate.key, rate.fallback)
		if err != nil {
			problems = append(problems, err.Error())
		}

		*rate.target = value
	}

	latency, err := envDuration("MOCK_LATENCY", 50*time.Millisecond)
	if err != nil {
		problems = append(problems, err.Error())
	}
	cfg.Latency = latency

	// Jitter 0 olabilir, bu yüzden envDuration'ın pozitiflik şartı kullanılamaz:
	// değişken verilmemişse varsayılan 0 ve hiç ayrıştırma yapılmaz.
	if raw := strings.TrimSpace(envOrDefault("MOCK_LATENCY_JITTER", "0s")); raw != "" {
		parsed, parseErr := time.ParseDuration(raw)
		switch {
		case parseErr != nil:
			problems = append(problems,
				fmt.Sprintf("MOCK_LATENCY_JITTER süre olarak okunamadı (örn. 20ms): %q", raw))
		case parsed < 0:
			problems = append(problems,
				fmt.Sprintf("MOCK_LATENCY_JITTER negatif olamaz: %q", raw))
		default:
			cfg.LatencyJitter = parsed
		}
	}

	if !isKnownLogLevel(cfg.LogLevel) {
		problems = append(problems, "LOG_LEVEL geçersiz (debug|info|warn|error): "+cfg.LogLevel)
	}

	if len(problems) > 0 {
		return MockProviderConfig{}, fmt.Errorf("geçersiz yapılandırma: %s", strings.Join(problems, "; "))
	}

	return cfg, nil
}
