// Package config, tüm yapılandırmayı ortam değişkenlerinden okur.
//
// Yapılandırma yalnızca process başlangıcında bir kez okunur ve zorunlu bir alan
// eksikse process hemen hata ile durur. Böylece yarı yapılandırılmış bir servis
// trafik almaya başlamaz; hata, ilk istek yerine deploy anında görülür.
package config

import (
	"fmt"
	"os"
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

	for _, required := range []struct {
		name  string
		value string
	}{
		{"DATABASE_URL", cfg.DatabaseURL},
		{"REDIS_URL", cfg.RedisURL},
		{"PROVIDER_URL", cfg.ProviderURL},
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

func isKnownLogLevel(level string) bool {
	switch strings.ToLower(level) {
	case "debug", "info", "warn", "error":
		return true
	default:
		return false
	}
}
