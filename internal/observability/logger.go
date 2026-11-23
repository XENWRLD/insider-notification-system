// Package observability, log ve (sonraki adımlarda) metrik altyapısını kurar.
package observability

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger, stdout'a JSON yazan bir slog.Logger kurar ve global default olarak atar.
//
// JSON seçilmesinin nedeni: loglar container stdout'undan toplanıp (Loki/ELK gibi)
// bir sistemde alan bazlı sorgulanacak. service alanı sabitlenir, böylece aynı
// fonksiyon api ve worker binary'lerinde kullanılıp loglar kaynağına göre filtrelenebilir.
func NewLogger(service, level string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: ParseLevel(level),
	})

	logger := slog.New(handler).With(slog.String("service", service))
	slog.SetDefault(logger)

	return logger
}

// ParseLevel, metin log seviyesini slog seviyesine çevirir; tanınmayan değerde info döner.
// Yapılandırma doğrulaması config.Load içinde yapıldığı için burada sessiz varsayılan yeterli.
func ParseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
