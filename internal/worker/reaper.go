package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// runReaper, lease süresi dolmuş işleri periyodik olarak kurtarır.
//
// Çöken bir worker'ın claim ettiği satır processing'de kalır; lease dolduğunda bu
// döngü onu kuyruğa geri koyar veya deneme hakkı tükenmişse failed yapar. Worker
// kopyaları arasında çekişme yok: sorgu SKIP LOCKED kullanıyor.
//
// Tek bir reaper tüm kanalları kapsıyor — lease index'i kanala göre bölünmüş değil ve
// süpürme işi zaten hafif.
func (e *Engine) runReaper(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.ReaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.logger.InfoContext(ctx, "reaper kapandı")

			return

		case <-ticker.C:
			e.reapOnce(ctx)
		}
	}
}

// reapOnce, tek bir süpürme turu yapar.
//
// Hata döndürülmüyor: geçici bir veritabanı hatası worker'ı düşürmemeli, bir sonraki
// tick'te yeniden denenir.
func (e *Engine) reapOnce(ctx context.Context) {
	reapCtx, cancel := context.WithTimeout(ctx, e.cfg.ResultTimeout)
	defer cancel()

	reclaimed, err := e.queue.ReclaimExpiredLeases(reapCtx, e.cfg.ReaperBatchSize)
	if err != nil {
		e.logger.ErrorContext(ctx, "süresi dolmuş lease'ler kurtarılamadı",
			slog.String("error", err.Error()))

		return
	}

	for _, item := range reclaimed {
		// correlation_id veritabanından geliyor: API ve worker ayrı process olduğu
		// için isteğin kimliği kurtarma loguna buradan taşınıyor.
		attrs := []any{
			slog.String("correlation_id", item.CorrelationID),
			slog.String("notification_id", item.ID.String()),
			slog.String("channel", string(item.Channel)),
			slog.Int("attempt_count", item.AttemptCount),
		}

		if item.Status == domain.StatusFailed {
			e.logger.ErrorContext(ctx,
				"lease süresi doldu ve deneme hakkı tükendi, iş kalıcı olarak başarısız", attrs...)

			continue
		}

		e.logger.WarnContext(ctx, "lease süresi dolan iş kuyruğa geri kondu", attrs...)
	}
}
