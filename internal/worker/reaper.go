package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// runPeriodic, verilen işi aralıkla tekrarlayan ortak kapanış-güvenli döngü.
//
// Reaper ve janitor aynı şekli paylaşıyor (review bulgusu F21: iki kopya); ortak hâle
// getirmenin ikinci faydası, döngünün tick kanalını DIŞARIDAN alabilmesi — kapanış
// yarışı ancak böyle determinist test edilebiliyor (tickLoop testi iki dalı birden
// hazır kuruyor).
func (e *Engine) runPeriodic(
	ctx context.Context, interval time.Duration, name string, work func(context.Context),
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	e.tickLoop(ctx, ticker.C, name, work)
}

// tickLoop, tick geldikçe işi koşturur ve kapanışta döner.
//
// Kapanış kontrolü tick dalının İÇİNDE tekrar yapılıyor: Go select'te hazır dallar
// arasından rastgele seçiyor, yani SIGTERM ile aynı ana denk gelen bir tick bu dala
// düşebiliyor ve iş iptal edilmiş ctx ile sorgu atıp anında context.Canceled alıyordu.
// Doğruluk sorunu yoktu ama her normal kapanış, gerçek bir veritabanı arızasıyla aynı
// ERROR satırını basıyordu (review bulgusu F42; ölçümde tek kapanışta birden çok kez).
func (e *Engine) tickLoop(
	ctx context.Context, ticks <-chan time.Time, name string, work func(context.Context),
) {
	for {
		select {
		case <-ctx.Done():
			e.logger.InfoContext(ctx, name+" kapandı")

			return

		case <-ticks:
			if ctx.Err() != nil {
				e.logger.InfoContext(ctx, name+" kapandı")

				return
			}

			work(ctx)
		}
	}
}

// runReaper, lease süresi dolmuş işleri periyodik olarak kurtarır.
//
// Çöken bir worker'ın claim ettiği satır processing'de kalır; lease dolduğunda bu
// döngü onu kuyruğa geri koyar veya deneme hakkı tükenmişse failed yapar. Worker
// kopyaları arasında çekişme yok: sorgu SKIP LOCKED kullanıyor.
//
// Tek bir reaper tüm kanalları kapsıyor — lease index'i kanala göre bölünmüş değil ve
// süpürme işi zaten hafif.
func (e *Engine) runReaper(ctx context.Context) {
	e.runPeriodic(ctx, e.cfg.ReaperInterval, "reaper", e.reapOnce)
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
