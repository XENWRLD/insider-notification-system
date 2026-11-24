package worker

import (
	"context"
	"log/slog"
	"time"
)

// runJanitor, süresi dolmuş idempotency anahtarlarını periyodik olarak siler.
//
// Neden worker'da, API'de değil: temizlik tek seferlik bir arka plan işi ve API
// kopyaları ölçeklenebilir — her kopyanın kendi temizlik döngüsünü koşturması aynı
// satırları yarıştırırdı. Worker da ölçeklenebilir ama DELETE sorgusu LIMIT'li ve
// idempotent olduğu için birden çok kopya zarar vermez.
//
// Doğruluk bu temizliğe BAĞLI DEĞİL: lookup sorgusu süresi dolmuş anahtarı zaten yok
// sayıyor ve yeni bir istek onu devralabiliyor (Adım 4). Buradaki iş yalnızca tabloyu
// küçük tutmak.
func (e *Engine) runJanitor(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.IdempotencyCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.logger.InfoContext(ctx, "janitor kapandı")

			return

		case <-ticker.C:
			e.cleanOnce(ctx)
		}
	}
}

// cleanOnce, tek bir temizlik turu yapar.
func (e *Engine) cleanOnce(ctx context.Context) {
	cleanCtx, cancel := context.WithTimeout(ctx, e.cfg.ResultTimeout)
	defer cancel()

	deleted, err := e.queue.DeleteExpiredIdempotencyKeys(
		cleanCtx, e.cfg.IdempotencyTTL, e.cfg.IdempotencyCleanupBatch)
	if err != nil {
		// Doğruluğu etkilemediği için hata ölümcül değil: bir sonraki tick'te yeniden denenir.
		e.logger.ErrorContext(ctx, "süresi dolmuş idempotency anahtarları silinemedi",
			slog.String("error", err.Error()))

		return
	}

	if deleted == 0 {
		return
	}

	e.logger.InfoContext(ctx, "süresi dolmuş idempotency anahtarları silindi",
		slog.Int64("deleted", deleted))
}
