package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// handle, tek bir bildirimi sağlayıcıya gönderir ve sonucu yazar.
//
// KAPANIŞ DAYANIKLILIĞI: hem gönderim hem sonuç yazımı context.WithoutCancel ile
// çalışır, yani SIGTERM bu iki işlemi KESMEZ.
//
//   - Uçan sağlayıcı çağrısını iptal etmek mesajın gidip gitmediğini bilinmez kılar;
//     at-least-once'tan daha kötüsü, "bilmiyoruz"dur.
//   - Sonuç yazımını iptal etmek düpedüz hata olurdu: gönderim başarılı, satır
//     processing'de kalır, reaper devralır ve aynı mesaj ikinci kez gider.
//
// Süre yine sınırlı: ProviderTimeout ve ResultTimeout kendi zaman aşımlarını koyuyor,
// Config.validate de DrainTimeout'un bu pencereyi kapsamasını şart koşuyor.
func (e *Engine) handle(ctx context.Context, notification *domain.Notification) {
	log := e.logger.With(
		slog.String("correlation_id", notification.CorrelationID),
		slog.String("notification_id", notification.ID.String()),
		slog.String("channel", string(notification.Channel)),
		slog.Int("attempt", notification.AttemptCount),
	)

	if notification.LeaseToken == nil {
		// Claim her satıra token yazar; token'sız satır bir değişmez ihlalidir ve
		// sonuç yazımı zaten lease koşuluna takılırdı.
		log.ErrorContext(ctx, "lease token'ı olmayan iş işlenemedi")

		return
	}

	resp, elapsed, sendErr := e.send(ctx, notification)

	result := classify(notification, resp, sendErr, e.cfg.FixedRetryDelay, elapsed)

	logOutcome(ctx, log, result, sendErr, elapsed)

	e.writeResult(ctx, log, result)
}

// send, sağlayıcı çağrısını yapar ve geçen süreyi döner.
//
// Süre sonuçla birlikte dönüyor: deneme kaydına yazılacak duration_ms, çağrının
// başarılı olup olmamasından bağımsız olarak ölçülmeli.
func (e *Engine) send(
	ctx context.Context, notification *domain.Notification,
) (provider.SendResponse, time.Duration, error) {
	sendCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), e.cfg.ProviderTimeout)
	defer cancel()

	started := time.Now()

	resp, err := e.sender.Send(sendCtx, provider.SendRequest{
		To:      notification.Recipient,
		Channel: string(notification.Channel),
		Content: notification.Content,

		// Bildirim kimliği idempotency anahtarı olarak gider: tekrar denemeler aynı
		// anahtarı taşır, destekleyen bir sağlayıcı duplicate'i tekilleştirebilir.
		IdempotencyKey: notification.ID.String(),
		CorrelationID:  notification.CorrelationID,
	})

	return resp, time.Since(started), err
}

// writeResult, sonucu veritabanına yazar.
func (e *Engine) writeResult(
	ctx context.Context, log *slog.Logger, result store.NotificationResult,
) {
	resultCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), e.cfg.ResultTimeout)
	defer cancel()

	err := e.queue.CompleteNotification(resultCtx, result)

	switch {
	case err == nil:
		return

	case errors.Is(err, store.ErrLeaseLost):
		// Lease dolmuş ve reaper satırı devralmış: bu worker'ın sonucu artık geçersiz.
		// Hata değil, yarışı kaybetmek. Yine de görünür olmalı — sıklaşması lease
		// süresinin kısa kaldığının işareti.
		log.WarnContext(ctx, "sonuç yazılamadı, lease başka worker'a geçmiş",
			slog.String("status", string(result.Status)))

	default:
		// EN CİDDİ DURUM: gönderim yapıldı ama sonucu yazamadık. Satır reaper'a kadar
		// processing'de kalır ve mesaj ikinci kez gidebilir. At-least-once teslimatın
		// bedeli (docs/PLAN.md §2) — ölçülebilmesi için ayrı loglanıyor.
		log.ErrorContext(ctx, "gönderim sonucu yazılamadı, iş yeniden gönderilebilir",
			slog.String("status", string(result.Status)),
			slog.String("error", err.Error()))
	}
}

// logOutcome, denemenin sonucunu uygun seviyede loglar.
func logOutcome(
	ctx context.Context,
	log *slog.Logger,
	result store.NotificationResult,
	sendErr error,
	elapsed time.Duration,
) {
	attrs := []any{
		slog.Int64("duration_ms", int64(durationMS(elapsed))),
	}

	if result.StatusCode != nil {
		attrs = append(attrs, slog.Int("status_code", *result.StatusCode))
	}

	var decodeErr *provider.DecodeError
	if errors.As(sendErr, &decodeErr) {
		// Gönderildi sayıldı ama sağlayıcının yanıtını okuyamadık: mesaj kimliği
		// yok, yani dış sistemde iz sürmek zorlaşıyor. Sessiz kalınmamalı.
		log.WarnContext(ctx,
			"sağlayıcı 2xx döndü ama yanıtı çözülemedi, gönderildi sayıldı",
			append(attrs, slog.String("error", decodeErr.Error()))...)

		return
	}

	switch result.Status {
	case domain.StatusSent:
		log.InfoContext(ctx, "bildirim gönderildi", attrs...)

	case domain.StatusRetrying:
		log.WarnContext(ctx, "gönderim başarısız, yeniden denenecek",
			append(attrs,
				slog.String("retry_after", result.RetryAfter.String()),
				slog.String("error", sendErr.Error()))...)

	case domain.StatusFailed:
		log.ErrorContext(ctx, "gönderim kalıcı olarak başarısız, deneme hakkı tükendi",
			append(attrs, slog.String("error", sendErr.Error()))...)

	default:
		log.ErrorContext(ctx, "beklenmeyen gönderim sonucu",
			append(attrs, slog.String("status", string(result.Status)))...)
	}
}
