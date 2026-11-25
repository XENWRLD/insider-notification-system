package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// breakerStateTTLFactor, yayınlanan durumun TTL'inin yazma periyoduna oranı.
//
// Üç kat: iki kaçırılmış yazıma tolerans var (GC duraklaması, geçici Redis hatası)
// ama ölen bir kopyanın "open" durumu en fazla 3 periyot sonra düşüyor. Daha büyük bir
// kat ölü kopyayı uzun süre canlı gösterir, daha küçüğü sağlıklı bir kopyanın
// metriğini titretir.
const breakerStateTTLFactor = 3

// StatePublisher, devre durumunu process dışına taşıyan taraf.
//
// Arayüz olarak alınıyor: worker testleri Redis kurmak zorunda kalmasın ve nil
// geçildiğinde özellik tamamen kapansın (ratelimit.Limiter'dan farkı bu — hız sınırı
// zorunlu, metrik yayını değil).
type StatePublisher interface {
	Publish(
		ctx context.Context,
		channel domain.Channel,
		workerID string,
		state breaker.State,
		ttl time.Duration,
	) error
}

// runBreakerStatePublisher, devre durumlarını periyodik olarak yayınlar.
//
// Neden PERİYODİK, yalnızca durum değişiminde değil: yayınlanan anahtarlar TTL'li ve
// değişim anında yazmak TTL'i tazelemiyor. Sağlıklı bir kanalın durumu hiç değişmediği
// için metriği de kaybolurdu — oysa "sms kapalı" bilgisi "sms açık" kadar değerli.
// Bedeli: durum değişikliği metrikte en fazla bir periyot gecikmeyle görünüyor.
//
// Hata ölümcül değil: Redis'e ulaşılamazsa yalnızca metrik eksik kalır, gönderim
// etkilenmez (hız sınırının aksine burada fail closed davranışı YANLIŞ olurdu —
// metrik yayını gönderimi durdurmamalı).
func (e *Engine) runBreakerStatePublisher(ctx context.Context) {
	if e.statePublisher == nil {
		return
	}

	ttl := breakerStateTTLFactor * e.cfg.BreakerStateInterval

	// İlk yayın hemen: scrape'in bir periyot boyunca boş metrik görmesini önler.
	e.publishBreakerStates(ctx, ttl)

	ticker := time.NewTicker(e.cfg.BreakerStateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.logger.InfoContext(ctx, "devre durumu yayıncısı kapandı")

			return

		case <-ticker.C:
			// Kapanış ile tick aynı anda hazır olabilir; select rastgele seçtiği için
			// ölü context ile yazmayı burada kesiyoruz.
			if ctx.Err() != nil {
				continue
			}

			e.publishBreakerStates(ctx, ttl)
		}
	}
}

// publishBreakerStates, bütün kanalların o anki durumunu yazar.
func (e *Engine) publishBreakerStates(ctx context.Context, ttl time.Duration) {
	publishCtx, cancel := context.WithTimeout(ctx, e.cfg.BreakerStateInterval)
	defer cancel()

	for _, channel := range e.cfg.Channels {
		// breakerFor çağrısı aynı zamanda gobreaker'ın açık → yarı açık geçişini
		// tetikleyebilir (State() saf bir okuma DEĞİL, süresi dolmuş açık devreyi
		// kendisi yarı açığa alır). Bu istenen davranış: yayıncı da claimer gibi
		// devrenin ilerlemesine katkıda bulunuyor.
		state := e.breakerFor(channel).State()

		if err := e.statePublisher.Publish(
			publishCtx, channel, e.cfg.WorkerID, state, ttl,
		); err != nil {
			e.logger.WarnContext(ctx, "devre durumu yayınlanamadı, metrik eksik kalacak",
				slog.String("channel", string(channel)),
				slog.String("error", err.Error()))
		}
	}
}
