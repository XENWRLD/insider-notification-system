package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/provider"
	"github.com/XENWRLD/insider-notification-system/internal/ratelimit"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

const (
	// rateWaitFloor/rateWaitCeil, izin beklemesinin katlanan penceresinin sınırları.
	//
	// Taban, sağlayıcı "hemen dene" dediğinde bile iki istek arasına nefes koyar;
	// tavan, bekleyen bir sender'ın en kötü yoklama sıklığını 4/sn'ye indirir.
	// 60 sender × 4/sn = 240 izin sorgusu/sn, ki saniyede 100 izni tüketmeye yeter
	// ama tavan olmadan ortaya çıkan binlerce istekten iki mertebe aşağıdadır.
	rateWaitFloor = 10 * time.Millisecond
	rateWaitCeil  = 250 * time.Millisecond

	// rateAllowTimeout, TEK bir izin sorgusunun üst sınırı.
	//
	// Ölçümle eklendi: Redis container'ı durdurulduğunda (reddedilen bağlantının
	// aksine) go-redis çağrısı istemci içinde uzun süre bloklanıyor. Zaman aşımı
	// olmadan tek çağrı RateWaitMax'ın tamamını yiyordu — ne hata loglanıyor ne
	// yeniden deneniyor, satır da 10 saniye lease altında kalıyordu.
	//
	// 1 saniye: sağlıklı bir Redis'te sorgu bir milisaniyenin altında, yani normal
	// yolu hiç etkilemiyor; ölü Redis'te ise bütçe ~10 denemeye bölünüyor ve
	// operatör ilk saniyede hata logunu görüyor.
	rateAllowTimeout = time.Second
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

	brk := e.breakerFor(notification.Channel)
	policy := e.retryPolicy()

	// ERKEN KAPI. Devre açıkken satır, rate limit izni HARCAMADAN kuyruğa dönüyor —
	// asıl kural bu: paylaşılan Redis bütçesi, sağlayıcıya hiç gitmeyecek gönderimler
	// için yanmamalı. Deneme de sayılmıyor.
	//
	// StateHalfOpen bu kapıdan GEÇER: sonda buradan geçmek zorunda, yoksa devre hiç
	// kapanamazdı.
	if brk.State() == breaker.StateOpen {
		log.WarnContext(ctx, "devre kesici açık, gönderim yapılmadı",
			slog.String("requeue_delay", policy.RequeueDelay.String()))
		e.deferLeases(ctx, log, []*domain.Notification{notification},
			policy.RequeueDelay, breaker.ErrOpen.Error())

		return
	}

	// Hız sınırı: izin alınmadan sağlayıcıya gidilmez. İzin alınamazsa satır
	// kuyruğa geri verilir — gönderim HİÇ yapılmaz.
	if !e.awaitPermit(ctx, log, notification) {
		e.releaseLeases(ctx, log, []*domain.Notification{notification})

		return
	}

	var (
		resp    provider.SendResponse
		elapsed time.Duration
		sendErr error
	)

	// İKİNCİ KAPI. Closure sonucu DIŞARIYA taşıyor (resp/elapsed/sendErr) ve devreye
	// yalnızca "kanal sağlıklı mı" bilgisini veriyor:
	//   breakerHealthy olan HER sonuçta nil → 2xx, DecodeError ve 400/422.
	//   DecodeError'da nil dönmek ŞART: gerçek hatayı döndürseydik IsFailure onu false
	//   sayıp "excluded" yapardı, yani mesaj gönderilmiş olmasına rağmen yarı açık sonda
	//   devreyi KAPATMAZDI — gövdesi JSON olmayan bir sağlayıcıda devre süresiz yarı
	//   açık kalırdı.
	brkErr := brk.Do(func() error {
		resp, elapsed, sendErr = e.send(ctx, notification)

		if breakerOutcomeFor(sendErr) == breakerHealthy {
			return nil
		}

		return sendErr
	})

	// Yarış: izin beklerken devre açılmış ya da yarı açık sonda kotası dolmuş.
	// İzin yandı ama satır kaybolmadı.
	if errors.Is(brkErr, breaker.ErrOpen) {
		log.WarnContext(ctx, "devre kesici izin beklerken devraldı, gönderim yapılmadı",
			slog.String("requeue_delay", policy.RequeueDelay.String()))
		e.deferLeases(ctx, log, []*domain.Notification{notification},
			policy.RequeueDelay, breaker.ErrOpen.Error())

		return
	}

	// Durum çağrıdan SONRA okunuyor: devre kapalı kaldıysa 500 alan mesaj sıradan bir
	// geçici hata gibi sayılmalı (zehirli mesaj açığı, bkz. classify).
	outcome := classify(notification, resp, sendErr, policy, elapsed, brk.State())

	logOutcome(ctx, log, outcome, sendErr, elapsed)

	switch outcome.action {
	case actionWriteResult:
		e.writeResult(ctx, log, outcome.result)

	case actionRequeue:
		e.deferLeases(ctx, log, []*domain.Notification{notification},
			outcome.requeueDelay, outcome.requeueReason)
	}
}

// deferLeases, denemeyi saymadan satırları kuyruğa geri verir.
//
// releaseLeases'in kardeşi; farkı sebebi yazması ve erteleme uygulaması. Aynı taze
// context gerekçesi geçerli: çağıranın context'i kapanışta iptal edilmiş olabilir ve
// onun çocuğu anında ölerek geri vermeyi hiç çalıştırmazdı.
func (e *Engine) deferLeases(
	ctx context.Context,
	log *slog.Logger,
	pending []*domain.Notification,
	delay time.Duration,
	reason string,
) {
	holds := leaseHolds(ctx, log, pending)
	if len(holds) == 0 {
		return
	}

	deferCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), e.cfg.ResultTimeout)
	defer cancel()

	deferred, err := e.queue.DeferLeases(deferCtx, holds, delay, reason)
	if err != nil {
		// ReleaseLeases'in hata dalıyla aynı bedel: satır artırılmış attempt_count ile
		// processing'de kalıyor ve reaper onu deneme sayarak geri koyuyor (gerekçe
		// releaseLeases'te yazılı). Erteleme de uygulanmıyor.
		log.ErrorContext(ctx,
			"lease'ler ertelenemedi: reaper devralacak ve bu deneme SAYILACAK",
			slog.Int("pending", len(holds)),
			slog.String("error", err.Error()))

		return
	}

	log.InfoContext(ctx, "işler denenmeden kuyruğa geri verildi",
		slog.Int64("deferred", deferred),
		slog.String("delay", delay.String()),
		slog.String("reason", reason))
}

// leaseHolds, bildirimleri lease çiftlerine çevirir ve token'sız olanları atlar.
func leaseHolds(
	ctx context.Context, log *slog.Logger, pending []*domain.Notification,
) []store.LeaseHold {
	holds := make([]store.LeaseHold, 0, len(pending))

	for _, notification := range pending {
		if notification.LeaseToken == nil {
			// Claim her satıra token yazar; buraya düşmek bir değişmez ihlali olurdu.
			log.ErrorContext(ctx, "lease token'ı olmayan iş geri verilemedi",
				slog.String("notification_id", notification.ID.String()))

			continue
		}

		holds = append(holds, store.LeaseHold{
			ID:    notification.ID,
			Token: *notification.LeaseToken,
		})
	}

	return holds
}

// awaitPermit, kanalın hız sınırından bir gönderim izni bekler.
//
// İzin alınırsa true; alınamazsa false döner ve çağıran satırı kuyruğa geri verir.
//
// RateWaitMax GERÇEK bir üst sınır: bütçe hem döngü başında (sorgudan önce), hem tek
// sorgunun zaman aşımında, hem de uykunun süresinde zorlanıyor. Bu, leaseWindow()
// hesabının dayanağı — satırın lease altında geçirdiği süre RateWaitMax + sağlayıcı +
// sonuç yazımını aşamaz.
//
// Context İPTAL EDİLEBİLİR — gönderimin ve sonuç yazımının aksine. İzin beklemenin
// hiçbir yan etkisi yok, dolayısıyla SIGTERM beklemeyi kesmeli ve satır kuyruğa
// dönmeli. Bu sayede DrainTimeout'un RateWaitMax'ı kapsaması gerekmiyor; yalnızca
// LeaseDuration onu kapsıyor (satır beklerken lease altında).
//
// FAIL CLOSED: Redis'e ulaşılamazsa gönderim YAPILMAZ. Hata, reddedilmiş izinle aynı
// muameleyi görüyor — yeniden denenir. "Hata hâlinde lease'i hemen geri ver" denseydi
// Redis düştüğünde claim → hata → release → claim döngüsü milisaniye ölçeğinde
// Postgres'i döverdi; burada beklemek jetonu sender'da tuttuğu için claimer
// kendiliğinden duruyor ve backpressure'ı semafor sağlıyor.
func (e *Engine) awaitPermit(
	ctx context.Context, log *slog.Logger, notification *domain.Notification,
) bool {
	deadline := time.Now().Add(e.cfg.RateWaitMax)

	var (
		window  time.Duration
		waits   int
		errSeen bool
	)

	for {
		// Bütçe kontrolü SORGUDAN ÖNCE. Önce sorgu atıp sonra kontrol etmek
		// RateWaitMax'ı gerçek bir üst sınır olmaktan çıkarıyordu: bütçe dolmasına
		// milisaniyeler kalmışken bir uyku (≤ rateWaitCeil) ve ardından takılı bir
		// Redis'e karşı tam bir izin sorgusu (≤ rateAllowTimeout) daha sığıyordu, yani
		// satır ~1.25 sn FAZLA lease altında kalabiliyordu. leaseWindow() bu süreyi
		// RateWaitMax ile hesapladığı için validate'in bastığı "gereken lease" değeri
		// de eksik kalıyordu — tam olarak o kontrolün engellemek için var olduğu
		// senaryo (lease gönderim ortasında dolar, reaper devralır, mesaj iki kez gider).
		remaining := time.Until(deadline)
		if remaining <= 0 {
			log.WarnContext(ctx, "izin beklemesi aşıldı, iş kuyruğa geri veriliyor",
				slog.String("rate_wait_max", e.cfg.RateWaitMax.String()),
				slog.Int("waits", waits))

			return false
		}

		decision, err := e.allowOnce(ctx, notification.Channel, remaining)

		switch {
		case err != nil:
			// İlk hatada loglanır: her yeniden denemede loglamak, Redis düştüğünde
			// log hacmini gönderim hacmiyle çarpardı.
			if !errSeen {
				errSeen = true

				log.ErrorContext(ctx, "gönderim izni alınamadı, gönderim yapılmıyor",
					slog.String("error", err.Error()))
			}

			window = grow(max(window, rateWaitFloor), rateWaitCeil)

		case decision.OK:
			if waits > 0 {
				log.DebugContext(ctx, "gönderim izni alındı",
					slog.Int("waits", waits))
			}

			return true

		default:
			// Reddedildi. İlk pencere sağlayıcının rehberinden gelir; sonra katlanır.
			//
			// RetryAfter'ı her turda doğrudan beklemek Redis'i döverdi: GCRA'nın
			// değeri "bir sonraki izne kadar" (~10 ms) ve BEKLEYEN HERKES AYNI
			// DEĞERİ ALIR. Kova boşken onlarca sender saniyede binlerce istek atar,
			// karşılığında saniyede yalnızca rate kadar izin üretilir.
			window = grow(max(window, max(decision.RetryAfter, rateWaitFloor)), rateWaitCeil)
		}

		waits++

		// Uyku da bütçeyi aşmayacak biçimde kısaltılıyor; aşan bir uyku, döngü başındaki
		// kontrol bütçeyi dolmuş bulana kadar satırı boşuna lease altında tutardı.
		wait := jitter(window)
		if left := time.Until(deadline); wait > left {
			wait = left
		}

		if !sleep(ctx, wait) {
			// Kapanış: bekleme kesiliyor, satır kuyruğa dönecek.
			return false
		}
	}
}

// allowOnce, tek bir izin sorgusu yapar ve sorguyu kendi zaman aşımıyla sınırlar.
//
// Zaman aşımı olmadan takılı bir Redis tek çağrıda bütün bekleme bütçesini
// tüketebilir (ölçüldü); böylece ne hata loglanır ne yeniden denenir.
//
// budget, çağıranın kalan bekleme bütçesi: zaman aşımı rateAllowTimeout ile bunun
// KÜÇÜĞÜ olur. Böylece bütçenin sonunda başlayan bir sorgu RateWaitMax'ı aşamıyor ve
// leaseWindow() hesabı gerçek en kötü süreyi kapsıyor.
func (e *Engine) allowOnce(
	ctx context.Context, channel domain.Channel, budget time.Duration,
) (ratelimit.Decision, error) {
	timeout := rateAllowTimeout
	if budget < timeout {
		timeout = budget
	}

	allowCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return e.limiter.Allow(allowCtx, channel)
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

// releaseLeases, claim edilmiş ama hiç denenmemiş işleri kuyruğa geri verir.
//
// İki çağıranı var: kapanışta dağıtılamayan işleri bırakan claimer, ve izin
// alamadığı için gönderimden vazgeçen sender. İkisi de "satır bizde ama sağlayıcıya
// hiç gitmedi" durumunda; ReleaseLeases bu yüzden attempt_count'u da geri alıyor.
//
// Taze context (WithoutCancel): çağıranın context'i kapanışta zaten iptal edilmiş
// oluyor ve onun çocuğu anında ölerek geri vermeyi hiç çalıştırmazdı.
func (e *Engine) releaseLeases(
	ctx context.Context, log *slog.Logger, pending []*domain.Notification,
) {
	holds := leaseHolds(ctx, log, pending)
	if len(holds) == 0 {
		return
	}

	releaseCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), e.cfg.ResultTimeout)
	defer cancel()

	released, err := e.queue.ReleaseLeases(releaseCtx, holds)
	if err != nil {
		// İş KAYBOLMUYOR ama bedeli "yalnızca gecikme" DEĞİL — bu yol bir deneme hakkı
		// yakıyor:
		//
		// Claim attempt_count'u artırmıştı ve bu geri verme onu geri alacaktı. UPDATE
		// düştüğü için satır artırılmış sayaçla processing'de kalıyor; lease dolunca
		// reaper devralıyor ve reaper attempt_count'u BİLİNÇLİ OLARAK KORUYOR
		// ("deneme gerçekten yapıldı" — store/reaper.go). Oysa bu satır sağlayıcıya hiç
		// gitmedi. Yani tur başına: bir deneme hakkı + lease süresi kadar gecikme +
		// o süre boyunca iptal edilemezlik, sıfır gönderim karşılığında. max_attempts
		// kez tekrarlanırsa satır hiç denenmemiş hâlde kalıcı failed olur.
		//
		// Yapısal çözüm (sağlayıcıya gidilmemiş bir lease aşımının deneme sayılmaması)
		// DeferLeases ile aynı sorunu paylaşıyor; ikisi tek yerde çözülmeli.
		log.ErrorContext(ctx,
			"lease'ler geri verilemedi: reaper devralacak ve bu deneme SAYILACAK",
			slog.Int("pending", len(holds)),
			slog.String("error", err.Error()))

		return
	}

	log.InfoContext(ctx, "işler kuyruğa geri verildi",
		slog.Int64("released", released),
		slog.Int("pending", len(holds)))
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
	outcome decision,
	sendErr error,
	elapsed time.Duration,
) {
	result := outcome.result

	attrs := []any{
		slog.Int64("duration_ms", int64(durationMS(elapsed))),
	}

	if result.StatusCode != nil {
		attrs = append(attrs, slog.Int("status_code", *result.StatusCode))
	}

	// Denemenin SAYILMADIĞI yollar: deneme kaydı yazılmadığı için tutanak yalnızca bu
	// log satırı ve satıra yazılan last_error. Bu yüzden Warn seviyesinde ve sebebiyle.
	if outcome.action == actionRequeue {
		log.WarnContext(ctx, "gönderim yapılmadı, iş denenmeden kuyruğa dönüyor",
			append(attrs,
				slog.String("requeue_delay", outcome.requeueDelay.String()),
				slog.String("reason", outcome.requeueReason))...)

		return
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
