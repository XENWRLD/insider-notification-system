package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// channelPool, tek bir kanalın iş çekme ve gönderme havuzu.
//
// Yapı: 1 claimer + Concurrency kadar sender + Concurrency kapasiteli jeton semaforu.
//
// Jeton semaforu neden var: claimer yalnızca BOŞTAKİ sender kadar iş çeker. Tamponlu
// bir dağıtım kuyruğu olsaydı claimer öne geçip bir sonraki grubu da claim eder,
// worker kanal başına 2×ClaimBatchSize satırın lease'ini tutar ve son satır sırada
// beklerken lease süresi dolardı — reaper satırı devralır, mesaj ikinci kez gider.
// Jetonla tutulan lease sayısı her zaman ≤ Concurrency ve bir satırın claim'den
// sonuca kadar geçirdiği süre tek dalga (sağlayıcı + sonuç yazımı) kadar.
//
// Dağıtım kanalı TAMPONSUZ: bir satır ya claimer'ın elinde ya da bir sender'da olur,
// üçüncü bir yer yok. Kapanışta claimer elindekileri geri verir, sender'lar
// aldıklarını bitirir; tamponda unutulmuş satır kalamaz. Jeton zaten claimer'ı
// sınırladığı için tamponsuzluk verimden bir şey götürmüyor: claimer jeton almışsa
// karşıda onu bırakmış, alıma dönen bir sender vardır.
type channelPool struct {
	engine  *Engine
	channel domain.Channel
	logger  *slog.Logger

	// sem, boş jetonları tutar. Alma: <-sem, bırakma: sem <- struct{}{}.
	// len(sem) o anda boşta olan sender sayısıdır.
	sem chan struct{}

	dispatch chan *domain.Notification
}

func newChannelPool(engine *Engine, channel domain.Channel) *channelPool {
	sem := make(chan struct{}, engine.cfg.Concurrency)
	for range engine.cfg.Concurrency {
		sem <- struct{}{}
	}

	return &channelPool{
		engine:   engine,
		channel:  channel,
		logger:   engine.logger.With(slog.String("channel", string(channel))),
		sem:      sem,
		dispatch: make(chan *domain.Notification),
	}
}

// run, havuzu çalıştırır ve context iptal edilip tüm gönderimler bitene kadar bloklar.
func (p *channelPool) run(ctx context.Context) {
	var senders sync.WaitGroup

	senders.Add(p.engine.cfg.Concurrency)

	for range p.engine.cfg.Concurrency {
		go func() {
			defer senders.Done()

			p.sendLoop(ctx)
		}()
	}

	// Claimer kapanışta önce durur; sender'lar ellerindeki işi bitirir.
	p.claimLoop(ctx)

	// Dağıtım kanalı kapanınca sender'lar döngüden çıkar; ellerindeki gönderim
	// kapanışa dayanıklı context ile çalıştığı için kesilmez, biter.
	close(p.dispatch)
	p.waitForSenders(ctx, &senders)

	p.logger.InfoContext(ctx, "kanal havuzu kapandı")
}

// waitForSenders, uçan gönderimlerin bitmesini bekler.
//
// Bekleme kesilmiyor, yalnızca uzarsa uyarı loglanıyor: erken dönmek main'deki
// pool.Close()'un uçan sonuç yazımlarının altından havuzu çekmesi demek olurdu ve
// başarıyla gönderilmiş satırlar processing'de kalıp reaper tarafından yeniden
// gönderilirdi. Süre zaten yapıca sınırlı — Config.validate, DrainTimeout'un
// sağlayıcı + sonuç yazımı penceresini kapsamasını şart koşuyor.
func (p *channelPool) waitForSenders(ctx context.Context, senders *sync.WaitGroup) {
	done := make(chan struct{})

	go func() {
		senders.Wait()
		close(done)
	}()

	timer := time.NewTimer(p.engine.cfg.DrainTimeout)
	defer timer.Stop()

	select {
	case <-done:
		return
	case <-timer.C:
		p.logger.WarnContext(ctx, "kapanış süresi aşıldı, uçan gönderimler hâlâ bekleniyor",
			slog.String("drain_timeout", p.engine.cfg.DrainTimeout.String()),
			slog.Int("in_flight", p.engine.cfg.Concurrency-len(p.sem)))
	}

	<-done
}

// claimLoop, iş çekip sender'lara dağıtır.
func (p *channelPool) claimLoop(ctx context.Context) {
	poll := newBackoff(p.engine.cfg.PollMinInterval, p.engine.cfg.PollMaxInterval)

	for {
		if ctx.Err() != nil {
			return
		}

		tokens := p.acquireTokens(ctx)
		if tokens == 0 {
			// Yalnızca kapanışta olur: jeton beklerken iptal geldi.
			return
		}

		// CLAIMER KAPISI — devre kesicinin ASIL kuralı.
		//
		// Devre açıkken o kanaldan iş ÇEKİLMİYOR: satırlar pending/retrying olarak
		// kuyrukta kalıyor, yani ne deneme hakkı ne rate limit izni harcanıyor ve
		// POST /cancel çalışmaya devam ediyor (satır processing'e geçmediği için).
		// Bunu sender'daki kapıyla öğrenmek, işin çekilmiş olmasını gerektirirdi.
		limit := tokens

		switch p.engine.breakerFor(p.channel).State() {
		case breaker.StateOpen:
			// Jetonlar BEKLEMEDEN ÖNCE geri veriliyor (Adım 6 kuralı).
			p.releaseTokens(tokens)

			if !poll.wait(ctx) {
				return
			}

			continue

		case breaker.StateHalfOpen:
			// Yarı açıkta TEK sonda var. Uçuşta iş varsa HİÇ claim etmiyoruz.
			//
			// Neden: başarılı bir claim'den sonra bu döngü poll.reset() + continue ile
			// BEKLEMEDEN dönüyor. Sonda uçuştayken her turda bir satır daha çekilse,
			// sender erken kapıyı geçer (durum Open değil, HalfOpen), izin alır ve
			// brk.Do'da ErrTooManyRequests → ErrOpen yer. Tur milisaniyeler sürdüğü
			// için saniyede yüzlerce kez: PAYLAŞILAN Redis bütçesinin tamamı (diğer
			// kopyaların gönderimleri dahil) ve yüzlerce gereksiz UPDATE boşa gider.
			//
			// tokens + len(p.sem) < Concurrency → "kanalda uçuşta iş var": tokens az
			// önce semafordan alındı, len(p.sem) kalan boşlar; toplamı Concurrency'den
			// küçükse fark sender'ların elindedir. Okuma yarışa açık ama hata yönü
			// güvenli: eski bir düşük değer bir claim'i atlar (zararsız), yüksek değer
			// en fazla bir fazla satır çeker ve o satır bir kez ErrOpen yolundan geri
			// döner — döngü kurulamıyor çünkü sonrasında poll.wait var.
			if tokens+len(p.sem) < p.engine.cfg.Concurrency {
				p.releaseTokens(tokens)

				if !poll.wait(ctx) {
					return
				}

				continue
			}

			if limit > 1 {
				p.releaseTokens(limit - 1)
				limit = 1
			}

		case breaker.StateClosed:
			// Normal akış: limit boştaki sender sayısı kadar.
		}

		claimed, err := p.claim(ctx, limit)
		if err != nil {
			// Jetonlar BEKLEMEDEN ÖNCE geri veriliyor: aksi hâlde her hatalı turda
			// semafordan jeton sızar ve havuz zamanla sessizce durur.
			p.releaseTokens(limit)

			// Hata döndürülmüyor: geçici bir veritabanı kopması bütün worker'ı
			// düşürmemeli. Loglanıp backoff ile yeniden denenir.
			p.logger.ErrorContext(ctx, "iş çekilemedi, yeniden denenecek",
				slog.String("error", err.Error()))

			if !poll.wait(ctx) {
				return
			}

			continue
		}

		// Kuyrukta istediğimizden az iş vardı: kullanılmayan jetonlar geri veriliyor.
		// Yarı açıkta limit 1'e indirildiği için hesap limit üzerinden yapılıyor;
		// fazla jetonlar yukarıda zaten bırakıldı.
		if unused := limit - len(claimed); unused > 0 {
			p.releaseTokens(unused)
		}

		if len(claimed) == 0 {
			if !poll.wait(ctx) {
				return
			}

			continue
		}

		poll.reset()

		if undispatched := p.dispatchAll(ctx, claimed); len(undispatched) > 0 {
			// Kapanış dağıtımın ortasında geldi: elde kalan satırların lease'i geri
			// verilir, böylece yeniden başlayan worker onları reaper'ı beklemeden alır.
			p.releaseTokens(len(undispatched))
			p.engine.releaseLeases(ctx, p.logger, undispatched)

			return
		}
	}
}

// claim, iş çekme sorgusunu KAPANIŞTAN MUAF bir context ile çalıştırır.
//
// ctx doğrudan geçilseydi SIGTERM sorguyu ORTADA kesebilirdi — ve pgx'in iptal isteği
// sunucudaki commit ile yarışıyor. Commit olmuşsa satırlar processing'de, lease'li ve
// attempt_count'ları artmış hâlde kalır; worker kimliklerini HİÇ öğrenmediği için
// kuyruğa geri veremez ve satırlar lease süresi dolana kadar (60 sn) kimseye gitmez.
// Kanal başına Concurrency satıra kadar, üç kanalda kopya başına 60 satıra kadar.
//
// WithoutCancel ile sorgu tamamlanır ve satırlar dönerse öğrenilir; ctx zaten iptal
// olduğu için dispatchAll hiçbirini dağıtmaz ve claimLoop hepsini anında geri verir.
// Belirsizlik yok edilmiyor, BİZİM SEÇTİĞİMİZ bir süreyle (ClaimTimeout)
// sınırlandırılıyor: kalan tek pencere sorgunun kendi zaman aşımına takılması.
func (p *channelPool) claim(ctx context.Context, limit int) ([]*domain.Notification, error) {
	claimCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), p.engine.cfg.ClaimTimeout)
	defer cancel()

	return p.engine.queue.ClaimNotifications(
		claimCtx, p.channel, limit, p.engine.cfg.LeaseDuration)
}

// acquireTokens, en az bir, en fazla ClaimBatchSize kadar jeton toplar.
//
// İlk jeton için beklenir (iş yoksa boşa dönmenin anlamı yok), kalanı bloklamadan
// alınır: böylece claim sorgusu "o anda gerçekten boş olan sender sayısı" kadar satır
// ister. Kapanışta 0 döner.
func (p *channelPool) acquireTokens(ctx context.Context) int {
	select {
	case <-ctx.Done():
		return 0
	case <-p.sem:
	}

	tokens := 1

	for tokens < p.engine.cfg.ClaimBatchSize {
		select {
		case <-p.sem:
			tokens++
		default:
			return tokens
		}
	}

	return tokens
}

// releaseTokens, kullanılmayan jetonları semafora geri verir.
//
// Semafor kapasitesi sabit ve bırakılan jeton sayısı her zaman alınandan fazla
// olamadığı için bu gönderim bloklanamaz.
func (p *channelPool) releaseTokens(count int) {
	for range count {
		p.sem <- struct{}{}
	}
}

// dispatchAll, işleri sender'lara verir ve dağıtılamayanları döner.
//
// Dağıtılan her satırın jetonu sender'a geçer ve sonuç yazıldıktan sonra onun
// tarafından bırakılır; dağıtılamayanların jetonu çağıranda kalır.
func (p *channelPool) dispatchAll(
	ctx context.Context, claimed []*domain.Notification,
) []*domain.Notification {
	for index, notification := range claimed {
		select {
		case <-ctx.Done():
			return claimed[index:]
		case p.dispatch <- notification:
		}
	}

	return nil
}

// sendLoop, dağıtım kanalından iş alıp gönderir.
func (p *channelPool) sendLoop(ctx context.Context) {
	for notification := range p.dispatch {
		p.engine.handle(ctx, notification)

		// Jeton gönderim VE sonuç yazımı bittikten sonra bırakılır: erken bırakmak
		// claimer'ın hâlâ işlenen bir satır için yeni iş çekmesine yol açardı.
		p.sem <- struct{}{}
	}
}

// durationMS, süreyi veritabanına yazılacak milisaniyeye çevirir.
func durationMS(elapsed time.Duration) int {
	if elapsed <= 0 {
		return 0
	}

	return int(elapsed.Milliseconds())
}
