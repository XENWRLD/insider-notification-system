package worker

import (
	"context"
	"math/rand/v2"
	"time"
)

// backoff, boş kuyruk ve geçici hata durumlarında bekleme süresini yönetir.
//
// DİKKAT: bu, bildirimlerin retry gecikmesi DEĞİL. Bildirim retry'ı veritabanında
// available_at ile yapılır ve worker onu beklemez (Adım 8'de exponential backoff +
// jitter oraya gelecek). Buradaki bekleme yalnızca "kuyruk boşken claim sorgusunu
// ne sıklıkla atalım" sorusunun cevabı.
//
// Full jitter: bekleme [1, pencere] aralığında rastgele seçilir ve pencere her turda
// iki katına çıkar. Sabit bekleme olsaydı üç worker kopyası aynı tick'te claim
// index'ine yüklenir ve gereksiz çekişme üretirdi.
type backoff struct {
	min    time.Duration
	max    time.Duration
	window time.Duration
}

func newBackoff(minDelay, maxDelay time.Duration) *backoff {
	return &backoff{min: minDelay, max: maxDelay, window: minDelay}
}

// reset, iş bulunduğunda pencereyi başa alır: kuyruk doluyken gecikme eklemeyelim.
func (b *backoff) reset() {
	b.window = b.min
}

// next, bu tur beklenecek süreyi verir ve pencereyi büyütür.
func (b *backoff) next() time.Duration {
	window := b.window
	b.window = grow(b.window, b.max)

	return jitter(window)
}

// wait, bekler ve context iptal edilmediyse true döner.
func (b *backoff) wait(ctx context.Context) bool {
	return sleep(ctx, b.next())
}

// grow, pencereyi iki katına çıkarır ve üst sınırda durdurur.
//
// Taşma kontrolü var: çok büyük bir pencere ikiyle çarpıldığında negatife düşebilir
// ve negatif süre beklemek sessizce beklememek olurdu.
func grow(window, max time.Duration) time.Duration {
	if window >= max {
		return max
	}

	if doubled := window * 2; doubled > max || doubled <= 0 {
		return max
	}

	return window * 2
}

// jitter, (0, window] aralığında rastgele bir süre verir (full jitter).
//
// Sabit bekleme olsaydı aynı anda bekleyen onlarca goroutine (worker kopyaları ya da
// aynı kovayı bekleyen sender'lar) aynı anda uyanır ve hedefe eşzamanlı sürü inerdi.
func jitter(window time.Duration) time.Duration {
	if window <= 0 {
		return 0
	}

	// Tam pencere kadar da beklenebilsin diye üst sınır dâhil ediliyor.
	//nolint:gosec // zamanlama jitter'ı; kriptografik rastgelelik gereksiz
	return time.Duration(rand.Int64N(int64(window)) + 1)
}

// sleep, verilen süre kadar bekler; context iptal edilirse false döner.
//
// Çıplak time.Sleep kullanılmıyor: kapanış sinyali gelince worker beklemenin
// sonunu değil, hemen çıkmayı seçmeli.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
