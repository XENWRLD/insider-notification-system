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

	if b.window < b.max {
		if doubled := b.window * 2; doubled > b.max || doubled <= 0 {
			b.window = b.max
		} else {
			b.window = doubled
		}
	}

	if window <= 0 {
		return 0
	}

	// Tam pencere kadar da beklenebilsin diye üst sınır dâhil ediliyor.
	//nolint:gosec // zamanlama jitter'ı; kriptografik rastgelelik gereksiz
	return time.Duration(rand.Int64N(int64(window)) + 1)
}

// wait, bekler ve context iptal edilmediyse true döner.
//
// Çıplak time.Sleep kullanılmıyor: kapanış sinyali gelince worker en fazla
// PollMaxInterval kadar değil, hemen çıkmalı.
func (b *backoff) wait(ctx context.Context) bool {
	delay := b.next()
	if delay <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
