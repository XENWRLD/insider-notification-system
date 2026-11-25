package store

import (
	"context"
	"fmt"
	"time"
)

// Bu dosya /metrics ucunun beslendiği sorguları tutuyor (Adım 9).
//
// Metrikler worker process'inde sayaç tutmak yerine VERİTABANINDAN türetiliyor.
// Gerekçe: worker `--scale worker=3` ile çoğaltılabiliyor ve her kopyanın kendi
// sayacını expose etmesi hem ayrı bir port hem kopya toplama demekti; veritabanı ise
// zaten tek doğruluk kaynağı. Bedeli, değerlerin counter değil GAUGE olması —
// adlandırma bunu yansıtıyor (bkz. internal/observability/metrics.go).
//
// Üç sorgunun üçü de sınırlı maliyetli olacak şekilde yazıldı:
//   - kuyruk derinliği yalnızca AKTİF durumları sayıyor (küme küçük, kısmi index'ler)
//   - terminal sayım ve gecikme yüzdelikleri 000005'teki index'lerle INDEX ONLY SCAN
//   - her sorgunun kendi zaman aşımı var (çağıranda), havuzu tüketemiyor

// activeStatuses, kuyrukta işi bekleyen ya da işlenen durumlar.
//
// Terminal durumlar (sent/failed/cancelled) BİLİNÇLİ OLARAK dışarıda: kuyruk derinliği
// "ne kadar iş bekliyor" sorusudur ve milyonlarca sent satırını saymak bu soruyu
// cevaplamıyor, yalnızca scrape'i pahalılaştırırdı.
var activeStatuses = []string{"pending", "retrying", "processing"}

// queueDepthQuery, kanal, durum ve HAZIR OLMA durumuna göre iş sayısı.
//
// available_at ayrımı zorunlu: retry backoff'u ve zamanlanmış gönderim satırı
// pending/retrying bırakıp available_at'i ileri taşıyor, yani "aktif satır" ile
// "şimdi gönderilebilir iş" aynı şey değil. Ayrım olmasa gelecek haftaya kurulmuş
// 10.000 bildirim, operatöre tıkanmış bir kuyruk gibi görünürdü.
//
// Partial index'ler bu koşulla bozulmuyor: available_at claim index'inin (channel,
// priority_rank, available_at, id) içinde ve processing satırları tanım gereği
// hiç 'deferred' olmuyor.
//
// Aktif küme sağlıklı bir sistemde küçük olduğu için planlayıcı Adım 2'nin kısmi
// index'lerini (notifications_claim_idx ve notifications_lease_expiry_idx) bitmap ile
// birleştirip heap'ten yalnızca o satırları okuyor — maliyet tablo boyutundan değil
// AKTİF satır sayısından geliyor. TestQueueDepthQueryAvoidsSeqScan bunu EXPLAIN ile
// sabitliyor.
const queueDepthQuery = `
	SELECT status, channel, available_at > now() AS deferred, count(*)
	FROM notifications
	WHERE status = ANY($1)
	GROUP BY status, channel, deferred`

// terminalCountsQuery, son PENCERE içinde biten işlerin sayısı.
//
// Kümülatif (tüm zamanlar) sayım denendi ve ÖLÇÜMLE REDDEDİLDİ: WHERE koşulu index'in
// kısmi yordamıyla birebir aynı olduğu için planlayıcı doğrudan Seq Scan seçiyor
// (15.500 satırda 317 buffer) ve maliyet TABLO BOYUTUYLA doğrusal büyüyor — 15 saniyede
// bir koşan bir scrape sorgusu için kabul edilemez. Tam bir sayım tanımı gereği
// O(terminal satır) olduğundan tek çıkış pencere.
//
// Bedeli: değer monoton DEĞİL, yani Prometheus tarafında rate() uygulanamaz. Metrik adı
// bunu söylüyor (notifications_terminal_recent) ve başarı/başarısızlık oranı doğrudan
// iki gauge'in bölümü olarak okunuyor.
//
// Kazanç: notifications_metrics_idx (status, updated_at, channel, created_at) üzerinde
// ARALIK taraması — maliyet tablo boyutundan değil son 15 dakikanın hacminden geliyor
// ve dört kolon da index'te olduğu için heap'e gidilmiyor.
const terminalCountsQuery = `
	SELECT status, channel, count(*)
	FROM notifications
	WHERE status IN ('sent', 'failed', 'cancelled')
	  AND updated_at >= now() - $1::interval
	GROUP BY status, channel`

// endToEndLatencyQuery, uçtan uca gönderim gecikmesinin yüzdelikleri.
//
// created_at → updated_at: bildirim kabul edildiği andan sent yazıldığı ana kadar,
// yani KUYRUKTA BEKLEME DÂHİL. Kullanıcının gerçekten yaşadığı gecikme bu.
//
// Pencere şart: tüm zamanların yüzdeliği bir olay sırasında hiç kıpırdamaz ve
// ölçmek istediğimiz şeyi gizler.
// extract(epoch ...) ile SANİYE cinsinden double döndürülüyor: percentile_cont bir
// interval üzerinde interval döndürür ve onu Go tarafında bir sayıya çevirmek pgx'te
// fazladan tip işi demek. Saniye cinsinden double hem Prometheus'un beklediği birim
// hem de en sade taşıma.
const endToEndLatencyQuery = `
	SELECT
		percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM updated_at - created_at)),
		percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM updated_at - created_at)),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM updated_at - created_at)),
		count(*)
	FROM notifications
	WHERE status = 'sent' AND updated_at >= now() - $1::interval`

// providerLatencyQuery, sağlayıcı çağrısının KENDİ süresinin yüzdelikleri.
//
// Uçtan uca gecikmenin yanında ayrıca gerekiyor: ikisi birlikte "yavaşlık kuyrukta mı
// sağlayıcıda mı" sorusunu cevaplıyor. Uçtan uca 30 sn ama sağlayıcı 50 ms ise sorun
// hız sınırı/kuyruk; ikisi birlikte yükseldiyse sorun sağlayıcıda.
//
// duration_ms başarısız denemelerde de yazılıyor (süre sonuçtan bağımsız ölçülüyor),
// bu yüzden sayım tüm denemeler üzerinden.
const providerLatencyQuery = `
	SELECT
		percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms),
		percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_ms),
		count(*)
	FROM notification_attempts
	WHERE created_at >= now() - $1::interval`

// LabelledCount, etiketli bir sayaç değeri.
type LabelledCount struct {
	Status  string
	Channel string
	Count   int64

	// Deferred, satırın zamanı HENÜZ GELMEDİĞİNİ söyler (available_at > now()):
	// zamanlanmış gönderim ya da retry backoff'u bekleyen iş. Yalnızca kuyruk
	// derinliği sorgusunda anlamlı.
	Deferred bool
}

// Percentiles, bir gecikme dağılımının özeti. Count 0 ise pencere boş ve yüzdelikler
// anlamsızdır — çağıran metriği hiç yayınlamamalı.
type Percentiles struct {
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Count int64
}

// QueueDepth, aktif durumlardaki iş sayısını kanal, durum ve hazır olma durumuna
// göre döner.
//
// Kendi satır okuma döngüsü var (labelledCounts kullanılmıyor): yalnızca bu sorgu
// dördüncü kolonu seçiyor ve ortak okuyucuya "bu çağrıda bir kolon daha var" bayrağı
// geçirmek, iki satır tasarruf için çağrı yerinde okunamayan bir koşul üretirdi.
func (s *Store) QueueDepth(ctx context.Context) ([]LabelledCount, error) {
	rows, err := s.pool.Query(ctx, queueDepthQuery, activeStatuses)
	if err != nil {
		return nil, fmt.Errorf("kuyruk derinliği okunamadı: %w", err)
	}
	defer rows.Close()

	// Üst sınır: 3 aktif durum × 3 kanal × 2 (hazır/bekleyen).
	counts := make([]LabelledCount, 0, 18)

	for rows.Next() {
		var count LabelledCount
		if scanErr := rows.Scan(
			&count.Status, &count.Channel, &count.Deferred, &count.Count,
		); scanErr != nil {
			return nil, fmt.Errorf("kuyruk derinliği çözülemedi: %w", scanErr)
		}

		counts = append(counts, count)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("kuyruk derinliği okunamadı: %w", rows.Err())
	}

	return counts, nil
}

// TerminalCounts, verilen pencerede bitmiş işlerin sayısını döner.
func (s *Store) TerminalCounts(
	ctx context.Context, window time.Duration,
) ([]LabelledCount, error) {
	return s.labelledCounts(ctx, terminalCountsQuery, window)
}

// labelledCounts, (status, channel, count) döndüren sorguları okur.
func (s *Store) labelledCounts(
	ctx context.Context, query string, args ...any,
) ([]LabelledCount, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("metrik sayımı okunamadı: %w", err)
	}
	defer rows.Close()

	// Üst sınır: 6 durum × 3 kanal.
	counts := make([]LabelledCount, 0, 18)

	for rows.Next() {
		var count LabelledCount
		if scanErr := rows.Scan(&count.Status, &count.Channel, &count.Count); scanErr != nil {
			return nil, fmt.Errorf("metrik sayımı çözülemedi: %w", scanErr)
		}

		counts = append(counts, count)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metrik sayımları okunamadı: %w", err)
	}

	return counts, nil
}

// EndToEndLatency, verilen pencerede sent olan bildirimlerin uçtan uca gecikmesini
// döner (kuyrukta bekleme dâhil).
func (s *Store) EndToEndLatency(ctx context.Context, window time.Duration) (Percentiles, error) {
	return s.percentiles(ctx, endToEndLatencyQuery, window, func(raw float64) time.Duration {
		// percentile_cont bir interval üzerinde çalıştığı için pgx'ten saniye cinsinden
		// float geliyor.
		return time.Duration(raw * float64(time.Second))
	})
}

// ProviderLatency, verilen penceredeki sağlayıcı çağrılarının süresini döner.
func (s *Store) ProviderLatency(ctx context.Context, window time.Duration) (Percentiles, error) {
	return s.percentiles(ctx, providerLatencyQuery, window, func(raw float64) time.Duration {
		// duration_ms milisaniye.
		return time.Duration(raw * float64(time.Millisecond))
	})
}

// percentiles, üç yüzdelik + sayım döndüren sorguları okur ve ham değerleri süreye
// çevirir.
//
// Pencere boşsa percentile_cont NULL döner; bu yüzden hedefler *float64 ve Count 0
// olduğunda çağıran metriği yayınlamıyor (0 ms gecikme yayınlamak yanıltıcı olurdu).
func (s *Store) percentiles(
	ctx context.Context,
	query string,
	window time.Duration,
	convert func(float64) time.Duration,
) (Percentiles, error) {
	var (
		p50, p95, p99 *float64
		count         int64
	)

	if err := s.pool.QueryRow(ctx, query, window).Scan(&p50, &p95, &p99, &count); err != nil {
		return Percentiles{}, fmt.Errorf("gecikme yüzdelikleri okunamadı: %w", err)
	}

	result := Percentiles{Count: count}

	if count == 0 || p50 == nil || p95 == nil || p99 == nil {
		return result, nil
	}

	result.P50 = convert(*p50)
	result.P95 = convert(*p95)
	result.P99 = convert(*p99)

	return result, nil
}
