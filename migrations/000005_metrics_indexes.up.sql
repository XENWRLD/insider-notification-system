-- /metrics uçları için iki index (Adım 9).
--
-- Adım 2'nin kuralı burada da geçerli: index, gerçek sorgu yazıldıktan sonra ve
-- EXPLAIN ile ölçülerek eklenir. Bu iki index, 15 saniyede bir koşan scrape
-- sorgularının tablo boyutuyla birlikte büyümemesi için var.
--
-- 1) notifications_metrics_idx — terminal satırların sayımı ve uçtan uca gecikme.
--
-- Kapsanan iki sorgu:
--   a) SELECT status, channel, count(*) ... WHERE status IN (terminal) GROUP BY 1,2
--   b) percentile_cont(...) ON (updated_at - created_at) WHERE status='sent'
--      AND updated_at >= now() - pencere
--
-- Kolon sırası sorgunun şeklini izliyor: eşitlik (status) → aralık (updated_at) →
-- gruplama ve hesap (channel, created_at). Dört kolonun hepsi index'te olduğu için
-- iki sorgu da INDEX ONLY SCAN yapıyor: heap'e hiç gidilmiyor, yani maliyet aktif
-- olmayan satır sayısından bağımsız.
--
-- Kısmi yordam neden terminal durumlar: kuyruk derinliği sorgusu (pending/retrying/
-- processing) Adım 2'nin kısmi index'lerini kullanıyor ve aktif küme küçük olduğu için
-- ayrı index gerekmiyor. Terminal satırlar ise milyonlara çıkan taraf.
CREATE INDEX notifications_metrics_idx
    ON notifications (status, updated_at, channel, created_at)
    WHERE status IN ('sent', 'failed', 'cancelled');

-- 2) notification_attempts_metrics_idx — sağlayıcı çağrısının KENDİ süresi.
--
-- Uçtan uca gecikme kuyrukta bekleme + sağlayıcı süresini birlikte içeriyor; sağlayıcı
-- yavaşladı mı sorusunu ayrı cevaplamak için denemenin duration_ms'i gerekiyor.
--
-- (created_at, duration_ms) sırası: zaman penceresi aralık koşulu, duration_ms ise
-- yüzdelik hesabının girdisi → index only scan. Tablo append-only olduğu için index
-- hep sağ uca yazılıyor, parçalanma yok.
CREATE INDEX notification_attempts_metrics_idx
    ON notification_attempts (created_at, duration_ms);
