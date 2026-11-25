package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// claimSelectQuery, çekilecek satırları seçip kilitleyen iç sorgu.
//
// Ayrı bir sabit olarak duruyor çünkü index testi EXPLAIN'i bunun üzerinde çalıştırır:
// EXPLAIN ANALYZE bir UPDATE'te satırları gerçekten değiştirir, bu yüzden plan ölçümü
// yalnızca SELECT ile yapılabilir. Böylece ölçülen metin ile koşan metin aynı kalır.
//
// Index uyumu: notifications_claim_idx (channel, priority_rank, available_at, id) ve
// kısmi yordamı status IN ('pending','retrying'). Eşitlik + sıralama kolonları index
// ile birebir aynı olduğu için Sort adımı gerekmez.
//
// attempt_count < max_attempts ikinci savunma hattıdır: sonuç yazıcısı hakkı tükenen
// satırı failed yapıyor, reaper da öyle — yani normal akışta bu koşul hiçbir satırı
// elemez. Elle düzenlenmiş, max_attempts'ı sonradan düşürülmüş veya gelecekteki bir
// hatanın ürettiği satır sonsuza kadar denenmesin diye burada. İki kolon da claim
// index'inde olmadığı için koşul heap üzerinde Filter olur; satırlar RETURNING için
// zaten heap'ten okunduğundan ek maliyet yok.
const claimSelectQuery = `
	SELECT id
	FROM notifications
	WHERE channel = $1
	  AND status IN ('pending', 'retrying')
	  AND available_at <= now()
	  AND attempt_count < max_attempts
	ORDER BY priority_rank, available_at, id
	LIMIT $2
	FOR UPDATE SKIP LOCKED`

// claimWorkQuery, işi çekip lease'i tek ifadede yazar.
//
// Neden tek statement? İki ifade (SELECT ... FOR UPDATE, sonra UPDATE) satır
// kilitlerinin UPDATE'e kadar açık bir transaction'da tutulmasını gerektirirdi: her
// hata yolunda transaction sızdırma riski ve sistemin en sıcak sorgusunda çift
// gidiş-dönüş. Tek ifade implicit transaction'da koşar, SKIP LOCKED semantiği aynıdır
// ve açık kalmış transaction imkânsızdır.
//
// Kilitleyen CTE planlayıcı tarafından inline edilmez, dolayısıyla claimed tam bir kez
// üretilir. RETURNING niteliksiz yazılabiliyor çünkü CTE, UPDATE'in FROM listesinde
// değil (UPDATE ... FROM (SELECT id ...) biçiminde id belirsiz olurdu) — bu sayede
// scanNotification ve notificationColumns aynen yeniden kullanılıyor.
//
// lease_token = gen_random_uuid() satır başına değerlendirilir: her satır farklı token
// alır ve token'ı worker uydurmaz, tek kaynak veritabanıdır.
//
// attempt_count claim anında artar. (a) Sağlayıcı çağrısından sonra ölen worker'ın
// denemesi gerçekten yapılmıştır; saymamak zehirli satırı sonsuz döngüye sokar.
// (b) notification_attempts.attempt_no, claim sonrası attempt_count'a eşit olur —
// fazladan okuma gerekmez ve UNIQUE (notification_id, attempt_no) ile uyumludur.
//
// Dış UPDATE'teki status koşulu teknik olarak gereksiz (satırlar kilitli) ama
// değişmezi okunur kılıyor ve CTE ileride değişirse ifadeyi güvende tutuyor.
// updated_at hiç yazılmıyor: trigger onu kendisi günceller.
const claimWorkQuery = `
	WITH claimed AS (` + claimSelectQuery + `
	)
	UPDATE notifications
	SET status           = 'processing',
	    lease_token      = gen_random_uuid(),
	    lease_expires_at = now() + $3::interval,
	    attempt_count    = attempt_count + 1
	WHERE id IN (SELECT id FROM claimed)
	  AND status IN ('pending', 'retrying')
	RETURNING ` + notificationColumns

// ClaimNotifications, verilen kanaldan en fazla limit kadar iş çeker ve her birine
// lease yazar. Kuyruk boşsa boş dilim döner, hata değil.
//
// Dönen satırlar önceliğe göre sıralanır: UPDATE ... RETURNING sırası garanti
// edilmediği için sıralama Go'da tekrar kurulur (bkz. domain.Priority.Rank).
func (s *Store) ClaimNotifications(
	ctx context.Context, channel domain.Channel, limit int, leaseFor time.Duration,
) ([]*domain.Notification, error) {
	rows, err := s.pool.Query(ctx, claimWorkQuery, string(channel), limit, leaseFor)
	if err != nil {
		return nil, fmt.Errorf("iş çekilemedi: %w", err)
	}
	defer rows.Close()

	claimed := make([]*domain.Notification, 0, limit)

	for rows.Next() {
		notification, scanErr := scanNotification(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("çekilen iş okunamadı: %w", scanErr)
		}

		claimed = append(claimed, notification)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("çekilen işler okunamadı: %w", err)
	}

	// claimSelectQuery'nin ORDER BY'ı ile aynı sıra: önce öncelik, sonra sıraya
	// girme zamanı, eşitlikte kimlik (determinist olsun).
	slices.SortFunc(claimed, func(a, b *domain.Notification) int {
		if byPriority := cmp.Compare(a.Priority.Rank(), b.Priority.Rank()); byPriority != 0 {
			return byPriority
		}

		if byTime := a.AvailableAt.Compare(b.AvailableAt); byTime != 0 {
			return byTime
		}

		return cmp.Compare(a.ID.String(), b.ID.String())
	})

	return claimed, nil
}

// LeaseHold, bir worker'ın elindeki lease: satır kimliği ve o claim'e ait token.
type LeaseHold struct {
	ID    uuid.UUID
	Token uuid.UUID
}

// releaseLeasesQuery, claim edilmiş ama hiç denenmemiş satırları kuyruğa geri verir.
//
// unnest ile id ve token ikili olarak eşlenir; id = ANY($1) AND lease_token = ANY($2)
// yazmak çapraz çarpım olur ve başka bir worker'ın satırını da serbest bırakabilirdi.
//
// attempt_count'un azaldığı TEK yer burası: sağlayıcı hiç çağrılmadı, dolayısıyla bu
// değere ait bir notification_attempts satırı da hiç yazılmadı (deneme kaydını yalnızca
// CompleteNotification yazar). Bu yüzden sayacı geri almak attempt_no çakışması
// üretmez. CHECK (attempt_count >= 0) güvende: claim onu artırmıştı ve lease hâlâ bizde.
//
// Durum pending değil retrying oluyor: processing -> pending geçişi durum makinesinde
// yasak. Sonuç, hiç denenmemiş satırın retrying + attempt_count 0 görünmesi.
const releaseLeasesQuery = `
	UPDATE notifications
	SET status = 'retrying',
	    attempt_count = attempt_count - 1,
	    lease_token = NULL, lease_expires_at = NULL
	WHERE (id, lease_token) IN (SELECT * FROM unnest($1::uuid[], $2::uuid[]))
	  AND status = 'processing'`

// ReleaseLeases, verilen lease'leri bırakıp işleri kuyruğa geri koyar ve geri verilen
// satır sayısını döner.
//
// Graceful shutdown'da çağrılır: claim edilmiş ama sender'a hiç geçmemiş satırlar
// lease süresi dolana kadar (dakikalar) beklemek yerine anında yeniden çekilebilir olur.
// Lease'i artık bize ait olmayan satırlar (reaper devraldı) sessizce atlanır.
func (s *Store) ReleaseLeases(ctx context.Context, holds []LeaseHold) (int64, error) {
	if len(holds) == 0 {
		return 0, nil
	}

	ids := make([]uuid.UUID, len(holds))
	tokens := make([]uuid.UUID, len(holds))

	for i, hold := range holds {
		ids[i] = hold.ID
		tokens[i] = hold.Token
	}

	tag, err := s.pool.Exec(ctx, releaseLeasesQuery, ids, tokens)
	if err != nil {
		return 0, fmt.Errorf("lease'ler bırakılamadı: %w", err)
	}

	return tag.RowsAffected(), nil
}

// deferLeasesQuery, denemeyi saymadan işi kuyruğa geri verir, sebebini yazar ve
// istenirse ileri tarihler.
//
// ReleaseLeases'ten iki farkı var:
//
//   - last_error YAZILIR. Bu yol deneme kaydı üretmiyor (attempt_count geri alındığı
//     için attempt_no çakışırdı, bkz. releaseLeasesQuery), dolayısıyla "satır neden
//     bekliyor" sorusunun TEK cevabı bu kolon. GET /notifications/{id} bunu gösteriyor.
//   - available_at İLERİ TARİHLENEBİLİR. delay 0 iken dokunulmuyor: satır kuyruk
//     sırasındaki yerini korur (devre kesici onu zaten bekletiyor). delay > 0 iken
//     sağlayıcının rehberi (429'da Retry-After) ya da devrenin açık kalma süresi
//     uygulanıyor.
//
// attempt_count'un azaldığı İKİNCİ yer burası (ilki releaseLeasesQuery).
// CHECK (attempt_count >= 0) güvende: claim onu artırmıştı ve lease hâlâ bizde.
// Durum pending değil retrying oluyor: processing -> pending geçişi yasak.
const deferLeasesQuery = `
	UPDATE notifications
	SET status = 'retrying',
	    attempt_count = attempt_count - 1,
	    last_error = $3,
	    available_at = CASE WHEN $4::interval > interval '0'
	                        THEN now() + $4::interval
	                        ELSE available_at END,
	    lease_token = NULL, lease_expires_at = NULL
	WHERE (id, lease_token) IN (SELECT * FROM unnest($1::uuid[], $2::uuid[]))
	  AND status = 'processing'`

// DeferLeases, verilen lease'leri denemeyi SAYMADAN bırakır ve etkilenen satır
// sayısını döner.
//
// Çağıranlar (Adım 8): hız sınırı yanıtı (429) ve devre kesicinin devraldığı
// gönderimler — ikisinde de "sağlayıcı mesajı kesin kabul etmedi" ve suç mesajın
// değil. reason satıra last_error olarak yazılır; delay 0 ise available_at korunur.
//
// Lease'i artık bize ait olmayan satırlar (reaper devraldı) sessizce atlanır.
func (s *Store) DeferLeases(
	ctx context.Context, holds []LeaseHold, delay time.Duration, reason string,
) (int64, error) {
	if len(holds) == 0 {
		return 0, nil
	}

	ids := make([]uuid.UUID, len(holds))
	tokens := make([]uuid.UUID, len(holds))

	for i, hold := range holds {
		ids[i] = hold.ID
		tokens[i] = hold.Token
	}

	tag, err := s.pool.Exec(ctx, deferLeasesQuery, ids, tokens, reason, delay)
	if err != nil {
		return 0, fmt.Errorf("lease'ler ertelenemedi: %w", err)
	}

	return tag.RowsAffected(), nil
}
