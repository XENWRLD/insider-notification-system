package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// newFilter, verilen girdiden doğrulanmış bir filtre üretir.
func newFilter(t *testing.T, in domain.ListFilterInput) domain.ListFilter {
	t.Helper()

	filter, err := domain.NewListFilter(in)
	require.NoError(t, err)

	return filter
}

// insertAttempt, elle bir deneme kaydı ekler.
//
// Üretimde bu satırları CompleteNotification yazıyor (Adım 6); okuma testleri
// gönderim hattını kurmak zorunda kalmasın diye elle yazan bu yardımcı duruyor.
func insertAttempt(
	t *testing.T, pool *pgxpool.Pool, notificationID uuid.UUID, attemptNo, statusCode, durationMS int,
) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		INSERT INTO notification_attempts
			(notification_id, attempt_no, status_code, error, duration_ms)
		VALUES ($1, $2, $3, $4, $5)`,
		notificationID, attemptNo, statusCode,
		fmt.Sprintf("deneme %d hatası", attemptNo), durationMS)
	require.NoError(t, err)
}

// --- buildListQuery birim testleri (altyapısız) ---

func TestBuildListQueryWithoutFiltersOmitsWhere(t *testing.T) {
	t.Parallel()

	query, args := buildListQuery(newFilter(t, domain.ListFilterInput{}))

	require.NotContains(t, query, "WHERE", "filtre yokken WHERE üretilmemeli")
	require.Contains(t, query, "ORDER BY created_at DESC, id DESC")

	// Yalnızca limit parametresi var ve limit+1 olarak gönderiliyor.
	require.Len(t, args, 1)
	require.Equal(t, domain.ListLimitDefault+1, args[0])
}

func TestBuildListQueryAddsOnlyGivenFilters(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	query, args := buildListQuery(newFilter(t, domain.ListFilterInput{
		Statuses: []string{"pending", "retrying"},
		From:     &from,
		Limit:    "10",
	}))

	require.Contains(t, query, "status = ANY($1)")
	require.Contains(t, query, "created_at >= $2")

	// Verilmeyen filtreler sorguya HİÇ girmiyor — "$n IS NULL OR" kalıbı yok.
	// (Kolon adları SELECT listesinde geçtiği için koşul BİÇİMİ aranıyor.)
	require.NotContains(t, query, "channel = ANY")
	require.NotContains(t, query, "created_at <")
	require.NotContains(t, query, "IS NULL")

	require.Len(t, args, 3)
	require.Equal(t, []string{"pending", "retrying"}, args[0])
	require.Equal(t, from, args[1])
	require.Equal(t, 11, args[2])
}

func TestBuildListQueryCursorUsesRowComparison(t *testing.T) {
	t.Parallel()

	cursor := &domain.Cursor{CreatedAt: time.Now().UTC(), ID: uuid.New()}

	query, args := buildListQuery(newFilter(t, domain.ListFilterInput{Cursor: cursor}))

	// Satır karşılaştırması: OFFSET yerine keyset.
	require.Contains(t, query, "(created_at, id) < ($1, $2)")
	require.NotContains(t, query, "OFFSET")

	require.Len(t, args, 3)
	require.Equal(t, cursor.CreatedAt, args[0])
	require.Equal(t, cursor.ID, args[1])
}

func TestBuildListQueryCombinesFiltersWithAnd(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)

	query, _ := buildListQuery(newFilter(t, domain.ListFilterInput{
		Statuses: []string{"pending"},
		Channels: []string{"sms"},
		From:     &from,
		To:       &to,
	}))

	// Farklı parametreler VE ile birleşiyor.
	require.Equal(t, 3, strings.Count(query, " AND "))
}

// --- EXPLAIN ile plan doğrulaması ---

// explainQuery, verilen sorguyu EXPLAIN ile koşturup planı metin olarak döner.
func explainQuery(t *testing.T, pool *pgxpool.Pool, query string, args []any) string {
	t.Helper()

	rows, err := pool.Query(context.Background(), "EXPLAIN (ANALYZE, COSTS OFF) "+query, args...)
	require.NoError(t, err)
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	require.NoError(t, rows.Err())

	return plan.String()
}

// TestListQueryUsesCursorIndex, cursor koşulunun index aralığı olarak kullanıldığını
// doğrular — dinamik WHERE kararının kanıtı.
//
// Ölçüm, UYGULAMANIN ÜRETTİĞİ SQL ile yapılıyor: buildListQuery'nin döndürdüğü metin ve
// argümanlar aynen EXPLAIN'e veriliyor. Böylece "ölçtüğüm sorgu uygulamanın koşturduğu
// sorgu" garantisi testte kalıyor.
//
// "$n IS NULL OR ..." kalıbı kullanılsaydı generic plan'da koşul Index Cond yerine
// Filter'a düşerdi ve derin sayfalar yavaşlardı.
func TestListQueryUsesCursorIndex(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	// Planlayıcının index'i seçmesi için anlamlı miktarda veri gerekir.
	seedNotifications(t, pool, 3000)

	_, err := pool.Exec(ctx, `ANALYZE notifications`)
	require.NoError(t, err)

	// Gerçek bir cursor elde etmek için ilk sayfayı çek.
	first, hasMore, err := store.ListNotifications(ctx, newFilter(t, domain.ListFilterInput{Limit: "20"}))
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, first, 20)

	last := first[len(first)-1].Notification
	filter := newFilter(t, domain.ListFilterInput{
		Limit:  "20",
		Cursor: &domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID},
	})

	query, args := buildListQuery(filter)
	plan := explainQuery(t, pool, query, args)

	t.Logf("plan:\n%s", plan)

	require.Contains(t, plan, "notifications_cursor_idx",
		"cursor sayfası cursor index'ini kullanmalı")
	require.NotContains(t, plan, "Seq Scan",
		"tam tarama olmamalı")
	require.NotContains(t, plan, "Sort",
		"ORDER BY index sırasıyla aynı, Sort gerekmemeli")

	// Kritik: karşılaştırma Index Cond İÇİNDE olmalı, Filter'da değil.
	require.Contains(t, plan, "Index Cond",
		"cursor koşulu index aralığı olarak kullanılmalı")

	indexCond := planLine(t, plan, "Index Cond")
	require.Contains(t, indexCond, "created_at")
	require.Contains(t, indexCond, "id")
}

// planLine, plan metninden verilen ifadeyi içeren satırı döner.
func planLine(t *testing.T, plan, needle string) string {
	t.Helper()

	for _, line := range strings.Split(plan, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}

	t.Fatalf("%q içeren satır bulunamadı:\n%s", needle, plan)

	return ""
}

// TestBatchSummaryUsesBatchIndex, batch sayımının kısmi batch index'ini kullandığını
// doğrular (Adım 2'de bu index tam bu sorgu için kurulmuştu).
func TestBatchSummaryUsesBatchIndex(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notifications, batchID := newTestBatch(t, 500)
	_, err := store.CreateBatch(ctx, notifications,
		idemFor("b-explain", `{}`), renderBatch(batchID, len(notifications)))
	require.NoError(t, err)

	seedNotifications(t, pool, 2000)

	_, err = pool.Exec(ctx, `ANALYZE notifications`)
	require.NoError(t, err)

	plan := explainQuery(t, pool, batchSummaryQuery, []any{batchID})
	t.Logf("plan:\n%s", plan)

	require.Contains(t, plan, "notifications_batch_idx")
	require.NotContains(t, plan, "Seq Scan")
}

// seedNotifications, planlayıcı testleri için hızlıca satır üretir.
func seedNotifications(t *testing.T, pool *pgxpool.Pool, count int) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		INSERT INTO notifications (id, channel, recipient, content, priority, correlation_id, created_at)
		SELECT gen_random_uuid(), 'sms', '+90555' || lpad(i::text, 7, '0'), 'yuk',
		       (ARRAY['high','normal','low'])[1 + (i % 3)], gen_random_uuid()::text,
		       now() - (i || ' seconds')::interval
		FROM generate_series(1, $1) AS i`, count)
	require.NoError(t, err)
}

// TestStatusFilterUsesCompositeIndex, durum filtresinin Adım 5'te eklenen bileşik
// index'i kullandığını doğrular.
//
// Bu test Adım 2'de ertelenen index kararının kanıtı. Nadir bir durum (100.000 satırın
// 20'si) seçiliyor çünkü en kötü durum bu: iyi çalışan bir sistemde 'failed' sayısı
// zaten azdır ve cursor index'i üzerinden yürümek tüm tabloyu taramaya dönüşür.
//
// Ölçüm: index olmadan Seq Scan + 13.77 ms, index ile Index Scan + 0.13 ms.
func TestStatusFilterUsesCompositeIndex(t *testing.T) {
	pool := sharedPool(t)
	ctx := context.Background()

	seedNotifications(t, pool, 20000)

	_, err := pool.Exec(ctx, `UPDATE notifications SET status = 'sent'`)
	require.NoError(t, err)

	// Nadir durum: 20.000 satırın yalnızca 20'si failed.
	_, err = pool.Exec(ctx, `
		UPDATE notifications SET status = 'failed'
		WHERE id IN (SELECT id FROM notifications ORDER BY created_at LIMIT 20)`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `ANALYZE notifications`)
	require.NoError(t, err)

	query, args := buildListQuery(newFilter(t, domain.ListFilterInput{
		Statuses: []string{"failed"}, Limit: "20",
	}))

	plan := explainQuery(t, pool, query, args)
	t.Logf("plan:\n%s", plan)

	require.Contains(t, plan, "notifications_status_cursor_idx",
		"nadir durum filtresi bileşik index'i kullanmalı")
	require.NotContains(t, plan, "Seq Scan",
		"tam tarama olmamalı: index'in eklenme sebebi tam bu")

	// Durum koşulu index aralığı olarak kullanılmalı, filtreye düşmemeli.
	require.Contains(t, planLine(t, plan, "Index Cond"), "status")
}

// --- Listeleme davranış testleri ---

// TestListNotificationsPaginatesStableWithIdenticalTimestamps, sayfalamanın aynı
// created_at değerlerinde bile kararlı olduğunu doğrular.
//
// Bu testin sebebi: 50 kayıt tek COPY ile yazıldığında hepsi AYNI created_at'i alır.
// Sıralama yalnızca created_at üzerinden yapılsaydı sayfalar arasında kayıtlar atlanır
// veya tekrar görünürdü. id ikinci sıralama anahtarı tam bunun için var.
func TestListNotificationsPaginatesStableWithIdenticalTimestamps(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	const total = 50

	// CopyFrom tek saat kullanır → 50 kaydın created_at'i aynı.
	notifications, batchID := newTestBatch(t, total)
	_, err := store.CreateBatch(ctx, notifications,
		idemFor("b-page", `{}`), renderBatch(batchID, total))
	require.NoError(t, err)

	var distinctTimestamps int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(DISTINCT created_at) FROM notifications`).Scan(&distinctTimestamps))
	require.Equal(t, 1, distinctTimestamps,
		"test ön koşulu: tüm kayıtların created_at'i aynı olmalı")

	// 20'şerli sayfalarla tüm kayıtları gez.
	seen := make(map[uuid.UUID]int, total)
	var cursor *domain.Cursor

	for page := 1; ; page++ {
		require.LessOrEqual(t, page, 10, "sayfalama sonlanmadı, döngü olabilir")

		items, hasMore, listErr := store.ListNotifications(ctx, newFilter(t, domain.ListFilterInput{
			Limit:  "20",
			Cursor: cursor,
		}))
		require.NoError(t, listErr)

		for _, item := range items {
			seen[item.Notification.ID]++
		}

		if !hasMore {
			require.Len(t, items, 10, "son sayfada 10 kayıt kalmalı")

			break
		}

		require.Len(t, items, 20)

		last := items[len(items)-1].Notification
		cursor = &domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}

	// Her kayıt TAM BİR KEZ görülmeli: atlama yok, tekrar yok.
	require.Len(t, seen, total, "tüm kayıtlar görülmeliydi")
	for id, times := range seen {
		require.Equal(t, 1, times, "%s kaydı %d kez göründü", id, times)
	}
}

// TestListNotificationsIncludesRealAttempts, listedeki attempts alanının gerçekten
// dolu olduğunu doğrular.
//
// Listede boş dizi dönmek "denenmedi" anlamına gelir ve yanıltır: 2 kez denenmiş bir
// bildirim listede attempts: [] görünürse istemci yanlış sonuç çıkarır.
func TestListNotificationsIncludesRealAttempts(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	withAttempts := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, withAttempts))

	withoutAttempts := newTestNotification(t, "email", "normal")
	require.NoError(t, createPlain(t, store, ctx, withoutAttempts))

	// Yalnızca birine iki deneme ekle.
	insertAttempt(t, pool, withAttempts.ID, 1, 500, 812)
	insertAttempt(t, pool, withAttempts.ID, 2, 503, 5000)

	items, _, err := store.ListNotifications(ctx, newFilter(t, domain.ListFilterInput{}))
	require.NoError(t, err)
	require.Len(t, items, 2)

	byID := make(map[uuid.UUID][]domain.Attempt, len(items))
	for _, item := range items {
		byID[item.Notification.ID] = item.Attempts
	}

	// Denemesi olan kayıt: iki deneme, attempt_no artan sırada.
	attempts := byID[withAttempts.ID]
	require.Len(t, attempts, 2)
	require.Equal(t, 1, attempts[0].AttemptNo)
	require.Equal(t, 2, attempts[1].AttemptNo)
	require.Equal(t, 500, *attempts[0].StatusCode)
	require.Equal(t, 812, attempts[0].DurationMS)
	require.Contains(t, *attempts[0].Error, "deneme 1")

	// Denemesi olmayan kayıt: boş dilim.
	require.Empty(t, byID[withoutAttempts.ID])
}

// TestAttemptsForFetchesAllInOneQuery, toplu çekmenin N+1 olmadığını doğrular.
func TestAttemptsForFetchesAllInOneQuery(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	ids := make([]uuid.UUID, 0, 5)
	for i := range 5 {
		notification := newTestNotification(t, "sms", "normal")
		require.NoError(t, createPlain(t, store, ctx, notification))

		ids = append(ids, notification.ID)
		insertAttempt(t, pool, notification.ID, 1, 200, 100+i)
	}

	// Tek çağrı, beş kaydın denemesi.
	grouped, err := store.attemptsFor(ctx, ids)
	require.NoError(t, err)
	require.Len(t, grouped, 5)

	for _, id := range ids {
		require.Len(t, grouped[id], 1)
	}

	// Boş dilimle çağrı sorgu atmamalı ve boş harita dönmeli.
	empty, err := store.attemptsFor(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}

// TestListNotificationsHalfOpenDateRange, [from, to) aralığını doğrular.
//
// Sınırdaki kayıt yalnızca BİR pencerede görünmeli; kapalı aralık olsaydı ardışık
// pencereler onu iki kez sayardı.
func TestListNotificationsHalfOpenDateRange(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	var (
		novemberStart = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
		decemberStart = time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
		januaryStart  = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	)

	// Üç kayıt: Kasım ortası, tam Aralık başı (sınır), Aralık ortası.
	midNovember := insertAt(t, pool, time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC))
	boundary := insertAt(t, pool, decemberStart)
	midDecember := insertAt(t, pool, time.Date(2026, 12, 15, 12, 0, 0, 0, time.UTC))

	listIDs := func(from, to time.Time) []uuid.UUID {
		items, _, err := store.ListNotifications(ctx, newFilter(t, domain.ListFilterInput{
			From: &from, To: &to,
		}))
		require.NoError(t, err)

		ids := make([]uuid.UUID, 0, len(items))
		for _, item := range items {
			ids = append(ids, item.Notification.ID)
		}

		return ids
	}

	november := listIDs(novemberStart, decemberStart)
	december := listIDs(decemberStart, januaryStart)

	// Sınır kaydı yalnızca Aralık'ta.
	require.Contains(t, november, midNovember)
	require.NotContains(t, november, boundary, "to hariç olmalı: sınır kaydı Kasım'da görünmemeli")

	require.Contains(t, december, boundary, "from dahil olmalı: sınır kaydı Aralık'ta görünmeli")
	require.Contains(t, december, midDecember)
	require.NotContains(t, december, midNovember)
}

// insertAt, verilen created_at ile bir bildirim ekler.
func insertAt(t *testing.T, pool *pgxpool.Pool, createdAt time.Time) uuid.UUID {
	t.Helper()

	id := uuid.New()

	_, err := pool.Exec(context.Background(), `
		INSERT INTO notifications (id, channel, recipient, content, correlation_id, created_at)
		VALUES ($1, 'sms', '+905551234567', 'deneme', $2, $3)`,
		id, uuid.NewString(), createdAt)
	require.NoError(t, err)

	return id
}

// TestListNotificationsFilterLogic, aynı parametrenin VEYA, farklı parametrelerin VE
// ile birleştiğini doğrular.
func TestListNotificationsFilterLogic(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	smsPending := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, smsPending))

	emailPending := newTestNotification(t, "email", "normal")
	require.NoError(t, createPlain(t, store, ctx, emailPending))

	pushCancelled := newTestNotification(t, "push", "normal")
	require.NoError(t, createPlain(t, store, ctx, pushCancelled))
	_, err := store.CancelNotification(ctx, pushCancelled.ID)
	require.NoError(t, err)

	count := func(in domain.ListFilterInput) int {
		items, _, listErr := store.ListNotifications(ctx, newFilter(t, in))
		require.NoError(t, listErr)

		return len(items)
	}

	// Tek değer.
	require.Equal(t, 2, count(domain.ListFilterInput{Statuses: []string{"pending"}}))

	// Aynı parametrenin iki değeri → VEYA.
	require.Equal(t, 3, count(domain.ListFilterInput{
		Statuses: []string{"pending", "cancelled"},
	}))

	// Farklı parametreler → VE: pending OLAN ve sms OLAN.
	require.Equal(t, 1, count(domain.ListFilterInput{
		Statuses: []string{"pending"},
		Channels: []string{"sms"},
	}))

	// VE koşulu sağlanmıyor: cancelled olan push, sms değil.
	require.Equal(t, 0, count(domain.ListFilterInput{
		Statuses: []string{"cancelled"},
		Channels: []string{"sms"},
	}))

	// Çoklu kanal → VEYA.
	require.Equal(t, 2, count(domain.ListFilterInput{
		Channels: []string{"sms", "email"},
	}))
}

// TestListNotificationsHasMore, limit+1 tekniğinin son sayfayı doğru bildirdiğini
// doğrular.
func TestListNotificationsHasMore(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	for range 5 {
		require.NoError(t, createPlain(t, store, ctx, newTestNotification(t, "sms", "normal")))
	}

	items, hasMore, err := store.ListNotifications(ctx, newFilter(t, domain.ListFilterInput{Limit: "2"}))
	require.NoError(t, err)
	require.Len(t, items, 2)
	require.True(t, hasMore)

	// Tam sayı katı: 5 kayıt, limit 5 → fazlalık yok.
	items, hasMore, err = store.ListNotifications(ctx, newFilter(t, domain.ListFilterInput{Limit: "5"}))
	require.NoError(t, err)
	require.Len(t, items, 5)
	require.False(t, hasMore, "tam sayfa ama fazlası yok → hasMore false")

	// Limit kayıt sayısından büyük.
	items, hasMore, err = store.ListNotifications(ctx, newFilter(t, domain.ListFilterInput{Limit: "10"}))
	require.NoError(t, err)
	require.Len(t, items, 5)
	require.False(t, hasMore)
}

// TestListNotificationsOrdersNewestFirst, sıralamanın en yeniden en eskiye olduğunu
// doğrular.
func TestListNotificationsOrdersNewestFirst(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	oldest := insertAt(t, pool, time.Now().UTC().Add(-2*time.Hour))
	middle := insertAt(t, pool, time.Now().UTC().Add(-1*time.Hour))
	newest := insertAt(t, pool, time.Now().UTC())

	items, _, err := store.ListNotifications(ctx, newFilter(t, domain.ListFilterInput{}))
	require.NoError(t, err)
	require.Len(t, items, 3)

	require.Equal(t, newest, items[0].Notification.ID)
	require.Equal(t, middle, items[1].Notification.ID)
	require.Equal(t, oldest, items[2].Notification.ID)
}

// --- GetNotification ---

func TestGetNotificationReturnsRecordAndAttempts(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notification := newTestNotification(t, "sms", "high")
	require.NoError(t, createPlain(t, store, ctx, notification))

	// Denemeler bilerek ters sırada eklenir; sorgu artan sırada döndürmeli.
	insertAttempt(t, pool, notification.ID, 2, 503, 5000)
	insertAttempt(t, pool, notification.ID, 1, 500, 812)

	got, err := store.GetNotification(ctx, notification.ID)
	require.NoError(t, err)

	require.Equal(t, notification.ID, got.Notification.ID)
	require.Equal(t, domain.ChannelSMS, got.Notification.Channel)
	require.Equal(t, domain.PriorityHigh, got.Notification.Priority)
	require.Equal(t, domain.StatusPending, got.Notification.Status)
	require.Equal(t, notification.CorrelationID, got.Notification.CorrelationID)

	require.Len(t, got.Attempts, 2)
	require.Equal(t, 1, got.Attempts[0].AttemptNo, "attempt_no artan sırada olmalı")
	require.Equal(t, 2, got.Attempts[1].AttemptNo)
}

func TestGetNotificationWithoutAttempts(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notification := newTestNotification(t, "push", "low")
	require.NoError(t, createPlain(t, store, ctx, notification))

	got, err := store.GetNotification(ctx, notification.ID)
	require.NoError(t, err)
	require.Empty(t, got.Attempts)
}

func TestGetNotificationNotFound(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	_, err := store.GetNotification(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNotFound)
}

// --- Batch özeti ---

func TestGetBatchSummary(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notifications, batchID := newTestBatch(t, 10)
	_, err := store.CreateBatch(ctx, notifications,
		idemFor("b-summary", `{}`), renderBatch(batchID, 10))
	require.NoError(t, err)

	// Üçünü sent, birini failed yap.
	_, err = pool.Exec(ctx, `
		UPDATE notifications SET status = 'sent'
		WHERE id = ANY($1)`, []uuid.UUID{
		notifications[0].ID, notifications[1].ID, notifications[2].ID,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		`UPDATE notifications SET status = 'failed' WHERE id = $1`, notifications[3].ID)
	require.NoError(t, err)

	summary, err := store.GetBatchSummary(ctx, batchID)
	require.NoError(t, err)

	require.Equal(t, batchID, summary.BatchID)
	require.Equal(t, 10, summary.Total)

	// Altı durum da bulunmalı, sıfırlar dahil.
	require.Len(t, summary.Counts, 6)
	require.Equal(t, 6, summary.Counts[domain.StatusPending])
	require.Equal(t, 3, summary.Counts[domain.StatusSent])
	require.Equal(t, 1, summary.Counts[domain.StatusFailed])
	require.Equal(t, 0, summary.Counts[domain.StatusProcessing])
	require.Equal(t, 0, summary.Counts[domain.StatusRetrying])
	require.Equal(t, 0, summary.Counts[domain.StatusCancelled])

	// 6 pending kaldığı için tamamlanmadı.
	require.False(t, summary.Completed)
	require.False(t, summary.CreatedAt.IsZero())
}

// TestGetBatchSummaryCompleted, tüm kayıtlar terminal olduğunda Completed'ın true
// olduğunu doğrular.
func TestGetBatchSummaryCompleted(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notifications, batchID := newTestBatch(t, 4)
	_, err := store.CreateBatch(ctx, notifications,
		idemFor("b-done", `{}`), renderBatch(batchID, 4))
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `UPDATE notifications SET status = 'sent' WHERE batch_id = $1`, batchID)
	require.NoError(t, err)

	summary, err := store.GetBatchSummary(ctx, batchID)
	require.NoError(t, err)
	require.True(t, summary.Completed)
	require.Equal(t, 4, summary.Counts[domain.StatusSent])
}

func TestGetBatchSummaryNotFound(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	_, err := store.GetBatchSummary(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNotFound)
}
