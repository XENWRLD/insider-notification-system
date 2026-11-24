package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// testLease, testlerde kullanılan lease süresi. Gerçek varsayılandan kısa:
// testler lease'in dolmasını beklemek yerine süreyi elle geri alır.
const testLease = 30 * time.Second

// seedClaimable, çekilmeye hazır satırları doğrudan SQL ile üretir.
//
// seedNotifications kanalı 'sms' olarak sabitliyor; claim testleri kanal izolasyonunu
// da ölçtüğü için kanal parametreli bir varyant gerekiyor.
func seedClaimable(t *testing.T, pool *pgxpool.Pool, channel string, count int) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		INSERT INTO notifications (id, channel, recipient, content, priority, correlation_id, available_at)
		SELECT gen_random_uuid(), $2, '+90555' || lpad(i::text, 7, '0'), 'yuk',
		       (ARRAY['high','normal','low'])[1 + (i % 3)], gen_random_uuid()::text,
		       now() - (i || ' seconds')::interval
		FROM generate_series(1, $1) AS i`, count, channel)
	require.NoError(t, err)
}

// TestClaimNotificationsSetsLeaseAndIncrementsAttempt, claim'in satıra sahiplik
// yazdığını doğrular: durum, token, bitiş zamanı ve deneme sayacı.
func TestClaimNotificationsSetsLeaseAndIncrementsAttempt(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	got := claimed[0]
	require.Equal(t, notification.ID, got.ID)
	require.Equal(t, domain.StatusProcessing, got.Status)
	require.NotNil(t, got.LeaseToken, "lease token'ı dönen satırda bulunmalı")
	require.NotEqual(t, uuid.Nil, *got.LeaseToken)
	require.NotNil(t, got.LeaseExpiresAt)
	require.WithinDuration(t, time.Now().Add(testLease), *got.LeaseExpiresAt, 5*time.Second)
	require.Equal(t, 1, got.AttemptCount, "deneme sayacı claim anında artmalı")

	// correlation_id worker loglarına buradan taşınır.
	require.Equal(t, notification.CorrelationID, got.CorrelationID)

	// updated_at'i trigger yazar; store elle dokunmuyor.
	require.True(t, got.UpdatedAt.After(got.CreatedAt) || got.UpdatedAt.Equal(got.CreatedAt))
}

// TestClaimNotificationsOrdersByPriorityThenAvailableAt, yüksek önceliğin öne
// geçtiğini doğrular.
//
// Sıralama hem SQL'de (ORDER BY priority_rank) hem Go'da kuruluyor: UPDATE ...
// RETURNING satır sırasını garanti etmediği için Go tarafı da gerekli.
func TestClaimNotificationsOrdersByPriorityThenAvailableAt(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	// Yazma sırası önceliğin tersi: sıralama gerçekten uygulanıyor mu?
	for _, priority := range []string{"low", "normal", "high"} {
		notification := newTestNotification(t, "sms", priority)
		require.NoError(t, createPlain(t, store, ctx, notification))
	}

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 3)

	require.Equal(t, domain.PriorityHigh, claimed[0].Priority)
	require.Equal(t, domain.PriorityNormal, claimed[1].Priority)
	require.Equal(t, domain.PriorityLow, claimed[2].Priority)
}

// TestClaimNotificationsOrdersEqualPriorityByAvailableAt, aynı öncelikte en uzun
// bekleyenin öne geçtiğini doğrular (starvation'ın ilk savunması).
func TestClaimNotificationsOrdersEqualPriorityByAvailableAt(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	older := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, older))

	newer := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, newer))

	_, err := pool.Exec(ctx,
		`UPDATE notifications SET available_at = now() - interval '1 hour' WHERE id = $1`, older.ID)
	require.NoError(t, err)

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	require.Equal(t, older.ID, claimed[0].ID, "daha uzun bekleyen önce çekilmeliydi")
}

// TestClaimNotificationsSkipsFutureAvailableAt, zamanı gelmemiş işin çekilmediğini
// doğrular. Retry gecikmesi ve (bonus) zamanlanmış bildirim aynı alana dayanıyor.
func TestClaimNotificationsSkipsFutureAvailableAt(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	notification := newTestNotification(t, "sms", "high")
	require.NoError(t, createPlain(t, store, ctx, notification))

	_, err := pool.Exec(ctx,
		`UPDATE notifications SET available_at = now() + interval '1 hour' WHERE id = $1`,
		notification.ID)
	require.NoError(t, err)

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Empty(t, claimed, "zamanı gelmemiş iş çekilmemeliydi")
}

// TestClaimNotificationsOnlyClaimsRequestedChannel, kanal havuzlarının birbirinin
// işini çekmediğini doğrular: kanal izolasyonunun veritabanı tarafı.
func TestClaimNotificationsOnlyClaimsRequestedChannel(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	for _, channel := range []string{"sms", "email", "push"} {
		notification := newTestNotification(t, channel, "normal")
		require.NoError(t, createPlain(t, store, ctx, notification))
	}

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelEmail, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, domain.ChannelEmail, claimed[0].Channel)
}

// TestClaimNotificationsIgnoresCancelledAndTerminalRows, yalnızca pending/retrying
// satırların çekildiğini doğrular.
func TestClaimNotificationsIgnoresCancelledAndTerminalRows(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	for _, status := range []domain.Status{
		domain.StatusCancelled, domain.StatusSent, domain.StatusFailed, domain.StatusProcessing,
	} {
		notification := newTestNotification(t, "sms", "normal")
		require.NoError(t, createPlain(t, store, ctx, notification))
		setStatus(t, store, notification.ID, status)
	}

	retrying := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, retrying))
	setStatus(t, store, retrying.ID, domain.StatusRetrying)

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 1, "yalnızca retrying satır çekilmeliydi")
	require.Equal(t, retrying.ID, claimed[0].ID)
}

// TestClaimNotificationsSkipsExhaustedAttempts, deneme hakkı tükenmiş satırın
// çekilmediğini doğrular — zehirli satıra karşı ikinci savunma hattı.
func TestClaimNotificationsSkipsExhaustedAttempts(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	notification := newTestNotification(t, "sms", "high")
	require.NoError(t, createPlain(t, store, ctx, notification))

	_, err := pool.Exec(ctx,
		`UPDATE notifications SET attempt_count = max_attempts WHERE id = $1`, notification.ID)
	require.NoError(t, err)

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Empty(t, claimed, "hakkı tükenmiş satır yeniden çekilmemeliydi")
}

// TestClaimNotificationsReturnsEmptyWhenQueueEmpty, boş kuyruğun hata değil boş dilim
// döndürdüğünü doğrular: worker boşta dönerken hata loglamamalı.
func TestClaimNotificationsReturnsEmptyWhenQueueEmpty(t *testing.T) {
	store := New(sharedPool(t))

	claimed, err := store.ClaimNotifications(context.Background(), domain.ChannelSMS, 20, testLease)

	require.NoError(t, err)
	require.Empty(t, claimed)
}

// TestClaimNotificationsRespectsLimit, tek turda en fazla limit kadar iş çekildiğini
// doğrular: lease tutma süresinin üst sınırı buna dayanıyor.
func TestClaimNotificationsRespectsLimit(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	seedClaimable(t, pool, "sms", 50)

	claimed, err := store.ClaimNotifications(context.Background(), domain.ChannelSMS, 20, testLease)

	require.NoError(t, err)
	require.Len(t, claimed, 20)
}

// TestClaimNotificationsSkipsRowsLockedByAnotherClaim, SKIP LOCKED'ın işini yaptığını
// doğrular: başka bir transaction'ın kilitlediği satır beklenmeden atlanır.
//
// Determinist kurgu: açık bir transaction ilk satırı kilitler, ardından gerçek claim
// koşar. Beklenen, claim'in o satırı atlayıp diğerini alması — kilitte BEKLEMEMESİ.
func TestClaimNotificationsSkipsRowsLockedByAnotherClaim(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	first := newTestNotification(t, "sms", "high")
	require.NoError(t, createPlain(t, store, ctx, first))

	second := newTestNotification(t, "sms", "low")
	require.NoError(t, createPlain(t, store, ctx, second))

	// Başka bir worker'ın claim'i: yüksek öncelikli satırı kilitler ve tutar.
	lockTx, err := pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = lockTx.Rollback(ctx) }()

	var lockedID uuid.UUID
	require.NoError(t, lockTx.QueryRow(ctx, claimSelectQuery, "sms", 1).Scan(&lockedID))
	require.Equal(t, first.ID, lockedID)

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 1, "kilitli satır atlanıp diğeri alınmalıydı")
	require.Equal(t, second.ID, claimed[0].ID)
}

// TestConcurrentClaimsPartitionTheQueue, aynı anda çalışan claim'lerin aynı işi iki kez
// vermediğini doğrular — PLAN §9 "İki worker aynı anda claim" satırı.
func TestConcurrentClaimsPartitionTheQueue(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	const (
		workers   = 3
		batchSize = 20
		total     = workers * batchSize
	)

	seedClaimable(t, pool, "sms", total)

	var (
		mu      sync.Mutex
		seen    = make(map[uuid.UUID]bool, total)
		results = make([][]*domain.Notification, workers)
		wg      sync.WaitGroup
	)

	wg.Add(workers)

	for i := range workers {
		go func() {
			defer wg.Done()

			claimed, err := store.ClaimNotifications(
				context.Background(), domain.ChannelSMS, batchSize, testLease)
			if err != nil {
				t.Errorf("claim hatası: %v", err)

				return
			}

			results[i] = claimed
		}()
	}

	wg.Wait()

	for _, claimed := range results {
		for _, n := range claimed {
			mu.Lock()
			require.False(t, seen[n.ID], "aynı iş iki worker'a verildi: %s", n.ID)
			seen[n.ID] = true
			mu.Unlock()
		}
	}

	require.Len(t, seen, total, "tüm işler paylaşılmalıydı, kayıp yok")
}

// TestReleaseLeasesReturnsWorkAndDecrementsAttempt, kapanışta geri verilen işin
// yeniden çekilebilir hâle geldiğini doğrular.
func TestReleaseLeasesReturnsWorkAndDecrementsAttempt(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	released, err := store.ReleaseLeases(ctx, []LeaseHold{
		{ID: claimed[0].ID, Token: *claimed[0].LeaseToken},
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, released)

	after, err := store.GetNotification(ctx, notification.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusRetrying, after.Notification.Status)
	require.Equal(t, 0, after.Notification.AttemptCount,
		"sağlayıcı hiç çağrılmadı, deneme sayılmamalı")
	require.Nil(t, after.Notification.LeaseToken)
	require.Nil(t, after.Notification.LeaseExpiresAt)

	// En önemlisi: iş gerçekten kuyrukta.
	again, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, again, 1)
	require.Equal(t, notification.ID, again[0].ID)
}

// TestReleaseLeasesIgnoresForeignToken, başka bir worker'ın lease'inin yanlışlıkla
// bırakılamadığını doğrular.
func TestReleaseLeasesIgnoresForeignToken(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	released, err := store.ReleaseLeases(ctx, []LeaseHold{
		{ID: claimed[0].ID, Token: uuid.New()},
	})
	require.NoError(t, err)
	require.Zero(t, released, "yabancı token ile lease bırakılmamalı")

	after, err := store.GetNotification(ctx, notification.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusProcessing, after.Notification.Status)
	require.Equal(t, 1, after.Notification.AttemptCount)
}

// TestReleaseLeasesPairsIdsAndTokens, unnest eşlemesinin çapraz çarpıma dönüşmediğini
// doğrular: A'nın kimliği + B'nin token'ı hiçbir satırı serbest bırakmamalı.
func TestReleaseLeasesPairsIdsAndTokens(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	for range 2 {
		notification := newTestNotification(t, "sms", "normal")
		require.NoError(t, createPlain(t, store, ctx, notification))
	}

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 2)

	// Kimlikler ile token'lar çaprazlanıyor.
	released, err := store.ReleaseLeases(ctx, []LeaseHold{
		{ID: claimed[0].ID, Token: *claimed[1].LeaseToken},
		{ID: claimed[1].ID, Token: *claimed[0].LeaseToken},
	})
	require.NoError(t, err)
	require.Zero(t, released, "çapraz eşleşme hiçbir satırı bırakmamalı")
}

// TestReleaseLeasesWithoutHoldsSkipsQuery, boş listede veritabanına gidilmediğini
// doğrular: kapanışta elinde iş olmayan havuz gereksiz sorgu atmasın.
func TestReleaseLeasesWithoutHoldsSkipsQuery(t *testing.T) {
	store := New(sharedPool(t))

	released, err := store.ReleaseLeases(context.Background(), nil)

	require.NoError(t, err)
	require.Zero(t, released)
}

// TestPriorityRankMatchesGeneratedColumn, Go tarafındaki sıralama anahtarının
// veritabanındaki generated kolonla aynı olduğunu doğrular.
//
// İkisi ayrışırsa worker'ın Go'da sıraladığı grup, veritabanının seçtiği sıradan
// farklı olur ve bu sessizce yanlış öncelik sırası üretir.
func TestPriorityRankMatchesGeneratedColumn(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	for _, priority := range []string{"high", "normal", "low"} {
		notification := newTestNotification(t, "sms", priority)
		require.NoError(t, createPlain(t, store, ctx, notification))

		var rank int16
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT priority_rank FROM notifications WHERE id = $1`, notification.ID).Scan(&rank))

		require.EqualValues(t, rank, domain.Priority(priority).Rank(),
			"%q için Go ve veritabanı sırası ayrıştı", priority)
	}
}

// TestClaimQueryUsesClaimIndex, claim sorgusunun Adım 2'de kurulan kısmi index'i
// kullandığını kanıtlar.
//
// Ölçüm UYGULAMANIN SQL'i ile yapılıyor: claimSelectQuery sabiti aynen EXPLAIN'e
// veriliyor (Adım 5'teki ilke). İç SELECT ölçülüyor çünkü EXPLAIN ANALYZE bir UPDATE'te
// satırları gerçekten değiştirirdi.
//
// Beklenen: Seq Scan yok (tablo büyüdükçe claim yavaşlamasın) ve Sort yok (index
// sırası ORDER BY ile birebir aynı olduğu için sıralama bedava).
func TestClaimQueryUsesClaimIndex(t *testing.T) {
	pool := sharedPool(t)
	ctx := context.Background()

	seedClaimable(t, pool, "sms", 20000)
	seedClaimable(t, pool, "email", 5000)

	// Gerçekçi dağılım: satırların çoğu gönderilmiş durumda ve kısmi index'in dışında.
	_, err := pool.Exec(ctx, `
		UPDATE notifications SET status = 'sent'
		WHERE id IN (SELECT id FROM notifications ORDER BY available_at LIMIT 24000)`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `ANALYZE notifications`)
	require.NoError(t, err)

	plan := explainQuery(t, pool, claimSelectQuery, []any{"sms", 20})

	require.Contains(t, plan, "notifications_claim_idx", "claim index'i kullanılmalıydı")
	require.NotContains(t, plan, "Seq Scan", "tüm tablo taranmamalıydı")
	require.NotContains(t, plan, "Sort", "index sırası ORDER BY ile aynı, sıralama gerekmemeli")
	require.Contains(t, planLine(t, plan, "Index Cond"), "channel")
}

// seedPriority, belirli öncelikte çekilmeye hazır satırlar üretir.
func seedPriority(t *testing.T, pool *pgxpool.Pool, priority string, count int) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		INSERT INTO notifications (id, channel, recipient, content, priority, correlation_id, available_at)
		SELECT gen_random_uuid(), 'sms', '+90555' || lpad(i::text, 7, '0'), 'yuk',
		       $2, gen_random_uuid()::text, now() - interval '1 second'
		FROM generate_series(1, $1) AS i`, count, priority)
	require.NoError(t, err)
}

// TestClaimLeavesLowPriorityBehindWhileHighPriorityArrives, starvation davranışını
// SABİTLER — düzeltmez.
//
// PLAN §11 soru 7'nin ("Low priority mesajlar hiç gönderilmeyebilir mi?") cevabı:
// EVET. Kuyruk sırası tek bir ORDER BY ile belirleniyor (priority_rank, available_at,
// id) ve yüksek öncelikli satır akışı sürdükçe düşük öncelikli satır hiç öne geçemez.
//
// Aging bilinçli olarak UYGULANMADI: sıra anahtarına bir "yaş" terimi eklemek
// (örn. priority_rank - floor(age / threshold)) claim index'ini ve onun üzerine
// kurulu bütün plan kararlarını değiştirir. Yük altında ölçmeden yapılacak bir
// değişiklik değil; README'de gerekçesi ve çözüm yolu yazılı.
//
// Bu test o davranışın BİLİNÇLİ olduğunun kanıtı: aging eklenirse kırılır ve
// kıranın kararı yeniden gözden geçirmesi gerekir.
func TestClaimLeavesLowPriorityBehindWhileHighPriorityArrives(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	// Tek bir düşük öncelikli satır, herkesten önce kuyruğa girdi.
	lowPriority := newTestNotification(t, "sms", "low")
	require.NoError(t, createPlain(t, store, ctx, lowPriority))

	_, err := pool.Exec(ctx,
		`UPDATE notifications SET available_at = now() - interval '1 hour' WHERE id = $1`,
		lowPriority.ID)
	require.NoError(t, err)

	// Üç tur: her turda yüksek öncelikli yeni iş geliyor ve bir parti çekiliyor.
	for round := 1; round <= 3; round++ {
		seedPriority(t, pool, "high", 20)

		claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
		require.NoError(t, err)
		require.Len(t, claimed, 20, "tur %d", round)

		for _, notification := range claimed {
			require.Equal(t, domain.PriorityHigh, notification.Priority,
				"tur %d: yüksek öncelik varken düşük öncelik çekilmemeli", round)
		}

		// Çekilenleri tamamla ki bir sonraki tur temiz başlasın.
		for _, notification := range claimed {
			require.NoError(t, store.CompleteNotification(ctx, NotificationResult{
				ID:         notification.ID,
				LeaseToken: *notification.LeaseToken,
				Status:     domain.StatusSent,
				StatusCode: ptr(202),
			}))
		}
	}

	// En eski satır hâlâ gönderilmedi: bir saattir bekliyor ve sırası hiç gelmedi.
	after, err := store.GetNotification(ctx, lowPriority.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusPending, after.Notification.Status,
		"düşük öncelikli satır yüksek öncelik akışının arkasında kaldı (starvation)")
	require.Zero(t, after.Notification.AttemptCount)

	// Yüksek öncelikli iş bittiğinde sıra ona geliyor: starvation kalıcı değil,
	// yüksek öncelikli akışın sürmesine bağlı.
	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, lowPriority.ID, claimed[0].ID)
}
