package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// expireLease, bir satırın lease'ini geçmişe çeker: worker çökmüş gibi davranılır.
//
// Testin gerçek lease süresini beklemesine gerek yok; önemli olan reaper'ın
// "lease_expires_at <= now()" koşulunu nasıl değerlendirdiği.
func expireLease(t *testing.T, store *Store, id uuid.UUID) {
	t.Helper()

	_, err := store.pool.Exec(context.Background(), `
		UPDATE notifications SET lease_expires_at = now() - interval '1 second'
		WHERE id = $1`, id)
	require.NoError(t, err)
}

// TestReclaimExpiredLeasesReturnsWorkToQueue, çöken worker'ın işinin takılı
// kalmadığını doğrular — PLAN §9 "Lease süresi dolar → reaper işi geri koyar".
func TestReclaimExpiredLeasesReturnsWorkToQueue(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)
	expireLease(t, store, claimed.ID)

	reclaimed, err := store.ReclaimExpiredLeases(ctx, 100)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)

	require.Equal(t, claimed.ID, reclaimed[0].ID)
	require.Equal(t, domain.ChannelSMS, reclaimed[0].Channel)
	require.Equal(t, domain.StatusRetrying, reclaimed[0].Status)
	require.Equal(t, claimed.CorrelationID, reclaimed[0].CorrelationID,
		"kurtarma, isteği başlatan çağrının kimliğiyle loglanabilmeli")
	require.Equal(t, 1, reclaimed[0].AttemptCount)

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusRetrying, after.Notification.Status)
	require.Equal(t, 1, after.Notification.AttemptCount, "deneme gerçekten yapıldı, sayaç korunmalı")
	require.Nil(t, after.Notification.LeaseToken)
	require.Nil(t, after.Notification.LeaseExpiresAt)
	require.NotNil(t, after.Notification.LastError)
	require.Contains(t, *after.Notification.LastError, "lease süresi doldu")

	// En önemlisi: iş gerçekten yeniden çekilebilir.
	again, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Len(t, again, 1)
	require.Equal(t, claimed.ID, again[0].ID)
	require.Equal(t, 2, again[0].AttemptCount)
}

// TestReclaimExpiredLeasesInvalidatesOldToken, kurtarma sonrası eski worker'ın
// sonucunu yazamadığını doğrular: reaper ile zombi koruması birlikte çalışıyor.
func TestReclaimExpiredLeasesInvalidatesOldToken(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)
	expireLease(t, store, claimed.ID)

	_, err := store.ReclaimExpiredLeases(ctx, 100)
	require.NoError(t, err)

	err = store.CompleteNotification(ctx, NotificationResult{
		ID:         claimed.ID,
		LeaseToken: *claimed.LeaseToken,
		Status:     domain.StatusSent,
		StatusCode: ptr(202),
	})

	require.ErrorIs(t, err, ErrLeaseLost, "reaper devraldıktan sonra eski sonuç yazılmamalı")
	require.Zero(t, countAttempts(t, store, claimed.ID))
}

// TestReclaimExpiredLeasesFailsExhaustedRows, hakkı tükenmiş satırın retrying'de
// gömülü kalmak yerine failed olduğunu doğrular.
//
// Senaryo: worker her turda sonuç yazmadan çöküyor. attempt_count max_attempts'a
// ulaştığında claim sorgusu (attempt_count < max_attempts) satırı artık çekmez;
// reaper onu retrying'e döndürseydi satır sonsuza kadar kuyrukta görünüp hiç
// işlenmeyecekti.
func TestReclaimExpiredLeasesFailsExhaustedRows(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))

	// Her tur: claim + lease'i süresiz bırakmadan çök.
	for attempt := 1; attempt <= domain.DefaultMaxAttempts; attempt++ {
		claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
		require.NoError(t, err, "tur %d", attempt)
		require.Len(t, claimed, 1, "tur %d: iş çekilebilir olmalıydı", attempt)
		require.Equal(t, attempt, claimed[0].AttemptCount)

		expireLease(t, store, claimed[0].ID)

		reclaimed, err := store.ReclaimExpiredLeases(ctx, 100)
		require.NoError(t, err)
		require.Len(t, reclaimed, 1)

		if attempt < domain.DefaultMaxAttempts {
			require.Equal(t, domain.StatusRetrying, reclaimed[0].Status,
				"tur %d: hak tükenmedi, kuyruğa dönmeli", attempt)
		}
	}

	require.Equal(t, domain.StatusFailed, func() domain.Status {
		after, err := store.GetNotification(ctx, notification.ID)
		require.NoError(t, err)

		return after.Notification.Status
	}(), "deneme hakkı tükenince satır failed olmalı")

	after, err := store.GetNotification(ctx, notification.ID)
	require.NoError(t, err)
	require.Equal(t, domain.DefaultMaxAttempts, after.Notification.AttemptCount)
	require.NotNil(t, after.Notification.LastError)
	require.Contains(t, *after.Notification.LastError, "deneme hakkı tükendi")

	// Takılı satır kalmadı: ne çekilebilir ne processing'de.
	claimable, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Empty(t, claimable)
}

// TestReclaimExpiredLeasesIgnoresLiveLeases, süresi dolmamış lease'in elinden işin
// alınmadığını doğrular: çalışan worker'ın işi kesilmemeli.
func TestReclaimExpiredLeasesIgnoresLiveLeases(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)

	reclaimed, err := store.ReclaimExpiredLeases(ctx, 100)
	require.NoError(t, err)
	require.Empty(t, reclaimed, "lease hâlâ geçerliyken iş alınmamalı")

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusProcessing, after.Notification.Status)
	require.Equal(t, claimed.LeaseToken, after.Notification.LeaseToken)
}

// TestReclaimExpiredLeasesRespectsLimit, tek süpürmenin sınırsız yazmaya dönüşmediğini
// doğrular.
func TestReclaimExpiredLeasesRespectsLimit(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	seedClaimable(t, pool, "sms", 10)

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 10, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 10)

	_, err = pool.Exec(ctx,
		`UPDATE notifications SET lease_expires_at = now() - interval '1 second'
		 WHERE status = 'processing'`)
	require.NoError(t, err)

	first, err := store.ReclaimExpiredLeases(ctx, 4)
	require.NoError(t, err)
	require.Len(t, first, 4, "limit kadar satır kurtarılmalı")

	rest, err := store.ReclaimExpiredLeases(ctx, 100)
	require.NoError(t, err)
	require.Len(t, rest, 6, "kalanı sonraki turda gelmeli")
}

// TestReclaimExpiredLeasesReturnsEmptyWhenNothingExpired, boş süpürmenin hata
// olmadığını doğrular: reaper her tick'te hata loglamamalı.
func TestReclaimExpiredLeasesReturnsEmptyWhenNothingExpired(t *testing.T) {
	store := New(sharedPool(t))

	reclaimed, err := store.ReclaimExpiredLeases(context.Background(), 100)

	require.NoError(t, err)
	require.Empty(t, reclaimed)
}

// TestReclaimExpiredLeasesKeepsAvailableAtInPast, kurtarılan işin beklemeden
// çekilebildiğini doğrular: available_at zaten geçmişte, dokunulmamalı.
func TestReclaimExpiredLeasesKeepsAvailableAtInPast(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)
	expireLease(t, store, claimed.ID)

	_, err := store.ReclaimExpiredLeases(ctx, 100)
	require.NoError(t, err)

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)
	require.True(t, after.Notification.AvailableAt.Before(time.Now()),
		"kurtarılan iş anında çekilebilir olmalı")
}

// TestReaperQueryUsesLeaseExpiryIndex, reaper'ın Adım 2'de kurulan kısmi index'i
// kullandığını kanıtlar.
//
// Kısmi index (lease_expires_at) WHERE status = 'processing' sayesinde süpürme,
// gönderilmiş milyonlarca satırdan etkilenmez. EXPLAIN iç SELECT üzerinde koşuyor:
// UPDATE üzerinde ANALYZE satırları gerçekten değiştirirdi.
func TestReaperQueryUsesLeaseExpiryIndex(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)
	ctx := context.Background()

	seedClaimable(t, pool, "sms", 20000)

	// Satırların çoğu gönderilmiş; yalnızca küçük bir kısmı processing.
	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 50, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 50)

	_, err = pool.Exec(ctx, `
		UPDATE notifications SET status = 'sent'
		WHERE status IN ('pending', 'retrying')`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `ANALYZE notifications`)
	require.NoError(t, err)

	plan := explainQuery(t, pool, reaperSelectQuery, []any{100})

	require.Contains(t, plan, "notifications_lease_expiry_idx", "reaper index'i kullanılmalıydı")
	require.NotContains(t, plan, "Seq Scan", "tüm tablo taranmamalıydı")
}
