package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// claimOne, tek bir bildirim yazıp onu claim eder ve lease'iyle döner.
//
// Sonuç yazma testlerinin hepsi "elinde lease olan bir worker" durumundan başlıyor.
func claimOne(t *testing.T, store *Store) *domain.Notification {
	t.Helper()

	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))

	claimed, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 1, testLease)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	return claimed[0]
}

// countAttempts, bir bildirimin deneme kaydı sayısını verir.
func countAttempts(t *testing.T, store *Store, id uuid.UUID) int {
	t.Helper()

	var count int
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM notification_attempts WHERE notification_id = $1`, id).Scan(&count))

	return count
}

func ptr[T any](v T) *T { return &v }

// TestCompleteNotificationMarksSentAndRecordsAttempt, başarılı gönderimin hem durumu
// hem deneme kaydını yazdığını doğrular.
func TestCompleteNotificationMarksSentAndRecordsAttempt(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)

	require.NoError(t, store.CompleteNotification(ctx, NotificationResult{
		ID:                claimed.ID,
		LeaseToken:        *claimed.LeaseToken,
		Status:            domain.StatusSent,
		ProviderMessageID: ptr("msg-42"),
		StatusCode:        ptr(202),
		DurationMS:        37,
	}))

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)

	require.Equal(t, domain.StatusSent, after.Notification.Status)
	require.Equal(t, ptr("msg-42"), after.Notification.ProviderMessageID)
	require.Nil(t, after.Notification.LastError)

	// Lease çifti birlikte boşaltılmalı: satır artık kimsenin değil.
	require.Nil(t, after.Notification.LeaseToken)
	require.Nil(t, after.Notification.LeaseExpiresAt)

	require.Len(t, after.Attempts, 1)
	require.Equal(t, 1, after.Attempts[0].AttemptNo, "attempt_no claim sonrası sayaca eşit olmalı")
	require.Equal(t, ptr(202), after.Attempts[0].StatusCode)
	require.Nil(t, after.Attempts[0].Error)
	require.Equal(t, 37, after.Attempts[0].DurationMS)
}

// TestCompleteNotificationMarksFailed, kalıcı hatanın durumu ve sebebini yazdığını
// doğrular.
func TestCompleteNotificationMarksFailed(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)

	require.NoError(t, store.CompleteNotification(ctx, NotificationResult{
		ID:         claimed.ID,
		LeaseToken: *claimed.LeaseToken,
		Status:     domain.StatusFailed,
		StatusCode: ptr(400),
		LastError:  ptr("alıcı reddedildi"),
		DurationMS: 12,
	}))

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)

	require.Equal(t, domain.StatusFailed, after.Notification.Status)
	require.Equal(t, ptr("alıcı reddedildi"), after.Notification.LastError)
	require.Nil(t, after.Notification.LeaseToken)

	require.Len(t, after.Attempts, 1)
	require.Equal(t, ptr(400), after.Attempts[0].StatusCode)
	require.Equal(t, ptr("alıcı reddedildi"), after.Attempts[0].Error)
}

// TestCompleteNotificationSetsRetryAvailableAt, retry'ın işi ileri tarihlediğini
// doğrular: worker beklemez, satır bekler.
func TestCompleteNotificationSetsRetryAvailableAt(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)

	require.NoError(t, store.CompleteNotification(ctx, NotificationResult{
		ID:         claimed.ID,
		LeaseToken: *claimed.LeaseToken,
		Status:     domain.StatusRetrying,
		StatusCode: ptr(500),
		LastError:  ptr("sağlayıcı hatası"),
		DurationMS: 5000,
		RetryAfter: 5 * time.Second,
	}))

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)

	require.Equal(t, domain.StatusRetrying, after.Notification.Status)
	require.WithinDuration(t, time.Now().Add(5*time.Second), after.Notification.AvailableAt, 3*time.Second)
	require.Equal(t, 1, after.Notification.AttemptCount, "deneme sayacı korunmalı")
	require.Nil(t, after.Notification.LeaseToken)
	require.Len(t, after.Attempts, 1)

	// Zamanı gelmediği için hemen çekilemez.
	claimedAgain, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 20, testLease)
	require.NoError(t, err)
	require.Empty(t, claimedAgain, "retry gecikmesi dolmadan çekilmemeliydi")
}

// TestCompleteNotificationRejectsStaleLeaseToken, gecikmiş ("zombi") worker'ın sonucu
// yazamadığını doğrular — PLAN §9 "Eski lease token ile sonuç yazma → reddedilir".
//
// Kritik ikinci iddia: deneme kaydı da yazılmamalı. Sonuç ve kayıt tek statement'ta
// olduğu için UPDATE eşleşmeyince INSERT de boş kalır.
func TestCompleteNotificationRejectsStaleLeaseToken(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)

	before, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)

	err = store.CompleteNotification(ctx, NotificationResult{
		ID:         claimed.ID,
		LeaseToken: uuid.New(), // artık bizim olmayan token
		Status:     domain.StatusSent,
		StatusCode: ptr(202),
	})
	require.ErrorIs(t, err, ErrLeaseLost)

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusProcessing, after.Notification.Status, "satır değişmemeliydi")
	require.Equal(t, before.Notification.AttemptCount, after.Notification.AttemptCount)
	require.Equal(t, before.Notification.LeaseToken, after.Notification.LeaseToken)
	require.Zero(t, countAttempts(t, store, claimed.ID), "zombi worker deneme kaydı bırakmamalı")
}

// TestCompleteNotificationRejectsNonProcessingRow, processing olmayan satıra sonuç
// yazılamadığını doğrular: iptal edilmiş iş "gönderildi" olamaz.
func TestCompleteNotificationRejectsNonProcessingRow(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)

	// Reaper devraldı ve satır başka bir duruma geçti.
	_, err := store.pool.Exec(ctx, `
		UPDATE notifications SET status = 'cancelled', lease_token = NULL, lease_expires_at = NULL
		WHERE id = $1`, claimed.ID)
	require.NoError(t, err)

	err = store.CompleteNotification(ctx, NotificationResult{
		ID:         claimed.ID,
		LeaseToken: *claimed.LeaseToken,
		Status:     domain.StatusSent,
	})
	require.ErrorIs(t, err, ErrLeaseLost)
	require.Zero(t, countAttempts(t, store, claimed.ID))
}

// TestReleasedLeaseDoesNotCollideOnAttemptNo, kapanışta geri verilen işin yeniden
// claim edilip tamamlanabildiğini doğrular.
//
// Değişmez: deneme kaydını yalnızca CompleteNotification yazar, ReleaseLeases ise
// yalnızca sonuç üretmemiş claim'leri geri alır. Dolayısıyla geri alınan attempt_count
// değerine ait bir kayıt hiç yazılmamıştır ve UNIQUE (notification_id, attempt_no)
// ihlal edilemez. Bu test o değişmezi sabitliyor.
func TestReleasedLeaseDoesNotCollideOnAttemptNo(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)

	released, err := store.ReleaseLeases(ctx,
		[]LeaseHold{{ID: claimed.ID, Token: *claimed.LeaseToken}})
	require.NoError(t, err)
	require.EqualValues(t, 1, released)

	again, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 1, testLease)
	require.NoError(t, err)
	require.Len(t, again, 1)
	require.Equal(t, 1, again[0].AttemptCount, "sayaç geri alındığı için yine 1 olmalı")

	require.NoError(t, store.CompleteNotification(ctx, NotificationResult{
		ID:         again[0].ID,
		LeaseToken: *again[0].LeaseToken,
		Status:     domain.StatusSent,
		StatusCode: ptr(202),
	}))

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusSent, after.Notification.Status)
	require.Len(t, after.Attempts, 1)
	require.Equal(t, 1, after.Attempts[0].AttemptNo)
}

// TestCompleteNotificationWritesSequentialAttemptNumbers, ardışık denemelerin ayrı
// kayıtlar ürettiğini doğrular: deneme geçmişi gerçekten geçmiş olsun.
func TestCompleteNotificationWritesSequentialAttemptNumbers(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)

	require.NoError(t, store.CompleteNotification(ctx, NotificationResult{
		ID:         claimed.ID,
		LeaseToken: *claimed.LeaseToken,
		Status:     domain.StatusRetrying,
		StatusCode: ptr(500),
		LastError:  ptr("ilk deneme"),
		RetryAfter: 0,
	}))

	second, err := store.ClaimNotifications(ctx, domain.ChannelSMS, 1, testLease)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, 2, second[0].AttemptCount)

	require.NoError(t, store.CompleteNotification(ctx, NotificationResult{
		ID:         second[0].ID,
		LeaseToken: *second[0].LeaseToken,
		Status:     domain.StatusSent,
		StatusCode: ptr(202),
	}))

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)
	require.Len(t, after.Attempts, 2)
	require.Equal(t, 1, after.Attempts[0].AttemptNo)
	require.Equal(t, 2, after.Attempts[1].AttemptNo)
	require.Equal(t, ptr("ilk deneme"), after.Attempts[0].Error)
}

// TestValidateResultStatusRejectsIllegalTransitions, durum makinesine uymayan hedefin
// veritabanına hiç gitmediğini doğrular. Saf birim testi: Docker gerektirmez.
func TestValidateResultStatusRejectsIllegalTransitions(t *testing.T) {
	t.Parallel()

	for _, status := range []domain.Status{
		domain.StatusSent, domain.StatusFailed, domain.StatusRetrying,
	} {
		require.NoError(t, validateResultStatus(status), "%s processing'den geçilebilir", status)
	}

	for _, status := range []domain.Status{
		domain.StatusPending, domain.StatusProcessing, domain.StatusCancelled, domain.Status("bogus"),
	} {
		require.ErrorIs(t, validateResultStatus(status), ErrInvalidResultStatus,
			"%s processing'den geçilemez", status)
	}
}

// TestCompleteNotificationRejectsInvalidStatusWithoutQuery, geçersiz durumda
// veritabanına gidilmediğini doğrular: hata programlama hatası, veri hatası değil.
func TestCompleteNotificationRejectsInvalidStatusWithoutQuery(t *testing.T) {
	store := New(sharedPool(t))
	ctx := context.Background()

	claimed := claimOne(t, store)

	err := store.CompleteNotification(ctx, NotificationResult{
		ID:         claimed.ID,
		LeaseToken: *claimed.LeaseToken,
		Status:     domain.StatusCancelled,
	})

	require.ErrorIs(t, err, ErrInvalidResultStatus)

	after, err := store.GetNotification(ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusProcessing, after.Notification.Status)
	require.Zero(t, countAttempts(t, store, claimed.ID))
}
