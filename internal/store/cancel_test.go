package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// setStatus, testler için durumu doğrudan değiştirir.
//
// processing durumu lease zorunlu kılıyor (CHECK kısıtı), bu yüzden o duruma geçerken
// lease alanları da doldurulur — worker'ın yapacağı şeyin elle taklidi.
func setStatus(t *testing.T, store *Store, id uuid.UUID, status domain.Status) {
	t.Helper()

	ctx := context.Background()

	if status == domain.StatusProcessing {
		_, err := store.pool.Exec(ctx, `
			UPDATE notifications
			SET status = $2, lease_token = gen_random_uuid(),
			    lease_expires_at = now() + interval '30 seconds'
			WHERE id = $1`, id, string(status))
		require.NoError(t, err)

		return
	}

	_, err := store.pool.Exec(ctx,
		`UPDATE notifications SET status = $2 WHERE id = $1`, id, string(status))
	require.NoError(t, err)
}

// TestCancelNotificationFromPending, bekleyen bir bildirimin iptal edilebildiğini doğrular.
func TestCancelNotificationFromPending(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))

	before := notification.UpdatedAt

	// updated_at trigger'ının ilerlediğini ölçebilmek için kısa bekleme.
	time.Sleep(10 * time.Millisecond)

	cancelled, err := store.CancelNotification(ctx, notification.ID)
	require.NoError(t, err)

	require.Equal(t, domain.StatusCancelled, cancelled.Status)
	require.Equal(t, notification.ID, cancelled.ID)
	require.Equal(t, notification.CorrelationID, cancelled.CorrelationID)

	// Trigger çalışmış olmalı.
	require.True(t, cancelled.UpdatedAt.After(before),
		"updated_at ilerlememiş: %s → %s", before, cancelled.UpdatedAt)

	// Veritabanında da kalıcı.
	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM notifications WHERE id = $1`, notification.ID).Scan(&status))
	require.Equal(t, "cancelled", status)
}

// TestCancelNotificationFromRetrying, retry bekleyen bir bildirimin de iptal
// edilebildiğini doğrular (PLAN §4: yalnızca pending / retrying).
func TestCancelNotificationFromRetrying(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notification := newTestNotification(t, "email", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))
	setStatus(t, store, notification.ID, domain.StatusRetrying)

	cancelled, err := store.CancelNotification(ctx, notification.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusCancelled, cancelled.Status)
}

// TestCancelNotificationRejectsNonCancellableStates, iptal edilemeyen durumları doğrular.
//
// En önemlisi processing: gönderim başladıktan sonra "iptal ettim" demek yanlış bilgi
// olurdu, çünkü sağlayıcıya istek gitmiş olabilir.
func TestCancelNotificationRejectsNonCancellableStates(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	for _, status := range []domain.Status{
		domain.StatusProcessing,
		domain.StatusSent,
		domain.StatusFailed,
		domain.StatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			notification := newTestNotification(t, "sms", "normal")
			require.NoError(t, createPlain(t, store, ctx, notification))
			setStatus(t, store, notification.ID, status)

			_, err := store.CancelNotification(ctx, notification.ID)
			require.ErrorIs(t, err, ErrNotCancellable)

			// Kritik: durum DEĞİŞMEMELİ.
			var current string
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT status FROM notifications WHERE id = $1`, notification.ID).Scan(&current))
			require.Equal(t, string(status), current, "başarısız iptal durumu değiştirmemeli")
		})
	}
}

// TestCancelNotificationNotFound, olmayan kayıt için ErrNotFound döndüğünü doğrular.
//
// 404 ile 409'un ayrılması önemli: "böyle bir bildirim yok" ile "var ama iptal edilemez"
// istemci için çok farklı iki durum.
func TestCancelNotificationNotFound(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	_, err := store.CancelNotification(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNotFound)
	require.NotErrorIs(t, err, ErrNotCancellable)
}

// TestCancelNotificationIsIdempotentlyRejected, ikinci iptal denemesinin 409 verdiğini
// doğrular: cancelled terminal bir durum.
func TestCancelNotificationIsIdempotentlyRejected(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))

	_, err := store.CancelNotification(ctx, notification.ID)
	require.NoError(t, err)

	_, err = store.CancelNotification(ctx, notification.ID)
	require.ErrorIs(t, err, ErrNotCancellable)
}

// TestCancelNotificationLosesRaceAgainstClaim, iptal ile worker claim'i aynı anda
// geldiğinde ne olduğunu doğrular (docs/PLAN.md §11 sorusu).
//
// Senaryo: claim bir transaction içinde satırı FOR UPDATE ile kilitler ve processing
// yazar ama henüz commit etmez. İptal bu sırada gelir ve satır kilidinde BEKLER.
// Claim commit edince iptalin WHERE koşulu YENİDEN değerlendirilir; durum artık
// processing olduğu için hiçbir satır güncellenmez → ErrNotCancellable.
//
// Yani "iptal ettim" denip mesajın yine gitmesi mümkün değil.
func TestCancelNotificationLosesRaceAgainstClaim(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))

	// Worker'ın claim transaction'ı: satırı kilitle, processing yaz, commit ETME.
	claimTx, err := pool.Begin(ctx)
	require.NoError(t, err)

	var claimedID uuid.UUID
	require.NoError(t, claimTx.QueryRow(ctx, `
		SELECT id FROM notifications
		WHERE channel = 'sms' AND status IN ('pending', 'retrying')
		ORDER BY priority_rank, available_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`).Scan(&claimedID))
	require.Equal(t, notification.ID, claimedID)

	_, err = claimTx.Exec(ctx, `
		UPDATE notifications
		SET status = 'processing', lease_token = gen_random_uuid(),
		    lease_expires_at = now() + interval '30 seconds'
		WHERE id = $1`, claimedID)
	require.NoError(t, err)

	// İptal ayrı bir goroutine'de: satır kilidinde bekleyecek.
	cancelResult := make(chan error, 1)
	go func() {
		_, cancelErr := store.CancelNotification(context.Background(), notification.ID)
		cancelResult <- cancelErr
	}()

	// İptalin gerçekten kilitte beklediğini doğrula: henüz sonuç gelmemeli.
	select {
	case err := <-cancelResult:
		t.Fatalf("iptal kilitte beklemeliydi, hemen döndü: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Claim commit ediyor → iptal uyanıyor ve WHERE'i yeniden değerlendiriyor.
	require.NoError(t, claimTx.Commit(ctx))

	select {
	case err := <-cancelResult:
		require.ErrorIs(t, err, ErrNotCancellable,
			"claim kazandıktan sonra iptal reddedilmeliydi")
	case <-time.After(5 * time.Second):
		t.Fatal("iptal commit sonrası sonuçlanmadı")
	}

	// Durum processing kaldı: iptal satırı ezmedi.
	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM notifications WHERE id = $1`, notification.ID).Scan(&status))
	require.Equal(t, "processing", status)
}

// TestCancelNotificationWinsRaceWhenClaimRollsBack, claim geri alınırsa iptalin
// başarılı olduğunu doğrular.
func TestCancelNotificationWinsRaceWhenClaimRollsBack(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	notification := newTestNotification(t, "sms", "normal")
	require.NoError(t, createPlain(t, store, ctx, notification))

	claimTx, err := pool.Begin(ctx)
	require.NoError(t, err)

	_, err = claimTx.Exec(ctx, `
		UPDATE notifications
		SET status = 'processing', lease_token = gen_random_uuid(),
		    lease_expires_at = now() + interval '30 seconds'
		WHERE id = $1`, notification.ID)
	require.NoError(t, err)

	cancelResult := make(chan error, 1)
	go func() {
		_, cancelErr := store.CancelNotification(context.Background(), notification.ID)
		cancelResult <- cancelErr
	}()

	time.Sleep(200 * time.Millisecond)

	// Claim çöktü / geri alındı: satır pending'e döner.
	require.NoError(t, claimTx.Rollback(ctx))

	select {
	case err := <-cancelResult:
		require.NoError(t, err, "claim geri alındıysa iptal başarılı olmalı")
	case <-time.After(5 * time.Second):
		t.Fatal("iptal rollback sonrası sonuçlanmadı")
	}

	var status string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status FROM notifications WHERE id = $1`, notification.ID).Scan(&status))
	require.Equal(t, "cancelled", status)
}
