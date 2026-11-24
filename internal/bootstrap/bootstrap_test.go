package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testDelay, testlerde gerçek 2 saniyelik bekleme yerine kullanılan gecikme.
const testDelay = time.Millisecond

// discardLogger, test çıktısını kirletmemek için logları yutar.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestWaitForDependencySucceedsAfterTransientFailures, soğuk açılışta bağımlılığın
// birkaç saniye geç gelmesinin tolere edildiğini doğrular.
func TestWaitForDependencySucceedsAfterTransientFailures(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	ping := func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("bağlantı reddedildi")
		}

		return nil
	}

	err := waitForDependency(context.Background(), discardLogger(), "postgres", ping, 10, testDelay)

	require.NoError(t, err)
	require.EqualValues(t, 3, calls.Load(), "üçüncü denemede başarılı olmalıydı")
}

// TestWaitForDependencyGivesUpAfterMaxAttempts, süresiz beklemediğini ve son hatayı
// sardığını doğrular: hatalı yapılandırma gizlenmemeli.
func TestWaitForDependencyGivesUpAfterMaxAttempts(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("bağlantı reddedildi")

	var calls atomic.Int32

	ping := func(context.Context) error {
		calls.Add(1)

		return wantErr
	}

	err := waitForDependency(context.Background(), discardLogger(), "redis", ping, 3, testDelay)

	require.ErrorIs(t, err, wantErr, "son hata sarılarak dönmeliydi")
	require.Contains(t, err.Error(), "redis")
	require.EqualValues(t, 3, calls.Load(), "deneme hakkı kadar denenmeliydi")
}

// TestWaitForDependencyStopsOnContextCancel, kapanış sinyali gelince beklemenin
// kesildiğini doğrular: SIGTERM alan process 20 saniye asılı kalmamalı.
func TestWaitForDependencyStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	var calls atomic.Int32

	ping := func(context.Context) error {
		// İlk denemeden sonra kapanış sinyali gelmiş gibi davranılır.
		calls.Add(1)
		cancel()

		return errors.New("bağlantı reddedildi")
	}

	err := waitForDependency(ctx, discardLogger(), "postgres", ping, 10, time.Minute)

	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 1, calls.Load(), "iptalden sonra yeniden denenmemeliydi")
}

// TestWaitForDependencyBoundsEachAttempt, her denemenin kendi zaman aşımıyla
// yapıldığını doğrular: takılı bir ping tüm açılışı kilitlemesin.
func TestWaitForDependencyBoundsEachAttempt(t *testing.T) {
	t.Parallel()

	ping := func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "ping context'i zaman aşımlı olmalıydı")
		require.WithinDuration(t, time.Now().Add(testDelay), deadline, time.Second)

		return nil
	}

	require.NoError(t,
		waitForDependency(context.Background(), discardLogger(), "postgres", ping, 1, testDelay))
}
