package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBackoffWindowGrowsAndCaps, bekleme penceresinin iki katına çıkıp üst sınırda
// durduğunu doğrular.
func TestBackoffWindowGrowsAndCaps(t *testing.T) {
	t.Parallel()

	b := newBackoff(10*time.Millisecond, 80*time.Millisecond)

	wantWindows := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		80 * time.Millisecond,
		80 * time.Millisecond,
	}

	for i, want := range wantWindows {
		require.Equal(t, want, b.window, "tur %d", i)

		delay := b.next()

		// Full jitter: bekleme (0, pencere] aralığında.
		require.Positive(t, delay, "tur %d", i)
		require.LessOrEqual(t, delay, want, "tur %d: bekleme pencereyi aşmamalı", i)
	}
}

// TestBackoffAppliesJitter, beklemenin sabit olmadığını doğrular.
//
// Sabit bekleme olsaydı worker kopyaları aynı tick'te claim index'ine yüklenir ve
// gereksiz çekişme üretirdi.
func TestBackoffAppliesJitter(t *testing.T) {
	t.Parallel()

	b := newBackoff(time.Second, time.Second)

	seen := make(map[time.Duration]bool)

	for range 50 {
		seen[b.next()] = true
	}

	require.Greater(t, len(seen), 1, "full jitter farklı beklemeler üretmeli")
}

// TestBackoffResetsAfterWork, iş bulunduğunda pencerenin başa alındığını doğrular:
// kuyruk doluyken gecikme eklemeyelim (100/s hedefi bunu gerektiriyor).
func TestBackoffResetsAfterWork(t *testing.T) {
	t.Parallel()

	b := newBackoff(10*time.Millisecond, time.Second)

	for range 5 {
		b.next()
	}

	require.Greater(t, b.window, 10*time.Millisecond)

	b.reset()

	require.Equal(t, 10*time.Millisecond, b.window)
}

// TestBackoffWaitReturnsFalseOnCancel, kapanış sinyalinde beklemenin kesildiğini
// doğrular: worker PollMaxInterval kadar asılı kalmamalı.
func TestBackoffWaitReturnsFalseOnCancel(t *testing.T) {
	t.Parallel()

	b := newBackoff(time.Hour, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()

	require.False(t, b.wait(ctx), "iptal edilmiş context'te bekleme sürmemeli")
	require.Less(t, time.Since(started), time.Second, "bekleme hemen kesilmeliydi")
}

// TestBackoffWaitReturnsTrueAfterDelay, normal akışta beklemenin tamamlandığını
// doğrular.
func TestBackoffWaitReturnsTrueAfterDelay(t *testing.T) {
	t.Parallel()

	b := newBackoff(time.Millisecond, time.Millisecond)

	require.True(t, b.wait(context.Background()))
}

// TestBackoffHandlesOverflow, çok büyük pencerelerin taşıp negatife düşmediğini
// doğrular.
func TestBackoffHandlesOverflow(t *testing.T) {
	t.Parallel()

	b := newBackoff(time.Duration(1)<<62, time.Duration(1)<<62)

	for range 5 {
		require.Positive(t, b.next())
		require.Positive(t, b.window)
	}
}
