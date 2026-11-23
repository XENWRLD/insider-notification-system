package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		from    Status
		to      Status
		allowed bool
	}{
		{name: "pending → processing", from: StatusPending, to: StatusProcessing, allowed: true},
		{name: "pending → cancelled", from: StatusPending, to: StatusCancelled, allowed: true},
		{name: "pending → sent atlanamaz", from: StatusPending, to: StatusSent},

		{name: "processing → sent", from: StatusProcessing, to: StatusSent, allowed: true},
		{name: "processing → retrying", from: StatusProcessing, to: StatusRetrying, allowed: true},
		{name: "processing → failed", from: StatusProcessing, to: StatusFailed, allowed: true},
		// Gönderim başladıktan sonra iptal yok: sağlayıcıya istek gitmiş olabilir.
		{name: "processing → cancelled yasak", from: StatusProcessing, to: StatusCancelled},

		{name: "retrying → processing", from: StatusRetrying, to: StatusProcessing, allowed: true},
		{name: "retrying → cancelled", from: StatusRetrying, to: StatusCancelled, allowed: true},

		// Son durumlardan çıkış yok.
		{name: "sent → processing yasak", from: StatusSent, to: StatusProcessing},
		{name: "failed → processing yasak", from: StatusFailed, to: StatusProcessing},
		{name: "cancelled → processing yasak", from: StatusCancelled, to: StatusProcessing},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.allowed, test.from.CanTransitionTo(test.to))
		})
	}
}

func TestStatusIsTerminal(t *testing.T) {
	t.Parallel()

	for _, status := range []Status{StatusSent, StatusFailed, StatusCancelled} {
		require.Truef(t, status.IsTerminal(), "%s son durum olmalı", status)
	}

	for _, status := range []Status{StatusPending, StatusProcessing, StatusRetrying} {
		require.Falsef(t, status.IsTerminal(), "%s son durum olmamalı", status)
	}
}

// TestStatusIsCancellable, PLAN §4'teki "yalnızca pending / retrying iken iptal"
// kuralını sabitler.
func TestStatusIsCancellable(t *testing.T) {
	t.Parallel()

	require.True(t, StatusPending.IsCancellable())
	require.True(t, StatusRetrying.IsCancellable())

	require.False(t, StatusProcessing.IsCancellable())
	require.False(t, StatusSent.IsCancellable())
	require.False(t, StatusFailed.IsCancellable())
	require.False(t, StatusCancelled.IsCancellable())
}

func TestStatusValid(t *testing.T) {
	t.Parallel()

	require.True(t, StatusPending.Valid())
	require.False(t, Status("unknown").Valid())
	require.False(t, Status("").Valid())
}

func TestChannelAndPriorityValid(t *testing.T) {
	t.Parallel()

	require.True(t, ChannelSMS.Valid())
	require.True(t, ChannelEmail.Valid())
	require.True(t, ChannelPush.Valid())
	require.False(t, Channel("fax").Valid())

	require.True(t, PriorityHigh.Valid())
	require.True(t, PriorityNormal.Valid())
	require.True(t, PriorityLow.Valid())
	require.False(t, Priority("urgent").Valid())
}
