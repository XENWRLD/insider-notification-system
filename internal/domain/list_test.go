package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewListFilterDefaults(t *testing.T) {
	t.Parallel()

	filter, err := NewListFilter(ListFilterInput{})
	require.NoError(t, err)

	require.Equal(t, ListLimitDefault, filter.Limit)
	require.Empty(t, filter.Statuses)
	require.Empty(t, filter.Channels)
	require.Nil(t, filter.From)
	require.Nil(t, filter.To)
	require.Nil(t, filter.Cursor)
}

func TestNewListFilterAcceptsMultipleValues(t *testing.T) {
	t.Parallel()

	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)

	filter, err := NewListFilter(ListFilterInput{
		Statuses: []string{"pending", "retrying"},
		Channels: []string{"sms", "email"},
		From:     &from,
		To:       &to,
		Limit:    "50",
	})
	require.NoError(t, err)

	// Aynı parametrenin değerleri VEYA ile birleşecek; filtre ikisini de taşır.
	require.Equal(t, []Status{StatusPending, StatusRetrying}, filter.Statuses)
	require.Equal(t, []Channel{ChannelSMS, ChannelEmail}, filter.Channels)
	require.Equal(t, 50, filter.Limit)
	require.Equal(t, from, *filter.From)
	require.Equal(t, to, *filter.To)
}

func TestNewListFilterRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	later := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	earlier := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		input ListFilterInput
		field string
	}{
		{
			name:  "bilinmeyen durum",
			input: ListFilterInput{Statuses: []string{"unknown"}},
			field: "status",
		},
		{
			name:  "bilinmeyen kanal",
			input: ListFilterInput{Channels: []string{"fax"}},
			field: "channel",
		},
		{
			name:  "limit sayı değil",
			input: ListFilterInput{Limit: "yirmi"},
			field: "limit",
		},
		{
			name:  "limit sıfır",
			input: ListFilterInput{Limit: "0"},
			field: "limit",
		},
		{
			name:  "limit negatif",
			input: ListFilterInput{Limit: "-5"},
			field: "limit",
		},
		{
			name:  "limit sınırın üstü",
			input: ListFilterInput{Limit: "101"},
			field: "limit",
		},
		{
			name:  "from to'dan büyük",
			input: ListFilterInput{From: &later, To: &earlier},
			field: "from",
		},
		{
			// Yarı açık aralık [from, to): eşitlik boş küme demek.
			name:  "from to'ya eşit",
			input: ListFilterInput{From: &earlier, To: &earlier},
			field: "from",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewListFilter(test.input)
			require.Contains(t, fieldMessages(t, err), test.field)
		})
	}
}

// TestNewListFilterCollectsAllErrors, tüm filtre hatalarının tek yanıtta döndüğünü
// doğrular.
func TestNewListFilterCollectsAllErrors(t *testing.T) {
	t.Parallel()

	_, err := NewListFilter(ListFilterInput{
		Statuses: []string{"unknown"},
		Channels: []string{"fax"},
		Limit:    "500",
	})

	messages := fieldMessages(t, err)
	require.Contains(t, messages, "status")
	require.Contains(t, messages, "channel")
	require.Contains(t, messages, "limit")
	require.Len(t, messages, 3)
}

func TestNewListFilterAcceptsLimitBoundaries(t *testing.T) {
	t.Parallel()

	for _, limit := range []string{"1", "100"} {
		filter, err := NewListFilter(ListFilterInput{Limit: limit})
		require.NoError(t, err, "limit %s kabul edilmeliydi", limit)
		require.Positive(t, filter.Limit)
	}
}

func TestNewListFilterPassesCursorThrough(t *testing.T) {
	t.Parallel()

	cursor := &Cursor{CreatedAt: time.Now().UTC(), ID: uuid.New()}

	filter, err := NewListFilter(ListFilterInput{Cursor: cursor})
	require.NoError(t, err)
	require.Equal(t, cursor, filter.Cursor)
}

// TestNewBatchSummaryFillsAllStatuses, sabit şemayı doğrular: altı durum da bulunmalı.
func TestNewBatchSummaryFillsAllStatuses(t *testing.T) {
	t.Parallel()

	batchID := uuid.New()
	createdAt := time.Now().UTC()

	summary := NewBatchSummary(batchID, map[Status]int{
		StatusPending: 900,
		StatusSent:    100,
	}, createdAt)

	require.Equal(t, batchID, summary.BatchID)
	require.Equal(t, 1000, summary.Total)
	require.Equal(t, createdAt, summary.CreatedAt)

	// Altısı da var; verilmeyenler sıfır.
	require.Len(t, summary.Counts, 6)
	require.Equal(t, 900, summary.Counts[StatusPending])
	require.Equal(t, 100, summary.Counts[StatusSent])
	require.Equal(t, 0, summary.Counts[StatusProcessing])
	require.Equal(t, 0, summary.Counts[StatusRetrying])
	require.Equal(t, 0, summary.Counts[StatusFailed])
	require.Equal(t, 0, summary.Counts[StatusCancelled])
}

// TestNewBatchSummaryCompleted, Completed hesabının IsTerminal'a dayandığını doğrular.
func TestNewBatchSummaryCompleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		counts    map[Status]int
		completed bool
	}{
		{
			name:      "hepsi sent",
			counts:    map[Status]int{StatusSent: 10},
			completed: true,
		},
		{
			name:      "terminal durumlar karışık",
			counts:    map[Status]int{StatusSent: 5, StatusFailed: 3, StatusCancelled: 2},
			completed: true,
		},
		{
			// Tek bir pending bile batch'i tamamlanmamış yapar.
			name:   "bir tane pending kaldı",
			counts: map[Status]int{StatusSent: 999, StatusPending: 1},
		},
		{
			name:   "processing var",
			counts: map[Status]int{StatusSent: 50, StatusProcessing: 1},
		},
		{
			name:   "retrying var",
			counts: map[Status]int{StatusRetrying: 1},
		},
		{
			// Boş batch: terminal olmayan kayıt yok, teknik olarak tamamlanmış.
			// Store bu durumu 404'e çevirecek, buraya hiç gelmemeli.
			name:      "boş",
			counts:    map[Status]int{},
			completed: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			summary := NewBatchSummary(uuid.New(), test.counts, time.Now().UTC())
			require.Equal(t, test.completed, summary.Completed)
		})
	}
}
