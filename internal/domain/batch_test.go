package domain

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// validInputs, n adet geçerli SMS girdisi üretir.
func validInputs(n int) []NewNotificationInput {
	inputs := make([]NewNotificationInput, 0, n)
	for i := range n {
		inputs = append(inputs, NewNotificationInput{
			Channel:   "sms",
			Recipient: fmt.Sprintf("+9055500%05d", i),
			Content:   "Kampanya",
		})
	}

	return inputs
}

func TestNewNotificationBatchAcceptsValidSizes(t *testing.T) {
	t.Parallel()

	for _, size := range []int{BatchMinSize, 2, 500, BatchMaxSize} {
		t.Run(fmt.Sprintf("%d öğe", size), func(t *testing.T) {
			t.Parallel()

			notifications, batchID, err := NewNotificationBatch(validInputs(size))
			require.NoError(t, err)
			require.Len(t, notifications, size)
			require.NotEqual(t, "00000000-0000-0000-0000-000000000000", batchID.String())

			// Tüm satırlar aynı batch kimliğini taşımalı: Adım 5'teki batch özeti
			// sorgusu buna dayanacak.
			for _, notification := range notifications {
				require.NotNil(t, notification.BatchID)
				require.Equal(t, batchID, *notification.BatchID)
				require.Equal(t, StatusPending, notification.Status)
				require.Equal(t, DefaultMaxAttempts, notification.MaxAttempts)
			}
		})
	}
}

func TestNewNotificationBatchRejectsInvalidSizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		inputs []NewNotificationInput
	}{
		{name: "nil", inputs: nil},
		{name: "boş", inputs: []NewNotificationInput{}},
		{name: "sınırın bir üstü", inputs: validInputs(BatchMaxSize + 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := NewNotificationBatch(test.inputs)

			// Boyut hatası öğe bazlı değil, istek bazlı: ValidationError döner.
			var verr *ValidationError
			require.ErrorAs(t, err, &verr)
			require.Contains(t, fieldMessages(t, err), "notifications")
		})
	}
}

// TestNewNotificationBatchReportsAllFailingItems, hatalı tüm satırların tek yanıtta
// döndüğünü doğrular.
//
// İlk hatada dursaydı 1000 öğeli bir batch'i düzeltmek 1000 tur alırdı.
func TestNewNotificationBatchReportsAllFailingItems(t *testing.T) {
	t.Parallel()

	inputs := validInputs(10)
	inputs[3].Recipient = "0555" // geçersiz E.164
	inputs[7].Content = ""       // zorunlu alan boş
	inputs[9].Channel = "fax"    // geçersiz kanal

	_, _, err := NewNotificationBatch(inputs)

	var berr *BatchValidationError
	require.ErrorAs(t, err, &berr)
	require.Len(t, berr.Items, 3, "üç hatalı öğenin hepsi dönmeliydi")

	require.Contains(t, berr.Items, 3)
	require.Contains(t, berr.Items, 7)
	require.Contains(t, berr.Items, 9)

	// Hangi alanın bozuk olduğu öğe bazında korunuyor.
	require.Equal(t, "recipient", berr.Items[3].Fields[0].Field)
	require.Equal(t, "content", berr.Items[7].Fields[0].Field)

	// Error() metni sıra numaralarını artan sırada veriyor.
	message := berr.Error()
	require.Contains(t, message, "3. öğe")
	require.Contains(t, message, "7. öğe")
	require.Less(t, strings.Index(message, "3. öğe"), strings.Index(message, "7. öğe"))
}

// TestNewNotificationBatchRejectsWholeBatchOnSingleFailure, bir öğe hatalıysa hiçbir
// bildirim üretilmediğini doğrular (hep ya hiç).
func TestNewNotificationBatchRejectsWholeBatchOnSingleFailure(t *testing.T) {
	t.Parallel()

	inputs := validInputs(5)
	inputs[2].Recipient = "bozuk"

	notifications, batchID, err := NewNotificationBatch(inputs)

	require.Error(t, err)
	require.Nil(t, notifications, "geçerli öğeler bile üretilmemeliydi")
	require.Equal(t, "00000000-0000-0000-0000-000000000000", batchID.String())
}

// TestBatchValidationErrorIsUnwrappable, handler'ın errors.As ile öğe hatalarına
// ulaşabildiğini doğrular.
func TestBatchValidationErrorIsUnwrappable(t *testing.T) {
	t.Parallel()

	inputs := validInputs(2)
	inputs[0].Content = ""

	_, _, err := NewNotificationBatch(inputs)

	var berr *BatchValidationError
	require.True(t, errors.As(err, &berr))

	// Boyut hatası farklı bir tip: handler ikisini ayırt edebilmeli.
	var verr *ValidationError
	require.False(t, errors.As(err, &verr),
		"öğe hatası ValidationError olarak görünmemeli, aksi hâlde handler yanlış dalı seçer")
}

// TestNewNotificationBatchMixedChannels, üç kanalın tek batch'te karışabildiğini doğrular.
func TestNewNotificationBatchMixedChannels(t *testing.T) {
	t.Parallel()

	notifications, _, err := NewNotificationBatch([]NewNotificationInput{
		{Channel: "sms", Recipient: "+905551234567", Content: "Merhaba"},
		{Channel: "email", Recipient: "ali@example.com", Content: "Merhaba"},
		{Channel: "push", Recipient: "dGVzdC10b2tlbg", Content: "Merhaba", Priority: "high"},
	})
	require.NoError(t, err)
	require.Len(t, notifications, 3)

	require.Equal(t, ChannelSMS, notifications[0].Channel)
	require.Equal(t, ChannelEmail, notifications[1].Channel)
	require.Equal(t, ChannelPush, notifications[2].Channel)
	require.Equal(t, PriorityHigh, notifications[2].Priority)
	require.Equal(t, PriorityNormal, notifications[0].Priority)
}
