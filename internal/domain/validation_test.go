package domain

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fieldMessages, hataları alan adına göre haritaya çevirir.
func fieldMessages(t *testing.T, err error) map[string]string {
	t.Helper()

	var verr *ValidationError
	require.ErrorAs(t, err, &verr)

	messages := make(map[string]string, len(verr.Fields))
	for _, field := range verr.Fields {
		messages[field.Field] = field.Message
	}

	return messages
}

func TestNewNotificationValidInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input NewNotificationInput
	}{
		{
			name:  "sms",
			input: NewNotificationInput{Channel: "sms", Recipient: "+905551234567", Content: "Merhaba"},
		},
		{
			name:  "email",
			input: NewNotificationInput{Channel: "email", Recipient: "ali@example.com", Content: "Merhaba"},
		},
		{
			name:  "push",
			input: NewNotificationInput{Channel: "push", Recipient: "dGVzdC10b2tlbg", Content: "Merhaba"},
		},
		{
			name: "yüksek öncelik",
			input: NewNotificationInput{
				Channel: "sms", Recipient: "+905551234567", Content: "Merhaba", Priority: "high",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			notification, err := NewNotification(test.input)
			require.NoError(t, err)

			// Oluşturulan kayıt her zaman kuyruğa hazır ve hiç denenmemiş durumda.
			require.Equal(t, StatusPending, notification.Status)
			require.Zero(t, notification.AttemptCount)
			require.Equal(t, DefaultMaxAttempts, notification.MaxAttempts)
			require.NotEqual(t, "00000000-0000-0000-0000-000000000000", notification.ID.String())
			require.False(t, notification.AvailableAt.IsZero())

			// Lease boş: işi henüz kimse çekmedi.
			require.Nil(t, notification.LeaseToken)
			require.Nil(t, notification.LeaseExpiresAt)
		})
	}
}

func TestNewNotificationDefaultsPriorityToNormal(t *testing.T) {
	t.Parallel()

	notification, err := NewNotification(NewNotificationInput{
		Channel: "sms", Recipient: "+905551234567", Content: "Merhaba",
	})
	require.NoError(t, err)
	require.Equal(t, PriorityNormal, notification.Priority)
}

// TestNewNotificationCollectsAllErrors, tüm alan hatalarının tek yanıtta dönmesini sabitler.
func TestNewNotificationCollectsAllErrors(t *testing.T) {
	t.Parallel()

	_, err := NewNotification(NewNotificationInput{
		Channel: "fax", Recipient: "", Content: "", Priority: "urgent",
	})

	messages := fieldMessages(t, err)
	require.Contains(t, messages, "channel")
	require.Contains(t, messages, "priority")
	require.Contains(t, messages, "recipient")
	require.Contains(t, messages, "content")
	require.Len(t, messages, 4, "dört hatanın hepsi tek yanıtta dönmeliydi")
}

func TestNewNotificationRecipientRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		channel   string
		recipient string
		valid     bool
	}{
		{name: "sms e164", channel: "sms", recipient: "+905551234567", valid: true},
		{name: "sms artı yok", channel: "sms", recipient: "905551234567"},
		{name: "sms sıfırla başlıyor", channel: "sms", recipient: "+05551234567"},
		{name: "sms harf içeriyor", channel: "sms", recipient: "+90555abc4567"},
		{name: "sms çok uzun", channel: "sms", recipient: "+9055512345678901234"},
		{name: "sms boşluklu", channel: "sms", recipient: "+90 555 123 45 67"},

		{name: "email geçerli", channel: "email", recipient: "ali@example.com", valid: true},
		{name: "email alt adres", channel: "email", recipient: "ali+etiket@example.com", valid: true},
		{name: "email at yok", channel: "email", recipient: "ali.example.com"},
		{name: "email boş yerel kısım", channel: "email", recipient: "@example.com"},
		// "Ali <ali@example.com>" RFC'ye göre geçerli bir adres ifadesi ama bize
		// çıplak adres gerekiyor; sağlayıcıya gönderilecek değer bu alan.
		{name: "email görünen ad ile", channel: "email", recipient: "Ali <ali@example.com>"},
		{
			name:      "email çok uzun",
			channel:   "email",
			recipient: strings.Repeat("a", 250) + "@example.com",
		},

		{name: "push token", channel: "push", recipient: "dGVzdC10b2tlbg", valid: true},
		{name: "push boşluklu", channel: "push", recipient: "token with space"},
		{name: "push çok uzun", channel: "push", recipient: strings.Repeat("t", PushMaxTokenLen+1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewNotification(NewNotificationInput{
				Channel: test.channel, Recipient: test.recipient, Content: "Merhaba",
			})

			if test.valid {
				require.NoError(t, err)

				return
			}

			require.Contains(t, fieldMessages(t, err), "recipient")
		})
	}
}

// TestSMSContentLimits, SMS sınırının kodlamaya göre değiştiğini sabitler.
//
// Bu testin kalbi şu: aynı karakter sayısı, içerdiği harflere göre kabul veya ret
// alabiliyor. 'Çok' GSM-7'ye girer, 'çok' girmez.
func TestSMSContentLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		valid   bool
	}{
		// GSM-7: 160 sınırı.
		{name: "160 ascii", content: strings.Repeat("a", 160), valid: true},
		{name: "161 ascii", content: strings.Repeat("a", 161)},
		{name: "büyük Ç GSM-7", content: "Çok", valid: true},
		{name: "160 adet ü GSM-7", content: strings.Repeat("ü", 160), valid: true},
		{name: "161 adet ü", content: strings.Repeat("ü", 161)},
		// Uzatma karakteri 2 septet: 80 adet tam sınır, 81 aşıyor.
		{name: "80 euro", content: strings.Repeat("€", 80), valid: true},
		{name: "81 euro", content: strings.Repeat("€", 81)},

		// GSM-7 dışı → UCS-2, 70 kod birimi sınırı.
		{name: "küçük ç Unicode'a düşürür", content: "çok", valid: true},
		{name: "70 adet ş", content: strings.Repeat("ş", 70), valid: true},
		{name: "71 adet ş", content: strings.Repeat("ş", 71)},
		{name: "70 adet ı", content: strings.Repeat("ı", 70), valid: true},
		{name: "71 adet İ", content: strings.Repeat("İ", 71)},
		// 100 adet ç: GSM-7'de olmadığı için 70 sınırına tabi.
		{name: "100 adet ç", content: strings.Repeat("ç", 100)},

		// Emoji vekil çift: karakter başına 2 kod birimi.
		{name: "35 emoji tam sınır", content: strings.Repeat("🎉", 35), valid: true},
		{name: "36 emoji sınırı aşar", content: strings.Repeat("🎉", 36)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewNotification(NewNotificationInput{
				Channel: "sms", Recipient: "+905551234567", Content: test.content,
			})

			if test.valid {
				require.NoError(t, err)

				return
			}

			require.Contains(t, fieldMessages(t, err), "content")
		})
	}
}

func TestEmailAndPushContentLimits(t *testing.T) {
	t.Parallel()

	// E-posta: 10.000 karakter sınırı, SMS kuralları uygulanmaz.
	_, err := NewNotification(NewNotificationInput{
		Channel: "email", Recipient: "ali@example.com",
		Content: strings.Repeat("ş", EmailMaxContentRunes),
	})
	require.NoError(t, err, "e-postada Türkçe karakter sınırı değiştirmemeli")

	_, err = NewNotification(NewNotificationInput{
		Channel: "email", Recipient: "ali@example.com",
		Content: strings.Repeat("a", EmailMaxContentRunes+1),
	})
	require.Contains(t, fieldMessages(t, err), "content")

	// Push: 512 karakter.
	_, err = NewNotification(NewNotificationInput{
		Channel: "push", Recipient: "token",
		Content: strings.Repeat("a", PushMaxContentRunes),
	})
	require.NoError(t, err)

	_, err = NewNotification(NewNotificationInput{
		Channel: "push", Recipient: "token",
		Content: strings.Repeat("a", PushMaxContentRunes+1),
	})
	require.Contains(t, fieldMessages(t, err), "content")
}

// TestValidationErrorIsUnwrappable, handler'ın errors.As ile alan hatalarına
// ulaşabildiğini doğrular.
func TestValidationErrorIsUnwrappable(t *testing.T) {
	t.Parallel()

	_, err := NewNotification(NewNotificationInput{})
	require.Error(t, err)

	var verr *ValidationError
	require.True(t, errors.As(err, &verr))
	require.NotEmpty(t, verr.Fields)
	require.Contains(t, err.Error(), "doğrulama hatası")
}
