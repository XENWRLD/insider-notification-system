package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// scheduleInput, yalnızca send_at'i değişen geçerli bir girdi üretir.
func scheduleInput(sendAt string) NewNotificationInput {
	return NewNotificationInput{
		Channel:   "sms",
		Recipient: "+905551234567",
		Content:   "Zamanlanmis mesaj",
		SendAt:    sendAt,
	}
}

// fieldMessage, doğrulama hatasındaki alanın mesajını döner.
func fieldMessage(t *testing.T, err error, field string) string {
	t.Helper()

	var verr *ValidationError
	require.ErrorAs(t, err, &verr)

	for _, item := range verr.Fields {
		if item.Field == field {
			return item.Message
		}
	}

	t.Fatalf("%q alanı için hata yok: %v", field, verr.Fields)

	return ""
}

// TestSendAtSchedulesFutureNotification, ileri bir anın available_at'e yazıldığını
// doğrular.
func TestSendAtSchedulesFutureNotification(t *testing.T) {
	t.Parallel()

	sendAt := time.Now().Add(2 * time.Hour).Round(time.Second)

	notification, err := NewNotification(scheduleInput(sendAt.Format(time.RFC3339)))
	require.NoError(t, err)

	require.True(t, notification.AvailableAt.Equal(sendAt),
		"available_at = %s, beklenen %s", notification.AvailableAt, sendAt)

	// Satır pending yazılıyor: zamanı gelmemiş iş için ayrı bir durum YOK, tek
	// mekanizma available_at (docs/PLAN.md §2).
	require.Equal(t, StatusPending, notification.Status)

	// Saklanan temsil UTC: aynı an için tek bir yazım olsun.
	require.Equal(t, time.UTC, notification.AvailableAt.Location())
}

// TestSendAtEmptyMeansImmediate, alan verilmediğinde available_at'in SIFIR kaldığını
// doğrular.
//
// Sıfır değer "damgayı veritabanı bassın" demek: uygulama saatini yazmak, claim
// sorgusu DB saatine baktığı için iki saat farklıysa satırı görünmez yapıyordu (F12).
func TestSendAtEmptyMeansImmediate(t *testing.T) {
	t.Parallel()

	notification, err := NewNotification(scheduleInput(""))
	require.NoError(t, err)

	require.True(t, notification.AvailableAt.IsZero())
}

// TestSendAtWithinSkewToleranceIsImmediate, tolerans içindeki geçmiş anın "hemen"
// olarak işlendiğini doğrular.
//
// İstemcinin "şimdi" diye hesapladığı an ağ gecikmesi ve saat farkıyla sunucuya göre
// birkaç saniye geçmiş olabiliyor; bunu reddetmek geçerli bir isteği kırardı.
func TestSendAtWithinSkewToleranceIsImmediate(t *testing.T) {
	t.Parallel()

	sendAt := time.Now().Add(-ScheduleClockSkew / 2)

	notification, err := NewNotification(scheduleInput(sendAt.Format(time.RFC3339)))
	require.NoError(t, err)

	require.True(t, notification.AvailableAt.IsZero(),
		"tolerans içindeki geçmiş an hemen gönderilmeli")
}

// TestSendAtRejectsInvalidValues, geçersiz değerlerin alan hatası ürettiğini doğrular.
func TestSendAtRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		// Saat dilimi zorunlu: dilimsiz bir tarih, istemci ile sunucu farklı
		// dilimlerdeyse sessizce farklı bir an demek olurdu.
		"tarih-only":     "2026-12-01",
		"dilimsiz":       "2026-12-01T09:00:00",
		"anlamsız metin": "yarın",
		"geçmiş":         time.Now().Add(-time.Hour).Format(time.RFC3339),
		"ufuk dışı":      time.Now().Add(ScheduleMaxHorizon + time.Hour).Format(time.RFC3339),
	}

	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := NewNotification(scheduleInput(value))
			require.Error(t, err)
			require.NotEmpty(t, fieldMessage(t, err, "send_at"))
		})
	}
}

// TestSendAtErrorJoinsOtherFieldErrors, send_at hatasının diğer alan hatalarıyla aynı
// yanıtta döndüğünü doğrular: API sözleşmesi "tüm alan hataları tek yanıtta".
func TestSendAtErrorJoinsOtherFieldErrors(t *testing.T) {
	t.Parallel()

	_, err := NewNotification(NewNotificationInput{
		Channel:   "sms",
		Recipient: "905551234567",
		Content:   "Gecersiz alici ve zaman",
		SendAt:    "2026-12-01",
	})
	require.Error(t, err)

	require.NotEmpty(t, fieldMessage(t, err, "send_at"))
	require.NotEmpty(t, fieldMessage(t, err, "recipient"))
}
