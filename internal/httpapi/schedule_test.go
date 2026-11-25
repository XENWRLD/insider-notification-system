package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCreateAcceptsSendAt, zamanlanmış bildirimin store'a istenen anla gittiğini
// doğrular.
func TestCreateAcceptsSendAt(t *testing.T) {
	t.Parallel()

	sendAt := time.Now().Add(6 * time.Hour).UTC().Truncate(time.Second)
	store := &stubNotificationStore{}

	recorder := postJSON(t, newNotificationRouter(store), fmt.Sprintf(
		`{"channel":"sms","recipient":"+905551234567","content":"Zamanlanmis","send_at":%q}`,
		sendAt.Format(time.RFC3339)))

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.NotNil(t, store.saved)
	require.True(t, store.saved.AvailableAt.Equal(sendAt),
		"available_at = %s, beklenen %s", store.saved.AvailableAt, sendAt)
}

// TestCreateWithoutSendAtLeavesStampToDatabase, alan verilmediğinde handler'ın zaman
// damgası ÜRETMEDİĞİNİ doğrular.
//
// Sıfır değer store katmanına "damgayı sen bas" diyor; API process saatini yazmak,
// claim sorgusu DB saatine baktığı için iki saat farklıysa satırı görünmez yapıyordu.
func TestCreateWithoutSendAtLeavesStampToDatabase(t *testing.T) {
	t.Parallel()

	store := &stubNotificationStore{}

	recorder := postJSON(t, newNotificationRouter(store),
		`{"channel":"sms","recipient":"+905551234567","content":"Hemen"}`)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.NotNil(t, store.saved)
	require.True(t, store.saved.AvailableAt.IsZero())
}

// TestCreateRejectsInvalidSendAt, geçersiz send_at'in alan hatası ürettiğini doğrular.
func TestCreateRejectsInvalidSendAt(t *testing.T) {
	t.Parallel()

	recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}),
		`{"channel":"sms","recipient":"+905551234567","content":"Dilimsiz","send_at":"2026-12-01"}`)

	require.Equal(t, http.StatusBadRequest, recorder.Code)

	body := decodeError(t, recorder)
	require.Equal(t, CodeValidationFailed, body.Error)
	require.Contains(t, body.Fields, "send_at")
}

// TestBatchSendAtErrorCarriesItemPrefix, batch içindeki hatalı send_at'in satır
// numarasıyla döndüğünü doğrular.
//
// Ayrıştırma domain katmanında yapıldığı için bu önek bedavaya geliyor: httpapi'de
// elle ayrıştırılsaydı batch yolu için ikinci bir hata toplama kodu gerekirdi.
func TestBatchSendAtErrorCarriesItemPrefix(t *testing.T) {
	t.Parallel()

	body := `{"notifications":[
		{"channel":"sms","recipient":"+905551234567","content":"Birinci"},
		{"channel":"sms","recipient":"+905551234568","content":"Ikinci","send_at":"yarın"}
	]}`

	request := httptest.NewRequestWithContext(t.Context(),
		http.MethodPost, "/api/v1/notifications/batch", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	newNotificationRouter(&stubNotificationStore{}).ServeHTTP(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)

	envelope := decodeError(t, recorder)
	require.Contains(t, envelope.Fields, "notifications[1].send_at")
	require.NotContains(t, envelope.Fields, "notifications[0].send_at")
}

// TestBatchAcceptsMixedScheduling, karışık bir batch'te yalnızca zamanlanmış öğenin
// available_at taşıdığını doğrular.
func TestBatchAcceptsMixedScheduling(t *testing.T) {
	t.Parallel()

	sendAt := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	store := &stubNotificationStore{}

	body := fmt.Sprintf(`{"notifications":[
		{"channel":"sms","recipient":"+905551234567","content":"Hemen"},
		{"channel":"sms","recipient":"+905551234568","content":"Sonra","send_at":%q}
	]}`, sendAt.Format(time.RFC3339))

	request := httptest.NewRequestWithContext(t.Context(),
		http.MethodPost, "/api/v1/notifications/batch", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	newNotificationRouter(store).ServeHTTP(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Len(t, store.savedBatch, 2)
	require.True(t, store.savedBatch[0].AvailableAt.IsZero())
	require.True(t, store.savedBatch[1].AvailableAt.Equal(sendAt))
}
