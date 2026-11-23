package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/store"
)

const validSingleBody = `{"channel":"sms","recipient":"+905551234567","content":"Merhaba"}`

// postWithKey, tekil uca verilen idempotency anahtarıyla istek gönderir.
func postWithKey(
	t *testing.T, handler http.Handler, key, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequestWithContext(t.Context(),
		http.MethodPost, "/api/v1/notifications", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")

	if key != "" {
		request.Header.Set(IdempotencyKeyHeader, key)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	return recorder
}

// TestIdempotencyRequestBuiltFromRawBody, hash'in HAM gövde baytlarından üretildiğini
// doğrular.
//
// Baytlar değiştiğinde hash de değişmeli: "semantik olarak aynı" sayılacak bir
// normalizasyon yapılmıyor ve bu bilinçli bir tercih.
func TestIdempotencyRequestBuiltFromRawBody(t *testing.T) {
	t.Parallel()

	collect := func(body string) []byte {
		stub := &stubNotificationStore{}
		recorder := postWithKey(t, newNotificationRouter(stub), "k-1", body)
		require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())

		return stub.idem.Hash
	}

	base := collect(validSingleBody)
	require.Len(t, base, 32, "SHA-256 32 bayt olmalı")

	// Aynı baytlar → aynı hash.
	require.Equal(t, base, collect(validSingleBody))

	// Tek fazladan boşluk → FARKLI hash (dolayısıyla 409 alacak).
	spaced := `{"channel":"sms", "recipient":"+905551234567","content":"Merhaba"}`
	require.NotEqual(t, base, collect(spaced),
		"fazladan boşluk hash'i değiştirmeli: normalizasyon yapılmıyor")

	// Alan sırası farklı → FARKLI hash.
	reordered := `{"recipient":"+905551234567","channel":"sms","content":"Merhaba"}`
	require.NotEqual(t, base, collect(reordered),
		"alan sırası hash'i değiştirmeli")
}

// TestIdempotencyScopeForSingleEndpoint, tekil ucun kendi kapsamını kullandığını doğrular.
func TestIdempotencyScopeForSingleEndpoint(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{}
	recorder := postWithKey(t, newNotificationRouter(stub), "k-1", validSingleBody)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Equal(t, store.ScopeNotifications, stub.idem.Scope)
	require.Equal(t, "k-1", stub.idem.Key)
	require.Positive(t, stub.idem.TTL, "TTL store'a geçmeli")
}

// TestIdempotencyDisabledWithoutHeader, anahtar verilmediğinde korumanın kapalı
// olduğunu doğrular.
func TestIdempotencyDisabledWithoutHeader(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{}
	recorder := postWithKey(t, newNotificationRouter(stub), "", validSingleBody)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Empty(t, stub.idem.Key, "anahtar yoksa koruma kapalı olmalı")
	require.Empty(t, stub.idem.Hash, "koruma kapalıyken hash hesaplanmasına gerek yok")
}

// TestIdempotencyRejectsOverlongKey, aşırı uzun anahtarın reddedildiğini doğrular.
func TestIdempotencyRejectsOverlongKey(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{}
	recorder := postWithKey(t, newNotificationRouter(stub),
		strings.Repeat("k", idempotencyKeyMaxLen+1), validSingleBody)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, CodeValidationFailed, decodeError(t, recorder).Error)
	require.Nil(t, stub.saved, "geçersiz anahtarda store çağrılmamalı")

	// Sınırın tam üstü kabul edilmeli.
	recorder = postWithKey(t, newNotificationRouter(&stubNotificationStore{}),
		strings.Repeat("k", idempotencyKeyMaxLen), validSingleBody)
	require.Equal(t, http.StatusAccepted, recorder.Code)
}

// TestIdempotentReplayWritesStoredBodyWithoutReserializing, handler'ın saklanan gövdeyi
// Go tarafında yeniden serialize etmediğini doğrular.
//
// Gerçek sistemde gövde jsonb'den geldiği için anahtar sırası PostgreSQL tarafından
// normalize edilir (bkz. store testleri); burada test edilen şey handler'ın araya
// kendi dönüşümünü sokmaması.
func TestIdempotentReplayWritesStoredBodyWithoutReserializing(t *testing.T) {
	t.Parallel()

	// Bilerek alışılmadık alan sırası ve biçimde bir gövde.
	const stored = `{"status":"pending","id":"22222222-2222-2222-2222-222222222222","max_attempts":5}`

	stub := &stubNotificationStore{
		outcome: store.Outcome{Replayed: true, Status: http.StatusAccepted, Body: []byte(stored)},
	}

	recorder := postWithKey(t, newNotificationRouter(stub), "k-1", validSingleBody)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Equal(t, "true", recorder.Header().Get(IdempotentReplayHeader))
	require.Equal(t, "application/json; charset=utf-8", recorder.Header().Get("Content-Type"))

	// Bayt bayt aynı.
	require.Equal(t, stored, recorder.Body.String())

	require.Nil(t, stub.saved, "tekrar gönderimde iş yapılmamalı")

	// Location header'ı verilmiyor: tekrar gönderimde yeni kaynak oluşmadı.
	require.Empty(t, recorder.Header().Get("Location"))
}

// TestIdempotencyKeyReuseReturnsConflict, aynı anahtarın farklı gövdeyle kullanımının
// 409 döndüğünü doğrular.
func TestIdempotencyKeyReuseReturnsConflict(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{err: store.ErrIdempotencyKeyReused}

	recorder := postWithKey(t, newNotificationRouter(stub), "k-1", validSingleBody)

	require.Equal(t, http.StatusConflict, recorder.Code)

	body := decodeError(t, recorder)
	require.Equal(t, CodeConflict, body.Error)
	require.Contains(t, body.Message, "Idempotency-Key")
}

// TestIdempotencyConflictOnBatchEndpoint, batch ucunda da 409 döndüğünü doğrular.
func TestIdempotencyConflictOnBatchEndpoint(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{err: store.ErrIdempotencyKeyReused}

	recorder := postBatch(t, newNotificationRouter(stub), batchBody(2),
		map[string]string{IdempotencyKeyHeader: "b-1"})

	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Equal(t, CodeConflict, decodeError(t, recorder).Error)
}

// TestReplayedResponseStoredByHandler, handler'ın store'a sakladığı cevabın istemciye
// dönenle aynı olduğunu doğrular.
//
// Bu iki değer ayrışırsa tekrar gönderim ilk istekten farklı bir cevap verirdi.
func TestReplayedResponseStoredByHandler(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{}
	recorder := postWithKey(t, newNotificationRouter(stub), "k-1", validSingleBody)

	require.Equal(t, http.StatusAccepted, recorder.Code)

	saved, ok := stub.rendered.(notificationResponse)
	require.True(t, ok, "store'a saklanan cevap notificationResponse olmalı")

	require.Equal(t, stub.saved.ID, saved.ID)
	require.Equal(t, stub.saved.CorrelationID, saved.CorrelationID)

	// İstemciye dönen gövdedeki kimlik ile saklanan aynı olmalı.
	require.Contains(t, recorder.Body.String(), saved.ID.String())
}
