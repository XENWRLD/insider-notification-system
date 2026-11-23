package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// batchBody, n geçerli öğeden oluşan bir batch gövdesi üretir.
func batchBody(n int) string {
	var builder strings.Builder

	builder.WriteString(`{"notifications":[`)
	for i := range n {
		if i > 0 {
			builder.WriteByte(',')
		}
		fmt.Fprintf(&builder,
			`{"channel":"sms","recipient":"+9055500%05d","content":"Kampanya"}`, i)
	}
	builder.WriteString(`]}`)

	return builder.String()
}

// postBatch, batch ucuna istek gönderir.
func postBatch(
	t *testing.T, handler http.Handler, body string, headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequestWithContext(t.Context(),
		http.MethodPost, "/api/v1/notifications/batch", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")

	for name, value := range headers {
		request.Header.Set(name, value)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	return recorder
}

func TestCreateBatchAccepted(t *testing.T) {
	t.Parallel()

	for _, size := range []int{1, 10, domain.BatchMaxSize} {
		t.Run(fmt.Sprintf("%d öğe", size), func(t *testing.T) {
			t.Parallel()

			stub := &stubNotificationStore{}
			recorder := postBatch(t, newNotificationRouter(stub), batchBody(size), nil)

			require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())

			var body batchResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

			require.Equal(t, size, body.AcceptedCount)
			require.Len(t, body.NotificationIDs, size)
			require.NotEqual(t, "00000000-0000-0000-0000-000000000000", body.BatchID.String())
			require.NotEmpty(t, body.CorrelationID)

			// Store'a geçen kayıtlar yanıttaki kimliklerle aynı olmalı.
			require.Len(t, stub.savedBatch, size)
			for i, notification := range stub.savedBatch {
				require.Equal(t, body.NotificationIDs[i], notification.ID)
				require.NotNil(t, notification.BatchID)
				require.Equal(t, body.BatchID, *notification.BatchID)
				// Batch'in tamamı tek istekten doğdu: aynı correlation ID.
				require.Equal(t, body.CorrelationID, notification.CorrelationID)
			}
		})
	}
}

// TestCreateBatchResponseIsSummaryOnly, yanıtın özet olduğunu doğrular.
//
// 1000 öğenin tam kaydı dönerse yanıt megabaytlara çıkardı; istemciye takip için
// gereken kimlikler yeterli.
func TestCreateBatchResponseIsSummaryOnly(t *testing.T) {
	t.Parallel()

	recorder := postBatch(t, newNotificationRouter(&stubNotificationStore{}), batchBody(3), nil)
	require.Equal(t, http.StatusAccepted, recorder.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &raw))

	require.ElementsMatch(t,
		[]string{"batch_id", "accepted_count", "notification_ids", "correlation_id"},
		keysOf(raw),
		"batch yanıtı yalnızca özet alanlarını içermeli")

	// Location header'ı verilmiyor: /api/v1/batches/{id} ucu henüz yok (Adım 5).
	require.Empty(t, recorder.Header().Get("Location"))
}

// keysOf, haritanın anahtarlarını döner.
func keysOf(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	return keys
}

func TestCreateBatchRejectsInvalidSizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "alan yok", body: `{}`},
		{name: "boş dizi", body: `{"notifications":[]}`},
		{name: "null", body: `{"notifications":null}`},
		{name: "sınırın bir üstü", body: batchBody(domain.BatchMaxSize + 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stub := &stubNotificationStore{}
			recorder := postBatch(t, newNotificationRouter(stub), test.body, nil)

			require.Equal(t, http.StatusBadRequest, recorder.Code)

			body := decodeError(t, recorder)
			require.Equal(t, CodeValidationFailed, body.Error)
			// Boyut hatası istek bazlı: anahtar indekssiz "notifications".
			require.Contains(t, body.Fields, "notifications")

			require.Nil(t, stub.savedBatch, "doğrulama hatasında store çağrılmamalı")
		})
	}
}

// TestCreateBatchReportsIndexedFieldErrors, hatalı satırların indeksli anahtarlarla
// döndüğünü doğrular.
//
// Anahtar biçimi "notifications[2].recipient": istemci 1000 öğeli bir batch'te hangi
// satırın hangi alanının bozuk olduğunu doğrudan görür.
func TestCreateBatchReportsIndexedFieldErrors(t *testing.T) {
	t.Parallel()

	body := `{"notifications":[
		{"channel":"sms","recipient":"+905551234567","content":"ok"},
		{"channel":"sms","recipient":"+905551234568","content":"ok"},
		{"channel":"sms","recipient":"0555","content":"ok"},
		{"channel":"sms","recipient":"+905551234569","content":"ok"},
		{"channel":"sms","recipient":"+905551234570","content":"ok"},
		{"channel":"sms","recipient":"+905551234571","content":"ok"},
		{"channel":"email","recipient":"ali@example.com","content":""}
	]}`

	stub := &stubNotificationStore{}
	recorder := postBatch(t, newNotificationRouter(stub), body, nil)

	require.Equal(t, http.StatusBadRequest, recorder.Code)

	errorBody := decodeError(t, recorder)
	require.Equal(t, CodeValidationFailed, errorBody.Error)

	// İkisi birden dönmeli: ilk hatada durulmuyor.
	require.Contains(t, errorBody.Fields, "notifications[2].recipient")
	require.Contains(t, errorBody.Fields, "notifications[6].content")
	require.Len(t, errorBody.Fields, 2)

	// Hep ya hiç: geçerli beş öğe bile store'a gitmemeli.
	require.Nil(t, stub.savedBatch, "bir öğe hatalıysa hiçbiri yazılmamalı")
}

// TestBatchBodyLimitBoundary, batch ucunun gövde sınırını sınırın iki yanında doğrular.
//
// Küçük bir sınır kullanılıyor (tek öğeyle doldurulabilecek kadar): büyük sınırda
// doldurma içeriği domain'in 10.000 karakterlik e-posta sınırına önce takılırdı ve
// test 413 yerine 400 görürdü.
func TestBatchBodyLimitBoundary(t *testing.T) {
	t.Parallel()

	const smallBatchLimit int64 = 2 << 10 // 2 KiB

	bodyOfSize := func(t *testing.T, size int) string {
		t.Helper()

		const template = `{"notifications":[{"channel":"email","recipient":"ali@example.com","content":"%s"}]}`

		padding := size - len(fmt.Sprintf(template, ""))
		require.Positive(t, padding)
		require.Less(t, padding, domain.EmailMaxContentRunes,
			"doldurma içeriği domain sınırının altında kalmalı")

		body := fmt.Sprintf(template, strings.Repeat("a", padding))
		require.Len(t, body, size)

		return body
	}

	t.Run("sınırın tam üstünde kabul", func(t *testing.T) {
		t.Parallel()

		handler := newRouterWithLimits(&stubNotificationStore{}, testSingleLimit, smallBatchLimit)
		recorder := postBatch(t, handler, bodyOfSize(t, int(smallBatchLimit)), nil)

		require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	})

	t.Run("bir bayt fazlası 413", func(t *testing.T) {
		t.Parallel()

		handler := newRouterWithLimits(&stubNotificationStore{}, testSingleLimit, smallBatchLimit)
		recorder := postBatch(t, handler, bodyOfSize(t, int(smallBatchLimit)+1), nil)

		require.Equal(t, http.StatusRequestEntityTooLarge, recorder.Code)

		body := decodeError(t, recorder)
		require.Equal(t, CodePayloadTooLarge, body.Error)
		// Mesaj sınırı VE ne yapılacağını söylemeli.
		require.Contains(t, body.Message, fmt.Sprint(smallBatchLimit))
		require.Contains(t, body.Message, "bölün")
	})
}

// TestBatchUsesLargerLimitThanSingle, iki ucun ayrı sınırlara sahip olduğunu doğrular.
//
// Tekil sınırı aşan bir gövde batch ucunda kabul edilebilmeli; aksi hâlde 1000 öğeli
// batch hiç gönderilemezdi.
func TestBatchUsesLargerLimitThanSingle(t *testing.T) {
	t.Parallel()

	oversizedForSingle := batchBody(200)
	require.Greater(t, len(oversizedForSingle), testSingleLimit,
		"test gövdesi tekil sınırı aşmalı ki ayrım görünsün")
	require.Less(t, len(oversizedForSingle), testBatchLimit)

	recorder := postBatch(t, newNotificationRouter(&stubNotificationStore{}),
		oversizedForSingle, nil)
	require.Equal(t, http.StatusAccepted, recorder.Code)
}

// TestBatchIdempotencyKeyIsPassedToStore, anahtarın ve ham gövde hash'inin store'a
// doğru kapsamla geçtiğini doğrular.
func TestBatchIdempotencyKeyIsPassedToStore(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{}
	recorder := postBatch(t, newNotificationRouter(stub), batchBody(2),
		map[string]string{IdempotencyKeyHeader: "b-42"})

	require.Equal(t, http.StatusAccepted, recorder.Code)

	require.Equal(t, store.ScopeNotificationsBatch, stub.idem.Scope,
		"batch ucu kendi kapsamını kullanmalı")
	require.Equal(t, "b-42", stub.idem.Key)
	require.Len(t, stub.idem.Hash, 32, "SHA-256 32 bayt olmalı")
}

// TestBatchReplayReturnsStoredSummary, tekrar gönderimde saklanan özetin birebir
// döndüğünü doğrular.
func TestBatchReplayReturnsStoredSummary(t *testing.T) {
	t.Parallel()

	const stored = `{"batch_id":"11111111-1111-1111-1111-111111111111","accepted_count":7}`

	stub := &stubNotificationStore{
		outcome: store.Outcome{Replayed: true, Status: http.StatusAccepted, Body: []byte(stored)},
	}

	recorder := postBatch(t, newNotificationRouter(stub), batchBody(2),
		map[string]string{IdempotencyKeyHeader: "b-1"})

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Equal(t, "true", recorder.Header().Get(IdempotentReplayHeader))

	// Gövde BİREBİR aynı: yeniden serialize edilmiyor.
	require.JSONEq(t, stored, recorder.Body.String())
	require.Equal(t, stored, recorder.Body.String())

	require.Nil(t, stub.savedBatch, "tekrar gönderimde iş yapılmamalı")
}
