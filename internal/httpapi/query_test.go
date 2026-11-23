package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// stubNotification, testler için bir bildirim üretir.
func stubNotification(t *testing.T, status domain.Status) *domain.Notification {
	t.Helper()

	notification, err := domain.NewNotification(domain.NewNotificationInput{
		Channel:   "sms",
		Recipient: "+905551234567",
		Content:   "Merhaba",
	})
	require.NoError(t, err)

	notification.Status = status
	notification.CorrelationID = uuid.NewString()
	notification.CreatedAt = time.Now().UTC()
	notification.UpdatedAt = notification.CreatedAt

	return notification
}

// doGet, GET isteği atar.
func doGet(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))

	return recorder
}

// rawBody, yanıtı ham haritaya çözer (alan varlığı/null kontrolü için).
func rawBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var raw map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &raw))

	return raw
}

// encodeRaw, bozuk imleç testleri için ham metni base64'e çevirir.
func encodeRaw(raw string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// --- attempts her zaman dizi ---

// TestCreateResponseHasEmptyAttemptsArray, oluşturma yanıtında attempts'in BOŞ DİZİ
// olduğunu doğrular — null veya eksik değil.
//
// Go'da nil slice JSON'da null serialize eder; sözleşmemiz [] olduğu için
// newAttemptResponses her zaman make(..., 0, n) kullanıyor. Bu test o tuzağa karşı.
func TestCreateResponseHasEmptyAttemptsArray(t *testing.T) {
	t.Parallel()

	recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}),
		`{"channel":"sms","recipient":"+905551234567","content":"Merhaba"}`)
	require.Equal(t, http.StatusAccepted, recorder.Code)

	// Ham JSON'da alan var ve null DEĞİL.
	require.Contains(t, recorder.Body.String(), `"attempts":[]`,
		"attempts boş dizi olarak serialize edilmeli")
	require.NotContains(t, recorder.Body.String(), `"attempts":null`)

	raw := rawBody(t, recorder)
	require.Contains(t, raw, "attempts")

	attempts, ok := raw["attempts"].([]any)
	require.True(t, ok, "attempts bir dizi olmalı, %T geldi", raw["attempts"])
	require.Empty(t, attempts)
}

// --- GET /notifications/{id} ---

func TestGetNotificationReturnsAttempts(t *testing.T) {
	t.Parallel()

	notification := stubNotification(t, domain.StatusRetrying)
	notification.AttemptCount = 2

	statusCode := 500
	errMessage := "provider 500"

	stub := &stubNotificationStore{found: &store.NotificationWithAttempts{
		Notification: notification,
		Attempts: []domain.Attempt{
			{AttemptNo: 1, StatusCode: &statusCode, Error: &errMessage, DurationMS: 812, CreatedAt: time.Now().UTC()},
			{AttemptNo: 2, StatusCode: nil, Error: &errMessage, DurationMS: 5000, CreatedAt: time.Now().UTC()},
		},
	}}

	recorder := doGet(t, newNotificationRouter(stub), "/api/v1/notifications/"+notification.ID.String())
	require.Equal(t, http.StatusOK, recorder.Code)

	var body notificationResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	require.Equal(t, notification.ID, body.ID)
	require.Equal(t, domain.StatusRetrying, body.Status)
	require.Equal(t, 2, body.AttemptCount)

	require.Len(t, body.Attempts, 2)
	require.Equal(t, 1, body.Attempts[0].AttemptNo)
	require.Equal(t, 500, *body.Attempts[0].StatusCode)
	require.Equal(t, 812, body.Attempts[0].DurationMS)

	// Timeout denemesinde HTTP kodu yok → null.
	require.Equal(t, 2, body.Attempts[1].AttemptNo)
	require.Nil(t, body.Attempts[1].StatusCode)

	// İç mekanizma alanları burada da sızmamalı.
	raw := rawBody(t, recorder)
	for _, field := range []string{"lease_token", "lease_expires_at", "priority_rank"} {
		require.NotContains(t, raw, field)
	}
}

func TestGetNotificationNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "olmayan kimlik", path: "/api/v1/notifications/" + uuid.NewString()},
		// Geçersiz UUID 400 değil 404: çözümlenemeyen bir kimlik zaten var olamaz.
		{name: "geçersiz uuid", path: "/api/v1/notifications/bozuk-uuid"},
		{name: "boş görünen kimlik", path: "/api/v1/notifications/00000000"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			recorder := doGet(t, newNotificationRouter(&stubNotificationStore{}), test.path)

			require.Equal(t, http.StatusNotFound, recorder.Code)
			require.Equal(t, CodeNotFound, decodeError(t, recorder).Error)
		})
	}
}

// --- GET /notifications (listeleme) ---

// listedItems, n adet bildirim içeren bir sayfa üretir.
func listedItems(t *testing.T, n int) []store.NotificationWithAttempts {
	t.Helper()

	items := make([]store.NotificationWithAttempts, 0, n)
	for range n {
		items = append(items, store.NotificationWithAttempts{
			Notification: stubNotification(t, domain.StatusPending),
		})
	}

	return items
}

func TestListNotificationsReturnsPage(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{listed: listedItems(t, 3)}

	recorder := doGet(t, newNotificationRouter(stub), "/api/v1/notifications")
	require.Equal(t, http.StatusOK, recorder.Code)

	var body listResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	require.Len(t, body.Notifications, 3)
	require.Nil(t, body.NextCursor, "son sayfada next_cursor null olmalı")

	// Varsayılan limit store'a geçmiş olmalı.
	require.Equal(t, domain.ListLimitDefault, stub.filter.Limit)
}

// TestListNotificationsEmptyPageIsArray, boş sayfada notifications'ın dizi olduğunu
// doğrular (null değil).
func TestListNotificationsEmptyPageIsArray(t *testing.T) {
	t.Parallel()

	recorder := doGet(t, newNotificationRouter(&stubNotificationStore{}), "/api/v1/notifications")
	require.Equal(t, http.StatusOK, recorder.Code)

	require.Contains(t, recorder.Body.String(), `"notifications":[]`)
	require.NotContains(t, recorder.Body.String(), `"notifications":null`)
}

// TestListNotificationsIncludesAttempts, listedeki her kaydın deneme geçmişini
// taşıdığını doğrular.
//
// Listede boş dizi dönmek "denenmedi" anlamına gelir ve yanıltır.
func TestListNotificationsIncludesAttempts(t *testing.T) {
	t.Parallel()

	withAttempts := stubNotification(t, domain.StatusRetrying)
	withoutAttempts := stubNotification(t, domain.StatusPending)

	statusCode := 503

	stub := &stubNotificationStore{listed: []store.NotificationWithAttempts{
		{
			Notification: withAttempts,
			Attempts: []domain.Attempt{
				{AttemptNo: 1, StatusCode: &statusCode, DurationMS: 100, CreatedAt: time.Now().UTC()},
				{AttemptNo: 2, StatusCode: &statusCode, DurationMS: 200, CreatedAt: time.Now().UTC()},
			},
		},
		{Notification: withoutAttempts},
	}}

	recorder := doGet(t, newNotificationRouter(stub), "/api/v1/notifications")
	require.Equal(t, http.StatusOK, recorder.Code)

	var body listResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Len(t, body.Notifications, 2)

	require.Len(t, body.Notifications[0].Attempts, 2, "denemeleri olan kayıt dolu dönmeli")
	require.Equal(t, 1, body.Notifications[0].Attempts[0].AttemptNo)

	// Denemesi olmayan kayıt: boş dizi, null değil.
	require.Empty(t, body.Notifications[1].Attempts)
	require.NotContains(t, recorder.Body.String(), `"attempts":null`)
}

// TestListNotificationsProducesCursorWhenMore, sonraki sayfa varken imleç üretildiğini
// doğrular.
func TestListNotificationsProducesCursorWhenMore(t *testing.T) {
	t.Parallel()

	items := listedItems(t, 2)
	last := items[len(items)-1].Notification

	stub := &stubNotificationStore{listed: items, hasMore: true}

	recorder := doGet(t, newNotificationRouter(stub), "/api/v1/notifications?limit=2")
	require.Equal(t, http.StatusOK, recorder.Code)

	var body listResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	require.NotNil(t, body.NextCursor)

	// İmleç son kaydın konumunu taşımalı ve çözülebilir olmalı.
	decoded, err := decodeCursor(*body.NextCursor)
	require.NoError(t, err)
	require.Equal(t, last.ID, decoded.ID)
	require.WithinDuration(t, last.CreatedAt, decoded.CreatedAt, time.Microsecond)
}

// TestListNotificationsPassesFiltersToStore, query parametrelerinin doğrulanıp
// store'a geçtiğini doğrular.
func TestListNotificationsPassesFiltersToStore(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{}

	path := "/api/v1/notifications?" + url.Values{
		"status":  {"pending", "retrying"},
		"channel": {"sms"},
		"from":    {"2026-11-01T00:00:00Z"},
		"to":      {"2026-12-01T00:00:00Z"},
		"limit":   {"50"},
	}.Encode()

	recorder := doGet(t, newNotificationRouter(stub), path)
	require.Equal(t, http.StatusOK, recorder.Code)

	// Aynı parametrenin iki değeri de geçmiş (VEYA mantığı store'da uygulanıyor).
	require.Equal(t, []domain.Status{domain.StatusPending, domain.StatusRetrying}, stub.filter.Statuses)
	require.Equal(t, []domain.Channel{domain.ChannelSMS}, stub.filter.Channels)
	require.Equal(t, 50, stub.filter.Limit)

	require.NotNil(t, stub.filter.From)
	require.NotNil(t, stub.filter.To)
	require.Equal(t, 2026, stub.filter.From.Year())
	require.Equal(t, time.November, stub.filter.From.Month())
}

func TestListNotificationsRejectsInvalidQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		code  ErrorCode
		field string
	}{
		{
			name: "bilinmeyen durum", query: "status=unknown",
			code: CodeValidationFailed, field: "status",
		},
		{
			name: "bilinmeyen kanal", query: "channel=fax",
			code: CodeValidationFailed, field: "channel",
		},
		{
			name: "limit sınırın üstü", query: "limit=101",
			code: CodeValidationFailed, field: "limit",
		},
		{
			name: "limit sıfır", query: "limit=0",
			code: CodeValidationFailed, field: "limit",
		},
		{
			name: "limit sayı değil", query: "limit=abc",
			code: CodeValidationFailed, field: "limit",
		},
		{
			// Tarih-only reddedilir: saat dilimi olmadan sunucu ve istemci farklı
			// aralıklar üretir.
			name: "tarih-only from", query: "from=2026-11-01",
			code: CodeValidationFailed, field: "from",
		},
		{
			name: "tarih-only to", query: "to=2026-12-01",
			code: CodeValidationFailed, field: "to",
		},
		{
			name: "saat dilimsiz zaman", query: "from=2026-11-01T00:00:00",
			code: CodeValidationFailed, field: "from",
		},
		{
			name:  "from to'dan büyük",
			query: "from=2026-12-01T00:00:00Z&to=2026-11-01T00:00:00Z",
			code:  CodeValidationFailed, field: "from",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stub := &stubNotificationStore{}
			recorder := doGet(t, newNotificationRouter(stub), "/api/v1/notifications?"+test.query)

			require.Equal(t, http.StatusBadRequest, recorder.Code)

			body := decodeError(t, recorder)
			require.Equal(t, test.code, body.Error)
			require.Contains(t, body.Fields, test.field)
		})
	}
}

// TestListNotificationsRejectsBrokenCursor, bozuk imlecin kendi hata kodunu
// döndürdüğünü doğrular.
func TestListNotificationsRejectsBrokenCursor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cursor string
	}{
		{name: "base64 değil", cursor: "bu-base64-degil!!!"},
		{name: "tek parça", cursor: encodeRaw("yalnizca-bir-parca")},
		{name: "geçersiz zaman", cursor: encodeRaw("bozuk-zaman " + uuid.NewString())},
		{name: "geçersiz uuid", cursor: encodeRaw("2026-11-01T00:00:00Z bozuk-uuid")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			recorder := doGet(t, newNotificationRouter(&stubNotificationStore{}),
				"/api/v1/notifications?cursor="+url.QueryEscape(test.cursor))

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Equal(t, CodeInvalidCursor, decodeError(t, recorder).Error)
		})
	}
}

// TestCursorRoundTripPreservesMicroseconds, imlecin mikrosaniye hassasiyetini
// kaybetmediğini doğrular.
//
// RFC3339 (saniye) ile kodlasaydık aynı saniyedeki kayıtlar arasında konum kaybolur ve
// sayfalama atlama/tekrar üretirdi.
func TestCursorRoundTripPreservesMicroseconds(t *testing.T) {
	t.Parallel()

	original := domain.Cursor{
		// PostgreSQL timestamptz mikrosaniye tutar.
		CreatedAt: time.Date(2026, 11, 23, 15, 5, 10, 286432000, time.UTC),
		ID:        uuid.New(),
	}

	decoded, err := decodeCursor(encodeCursor(original))
	require.NoError(t, err)

	require.Equal(t, original.ID, decoded.ID)
	require.True(t, original.CreatedAt.Equal(decoded.CreatedAt),
		"mikrosaniye kaybolmuş: %s → %s", original.CreatedAt, decoded.CreatedAt)
}

// TestEncodedCursorIsOpaqueAndURLSafe, imlecin opak ve URL güvenli olduğunu doğrular.
func TestEncodedCursorIsOpaqueAndURLSafe(t *testing.T) {
	t.Parallel()

	encoded := encodeCursor(domain.Cursor{CreatedAt: time.Now().UTC(), ID: uuid.New()})

	// İç yapı okunabilir olmamalı.
	require.NotContains(t, encoded, "T00:")
	require.NotContains(t, encoded, "-")

	// URL'de kaçış gerektirmemeli (RawURLEncoding: dolgu yok, + / yok).
	require.Equal(t, encoded, url.QueryEscape(encoded))
}

// --- GET /batches/{id} ---

func TestBatchSummaryReturnsAllStatuses(t *testing.T) {
	t.Parallel()

	batchID := uuid.New()
	summary := domain.NewBatchSummary(batchID, map[domain.Status]int{
		domain.StatusPending: 900,
		domain.StatusSent:    100,
	}, time.Now().UTC())

	stub := &stubNotificationStore{summary: &summary}

	recorder := doGet(t, newNotificationRouter(stub), "/api/v1/batches/"+batchID.String())
	require.Equal(t, http.StatusOK, recorder.Code)

	var body batchSummaryResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	require.Equal(t, batchID, body.BatchID)
	require.Equal(t, 1000, body.Total)
	require.False(t, body.Completed)

	// Altı durumun TAMAMI bulunmalı, sıfırlar dahil.
	require.Len(t, body.Counts, 6)
	for status, expected := range map[string]int{
		"pending": 900, "sent": 100, "processing": 0,
		"retrying": 0, "failed": 0, "cancelled": 0,
	} {
		require.Equal(t, expected, body.Counts[status], "%s sayımı", status)
	}
}

func TestBatchSummaryCompleted(t *testing.T) {
	t.Parallel()

	batchID := uuid.New()
	summary := domain.NewBatchSummary(batchID, map[domain.Status]int{
		domain.StatusSent:   8,
		domain.StatusFailed: 2,
	}, time.Now().UTC())

	stub := &stubNotificationStore{summary: &summary}

	recorder := doGet(t, newNotificationRouter(stub), "/api/v1/batches/"+batchID.String())
	require.Equal(t, http.StatusOK, recorder.Code)

	var body batchSummaryResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.True(t, body.Completed, "tüm kayıtlar terminal durumdaysa completed true olmalı")
}

func TestBatchSummaryNotFound(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"/api/v1/batches/" + uuid.NewString(),
		"/api/v1/batches/bozuk-uuid",
	} {
		recorder := doGet(t, newNotificationRouter(&stubNotificationStore{}), path)

		require.Equal(t, http.StatusNotFound, recorder.Code, path)
		require.Equal(t, CodeNotFound, decodeError(t, recorder).Error)
	}
}

// --- POST /notifications/{id}/cancel ---

// doPost, gövdesiz POST isteği atar.
func doPost(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil))

	return recorder
}

func TestCancelNotificationSuccess(t *testing.T) {
	t.Parallel()

	notification := stubNotification(t, domain.StatusPending)
	stub := &stubNotificationStore{found: &store.NotificationWithAttempts{Notification: notification}}

	recorder := doPost(t, newNotificationRouter(stub),
		"/api/v1/notifications/"+notification.ID.String()+"/cancel")

	require.Equal(t, http.StatusOK, recorder.Code)

	var body notificationResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	require.Equal(t, domain.StatusCancelled, body.Status)
	require.Equal(t, notification.ID, body.ID)

	// attempts burada da dizi olmalı.
	require.Contains(t, recorder.Body.String(), `"attempts":[]`)

	// Doğru kimlik store'a geçmiş.
	require.Equal(t, notification.ID, stub.cancelled)
}

func TestCancelNotificationNotFound(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"/api/v1/notifications/" + uuid.NewString() + "/cancel",
		"/api/v1/notifications/bozuk-uuid/cancel",
	} {
		recorder := doPost(t, newNotificationRouter(&stubNotificationStore{}), path)

		require.Equal(t, http.StatusNotFound, recorder.Code, path)
		require.Equal(t, CodeNotFound, decodeError(t, recorder).Error)
	}
}

// TestCancelNotificationConflict, iptal edilemeyen durumun 409 verdiğini doğrular.
func TestCancelNotificationConflict(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{err: store.ErrNotCancellable}

	recorder := doPost(t, newNotificationRouter(stub),
		"/api/v1/notifications/"+uuid.NewString()+"/cancel")

	require.Equal(t, http.StatusConflict, recorder.Code)

	body := decodeError(t, recorder)
	require.Equal(t, CodeConflict, body.Error)
	// Mesaj hangi durumların iptal edilebildiğini söylemeli.
	require.Contains(t, body.Message, "pending")
	require.Contains(t, body.Message, "retrying")
}

// TestReadEndpointsHideInternalErrors, store hatasında iç ayrıntının sızmadığını
// doğrular.
func TestReadEndpointsHideInternalErrors(t *testing.T) {
	t.Parallel()

	stub := &stubNotificationStore{
		err: fmt.Errorf("bildirim okunamadı: connection refused to 10.0.0.5:5432"),
	}

	handler := newNotificationRouter(stub)

	paths := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/notifications"},
		{http.MethodGet, "/api/v1/notifications/" + uuid.NewString()},
		{http.MethodGet, "/api/v1/batches/" + uuid.NewString()},
		{http.MethodPost, "/api/v1/notifications/" + uuid.NewString() + "/cancel"},
	}

	for _, target := range paths {
		t.Run(target.method+" "+target.path, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder,
				httptest.NewRequestWithContext(t.Context(), target.method, target.path, nil))

			require.Equal(t, http.StatusInternalServerError, recorder.Code)
			require.Equal(t, CodeInternalError, decodeError(t, recorder).Error)

			require.NotContains(t, recorder.Body.String(), "connection refused")
			require.NotContains(t, recorder.Body.String(), "10.0.0.5")
		})
	}
}
