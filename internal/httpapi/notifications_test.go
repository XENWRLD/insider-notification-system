package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// Test gövde sınırları: gerçek varsayılanlardan küçük tutuluyor ki testler
// megabaytlarca veri üretmek zorunda kalmasın. Batch sınırı 1000 öğeyi rahat
// alacak kadar büyük — boyut doğrulamasını gövde sınırı gölgelemesin.
const (
	testSingleLimit = 4 << 10   // 4 KiB
	testBatchLimit  = 256 << 10 // 256 KiB
)

// stubNotificationStore, testlerde gerçek veritabanı yerine kullanılan store.
type stubNotificationStore struct {
	// err, her iki metodun da döneceği hata.
	err error
	// outcome, store'un döneceği sonuç (replay senaryoları için).
	outcome store.Outcome

	// saved, CreateNotification'a geçirilen kaydı tutar.
	saved *domain.Notification
	// savedBatch, CreateBatch'e geçirilen kayıtları tutar.
	savedBatch []*domain.Notification
	// idem, son çağrıda geçirilen koruma parametreleri.
	idem store.IdempotencyRequest
	// rendered, renderResponse'un ürettiği cevap — store'un ne sakladığını görmek için.
	rendered any
}

func (s *stubNotificationStore) CreateNotification(
	_ context.Context, n *domain.Notification,
	idem store.IdempotencyRequest, renderResponse func() any,
) (store.Outcome, error) {
	s.idem = idem

	if s.err != nil {
		return store.Outcome{}, s.err
	}
	if s.outcome.Replayed {
		return s.outcome, nil
	}

	s.saved = n
	s.rendered = renderResponse()

	return store.Outcome{}, nil
}

func (s *stubNotificationStore) CreateBatch(
	_ context.Context, notifications []*domain.Notification,
	idem store.IdempotencyRequest, renderResponse func() any,
) (store.Outcome, error) {
	s.idem = idem

	if s.err != nil {
		return store.Outcome{}, s.err
	}
	if s.outcome.Replayed {
		return s.outcome, nil
	}

	s.savedBatch = notifications
	s.rendered = renderResponse()

	return store.Outcome{}, nil
}

// newNotificationRouter, bildirim ucu kurulmuş bir router döner.
func newNotificationRouter(notifications NotificationStore) http.Handler {
	return newRouterWithLimits(notifications, testSingleLimit, testBatchLimit)
}

// newRouterWithLimits, gövde sınırları özelleştirilmiş bir router döner.
// Sınır testleri bunu kullanır: tek bir öğeyle doldurulabilecek kadar küçük bir
// batch sınırı gerekir, yoksa domain'in içerik sınırı önce devreye girer.
func newRouterWithLimits(
	notifications NotificationStore, single, batch int64,
) http.Handler {
	return NewRouter(Dependencies{
		Logger:          discardLogger(),
		Notifications:   notifications,
		BodyLimitSingle: single,
		BodyLimitBatch:  batch,
		IdempotencyTTL:  24 * time.Hour,
	})
}

// postJSON, verilen gövdeyi JSON olarak POST eder.
func postJSON(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequestWithContext(t.Context(),
		http.MethodPost, "/api/v1/notifications", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	return recorder
}

// decodeError, hata zarfını çözer.
func decodeError(t *testing.T, recorder *httptest.ResponseRecorder) errorResponse {
	t.Helper()

	var body errorResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	// Her hata yanıtında correlation_id bulunmalı.
	require.NotEmpty(t, body.CorrelationID, "hata yanıtında correlation_id yok")

	return body
}

func TestCreateNotificationAccepted(t *testing.T) {
	t.Parallel()

	store := &stubNotificationStore{}
	recorder := postJSON(t, newNotificationRouter(store),
		`{"channel":"sms","recipient":"+905551234567","content":"Merhaba"}`)

	// Gönderim henüz yapılmadı: 201 değil 202.
	require.Equal(t, http.StatusAccepted, recorder.Code)

	var body notificationResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	require.Equal(t, domain.ChannelSMS, body.Channel)
	require.Equal(t, "+905551234567", body.Recipient)
	require.Equal(t, "Merhaba", body.Content)
	require.Equal(t, domain.StatusPending, body.Status)
	require.Equal(t, domain.PriorityNormal, body.Priority, "öncelik verilmezse normal olmalı")
	require.Equal(t, domain.DefaultMaxAttempts, body.MaxAttempts)
	require.Zero(t, body.AttemptCount)
	require.NotEmpty(t, body.CorrelationID)

	// Location header'ı oluşan kaynağı göstermeli.
	require.Equal(t, "/api/v1/notifications/"+body.ID.String(), recorder.Header().Get("Location"))

	// Store'a giden kayıt ile dönen yanıt aynı olmalı.
	require.NotNil(t, store.saved)
	require.Equal(t, store.saved.ID, body.ID)
	require.Equal(t, store.saved.CorrelationID, body.CorrelationID)
}

// TestCreateNotificationDoesNotLeakInternalFields, iç mekanizma alanlarının yanıta
// sızmadığını doğrular.
//
// domain.Notification doğrudan serialize edilseydi lease_token, lease_expires_at ve
// priority_rank dışarı çıkardı. Bu test o sözleşmeyi sabitler.
func TestCreateNotificationDoesNotLeakInternalFields(t *testing.T) {
	t.Parallel()

	recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}),
		`{"channel":"sms","recipient":"+905551234567","content":"Merhaba"}`)
	require.Equal(t, http.StatusAccepted, recorder.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &raw))

	for _, field := range []string{"lease_token", "lease_expires_at", "priority_rank", "template_id"} {
		require.NotContainsf(t, raw, field, "%s alanı dışarı sızmamalı", field)
	}

	// Beklenen alanlar ise bulunmalı.
	for _, field := range []string{"id", "channel", "recipient", "content", "priority",
		"status", "attempt_count", "max_attempts", "available_at", "created_at", "correlation_id"} {
		require.Containsf(t, raw, field, "%s alanı yanıtta olmalı", field)
	}
}

// TestCreateNotificationPreservesCorrelationID, istemcinin verdiği kimliğin kayda
// yazıldığını doğrular.
func TestCreateNotificationPreservesCorrelationID(t *testing.T) {
	t.Parallel()

	store := &stubNotificationStore{}

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
		"/api/v1/notifications",
		strings.NewReader(`{"channel":"email","recipient":"ali@example.com","content":"Merhaba"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(CorrelationIDHeader, "deneme-123")

	recorder := httptest.NewRecorder()
	newNotificationRouter(store).ServeHTTP(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Equal(t, "deneme-123", store.saved.CorrelationID)
	require.Equal(t, "deneme-123", recorder.Header().Get(CorrelationIDHeader))
}

// TestCreateNotificationValidationErrors, alan bazlı hataların tek yanıtta döndüğünü
// doğrular.
func TestCreateNotificationValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		fields []string
	}{
		{
			name:   "boş gövde tüm zorunlu alanları bildirir",
			body:   `{}`,
			fields: []string{"channel", "recipient", "content"},
		},
		{
			// Kanal geçersizken alıcı BİÇİMİ doğrulanmaz: hangi kuralın (E.164 / e-posta /
			// token) geçerli olduğunu bilemeyiz ve uydurma bir hata basmak yanıltıcı olurdu.
			// Bu yüzden yalnızca kanal ve boş içerik bildirilir.
			name:   "geçersiz kanalda alıcı biçimi doğrulanmaz",
			body:   `{"channel":"fax","recipient":"0555","content":""}`,
			fields: []string{"channel", "content"},
		},
		{
			// Kanal geçerliyse alan hatalarının tümü tek yanıtta toplanır.
			name:   "geçerli kanalda tüm alan hataları birlikte döner",
			body:   `{"channel":"sms","recipient":"0555","content":"","priority":"urgent"}`,
			fields: []string{"recipient", "content", "priority"},
		},
		{
			name:   "geçersiz telefon",
			body:   `{"channel":"sms","recipient":"0555123","content":"Merhaba"}`,
			fields: []string{"recipient"},
		},
		{
			name:   "geçersiz e-posta",
			body:   `{"channel":"email","recipient":"ali.example.com","content":"Merhaba"}`,
			fields: []string{"recipient"},
		},
		{
			name:   "geçersiz öncelik",
			body:   `{"channel":"sms","recipient":"+905551234567","content":"x","priority":"urgent"}`,
			fields: []string{"priority"},
		},
		{
			// 71 adet 'ş': GSM-7'de olmadığı için Unicode sayılır, sınır 70.
			name:   "türkçe karakter unicode sınırını uygular",
			body:   `{"channel":"sms","recipient":"+905551234567","content":"` + strings.Repeat("ş", 71) + `"}`,
			fields: []string{"content"},
		},
		{
			// 36 emoji = 72 UTF-16 kod birimi.
			name:   "emoji vekil çift olarak sayılır",
			body:   `{"channel":"sms","recipient":"+905551234567","content":"` + strings.Repeat("🎉", 36) + `"}`,
			fields: []string{"content"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}), test.body)

			require.Equal(t, http.StatusBadRequest, recorder.Code)

			body := decodeError(t, recorder)
			require.Equal(t, CodeValidationFailed, body.Error)

			for _, field := range test.fields {
				require.Containsf(t, body.Fields, field, "%s alanı hata listesinde olmalı", field)
			}
		})
	}
}

// TestCreateNotificationAcceptsValidSMSLengths, sınırın doğru tarafında kalan
// içeriklerin kabul edildiğini doğrular.
func TestCreateNotificationAcceptsValidSMSLengths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
	}{
		{name: "160 ascii", content: strings.Repeat("a", 160)},
		{name: "160 adet ü GSM-7", content: strings.Repeat("ü", 160)},
		{name: "70 adet ş unicode", content: strings.Repeat("ş", 70)},
		{name: "35 emoji", content: strings.Repeat("🎉", 35)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			body, err := json.Marshal(createNotificationRequest{
				Channel: "sms", Recipient: "+905551234567", Content: test.content,
			})
			require.NoError(t, err)

			recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}), string(body))
			require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
		})
	}
}

func TestCreateNotificationRejectsBadRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		code ErrorCode
	}{
		{name: "bozuk json", body: `{"channel":`, code: CodeInvalidJSON},
		{
			// "prioritiy" yazım hatası sessizce yutulmamalı.
			name: "bilinmeyen alan",
			body: `{"channel":"sms","recipient":"+905551234567","content":"x","prioritiy":"high"}`,
			code: CodeInvalidJSON,
		},
		{
			name: "ikinci json nesnesi",
			body: `{"channel":"sms","recipient":"+905551234567","content":"x"}{"x":1}`,
			code: CodeInvalidJSON,
		},
		{name: "yanlış tip", body: `{"channel":123}`, code: CodeInvalidJSON},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}), test.body)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Equal(t, test.code, decodeError(t, recorder).Error)
		})
	}
}

func TestCreateNotificationRequiresJSONContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
	}{
		{name: "header yok", contentType: ""},
		{name: "form verisi", contentType: "application/x-www-form-urlencoded"},
		{name: "düz metin", contentType: "text/plain"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
				"/api/v1/notifications", strings.NewReader(`{}`))
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}

			recorder := httptest.NewRecorder()
			newNotificationRouter(&stubNotificationStore{}).ServeHTTP(recorder, request)

			require.Equal(t, http.StatusUnsupportedMediaType, recorder.Code)
			require.Equal(t, CodeUnsupportedMediaType, decodeError(t, recorder).Error)
		})
	}
}

// TestCreateNotificationAcceptsCharsetParameter, Content-Type parametrelerinin
// yok sayıldığını doğrular.
func TestCreateNotificationAcceptsCharsetParameter(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
		"/api/v1/notifications",
		strings.NewReader(`{"channel":"push","recipient":"token","content":"Merhaba"}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")

	recorder := httptest.NewRecorder()
	newNotificationRouter(&stubNotificationStore{}).ServeHTTP(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
}

// TestCreateNotificationStoreFailure, store hatasında iç ayrıntının sızmadığını doğrular.
func TestCreateNotificationStoreFailure(t *testing.T) {
	t.Parallel()

	store := &stubNotificationStore{
		err: errors.New("bildirim eklenemedi: connection refused to 10.0.0.5:5432"),
	}

	recorder := postJSON(t, newNotificationRouter(store),
		`{"channel":"sms","recipient":"+905551234567","content":"Merhaba"}`)

	require.Equal(t, http.StatusInternalServerError, recorder.Code)

	body := decodeError(t, recorder)
	require.Equal(t, CodeInternalError, body.Error)

	// Veritabanı adresi gibi iç ayrıntılar istemciye gitmemeli; log'da duruyor.
	require.NotContains(t, recorder.Body.String(), "connection refused")
	require.NotContains(t, recorder.Body.String(), "10.0.0.5")
}

// singleBodyOfSize, tam olarak verilen bayt uzunluğunda geçerli bir tekil istek gövdesi üretir.
func singleBodyOfSize(t *testing.T, size int) string {
	t.Helper()

	const template = `{"channel":"email","recipient":"ali@example.com","content":"%s"}`

	padding := size - len(fmt.Sprintf(template, ""))
	require.Positive(t, padding, "şablon zaten istenen boyutu aşıyor")

	body := fmt.Sprintf(template, strings.Repeat("a", padding))
	require.Len(t, body, size)

	return body
}

// TestSingleBodyLimitBoundary, tekil ucun gövde sınırını sınırın iki yanında doğrular.
func TestSingleBodyLimitBoundary(t *testing.T) {
	t.Parallel()

	t.Run("sınırın tam üstünde kabul", func(t *testing.T) {
		t.Parallel()

		recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}),
			singleBodyOfSize(t, testSingleLimit))

		require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	})

	t.Run("bir bayt fazlası 413", func(t *testing.T) {
		t.Parallel()

		recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}),
			singleBodyOfSize(t, testSingleLimit+1))

		require.Equal(t, http.StatusRequestEntityTooLarge, recorder.Code)

		body := decodeError(t, recorder)
		require.Equal(t, CodePayloadTooLarge, body.Error)
		// Mesaj sınırı söylemeli ki istemci neyi aştığını bilsin.
		require.Contains(t, body.Message, fmt.Sprint(testSingleLimit))
		// Tekil uçta "batch'i bölün" öğüdü anlamsız olurdu.
		require.NotContains(t, body.Message, "bölün")
	})
}

// TestNotFoundAndMethodNotAllowedUseErrorEnvelope, chi'nin varsayılan düz metin
// yanıtları yerine API'nin hata zarfının kullanıldığını doğrular.
func TestNotFoundAndMethodNotAllowedUseErrorEnvelope(t *testing.T) {
	t.Parallel()

	handler := newNotificationRouter(&stubNotificationStore{})

	t.Run("404", func(t *testing.T) {
		t.Parallel()

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(),
			http.MethodGet, "/api/v1/yok", nil))

		require.Equal(t, http.StatusNotFound, recorder.Code)
		require.Equal(t, CodeNotFound, decodeError(t, recorder).Error)
	})

	t.Run("405", func(t *testing.T) {
		t.Parallel()

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(),
			http.MethodDelete, "/api/v1/notifications", nil))

		require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
		require.Equal(t, CodeMethodNotAllowed, decodeError(t, recorder).Error)
	})
}
