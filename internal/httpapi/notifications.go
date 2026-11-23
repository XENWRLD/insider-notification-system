package httpapi

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

const (
	// createTimeout, bildirim yazma işlemine tanınan süre.
	// http.Server'ın WriteTimeout'undan kısa tutulur: sorgu takılırsa istemci bağlantı
	// resetlenmesi yerine düzgün bir 500 alır.
	createTimeout = 5 * time.Second

	// batchTimeout, 1000 satırlık toplu yazıma tanınan süre. Tekil yazımdan uzun:
	// CopyFrom hızlı ama 1000 satır yine de daha fazla iş.
	batchTimeout = 30 * time.Second

	// IdempotencyKeyHeader, istemcinin tekrar gönderim koruması için verdiği anahtar.
	IdempotencyKeyHeader = "Idempotency-Key"

	// IdempotentReplayHeader, yanıtın saklanan bir cevap olduğunu bildirir.
	// İstemci "isteğim yeni mi işlendi, yoksa tekrar mı" sorusunu buradan anlar.
	IdempotentReplayHeader = "Idempotent-Replay"

	// idempotencyKeyMaxLen, anahtar uzunluk sınırı. Sınırsız bırakmak tabloyu
	// istemcinin keyfine bırakırdı.
	idempotencyKeyMaxLen = 255
)

// NotificationStore, handler'ın ihtiyaç duyduğu veri erişimi.
//
// Arayüz olarak alınması (Pinger ile aynı felsefe) handler'ın gerçek veritabanı
// olmadan test edilebilmesini sağlar; httpapi paketi pgx import etmez.
type NotificationStore interface {
	CreateNotification(
		ctx context.Context, n *domain.Notification,
		idem store.IdempotencyRequest, renderResponse func() any,
	) (store.Outcome, error)

	CreateBatch(
		ctx context.Context, notifications []*domain.Notification,
		idem store.IdempotencyRequest, renderResponse func() any,
	) (store.Outcome, error)
}

// notificationHandler, bildirim uçlarını sunar.
type notificationHandler struct {
	logger *slog.Logger
	store  NotificationStore

	singleLimit bodyLimit
	batchLimit  bodyLimit

	idempotencyTTL time.Duration
}

// createNotificationRequest, tekil oluşturma gövdesi ve batch öğelerinin biçimi.
//
// priority isteğe bağlı; verilmezse domain katmanı "normal" uygular.
type createNotificationRequest struct {
	Channel   string `json:"channel"`
	Recipient string `json:"recipient"`
	Content   string `json:"content"`
	Priority  string `json:"priority"`
}

// toDomainInput, HTTP biçimini domain girdisine çevirir.
func (r createNotificationRequest) toDomainInput() domain.NewNotificationInput {
	return domain.NewNotificationInput{
		Channel:   r.Channel,
		Recipient: r.Recipient,
		Content:   r.Content,
		Priority:  r.Priority,
	}
}

// notificationResponse, bir bildirimin dışa verilen biçimi.
//
// domain.Notification doğrudan serialize EDİLMEZ: lease_token, lease_expires_at ve
// priority_rank iç mekanizma detayıdır (işin sahipliği ve sıralama hilesi) ve dışarı
// çıkmamalı. Alanlar bu yüzden elle seçiliyor — modele yeni bir iç alan eklendiğinde
// kazara API sözleşmesine sızmaz.
//
// GET /api/v1/notifications/{id} (Adım 5) aynı şemayı kullanacak.
type notificationResponse struct {
	ID      uuid.UUID  `json:"id"`
	BatchID *uuid.UUID `json:"batch_id,omitempty"`

	Channel   domain.Channel  `json:"channel"`
	Recipient string          `json:"recipient"`
	Content   string          `json:"content"`
	Priority  domain.Priority `json:"priority"`
	Status    domain.Status   `json:"status"`

	AttemptCount int `json:"attempt_count"`
	MaxAttempts  int `json:"max_attempts"`

	AvailableAt time.Time `json:"available_at"`

	ProviderMessageID *string `json:"provider_message_id,omitempty"`
	LastError         *string `json:"last_error,omitempty"`

	CorrelationID string `json:"correlation_id"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// newNotificationResponse, domain modelini API biçimine çevirir.
func newNotificationResponse(n *domain.Notification) notificationResponse {
	return notificationResponse{
		ID:                n.ID,
		BatchID:           n.BatchID,
		Channel:           n.Channel,
		Recipient:         n.Recipient,
		Content:           n.Content,
		Priority:          n.Priority,
		Status:            n.Status,
		AttemptCount:      n.AttemptCount,
		MaxAttempts:       n.MaxAttempts,
		AvailableAt:       n.AvailableAt,
		ProviderMessageID: n.ProviderMessageID,
		LastError:         n.LastError,
		CorrelationID:     n.CorrelationID,
		CreatedAt:         n.CreatedAt,
		UpdatedAt:         n.UpdatedAt,
	}
}

// idempotencyFor, istekten koruma parametrelerini çıkarır.
//
// Hash HAM GÖVDE baytlarından hesaplanır: istemci aynı isteği tekrar gönderdiğinde aynı
// baytları gönderir. Semantik olarak aynı ama baytı farklı istekler (fazladan boşluk,
// farklı alan sırası) bilinçli olarak 409 alır — "aynı istek"in tanımı sunucunun JSON
// yorumuna bırakılmaz.
func (h notificationHandler) idempotencyFor(
	r *http.Request, scope string, body []byte,
) (store.IdempotencyRequest, bool) {
	key := r.Header.Get(IdempotencyKeyHeader)
	if key == "" {
		return store.IdempotencyRequest{Scope: scope, TTL: h.idempotencyTTL}, true
	}

	if len(key) > idempotencyKeyMaxLen {
		return store.IdempotencyRequest{}, false
	}

	sum := sha256.Sum256(body)

	return store.IdempotencyRequest{
		Scope: scope,
		Key:   key,
		Hash:  sum[:],
		TTL:   h.idempotencyTTL,
	}, true
}

// writeStoreOutcome, store'dan dönen hatayı veya saklanan cevabı yazar.
// İş gerçekten yapıldıysa (replay değil) false döner ve çağıran kendi yanıtını yazar.
func (h notificationHandler) writeStoreOutcome(
	w http.ResponseWriter, r *http.Request, outcome store.Outcome, err error, operation string,
) bool {
	if errors.Is(err, store.ErrIdempotencyKeyReused) {
		writeError(w, r, http.StatusConflict, CodeConflict,
			"bu Idempotency-Key farklı bir istek gövdesiyle kullanılmış")

		return true
	}

	if err != nil {
		h.logger.ErrorContext(r.Context(), operation+" başarısız",
			slog.String("correlation_id", CorrelationID(r.Context())),
			slog.String("error", err.Error()),
		)
		// İç hata istemciye sızmaz: yalnızca kod ve correlation_id döner, ayrıntı log'da.
		writeError(w, r, http.StatusInternalServerError, CodeInternalError,
			"istek kaydedilemedi")

		return true
	}

	if outcome.Replayed {
		h.logger.InfoContext(r.Context(), "idempotent tekrar gönderim",
			slog.String("correlation_id", CorrelationID(r.Context())),
			slog.String("operation", operation),
		)

		w.Header().Set(IdempotentReplayHeader, "true")
		// Saklanan gövde birebir yazılır; yeniden serialize edilmez.
		writeStoredJSON(w, outcome.Status, outcome.Body)

		return true
	}

	return false
}

// create, tekil bildirim oluşturur.
//
// 202 döner, 201 değil: kayıt oluştu ama gönderim henüz yapılmadı. Asıl gönderimi
// worker yapacak, bu yüzden "kabul edildi" semantiği doğru olan.
func (h notificationHandler) create(w http.ResponseWriter, r *http.Request) {
	var request createNotificationRequest

	body, ok := decodeJSON(w, r, h.singleLimit, &request)
	if !ok {
		return
	}

	idem, ok := h.idempotencyFor(r, store.ScopeNotifications, body)
	if !ok {
		writeError(w, r, http.StatusBadRequest, CodeValidationFailed,
			"Idempotency-Key en fazla 255 karakter olabilir")

		return
	}

	notification, err := domain.NewNotification(request.toDomainInput())

	var verr *domain.ValidationError
	if errors.As(err, &verr) {
		// Tekil uçta önek yok; batch ucunda "notifications[i]." uygulanır.
		writeValidationError(w, r, "", verr)

		return
	}

	if err != nil {
		// NewNotification sözleşmesi gereği yalnızca *ValidationError döner;
		// buraya düşmek bir programlama hatasıdır.
		h.logger.ErrorContext(r.Context(), "beklenmeyen doğrulama hatası tipi",
			slog.String("correlation_id", CorrelationID(r.Context())),
			slog.String("error", err.Error()),
		)
		writeError(w, r, http.StatusInternalServerError, CodeInternalError, "istek işlenemedi")

		return
	}

	// Kimlik kayda yazılır: API ve worker ayrı process olduğu için correlation ID
	// veritabanı üzerinden worker loglarına taşınacak.
	notification.CorrelationID = CorrelationID(r.Context())

	ctx, cancel := context.WithTimeout(r.Context(), createTimeout)
	defer cancel()

	// renderResponse iş tamamlandıktan SONRA çağrılır: zaman damgaları o an dolu olur,
	// böylece saklanan cevap istemciye dönenle birebir aynı olur.
	render := func() any { return newNotificationResponse(notification) }

	outcome, err := h.store.CreateNotification(ctx, notification, idem, render)
	if h.writeStoreOutcome(w, r, outcome, err, "bildirim oluşturma") {
		return
	}

	h.logger.InfoContext(ctx, "bildirim oluşturuldu",
		slog.String("correlation_id", notification.CorrelationID),
		slog.String("notification_id", notification.ID.String()),
		slog.String("channel", string(notification.Channel)),
		slog.String("priority", string(notification.Priority)),
	)

	w.Header().Set("Location", "/api/v1/notifications/"+notification.ID.String())
	writeJSON(w, http.StatusAccepted, newNotificationResponse(notification))
}
