package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// createTimeout, bildirim yazma işlemine tanınan süre.
// http.Server'ın WriteTimeout'undan kısa tutulur: sorgu takılırsa istemci bağlantı
// resetlenmesi yerine düzgün bir 500 alır.
const createTimeout = 5 * time.Second

// NotificationStore, handler'ın ihtiyaç duyduğu veri erişimi.
//
// Arayüz olarak alınması (Pinger ile aynı felsefe) handler'ın gerçek veritabanı
// olmadan test edilebilmesini sağlar; httpapi paketi pgx import etmez.
type NotificationStore interface {
	CreateNotification(ctx context.Context, n *domain.Notification) error
}

// notificationHandler, bildirim uçlarını sunar.
type notificationHandler struct {
	logger *slog.Logger
	store  NotificationStore
}

// createNotificationRequest, POST /api/v1/notifications gövdesi.
//
// priority isteğe bağlı; verilmezse domain katmanı "normal" uygular.
type createNotificationRequest struct {
	Channel   string `json:"channel"`
	Recipient string `json:"recipient"`
	Content   string `json:"content"`
	Priority  string `json:"priority"`
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

// create, tekil bildirim oluşturur.
//
// 202 döner, 201 değil: kayıt oluştu ama gönderim henüz yapılmadı. Asıl gönderimi
// worker yapacak, bu yüzden "kabul edildi" semantiği doğru olan.
func (h notificationHandler) create(w http.ResponseWriter, r *http.Request) {
	var request createNotificationRequest
	if !decodeJSON(w, r, &request) {
		return
	}

	notification, err := domain.NewNotification(domain.NewNotificationInput{
		Channel:   request.Channel,
		Recipient: request.Recipient,
		Content:   request.Content,
		Priority:  request.Priority,
	})

	var verr *domain.ValidationError
	if errors.As(err, &verr) {
		// Tekil uçta önek yok; batch ucunda "notifications[i]." verilecek.
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

	if err := h.store.CreateNotification(ctx, notification); err != nil {
		h.logger.ErrorContext(ctx, "bildirim kaydedilemedi",
			slog.String("correlation_id", notification.CorrelationID),
			slog.String("channel", string(notification.Channel)),
			slog.String("error", err.Error()),
		)
		// İç hata istemciye sızmaz: yalnızca kod ve correlation_id döner, ayrıntı log'da.
		writeError(w, r, http.StatusInternalServerError, CodeInternalError,
			"bildirim kaydedilemedi")

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
