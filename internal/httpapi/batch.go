package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// batchRequest, POST /api/v1/notifications/batch gövdesi.
//
// Düz dizi yerine nesne içinde sarmalanıyor: ileride batch'e ait alanlar (ör. ortak
// send_at) eklendiğinde sözleşmeyi bozmadan büyüyebilsin.
type batchRequest struct {
	Notifications []createNotificationRequest `json:"notifications"`
}

// batchResponse, toplu oluşturmanın özet cevabı.
//
// Her bildirimin tam kaydı dönmüyor: 1000 öğe için bu yanıtı megabaytlara çıkarırdı.
// İstemciye takip için gereken kimlikler yeterli; ayrıntı GET uçlarından alınır (Adım 5).
type batchResponse struct {
	BatchID         uuid.UUID   `json:"batch_id"`
	AcceptedCount   int         `json:"accepted_count"`
	NotificationIDs []uuid.UUID `json:"notification_ids"`
	CorrelationID   string      `json:"correlation_id"`
}

// createBatch, 1-1000 bildirimi tek transaction'da oluşturur.
//
// Hep ya hiç: bir öğe geçersizse hiçbiri yazılmaz ve hatalı satırların tümü tek yanıtta
// listelenir (docs/PLAN.md §5). Yarısı yazılmış bir batch istemciyi "hangileri gitti?"
// sorusuyla baş başa bırakırdı.
func (h notificationHandler) createBatch(w http.ResponseWriter, r *http.Request) {
	var request batchRequest

	body, ok := decodeJSON(w, r, h.batchLimit, &request)
	if !ok {
		return
	}

	idem, ok := h.idempotencyFor(r, store.ScopeNotificationsBatch, body)
	if !ok {
		writeError(w, r, http.StatusBadRequest, CodeValidationFailed,
			"Idempotency-Key en fazla 255 karakter olabilir")

		return
	}

	inputs := make([]domain.NewNotificationInput, 0, len(request.Notifications))
	for _, item := range request.Notifications {
		inputs = append(inputs, item.toDomainInput())
	}

	notifications, batchID, err := domain.NewNotificationBatch(inputs)

	// İki hata tipi ayrı ele alınıyor: boyut sorunu istek bazlı (notifications alanı),
	// öğe hataları satır bazlı (notifications[3].recipient).
	var berr *domain.BatchValidationError
	if errors.As(err, &berr) {
		writeBatchValidationError(w, r, berr)

		return
	}

	var verr *domain.ValidationError
	if errors.As(err, &verr) {
		writeValidationError(w, r, "", verr)

		return
	}

	if err != nil {
		h.logger.ErrorContext(r.Context(), "beklenmeyen batch doğrulama hatası tipi",
			slog.String("correlation_id", CorrelationID(r.Context())),
			slog.String("error", err.Error()),
		)
		writeError(w, r, http.StatusInternalServerError, CodeInternalError, "istek işlenemedi")

		return
	}

	correlationID := CorrelationID(r.Context())

	ids := make([]uuid.UUID, 0, len(notifications))
	for _, notification := range notifications {
		// Tüm satırlar aynı correlation ID'yi taşır: batch'in tamamı tek bir istekten
		// doğdu ve worker loglarında birlikte izlenebilmeli.
		notification.CorrelationID = correlationID
		ids = append(ids, notification.ID)
	}

	response := batchResponse{
		BatchID:         batchID,
		AcceptedCount:   len(notifications),
		NotificationIDs: ids,
		CorrelationID:   correlationID,
	}

	ctx, cancel := context.WithTimeout(r.Context(), batchTimeout)
	defer cancel()

	outcome, err := h.store.CreateBatch(ctx, notifications, idem, func() any { return response })
	if h.writeStoreOutcome(w, r, outcome, err, "batch oluşturma") {
		return
	}

	h.logger.InfoContext(ctx, "batch oluşturuldu",
		slog.String("correlation_id", correlationID),
		slog.String("batch_id", batchID.String()),
		slog.Int("accepted_count", response.AcceptedCount),
	)

	// Location header'ı bilerek verilmiyor: /api/v1/batches/{id} ucu Adım 5'te geliyor,
	// şimdi 404'e işaret etmesi yanıltıcı olurdu.
	writeJSON(w, http.StatusAccepted, response)
}
