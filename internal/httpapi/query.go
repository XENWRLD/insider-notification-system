package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
	"github.com/XENWRLD/insider-notification-system/internal/store"
)

// readTimeout, okuma sorgularına tanınan süre.
const queryTimeout = 10 * time.Second

// listResponse, listeleme ucunun yanıtı.
type listResponse struct {
	Notifications []notificationResponse `json:"notifications"`
	// NextCursor son sayfada null olur: istemci "bitti" bilgisini buradan alır.
	NextCursor *string `json:"next_cursor"`
}

// batchSummaryResponse, batch özetinin dışa verilen biçimi.
type batchSummaryResponse struct {
	BatchID uuid.UUID `json:"batch_id"`
	Total   int       `json:"total"`
	// Counts altı durumun TAMAMINI içerir, sıfırlar dahil: istemci alan varlığını
	// kontrol etmek zorunda kalmasın.
	Counts    map[string]int `json:"counts"`
	Completed bool           `json:"completed"`
	CreatedAt time.Time      `json:"created_at"`
}

// pathUUID, URL'deki kimliği çözer.
//
// Çözülemeyen kimlik 400 değil 404 verir: geçersiz biçimli bir UUID zaten hiçbir kaydı
// gösteremez, yani "bulunamadı" daha dürüst bir cevap.
func pathUUID(r *http.Request, param string) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, param))

	return id, err == nil
}

// get, tek bir bildirimi deneme geçmişiyle döner.
func (h notificationHandler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(r, "id")
	if !ok {
		writeError(w, r, http.StatusNotFound, CodeNotFound, "bildirim bulunamadı")

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	result, err := h.store.GetNotification(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, CodeNotFound, "bildirim bulunamadı")

		return
	}

	if err != nil {
		h.logger.ErrorContext(ctx, "bildirim okunamadı",
			slog.String("correlation_id", CorrelationID(ctx)),
			slog.String("notification_id", id.String()),
			slog.String("error", err.Error()),
		)
		writeError(w, r, http.StatusInternalServerError, CodeInternalError, "bildirim okunamadı")

		return
	}

	writeJSON(w, http.StatusOK,
		newNotificationResponseWithAttempts(result.Notification, result.Attempts))
}

// list, filtreli ve cursor sayfalamalı listeleme yapar.
func (h notificationHandler) list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	input := domain.ListFilterInput{
		// Aynı parametrenin birden çok değeri VEYA ile birleşir; farklı parametreler VE.
		Statuses: query["status"],
		Channels: query["channel"],
		Limit:    query.Get("limit"),
	}

	// Tarih AYRIŞTIRMASI taşıma katmanının işi (RFC3339 bir aktarım kuralı);
	// aralık MANTIĞI domain'de doğrulanır.
	verr := &domain.ValidationError{}

	input.From = parseTimeParam(query.Get("from"), "from", verr)
	input.To = parseTimeParam(query.Get("to"), "to", verr)

	if len(verr.Fields) > 0 {
		writeValidationError(w, r, "", verr)

		return
	}

	if raw := query.Get("cursor"); raw != "" {
		cursor, err := decodeCursor(raw)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, CodeInvalidCursor,
				"cursor değeri geçersiz; yalnızca önceki yanıttan gelen next_cursor kullanılmalı")

			return
		}

		input.Cursor = &cursor
	}

	filter, err := domain.NewListFilter(input)

	var filterErr *domain.ValidationError
	if errors.As(err, &filterErr) {
		writeValidationError(w, r, "", filterErr)

		return
	}

	if err != nil {
		h.logger.ErrorContext(r.Context(), "beklenmeyen filtre hatası tipi",
			slog.String("correlation_id", CorrelationID(r.Context())),
			slog.String("error", err.Error()),
		)
		writeError(w, r, http.StatusInternalServerError, CodeInternalError, "istek işlenemedi")

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	items, hasMore, err := h.store.ListNotifications(ctx, filter)
	if err != nil {
		h.logger.ErrorContext(ctx, "bildirimler listelenemedi",
			slog.String("correlation_id", CorrelationID(ctx)),
			slog.String("error", err.Error()),
		)
		writeError(w, r, http.StatusInternalServerError, CodeInternalError,
			"bildirimler listelenemedi")

		return
	}

	response := listResponse{
		Notifications: make([]notificationResponse, 0, len(items)),
	}

	for _, item := range items {
		response.Notifications = append(response.Notifications,
			newNotificationResponseWithAttempts(item.Notification, item.Attempts))
	}

	// Sonraki sayfa varsa son kaydın konumundan imleç üretilir.
	if hasMore && len(items) > 0 {
		last := items[len(items)-1].Notification
		encoded := encodeCursor(domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
		response.NextCursor = &encoded
	}

	writeJSON(w, http.StatusOK, response)
}

// parseTimeParam, RFC3339 biçiminde bir zaman parametresi okur.
//
// Tarih-only değer (2026-11-01) bilerek reddedilir: saat dilimi belirtilmeyen bir tarih,
// sunucu ile istemcinin farklı dilimlerde olduğu durumda sessizce farklı aralıklar üretir.
func parseTimeParam(raw, field string, verr *domain.ValidationError) *time.Time {
	if raw == "" {
		return nil
	}

	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		verr.Fields = append(verr.Fields, domain.FieldError{
			Field: field,
			Message: "saat dilimi içeren RFC3339 biçiminde olmalı " +
				"(örn. 2026-11-01T00:00:00Z); yalnızca tarih kabul edilmiyor",
		})

		return nil
	}

	return &parsed
}

// batchSummary, bir batch'in durum bazında özetini döner.
func (h notificationHandler) batchSummary(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(r, "id")
	if !ok {
		writeError(w, r, http.StatusNotFound, CodeNotFound, "batch bulunamadı")

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	summary, err := h.store.GetBatchSummary(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, CodeNotFound, "batch bulunamadı")

		return
	}

	if err != nil {
		h.logger.ErrorContext(ctx, "batch özeti okunamadı",
			slog.String("correlation_id", CorrelationID(ctx)),
			slog.String("batch_id", id.String()),
			slog.String("error", err.Error()),
		)
		writeError(w, r, http.StatusInternalServerError, CodeInternalError,
			"batch özeti okunamadı")

		return
	}

	counts := make(map[string]int, len(summary.Counts))
	for status, count := range summary.Counts {
		counts[string(status)] = count
	}

	writeJSON(w, http.StatusOK, batchSummaryResponse{
		BatchID:   summary.BatchID,
		Total:     summary.Total,
		Counts:    counts,
		Completed: summary.Completed,
		CreatedAt: summary.CreatedAt,
	})
}

// cancel, bildirimi atomik olarak iptal eder.
//
// 404 ile 409 ayrımı istemci için anlamlı: "böyle bir bildirim yok" ile "var ama
// gönderimi başlamış" çok farklı iki durum.
func (h notificationHandler) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(r, "id")
	if !ok {
		writeError(w, r, http.StatusNotFound, CodeNotFound, "bildirim bulunamadı")

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	notification, err := h.store.CancelNotification(ctx, id)

	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, r, http.StatusNotFound, CodeNotFound, "bildirim bulunamadı")

		return

	case errors.Is(err, store.ErrNotCancellable):
		writeError(w, r, http.StatusConflict, CodeConflict,
			"bildirim iptal edilemez: yalnızca pending veya retrying durumundaki "+
				"bildirimler iptal edilebilir")

		return

	case err != nil:
		h.logger.ErrorContext(ctx, "bildirim iptal edilemedi",
			slog.String("correlation_id", CorrelationID(ctx)),
			slog.String("notification_id", id.String()),
			slog.String("error", err.Error()),
		)
		writeError(w, r, http.StatusInternalServerError, CodeInternalError,
			"bildirim iptal edilemedi")

		return
	}

	h.logger.InfoContext(ctx, "bildirim iptal edildi",
		slog.String("correlation_id", notification.CorrelationID),
		slog.String("notification_id", notification.ID.String()),
	)

	writeJSON(w, http.StatusOK, newNotificationResponse(notification))
}
