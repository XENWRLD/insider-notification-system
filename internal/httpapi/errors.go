package httpapi

import (
	"encoding/json"
	"errors"
	"mime"
	"net/http"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// maxBodyBytes, kabul edilen en büyük istek gövdesi.
//
// İçerik sınırı 10.000 karakter; UTF-8'de karakter başına 4 bayta kadar çıkabildiği
// için ~40 KB yeterli, 128 KB rahat bir üst sınır. Sınır olmazsa tek bir istemci
// belleği tüketebilirdi.
const maxBodyBytes = 128 << 10

// ErrorCode, API'nin döndüğü makine okunur hata kodu.
//
// Bu blok kod kümesinin TEK KAYNAĞIDIR: OpenAPI dokümanındaki (3. gün) error enum'u
// buradan türetilir. Yeni bir kod eklenirse OpenAPI de güncellenmelidir.
type ErrorCode string

// API hata kodları ve karşılık geldikleri HTTP durumları.
const (
	CodeValidationFailed     ErrorCode = "validation_failed"      // 400
	CodeInvalidJSON          ErrorCode = "invalid_json"           // 400
	CodeUnsupportedMediaType ErrorCode = "unsupported_media_type" // 415
	CodePayloadTooLarge      ErrorCode = "payload_too_large"      // 413
	CodeNotFound             ErrorCode = "not_found"              // 404
	CodeMethodNotAllowed     ErrorCode = "method_not_allowed"     // 405
	CodeConflict             ErrorCode = "conflict"               // 409 (Adım 4-5)
	CodeInternalError        ErrorCode = "internal_error"         // 500
)

// errorResponse, tüm hata yanıtlarının gövdesi.
//
// RFC 9457 (application/problem+json) yerine bu düz zarf seçildi: istemci tarafında
// ek bir şema gerektirmiyor ve alan bazlı hataları doğrudan forma bağlanabilen bir
// harita olarak veriyor. Gerekçe README'de de yazılı.
type errorResponse struct {
	Error   ErrorCode `json:"error"`
	Message string    `json:"message"`
	// Fields, alan adı → hata mesajı. Batch uçlarında anahtar
	// "notifications[3].recipient" biçimindedir.
	Fields map[string]string `json:"fields,omitempty"`
	// CorrelationID her hata yanıtında bulunur: kullanıcı destek talebinde bunu
	// verdiğinde ilgili log satırı tek sorguyla bulunur.
	CorrelationID string `json:"correlation_id"`
}

// writeError, alan bazlı olmayan bir hatayı yazar.
func writeError(w http.ResponseWriter, r *http.Request, status int, code ErrorCode, message string) {
	writeJSON(w, status, errorResponse{
		Error:         code,
		Message:       message,
		CorrelationID: CorrelationID(r.Context()),
	})
}

// writeValidationError, alan bazlı doğrulama hatalarını tek yanıtta yazar.
//
// fieldPrefix batch uçları için: "notifications[3]." öneki verildiğinde anahtar
// "notifications[3].recipient" olur ve istemci hangi satırın hangi alanının bozuk
// olduğunu doğrudan görür. Tekil uçlarda boş geçilir.
func writeValidationError(
	w http.ResponseWriter, r *http.Request, fieldPrefix string, verr *domain.ValidationError,
) {
	fields := make(map[string]string, len(verr.Fields))
	for _, field := range verr.Fields {
		fields[fieldPrefix+field.Field] = field.Message
	}

	writeJSON(w, http.StatusBadRequest, errorResponse{
		Error:         CodeValidationFailed,
		Message:       "istek doğrulanamadı",
		Fields:        fields,
		CorrelationID: CorrelationID(r.Context()),
	})
}

// decodeJSON, istek gövdesini çözer. Hata durumunda uygun yanıtı yazar ve false döner.
//
// Üç korumayı birlikte uygular: Content-Type kontrolü, gövde boyutu sınırı ve
// bilinmeyen alanların reddi.
func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if !hasJSONContentType(r) {
		writeError(w, r, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType,
			"Content-Type application/json olmalı")

		return false
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	decoder := json.NewDecoder(r.Body)
	// Bilinmeyen alan hata verir: "prioritiy" gibi bir yazım hatası sessizce
	// yutulup varsayılanla devam etmek, istemcinin fark etmediği bir bug olurdu.
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, r, http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
				"istek gövdesi çok büyük")

			return false
		}

		// Çözümleme hatasının metni istemciye verilir: hangi alanın bozuk olduğunu
		// söylüyor ve hassas bilgi içermiyor.
		writeError(w, r, http.StatusBadRequest, CodeInvalidJSON, err.Error())

		return false
	}

	// Gövdede ikinci bir JSON değeri olmamalı; sessizce yok saymak yerine reddediyoruz.
	if decoder.More() {
		writeError(w, r, http.StatusBadRequest, CodeInvalidJSON,
			"gövde tek bir JSON nesnesi olmalı")

		return false
	}

	return true
}

// hasJSONContentType, isteğin JSON gövdesi taşıdığını bildirip bildirmediğini söyler.
// Charset gibi parametreler yok sayılır ("application/json; charset=utf-8" geçerlidir).
func hasJSONContentType(r *http.Request) bool {
	header := r.Header.Get("Content-Type")
	if header == "" {
		return false
	}

	mediaType, _, err := mime.ParseMediaType(header)

	return err == nil && mediaType == "application/json"
}
