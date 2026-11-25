package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

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
	CodeInvalidCursor        ErrorCode = "invalid_cursor"         // 400
	CodeNotFound             ErrorCode = "not_found"              // 404
	CodeMethodNotAllowed     ErrorCode = "method_not_allowed"     // 405
	CodeConflict             ErrorCode = "conflict"               // 409 (Adım 4-5)
	CodeInternalError        ErrorCode = "internal_error"         // 500
)

// errorCodes, yukarıdaki kodların tamamı.
//
// Sabitler tek tek kullanılıyor ama OpenAPI dokümanındaki enum'un bu kümeyle aynı
// olduğunu doğrulayabilmek için listenin çalışma zamanında da elimizde olması
// gerekiyor (docs_test.go). Listeyi testin içine yazmak ikinci bir kopya olurdu ve
// "koda ekle, dokümana eklemeyi unut" durumunu kaçırırdı.
//
// Yeni bir sabit eklenip buraya yazılmazsa TestErrorCodeConstantsAreAllListed
// kırılır: o test errors.go'yu go/parser ile okuyup sabitleri bu dizinin içeriğiyle
// karşılaştırıyor.
var errorCodes = []ErrorCode{
	CodeValidationFailed,
	CodeInvalidJSON,
	CodeUnsupportedMediaType,
	CodePayloadTooLarge,
	CodeInvalidCursor,
	CodeNotFound,
	CodeMethodNotAllowed,
	CodeConflict,
	CodeInternalError,
}

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

// validationFields, alan hatalarını önek uygulayarak haritaya çevirir.
//
// fieldPrefix batch uçları için: "notifications[3]." öneki verildiğinde anahtar
// "notifications[3].recipient" olur ve istemci hangi satırın hangi alanının bozuk
// olduğunu doğrudan görür. Tekil uçlarda boş geçilir.
func validationFields(
	fieldPrefix string, verr *domain.ValidationError, into map[string]string,
) map[string]string {
	if into == nil {
		into = make(map[string]string, len(verr.Fields))
	}

	for _, field := range verr.Fields {
		into[fieldPrefix+field.Field] = field.Message
	}

	return into
}

// writeValidationError, alan bazlı doğrulama hatalarını tek yanıtta yazar.
func writeValidationError(
	w http.ResponseWriter, r *http.Request, fieldPrefix string, verr *domain.ValidationError,
) {
	writeFieldErrors(w, r, validationFields(fieldPrefix, verr, nil))
}

// writeBatchValidationError, batch içindeki tüm hatalı öğeleri tek yanıtta yazar.
//
// Anahtar biçimi "notifications[3].recipient": istemci 1000 öğeli bir batch'te hangi
// satırın hangi alanının bozuk olduğunu doğrudan görür ve tek tek denemek zorunda kalmaz.
func writeBatchValidationError(
	w http.ResponseWriter, r *http.Request, berr *domain.BatchValidationError,
) {
	fields := make(map[string]string)
	for index, itemError := range berr.Items {
		fields = validationFields(fmt.Sprintf("notifications[%d].", index), itemError, fields)
	}

	writeFieldErrors(w, r, fields)
}

// writeFieldErrors, alan hatası haritasını 400 olarak yazar.
func writeFieldErrors(w http.ResponseWriter, r *http.Request, fields map[string]string) {
	writeJSON(w, http.StatusBadRequest, errorResponse{
		Error:         CodeValidationFailed,
		Message:       "istek doğrulanamadı",
		Fields:        fields,
		CorrelationID: CorrelationID(r.Context()),
	})
}

// bodyLimit, bir ucun kabul ettiği gövde boyutu ve sınır aşıldığında verilecek öğüt.
//
// advice uca göre değişir: batch'te "batch'i bölün" demek işe yarar, tekil uçta anlamsız.
type bodyLimit struct {
	bytes  int64
	advice string
}

// decodeJSON, istek gövdesini okur ve çözer; HAM BAYTLARI da döner.
//
// Gövde bir kez okunup tamponlanıyor çünkü aynı baytlar iki işe yarıyor: idempotency
// hash'i ve JSON çözümlemesi. Akış (streaming) kullansaydık hash için gövdeyi ikinci kez
// okumak gerekirdi ve bu mümkün değil.
//
// Dört korumayı birlikte uygular: Content-Type kontrolü, gövde boyutu sınırı,
// bilinmeyen alanların reddi ve ikinci JSON değerinin reddi.
func decodeJSON(
	w http.ResponseWriter, r *http.Request, limit bodyLimit, target any,
) ([]byte, bool) {
	if !hasJSONContentType(r) {
		writeError(w, r, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType,
			"Content-Type application/json olmalı")

		return nil, false
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit.bytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			message := fmt.Sprintf("istek gövdesi çok büyük (sınır %d bayt)", limit.bytes)
			if limit.advice != "" {
				message += "; " + limit.advice
			}

			writeError(w, r, http.StatusRequestEntityTooLarge, CodePayloadTooLarge, message)

			return nil, false
		}

		writeError(w, r, http.StatusBadRequest, CodeInvalidJSON, "istek gövdesi okunamadı")

		return nil, false
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	// Bilinmeyen alan hata verir: "prioritiy" gibi bir yazım hatası sessizce
	// yutulup varsayılanla devam etmek, istemcinin fark etmediği bir bug olurdu.
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		// Hata METNİ olduğu gibi VERİLMEZ, çevrilir: encoding/json'un mesajı iç DTO
		// tiplerini içeriyor ("...Go struct field createNotificationRequest.channel of
		// type string", batch'te "[]httpapi.createNotificationRequest"). Bu hem paket
		// içi isimleri sızdırıyor hem de o isimleri fiilî API sözleşmesine çeviriyordu:
		// bir struct yeniden adlandırıldığında mesaja bakan istemci kırılırdı.
		writeError(w, r, http.StatusBadRequest, CodeInvalidJSON, decodeErrorMessage(err))

		return nil, false
	}

	// Gövdede ikinci bir JSON değeri olmamalı; sessizce yok saymak yerine reddediyoruz.
	if decoder.More() {
		writeError(w, r, http.StatusBadRequest, CodeInvalidJSON,
			"gövde tek bir JSON nesnesi olmalı")

		return nil, false
	}

	return body, true
}

// writeStoredJSON, saklanan bir cevabı Go tarafında yeniden serialize etmeden yazar.
//
// Not: gövde veritabanında jsonb olarak saklandığı için anahtar sırası ve boşluklar
// PostgreSQL tarafından normalize edilir — tekrar gönderimde dönen gövde ilk cevapla
// DEĞER olarak aynı, bayt olarak farklı olabilir. JSON'da anahtar sırası anlamsız
// (RFC 8259) olduğu için bu kabul edilebilir; karşılığında jsonb bize geçersiz JSON
// saklanamaması ve cevap üzerinde sorgu yapılabilmesini veriyor.
func writeStoredJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if _, err := w.Write(body); err != nil {
		slog.Error("saklanan JSON yanıt yazılamadı", slog.String("error", err.Error()))
	}
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

// decodeErrorMessage, JSON çözümleme hatasını istemciye verilebilir bir mesaja çevirir.
//
// Amaç ikisi birden: istemci hangi alanın bozuk olduğunu öğrensin (bu bilgi gerçekten
// yararlı) ama Go tip adları dışarı çıkmasın. Alan adı JSON etiketinden geldiği için
// istemcinin gönderdiği şeyin aynısı; iç adlandırma değil.
func decodeErrorMessage(err error) string {
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		if typeErr.Field != "" {
			return fmt.Sprintf("%q alanı için beklenen tip %s, gelen %s",
				typeErr.Field, jsonTypeName(typeErr.Type.String()), typeErr.Value)
		}

		return fmt.Sprintf("gövdede beklenmeyen bir değer var: %s", typeErr.Value)
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return fmt.Sprintf("gövde geçerli JSON değil (%d. bayt civarında)", syntaxErr.Offset)
	}

	// Bilinmeyen alan hatası tipli değil, yalnızca metin olarak geliyor
	// (`json: unknown field "prioritiy"`). Alan adı istemcinin kendi gönderdiği şey,
	// yani sızdırdığı bir iç isim yok; yalnızca önek Türkçeleştiriliyor.
	const unknownField = "json: unknown field "
	if field, found := strings.CutPrefix(err.Error(), unknownField); found {
		return "bilinmeyen alan: " + field
	}

	if errors.Is(err, io.EOF) {
		return "istek gövdesi boş"
	}

	// Geri kalanı (beklenmeyen EOF, çok derin iç içe yapı) tek bir genel mesaja
	// düşüyor: metni aktarmak yeni bir sızma yolu açardı.
	return "gövde çözümlenemedi"
}

// jsonTypeName, Go tip adını JSON tip adına çevirir.
//
// Tam bir eşleme değil, sözleşmede geçen tipler kadarı: gövdedeki alanların tamamı
// string, sayı, dizi veya nesne. Tanınmayan bir tipte "değer" deniyor — Go adını
// yazmak, düzeltilen sorunu geri getirirdi.
func jsonTypeName(goType string) string {
	switch {
	case goType == "string":
		return "string"
	case strings.HasPrefix(goType, "int") || strings.HasPrefix(goType, "uint") ||
		strings.HasPrefix(goType, "float"):
		return "sayı"
	case goType == "bool":
		return "boolean"
	case strings.HasPrefix(goType, "[]"):
		return "dizi"
	case strings.HasPrefix(goType, "map") || strings.HasPrefix(goType, "struct"):
		return "nesne"
	default:
		return "değer"
	}
}
