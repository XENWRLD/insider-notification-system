package provider

import "time"

// SendRequest, tek bir gönderim isteği.
//
// IdempotencyKey ve CorrelationID gövdeye değil header'a yazılır: gövde sözleşmesi
// docs/PLAN.md §5'te üç alanla sabitlendi, oraya alan eklemek sözleşmeyi bozardı.
type SendRequest struct {
	To      string
	Channel string
	Content string

	// IdempotencyKey, bildirim kimliği. Tekrar denemeler aynı anahtarı taşır, böylece
	// destekleyen bir sağlayıcı duplicate'i tekilleştirebilir.
	IdempotencyKey string

	// CorrelationID, isteği uçtan uca izlemek için; sağlayıcı loglarına da düşer.
	CorrelationID string
}

// wireRequest, sağlayıcıya gönderilen JSON gövdesi.
//
// Ayrı bir tip: SendRequest'e alan eklemek dış sözleşmeyi değiştirmesin.
type wireRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// wireResponse, sağlayıcının 2xx yanıtı.
type wireResponse struct {
	MessageID string    `json:"messageId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// SendResponse, başarılı bir gönderimin sonucu.
type SendResponse struct {
	// StatusCode, sağlayıcının HTTP durum kodu. notification_attempts'e yazılır.
	StatusCode int

	// MessageID, sağlayıcının verdiği kimlik. Zarf eksikse boş kalabilir.
	MessageID string

	Status    string
	Timestamp time.Time
}
