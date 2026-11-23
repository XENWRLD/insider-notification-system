package domain

import "time"

// Attempt, notification_attempts tablosunun bir satırı: tek bir gönderim denemesinin
// tutanağı.
//
// Bu kayıtları yazan kod Adım 6-8'de gelecek; okuma tarafı şimdiden tam çalışıyor.
type Attempt struct {
	AttemptNo int

	// StatusCode, sağlayıcının HTTP durum kodu. Timeout veya bağlantı hatasında
	// hiç yanıt gelmediği için nil olur.
	StatusCode *int

	// Error, hata mesajı. Başarılı denemede nil.
	Error *string

	DurationMS int
	CreatedAt  time.Time
}
