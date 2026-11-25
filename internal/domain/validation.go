package domain

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Kanal bazlı uzunluk sınırları (docs/PLAN.md §5).
const (
	// EmailMaxRecipientLen, RFC 5321'in yol (path) sınırı.
	EmailMaxRecipientLen = 254
	EmailMaxContentRunes = 10000

	PushMaxTokenLen     = 512
	PushMaxContentRunes = 512
)

// Zamanlanmış gönderim sınırları.
const (
	// ScheduleMaxHorizon, send_at'in en fazla ne kadar ileri olabileceği.
	//
	// Sınırsız bırakmak, kuyrukta yıllarca duracak satırların claim index'inde
	// birikmesi demekti: index kısmi ve yalnızca pending/retrying tutuyor, yani
	// "2030'a kurulmuş" bir kayıt her claim taramasının maliyetine kalıcı olarak
	// eklenirdi.
	ScheduleMaxHorizon = 30 * 24 * time.Hour

	// ScheduleClockSkew, geçmiş kontrolünde tanınan tolerans.
	//
	// İstemci "şimdi" diye hesapladığı bir anı gönderdiğinde ağ gecikmesi ve
	// istemci-sunucu saat farkı yüzünden değer sunucuya göre birkaç saniye geçmiş
	// olabilir; bunu reddetmek, aslında geçerli olan bir isteği saat farkı yüzünden
	// kırardı. Toleransın içinde kalan değer "hemen gönder" olarak işlenir.
	ScheduleClockSkew = time.Minute
)

// e164Pattern, ITU-T E.164: '+' ardından sıfırla başlamayan en fazla 15 hane.
var e164Pattern = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)

// FieldError, tek bir alana ait doğrulama hatası.
type FieldError struct {
	Field   string
	Message string
}

// ValidationError, bir istekteki tüm alan hatalarını taşır.
//
// Hatalar tek tek değil toplu döner: kullanıcı üç alanı birden yanlış doldurduysa
// "düzelt–gönder–yeni hata gör" döngüsünü üç kez yaşamasın (config.Load ile aynı felsefe).
type ValidationError struct {
	Fields []FieldError
}

// Error, error arayüzünü karşılar.
func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for _, field := range e.Fields {
		parts = append(parts, field.Field+": "+field.Message)
	}

	return "doğrulama hatası: " + strings.Join(parts, "; ")
}

// add, listeye yeni bir alan hatası ekler.
func (e *ValidationError) add(field, format string, args ...any) {
	e.Fields = append(e.Fields, FieldError{
		Field:   field,
		Message: fmt.Sprintf(format, args...),
	})
}

// NewNotificationInput, bir bildirim oluşturmak için gereken ham girdi.
//
// Alanlar bilinçli olarak string: taşıma katmanı (HTTP/JSON) ne gönderirse doğrulama
// burada yapılır. Böylece domain, JSON çözümlemesinin ayrıntılarından habersiz kalır.
type NewNotificationInput struct {
	Channel   string
	Recipient string
	Content   string
	Priority  string

	// SendAt, zamanlanmış gönderim anı (RFC3339, saat dilimi zorunlu).
	// Boşsa bildirim hemen gönderilebilir.
	//
	// Ayrıştırma da burada yapılıyor (httpapi'de değil): diğer alanlarla aynı yol,
	// ve batch ucunda hata anahtarının "notifications[3].send_at" biçiminde
	// öneklenmesi bu sayede bedavaya geliyor.
	SendAt string
}

// NewNotification, girdiyi doğrular ve gönderilmeye hazır bir bildirim üretir.
//
// Hata dönerse tipi her zaman *ValidationError'dır; çağıran errors.As ile alan bazlı
// hataları okuyabilir.
func NewNotification(in NewNotificationInput) (*Notification, error) {
	verr := &ValidationError{}

	channel := Channel(strings.TrimSpace(in.Channel))
	switch {
	case channel == "":
		verr.add("channel", "zorunlu")
	case !channel.Valid():
		verr.add("channel", "geçersiz kanal (sms|email|push): %q", string(channel))
	}

	// Öncelik verilmediyse normal kabul edilir.
	priority := Priority(strings.TrimSpace(in.Priority))
	if priority == "" {
		priority = PriorityNormal
	} else if !priority.Valid() {
		verr.add("priority", "geçersiz öncelik (high|normal|low): %q", string(priority))
	}

	// Alıcı ve içerik kuralları kanala göre değişir; kanal geçersizse hangi kuralı
	// uygulayacağımızı bilemeyiz, bu yüzden yalnızca boşluk kontrolü yapılır.
	recipient := strings.TrimSpace(in.Recipient)
	content := in.Content

	if channel.Valid() {
		validateRecipient(verr, channel, recipient)
		validateContent(verr, channel, content)
	} else {
		if recipient == "" {
			verr.add("recipient", "zorunlu")
		}
		if strings.TrimSpace(content) == "" {
			verr.add("content", "zorunlu")
		}
	}

	availableAt := validateSendAt(verr, in.SendAt)

	if len(verr.Fields) > 0 {
		return nil, verr
	}

	return &Notification{
		ID:        uuid.New(),
		Channel:   channel,
		Recipient: recipient,
		Content:   content,
		Priority:  priority,
		Status:    StatusPending,

		AttemptCount: 0,
		MaxAttempts:  DefaultMaxAttempts,

		// Zamanlanmış değilse SIFIR bırakılır: o zaman damgasını veritabanı basar
		// (store katmanı COALESCE ile now() uyguluyor). Uygulama saatini yazmak,
		// claim sorgusu DB saatine baktığı için API ile Postgres farklı saatlerdeyse
		// satırı görünmez yapıyordu — tek saat kaynağı DB.
		AvailableAt: availableAt,
	}, nil
}

// validateSendAt, zamanlanmış gönderim anını ayrıştırır ve sınırlara göre doğrular.
//
// Dönen sıfır değer "hemen" demektir: hem alan verilmediğinde hem de verilen an
// tolerans içinde geçmiş olduğunda. Geçmiş bir anı sessizce "şimdi"ye çekmek yerine
// reddetmek de bir seçenekti ama toleransın amacı tam olarak bu durumu kurtarmak.
func validateSendAt(verr *ValidationError, raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}

	sendAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		verr.add("send_at", "saat dilimi içeren RFC3339 biçiminde olmalı "+
			"(örn. 2026-12-01T09:00:00+03:00); yalnızca tarih kabul edilmiyor")

		return time.Time{}
	}

	now := time.Now().UTC()

	if sendAt.Before(now.Add(-ScheduleClockSkew)) {
		verr.add("send_at", "geçmiş bir an olamaz (en fazla %s tolerans)", ScheduleClockSkew)

		return time.Time{}
	}

	if sendAt.After(now.Add(ScheduleMaxHorizon)) {
		verr.add("send_at", "en fazla %d gün ileri olabilir",
			int(ScheduleMaxHorizon.Hours()/24))

		return time.Time{}
	}

	// Tolerans içindeki geçmiş an "hemen" olarak işlenir; ileri bir an UTC'ye
	// çevrilerek saklanır (tek temsil).
	if sendAt.Before(now) {
		return time.Time{}
	}

	return sendAt.UTC()
}

// validateRecipient, alıcıyı kanalın kurallarına göre doğrular.
func validateRecipient(verr *ValidationError, channel Channel, recipient string) {
	if recipient == "" {
		verr.add("recipient", "zorunlu")

		return
	}

	switch channel {
	case ChannelSMS:
		if !e164Pattern.MatchString(recipient) {
			verr.add("recipient", "E.164 biçiminde olmalı (örn. +905551234567)")
		}

	case ChannelEmail:
		if utf8.RuneCountInString(recipient) > EmailMaxRecipientLen {
			verr.add("recipient", "en fazla %d karakter olabilir", EmailMaxRecipientLen)

			return
		}
		// Regex ile e-posta doğrulamak klasik bir hata: RFC 5322 düzenli ifadeyle
		// güvenilir şekilde ifade edilemez. net/mail standart kütüphanede hazır.
		address, err := mail.ParseAddress(recipient)
		if err != nil || address.Address != recipient {
			// address.Address != recipient: "Ali <a@b.com>" biçimini reddeder,
			// bize çıplak adres gerekiyor.
			verr.add("recipient", "geçerli bir e-posta adresi olmalı")
		}

	case ChannelPush:
		if utf8.RuneCountInString(recipient) > PushMaxTokenLen {
			verr.add("recipient", "en fazla %d karakter olabilir", PushMaxTokenLen)
		}
		if strings.ContainsFunc(recipient, unicode.IsSpace) {
			verr.add("recipient", "device token boşluk içeremez")
		}
	}
}

// validateContent, içeriği kanalın kurallarına göre doğrular.
func validateContent(verr *ValidationError, channel Channel, content string) {
	// Yalnızca boşluktan oluşan içerik de boş sayılır: alıcıya anlamsız bir mesaj
	// gider. Kontrol kırpılmış kopya üzerinde yapılır, saklanan içerik değişmez —
	// baştaki/sondaki boşluk gönderenin bilinçli biçimlendirmesi olabilir.
	if strings.TrimSpace(content) == "" {
		verr.add("content", "zorunlu")

		return
	}

	switch channel {
	case ChannelSMS:
		validateSMSContent(verr, content)

	case ChannelEmail:
		if count := utf8.RuneCountInString(content); count > EmailMaxContentRunes {
			verr.add("content", "en fazla %d karakter olabilir (%d gönderildi)",
				EmailMaxContentRunes, count)
		}

	case ChannelPush:
		if count := utf8.RuneCountInString(content); count > PushMaxContentRunes {
			verr.add("content", "en fazla %d karakter olabilir (%d gönderildi)",
				PushMaxContentRunes, count)
		}
	}
}

// validateSMSContent, SMS sınırını operatörün kullandığı kodlamaya göre uygular.
//
// Metin GSM-7'ye sığıyorsa 160 septet, sığmıyorsa UCS-2'ye düşer ve 70 kod birimi
// sınırı geçerli olur. Hata mesajı hangi kodlamanın geçerli olduğunu da söyler:
// "neden 70?" sorusu tek bir Türkçe harften kaynaklanabiliyor.
func validateSMSContent(verr *ValidationError, content string) {
	if septets, encodable := GSM7Septets(content); encodable {
		if septets > SMSGSM7MaxSeptets {
			verr.add("content", "GSM-7 SMS en fazla %d septet olabilir (%d hesaplandı)",
				SMSGSM7MaxSeptets, septets)
		}

		return
	}

	if units := UTF16Units(content); units > SMSUnicodeMaxUnits {
		verr.add("content",
			"GSM-7 dışı karakter içerdiği için Unicode SMS sayılıyor; "+
				"en fazla %d kod birimi olabilir (%d hesaplandı)",
			SMSUnicodeMaxUnits, units)
	}
}
