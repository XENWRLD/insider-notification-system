// Package domain, bildirim modelini, durum makinesini ve iş kurallarını barındırır.
//
// Bu paket HTTP ve SQL bilmez: ne JSON etiketleri ne de veritabanı sorguları içerir.
// Böylece iş kuralları taşıma katmanından bağımsız test edilebilir ve aynı kurallar
// hem API hem worker tarafından kullanılabilir.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// DefaultMaxAttempts, bir bildirimin kaç kez denenebileceği (docs/PLAN.md §6).
const DefaultMaxAttempts = 5

// Channel, bildirimin gönderileceği kanal.
type Channel string

// Desteklenen kanallar. Değerler veritabanındaki CHECK kısıtı ile birebir aynı.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// AllChannels, desteklenen kanalların tamamı.
//
// Tek kaynak: worker havuzlarının varsayılanı ve sağlayıcı bağlantı havuzunun boyutu
// (kanal × concurrency) bu listeden türüyor. Yeni bir kanal eklenince ikisi birden
// doğru kalır — ayrı ayrı yazıldığında biri güncellenmeden kalıyordu.
func AllChannels() []Channel {
	return []Channel{ChannelSMS, ChannelEmail, ChannelPush}
}

// Valid, kanalın desteklenen değerlerden biri olup olmadığını söyler.
func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	default:
		return false
	}
}

// Priority, işin kuyruktan çekilme önceliği.
type Priority string

// Öncelik değerleri. Sıralama metin üzerinden yapılmaz; veritabanındaki
// priority_rank türetilmiş kolonu doğru sırayı garanti eder (alfabetik sırada
// 'low' < 'normal' çıkardı).
const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Valid, önceliğin desteklenen değerlerden biri olup olmadığını söyler.
func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	default:
		return false
	}
}

// Rank, önceliğin sıra numarası: küçük olan önce çekilir.
//
// Değerler migration 000001'deki priority_rank generated kolonuyla **birebir aynı
// kalmak zorunda**; iki kaynak ayrışırsa worker'ın Go tarafında sıraladığı grup
// veritabanının seçtiği sıradan farklı olur. TestPriorityRankMatchesGeneratedColumn
// bu eşitliği her koşuda doğrular.
//
// Neden Go'da da gerekiyor: claim sorgusu UPDATE ... RETURNING olduğu için dönen
// satırların sırası garanti edilmez (sıra dış planın tarama sırasına göredir, CTE'nin
// ORDER BY'ına göre değil) ve priority_rank okuma kolonları arasında yok.
func (p Priority) Rank() int {
	switch p {
	case PriorityHigh:
		return 1
	case PriorityNormal:
		return 2
	case PriorityLow:
		return 3
	default:
		// Tanınmayan değer en sona: geçersiz veri kuyruğun önünü kapatmasın.
		return 4
	}
}

// Notification, notifications tablosunun bir satırının karşılığı.
//
// Nullable kolonlar işaretçi (pointer) olarak tutulur: "değer yok" ile "sıfır değer"
// arasındaki fark anlamlı — örneğin lease_token'ın boş olması işin sahipsiz olduğunu,
// attempt_count'un 0 olması ise hiç denenmediğini söyler.
type Notification struct {
	ID      uuid.UUID
	BatchID *uuid.UUID

	Channel   Channel
	Recipient string
	Content   string
	Priority  Priority
	Status    Status

	AttemptCount int
	MaxAttempts  int

	// AvailableAt, işin çekilebilir hâle geldiği an. Normal gönderim, retry gecikmesi
	// ve zamanlanmış bildirim aynı alan üzerinden çalışır.
	//
	// Yeni bir bildirimde SIFIR olabilir: "hemen gönderilebilir, damgayı veritabanı
	// bassın" demektir (store COALESCE ile now() uyguluyor ve değeri RETURNING ile
	// geri yazıyor). Veritabanından okunan bir kayıtta her zaman doludur.
	AvailableAt time.Time

	// Lease: işi çeken worker'ın süreli sahipliği. İkisi birlikte dolu ya da birlikte
	// boş olur; veritabanında CHECK kısıtı bunu garanti eder.
	LeaseToken     *uuid.UUID
	LeaseExpiresAt *time.Time

	ProviderMessageID *string
	LastError         *string

	// CorrelationID, isteği uçtan uca izlemeye yarar. API ve worker ayrı process
	// olduğu için kimlik veritabanı üzerinden worker loglarına taşınır.
	CorrelationID string

	TemplateID *uuid.UUID

	CreatedAt time.Time
	UpdatedAt time.Time
}
