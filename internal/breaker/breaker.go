// Package breaker, kanal başına devre kesici (circuit breaker) sağlar.
//
// Amaç sağlayıcı çöktüğünde o kanala gitmeyi tamamen bırakmak: hem sağlayıcıyı
// dövmemek hem de boşa gidecek her denemenin bir deneme hakkı ve bir rate limit izni
// yemesini önlemek. Worker, devre açıkken o kanaldan iş ÇEKMİYOR — satırlar kuyrukta,
// iptal edilebilir hâlde bekliyor.
//
// internal/ratelimit ile aynı kalıp: dış kütüphane (sony/gobreaker) kendi arayüzümüzün
// ve kendi tiplerimizin altında kalıyor, politika ise çağırana ait.
package breaker

import (
	"errors"
	"time"
)

// State, devrenin o anki durumu.
//
// String tipi seçildi: doğrudan loglanabiliyor ve gobreaker'ın sayısal durumları
// dışarıya sızmıyor.
type State string

const (
	// StateClosed, her istek geçer.
	StateClosed State = "closed"

	// StateHalfOpen, yalnızca tek bir sonda geçer: başarılıysa devre kapanır,
	// başarısızsa tekrar açılır.
	StateHalfOpen State = "half-open"

	// StateOpen, hiçbir istek geçmez.
	StateOpen State = "open"
)

// ErrOpen, çağrının devre kesici tarafından reddedildiğini bildirir: sağlayıcıya
// HİÇ gidilmedi.
//
// Sentinel hata: çağıran errors.Is ile tanır. Tipli bir hata olmasına gerek yok,
// çünkü içinden okunacak bir alan yok — yalnızca "reddedildi" bilgisi taşıyor.
var ErrOpen = errors.New("devre kesici açık: sağlayıcıya gidilmedi")

// Breaker, bir kanalın sağlayıcı çağrılarını koruyan devre kesici.
//
// Arayüz olarak alınıyor ki worker testleri açık/yarı açık durumları determinist
// biçimde kurabilsin (ratelimit.Limiter ile aynı gerekçe).
type Breaker interface {
	// Do, fn'i devre kesicinin altında çalıştırır.
	//
	// Devre açıksa (ya da yarı açıkta sonda kotası dolmuşsa) fn HİÇ çağrılmaz ve
	// ErrOpen döner. fn'in döndürdüğü hata Config.IsFailure'a göre sınıflandırılır.
	//
	// context almıyor: devre kesici kararı anlık ve yerel, bekleme içermiyor.
	// Açık devrede ne kadar sonra yeniden denenecek kararı çağıranda, çünkü onu
	// veritabanına (available_at) yazması gerekiyor.
	Do(fn func() error) error

	// State, çağrı yapmadan durumu söyler.
	//
	// Claimer bunu kullanıyor: devre açıkken o kanaldan iş çekmemek için. Do ile
	// öğrenmek, iş çekilmiş olmasını gerektirirdi — yani denemeyi harcamayı.
	State() State
}

// Config, bir devre kesicinin ayarları.
type Config struct {
	// Name, log ve metriklerde görünen ad. Üretimde kanal adı.
	Name string

	// FailureThreshold, devrenin açılması için gereken ARDIŞIK hata sayısı.
	FailureThreshold int

	// OpenTimeout, açık durumdan yarı açığa geçmek için beklenen süre.
	OpenTimeout time.Duration

	// OnStateChange, her durum geçişinde çağrılır. Loglama için: her yoklamada
	// loglamak yerine geçiş başına bir satır.
	OnStateChange func(name string, from, to State)

	// IsFailure, hatanın DEVREYİ ilgilendirip ilgilendirmediğini söyler.
	//
	// Üç sonuç var, iki değil — "hata" ile "kanal arızalı" aynı şey değil:
	//
	//	nil hata        → BAŞARI    (ardışık hata sayacı sıfırlanır, sonda devreyi kapatır)
	//	IsFailure true  → HATA      (sayaç artar, eşikte devre açılır)
	//	IsFailure false → SAYILMAZ  (sayaçlara hiç dokunulmaz, durum değişmez)
	//
	// Üçüncü durum şart: hız sınırı yanıtı (429) ne kanalın sağlığının ne arızasının
	// kanıtı. Başarı sayılsaydı ardışık hata sayacını sıfırlar ve kâh 503 kâh 429
	// dönen karışık bir kesintide devre çok geç açılırdı.
	//
	// nil ise her hata HATA sayılır.
	IsFailure func(error) bool
}

// validate, ayarların tutarlı olduğunu doğrular.
func (c Config) validate() error {
	var problems []string

	if c.Name == "" {
		problems = append(problems, "Name boş olamaz")
	}

	if c.FailureThreshold < 1 {
		problems = append(problems, "FailureThreshold en az 1 olmalı")
	}

	if c.OpenTimeout <= 0 {
		problems = append(problems, "OpenTimeout pozitif olmalı")
	}

	if len(problems) > 0 {
		return errors.New("geçersiz devre kesici yapılandırması: " + joinProblems(problems))
	}

	return nil
}

// joinProblems, sorunları tek satırda birleştirir (projedeki kalıp).
func joinProblems(problems []string) string {
	result := problems[0]
	for _, problem := range problems[1:] {
		result += "; " + problem
	}

	return result
}

// alwaysClosed, hiç açılmayan devre kesici.
type alwaysClosed struct{}

// AlwaysClosed, devre kesici uygulamayan bir Breaker döner.
//
// Testler için: devre kesici dışındaki davranışı ölçen testler gerçek bir durum
// makinesi kurmak zorunda kalmasın (ratelimit.Unlimited ile aynı rol).
func AlwaysClosed() Breaker {
	return alwaysClosed{}
}

func (alwaysClosed) Do(fn func() error) error { return fn() }

func (alwaysClosed) State() State { return StateClosed }

// Derleme zamanında arayüzün karşılandığını doğrular.
var _ Breaker = alwaysClosed{}
