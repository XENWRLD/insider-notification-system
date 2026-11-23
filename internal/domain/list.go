package domain

import (
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Listeleme sayfa boyutu sınırları (docs/PLAN.md §5).
//
// Üst sınır neden var? Sınırsız limit tek istekte tüm tabloyu çekmeye izin verirdi;
// hem bellek hem yanıt süresi istemcinin keyfine kalırdı.
const (
	ListLimitDefault = 20
	ListLimitMax     = 100
)

// Cursor, keyset sayfalamanın konumu: "bu kayıttan sonrasını getir".
//
// Kodlaması (base64) taşıma katmanının işi; domain yalnızca hangi alanların sıralama
// anahtarı olduğunu bilir.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// ListFilterInput, ham (doğrulanmamış) listeleme ölçütleri.
//
// Alanlar string ve dilim: taşıma katmanı query parametrelerini olduğu gibi aktarır,
// doğrulama burada yapılır. Aynı parametrenin birden çok değeri VEYA, farklı
// parametreler VE ile birleşir.
type ListFilterInput struct {
	Statuses []string
	Channels []string

	// From / To, created_at üzerinde yarı açık aralık: [From, To).
	// Taşıma katmanı RFC3339 ayrıştırmasını yapar; burada yalnızca mantık doğrulanır.
	From *time.Time
	To   *time.Time

	Cursor *Cursor

	// Limit boşsa varsayılan uygulanır.
	Limit string
}

// ListFilter, doğrulanmış listeleme ölçütleri.
type ListFilter struct {
	Statuses []Status
	Channels []Channel

	From *time.Time
	To   *time.Time

	Cursor *Cursor
	Limit  int
}

// NewListFilter, ham ölçütleri doğrular.
//
// Hata dönerse tipi her zaman *ValidationError'dır; tüm alan hataları tek yanıtta
// toplanır (NewNotification ile aynı felsefe).
func NewListFilter(in ListFilterInput) (ListFilter, error) {
	verr := &ValidationError{}

	filter := ListFilter{
		From:   in.From,
		To:     in.To,
		Cursor: in.Cursor,
		Limit:  ListLimitDefault,
	}

	for _, raw := range in.Statuses {
		status := Status(strings.TrimSpace(raw))
		if !status.Valid() {
			verr.add("status", "geçersiz durum (pending|processing|sent|retrying|failed|cancelled): %q", raw)

			continue
		}

		filter.Statuses = append(filter.Statuses, status)
	}

	for _, raw := range in.Channels {
		channel := Channel(strings.TrimSpace(raw))
		if !channel.Valid() {
			verr.add("channel", "geçersiz kanal (sms|email|push): %q", raw)

			continue
		}

		filter.Channels = append(filter.Channels, channel)
	}

	// Yarı açık aralık [From, To): From == To boş küme demek, bu bir hata.
	if in.From != nil && in.To != nil && !in.From.Before(*in.To) {
		verr.add("from", "to değerinden küçük olmalı (aralık [from, to) biçiminde)")
	}

	if raw := strings.TrimSpace(in.Limit); raw != "" {
		limit, err := strconv.Atoi(raw)
		switch {
		case err != nil:
			verr.add("limit", "sayı olmalı: %q", raw)
		case limit < 1:
			verr.add("limit", "en az 1 olmalı: %d", limit)
		case limit > ListLimitMax:
			verr.add("limit", "en fazla %d olabilir: %d", ListLimitMax, limit)
		default:
			filter.Limit = limit
		}
	}

	if len(verr.Fields) > 0 {
		return ListFilter{}, verr
	}

	return filter, nil
}
