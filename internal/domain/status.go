package domain

// Status, bildirimin yaşam döngüsündeki konumu.
type Status string

// Durumlar. Değerler veritabanındaki CHECK kısıtı ile birebir aynı.
const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent"
	StatusRetrying   Status = "retrying"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// allowedTransitions, docs/PLAN.md §4'teki durum makinesinin kod hâli.
//
//	pending ──▶ processing ──▶ sent
//	   │            ├──▶ retrying ──▶ processing (available_at gelince)
//	   │            └──▶ failed   (kalıcı hata veya max deneme)
//	   ▼
//	cancelled   (yalnızca pending / retrying iken)
//
// Geçişleri tek yerde tablo hâlinde tutmanın nedeni: worker, reaper ve iptal ucu aynı
// kuralı üç ayrı yerde yeniden yorumlamasın. Boş liste = son durum (terminal).
var allowedTransitions = map[Status][]Status{
	StatusPending:    {StatusProcessing, StatusCancelled},
	StatusProcessing: {StatusSent, StatusRetrying, StatusFailed},
	StatusRetrying:   {StatusProcessing, StatusCancelled},
	StatusSent:       {},
	StatusFailed:     {},
	StatusCancelled:  {},
}

// Valid, durumun tanınan değerlerden biri olup olmadığını söyler.
func (s Status) Valid() bool {
	_, ok := allowedTransitions[s]

	return ok
}

// CanTransitionTo, bu durumdan hedef duruma geçişin kurallara uygun olup olmadığını söyler.
func (s Status) CanTransitionTo(target Status) bool {
	for _, allowed := range allowedTransitions[s] {
		if allowed == target {
			return true
		}
	}

	return false
}

// IsTerminal, durumun son durum olup olmadığını söyler: buradan çıkış yok.
func (s Status) IsTerminal() bool {
	targets, known := allowedTransitions[s]

	return known && len(targets) == 0
}

// IsCancellable, bildirimin iptal edilebilir olup olmadığını söyler (Adım 5).
//
// Gönderim başladıktan sonra (processing) iptal edilemez: sağlayıcıya istek gitmiş
// olabilir ve "iptal ettim" demek yanlış bilgi olurdu.
func (s Status) IsCancellable() bool {
	return s.CanTransitionTo(StatusCancelled)
}
