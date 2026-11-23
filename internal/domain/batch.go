package domain

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Batch boyut sınırları (docs/PLAN.md §5).
//
// Üst sınır neden var? Tek transaction'da yazılan satır sayısı arttıkça kilit süresi ve
// bellek kullanımı büyür; 1000 hem ölçülebilir hem de istemci için makul bir parça.
const (
	BatchMinSize = 1
	BatchMaxSize = 1000
)

// BatchValidationError, batch içindeki hatalı öğeleri sıra numarasıyla taşır.
//
// Alan adlarının "notifications[3].recipient" biçimine çevrilmesi taşıma katmanının işi;
// domain JSON alan adlarını bilmez.
type BatchValidationError struct {
	Items map[int]*ValidationError
}

// Error, error arayüzünü karşılar.
func (e *BatchValidationError) Error() string {
	indexes := make([]int, 0, len(e.Items))
	for index := range e.Items {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)

	parts := make([]string, 0, len(indexes))
	for _, index := range indexes {
		parts = append(parts, fmt.Sprintf("%d. öğe: %s", index, e.Items[index].Error()))
	}

	return "batch doğrulama hatası: " + strings.Join(parts, "; ")
}

// BatchSummary, bir batch'in durum bazında özeti.
type BatchSummary struct {
	BatchID uuid.UUID
	Total   int

	// Counts, altı durumun TAMAMINI içerir; hiç kaydı olmayan durum için 0.
	// Sabit şema sayesinde istemci `counts.sent` diye okur, alan varlığını kontrol
	// etmek zorunda kalmaz.
	Counts map[Status]int

	// Completed, tüm bildirimlerin terminal duruma ulaşıp ulaşmadığı.
	Completed bool

	CreatedAt time.Time
}

// allStatuses, Counts'un sabit şemasını üretmek için tüm durumlar.
var allStatuses = []Status{
	StatusPending, StatusProcessing, StatusSent,
	StatusRetrying, StatusFailed, StatusCancelled,
}

// NewBatchSummary, durum sayımlarından özet üretir.
//
// Eksik durumlar sıfırla doldurulur ve Completed, durum listesini burada ikinci kez
// yazmak yerine IsTerminal() üzerinden hesaplanır — durum makinesi tek kaynak kalır.
func NewBatchSummary(batchID uuid.UUID, counts map[Status]int, createdAt time.Time) BatchSummary {
	summary := BatchSummary{
		BatchID:   batchID,
		Counts:    make(map[Status]int, len(allStatuses)),
		Completed: true,
		CreatedAt: createdAt,
	}

	for _, status := range allStatuses {
		count := counts[status]

		summary.Counts[status] = count
		summary.Total += count

		// Terminal olmayan tek bir kayıt bile batch'i "tamamlanmadı" yapar.
		if count > 0 && !status.IsTerminal() {
			summary.Completed = false
		}
	}

	return summary
}

// NewNotificationBatch, bir batch isteğini doğrular ve hazır bildirimleri üretir.
//
// Tümü doğrulanır, ilk hatada durulmaz: bir öğe hatalıysa batch'in tamamı reddedilir
// (docs/PLAN.md §5) ama istemci bütün hatalı satırları tek yanıtta görür — 1000 öğeli bir
// batch'i tek tek düzeltmek zorunda kalmaz.
//
// Hata tipleri: boyut sorunlarında *ValidationError, öğe hatalarında *BatchValidationError.
func NewNotificationBatch(inputs []NewNotificationInput) ([]*Notification, uuid.UUID, error) {
	if len(inputs) < BatchMinSize || len(inputs) > BatchMaxSize {
		verr := &ValidationError{}
		verr.add("notifications", "%d-%d arası olmalı (%d gönderildi)",
			BatchMinSize, BatchMaxSize, len(inputs))

		return nil, uuid.Nil, verr
	}

	// Batch kimliği önden üretilir: tüm satırlar aynı kimliği taşıyacak, böylece
	// "bu isteğin kaç tanesi gönderildi" sorusu tek sorguyla cevaplanabilir (Adım 5).
	batchID := uuid.New()

	notifications := make([]*Notification, 0, len(inputs))
	itemErrors := make(map[int]*ValidationError)

	for index, input := range inputs {
		notification, err := NewNotification(input)
		if err != nil {
			// NewNotification sözleşmesi gereği yalnızca *ValidationError döner.
			var verr *ValidationError
			if errors.As(err, &verr) {
				itemErrors[index] = verr

				continue
			}

			return nil, uuid.Nil, err
		}

		notification.BatchID = &batchID
		notifications = append(notifications, notification)
	}

	if len(itemErrors) > 0 {
		return nil, uuid.Nil, &BatchValidationError{Items: itemErrors}
	}

	return notifications, batchID, nil
}
