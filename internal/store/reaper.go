package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// reaperSelectQuery, lease'i dolmuş satırları seçip kilitler.
//
// Ayrı sabit: index testi EXPLAIN'i bunun üzerinde çalıştırır (aynı gerekçe
// claimSelectQuery'de). notifications_lease_expiry_idx kısmi index'i
// (lease_expires_at) WHERE status = 'processing' tam bu sorgu için kuruldu.
//
// SKIP LOCKED: birden çok worker kopyası reaper koşturabilir, birbirini beklemesin.
// LIMIT: tek süpürme sınırsız yazmaya dönüşmesin.
const reaperSelectQuery = `
	SELECT id FROM notifications
	WHERE status = 'processing' AND lease_expires_at <= now()
	ORDER BY lease_expires_at
	LIMIT $1
	FOR UPDATE SKIP LOCKED`

// reclaimQuery, sahipsiz kalmış işleri kuyruğa geri koyar veya hakkı tükenmişse bitirir.
//
// Neden pending değil retrying? processing -> pending geçişi durum makinesinde yasak ve
// satır gerçekten denendi. İkisi de claim index'inin kısmi yordamı içinde olduğu için
// davranış açısından fark yok.
//
// Neden failed dalı? Sonuç yazmadan çöken bir worker'ın satırı her turda attempt_count'u
// artırır. attempt_count max_attempts'a ulaştığında claim sorgusu (attempt_count <
// max_attempts) satırı artık çekmez; bu dal olmasa satır sonsuza kadar retrying'de
// gömülü kalırdı. İki değişiklik birbirine bağlı: biri olmadan diğeri hata üretir.
//
// available_at'e dokunulmuyor: zaten geçmişte, satır anında çekilebilir olmalı.
// attempt_count korunuyor: deneme gerçekten yapıldı.
const reclaimQuery = `
	WITH expired AS (` + reaperSelectQuery + `
	)
	UPDATE notifications
	SET status = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'retrying' END,
	    last_error = CASE WHEN attempt_count >= max_attempts
	        THEN 'lease süresi doldu ve deneme hakkı tükendi: sonuç hiç yazılmadı'
	        ELSE 'lease süresi doldu: işi çeken worker sonucu yazmadı' END,
	    lease_token = NULL, lease_expires_at = NULL
	WHERE id IN (SELECT id FROM expired)
	  AND status = 'processing'
	RETURNING id, channel, status, correlation_id, attempt_count`

// ReclaimedNotification, reaper'ın kurtardığı bir satırın özeti.
//
// correlation_id dönüyor ki kurtarma, isteği başlatan çağrının kimliğiyle loglanabilsin;
// Status ise satırın hangi dala gittiğini (retrying mi, failed mi) söyler.
type ReclaimedNotification struct {
	ID            uuid.UUID
	Channel       domain.Channel
	Status        domain.Status
	CorrelationID string
	AttemptCount  int
}

// ReclaimExpiredLeases, lease'i dolmuş en fazla limit kadar işi kurtarır.
//
// Çöken worker'ın işi böylece takılı kalmaz. Kurtarılacak satır yoksa boş dilim döner.
func (s *Store) ReclaimExpiredLeases(
	ctx context.Context, limit int,
) ([]ReclaimedNotification, error) {
	rows, err := s.pool.Query(ctx, reclaimQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("süresi dolmuş lease'ler kurtarılamadı: %w", err)
	}
	defer rows.Close()

	reclaimed := make([]ReclaimedNotification, 0, limit)

	for rows.Next() {
		var (
			item    ReclaimedNotification
			channel string
			status  string
		)

		if scanErr := rows.Scan(
			&item.ID, &channel, &status, &item.CorrelationID, &item.AttemptCount,
		); scanErr != nil {
			return nil, fmt.Errorf("kurtarılan iş okunamadı: %w", scanErr)
		}

		item.Channel = domain.Channel(channel)
		item.Status = domain.Status(status)

		reclaimed = append(reclaimed, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("kurtarılan işler okunamadı: %w", err)
	}

	return reclaimed, nil
}
