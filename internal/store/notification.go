package store

import (
	"context"
	"fmt"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// createNotificationQuery, tekil bildirim ekler.
//
// status ve attempt_count veritabanı varsayılanlarına bırakılmıyor, açıkça yazılıyor:
// kaydın hangi durumda oluştuğu çağıran tarafın kararı ve bu karar domain'de verildi.
//
// RETURNING ile zaman damgaları veritabanından okunuyor; uygulama saati ile DB saati
// arasındaki fark yanıta sızmasın ve istemci gerçekten kaydedilen değeri görsün.
const createNotificationQuery = `
	INSERT INTO notifications
		(id, channel, recipient, content, priority, status,
		 attempt_count, max_attempts, available_at, correlation_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	RETURNING created_at, updated_at`

// CreateNotification, bildirimi pending olarak yazar ve zaman damgalarını kayda doldurur.
//
// Kayıt ile iş kuyruğu aynı tablo olduğu için ayrı bir "kuyruğa ekle" adımı yok:
// INSERT tamamlandığı anda iş worker tarafından çekilebilir hâle gelir.
func (s *Store) CreateNotification(ctx context.Context, n *domain.Notification) error {
	// Alan tipleri string tabanlı olduğu için açıkça string'e çevriliyor; pgx'in
	// adlandırılmış tipleri tahmin etmesine güvenmiyoruz.
	err := s.pool.QueryRow(ctx, createNotificationQuery,
		n.ID,
		string(n.Channel),
		n.Recipient,
		n.Content,
		string(n.Priority),
		string(n.Status),
		n.AttemptCount,
		n.MaxAttempts,
		n.AvailableAt,
		n.CorrelationID,
	).Scan(&n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		return fmt.Errorf("bildirim eklenemedi: %w", err)
	}

	return nil
}
