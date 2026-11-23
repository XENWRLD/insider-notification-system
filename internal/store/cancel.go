package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// ErrNotCancellable, bildirimin iptal edilebilir bir durumda olmadığını bildirir.
// Çağıran bunu 409'a çevirir.
var ErrNotCancellable = errors.New("bildirim iptal edilebilir durumda değil")

// cancelQuery, iptali tek ve atomik bir ifadede yapar.
//
// Neden SELECT-sonra-UPDATE değil? İkisinin arasındaki boşlukta worker işi claim edip
// processing'e geçirebilir; o zaman istemciye "iptal ettim" denir ama mesaj yine gider.
// Burada karar tamamen veritabanında: WHERE koşulu durumu kontrol ediyor ve satır
// kilidi altında değerlendiriliyor.
//
// Worker yarışı: claim FOR UPDATE SKIP LOCKED ile satırı kilitler. İptal aynı anda
// gelirse bu UPDATE satır kilidinde BEKLER; claim commit edince WHERE yeniden
// değerlendirilir, durum artık processing olduğu için hiçbir satır güncellenmez.
const cancelQuery = `
	UPDATE notifications
	SET status = 'cancelled'
	WHERE id = $1 AND status IN ('pending', 'retrying')
	RETURNING ` + notificationColumns

// notificationExistsQuery, iptal başarısız olduğunda 404 ile 409'u ayırmaya yarar.
const notificationExistsQuery = `SELECT EXISTS (SELECT 1 FROM notifications WHERE id = $1)`

// CancelNotification, bildirimi atomik olarak iptal eder ve güncellenmiş kaydı döner.
//
// Hatalar: kayıt yoksa ErrNotFound, durumu uygun değilse ErrNotCancellable.
//
// Güncelleme olmadığında ikinci bir sorgu atılıyor; bu yalnızca hata yolunda çalışır ve
// atomikliği etkilemez — iptal kararı zaten UPDATE anında verildi. Buradaki okuma
// yalnızca istemciye doğru durum kodunu söylemek için.
func (s *Store) CancelNotification(
	ctx context.Context, id uuid.UUID,
) (*domain.Notification, error) {
	notification, err := scanNotification(s.pool.QueryRow(ctx, cancelQuery, id))

	switch {
	case err == nil:
		return notification, nil

	case errors.Is(err, pgx.ErrNoRows):
		// Satır ya hiç yok ya da durumu uygun değildi.
		var exists bool
		if existsErr := s.pool.QueryRow(ctx, notificationExistsQuery, id).Scan(&exists); existsErr != nil {
			return nil, fmt.Errorf("bildirim durumu kontrol edilemedi: %w", existsErr)
		}

		if !exists {
			return nil, ErrNotFound
		}

		return nil, ErrNotCancellable

	default:
		return nil, fmt.Errorf("bildirim iptal edilemedi: %w", err)
	}
}
