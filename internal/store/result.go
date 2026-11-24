package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// ErrLeaseLost, sonucun yazılamadığını bildirir: satır artık bu worker'a ait değil.
//
// Pratikte iki yoldan gelir: lease süresi dolup reaper satırı devraldı, ya da satır
// başka bir sebeple processing'den çıktı. Çağıran bunu hata değil, "bu işi başkası
// devraldı" bilgisi olarak ele alır.
var ErrLeaseLost = errors.New("lease artık bu worker'a ait değil")

// ErrInvalidResultStatus, processing'den geçilemeyecek bir duruma yazma denemesini
// bildirir. Programlama hatasıdır; veritabanına hiç gidilmez.
var ErrInvalidResultStatus = errors.New("geçersiz sonuç durumu")

// NotificationResult, bir gönderim denemesinin sonucu.
//
// LeaseToken zorunlu: sonuç yazımı yalnızca claim'i yapan worker'a açıktır.
type NotificationResult struct {
	ID         uuid.UUID
	LeaseToken uuid.UUID

	// Status yalnızca sent, failed veya retrying olabilir.
	Status domain.Status

	// ProviderMessageID yalnızca sent durumunda anlamlı.
	ProviderMessageID *string

	// StatusCode, sağlayıcının HTTP durum kodu. Timeout veya bağlantı hatasında hiç
	// yanıt gelmediği için nil olur.
	StatusCode *int

	// LastError, başarısız denemenin sebebi. Başarılı denemede nil.
	LastError *string

	DurationMS int

	// RetryAfter, yalnızca retrying durumunda kullanılır: satır bu kadar süre sonra
	// yeniden çekilebilir olur.
	RetryAfter time.Duration
}

// Sonuç yazan üç ifade. Ortak noktaları: satır kilidi yerine lease token'ı ile
// korunuyorlar ve lease çiftini NULL'lıyorlar.
//
// Neden lease koşulu? Gecikmiş ("zombi") bir worker, lease'i dolup reaper tarafından
// devralınan bir satıra sonuç yazmaya çalışabilir. WHERE lease_token = $2 bunu keser:
// token değişmiş veya NULL olmuştur, UPDATE hiçbir satır etkilemez. Böylece geç kalan
// worker yeni denemenin sonucunu ezemez.
//
// Neden deneme kaydı aynı ifadede? Durum güncellemesi ile notification_attempts satırı
// tek statement'ta, yani tek transaction'da yazılır: biri yazılıp diğeri yazılmadan
// süreç ölemez. Lease kaybedilmişse UPDATE 0 satır döndürür, CTE boş kalır ve INSERT de
// hiçbir şey yazmaz — zombi worker iz bırakmaz. Bu kalıp, Adım 8 için de yeterli:
// withTx gibi bir soyutlama eklemeye gerek yok.
//
// Lease çiftinin NULL'lanması zorunlu: notifications_lease_pair kısıtı ikisinin birlikte
// dolu ya da birlikte boş olmasını istiyor ve durum artık processing değil. Aynı zamanda
// dürüst: satır artık kimsenin değil.
const resultUpdateTail = `
	WHERE id = $1 AND lease_token = $2 AND status = 'processing'
		RETURNING id, attempt_count
	)
	INSERT INTO notification_attempts
		(notification_id, attempt_no, status_code, error, duration_ms)
	SELECT id, attempt_count, $4, $5, $6 FROM upd`

const sentQuery = `
	WITH upd AS (
		UPDATE notifications
		SET status = 'sent', provider_message_id = $3, last_error = NULL,
		    lease_token = NULL, lease_expires_at = NULL` + resultUpdateTail

const failedQuery = `
	WITH upd AS (
		UPDATE notifications
		SET status = 'failed', last_error = $3,
		    lease_token = NULL, lease_expires_at = NULL` + resultUpdateTail

// retryingQuery'de $7 gecikme: worker beklemez, satır ileri tarihlenir.
const retryingQuery = `
	WITH upd AS (
		UPDATE notifications
		SET status = 'retrying', last_error = $3,
		    available_at = now() + $7::interval,
		    lease_token = NULL, lease_expires_at = NULL` + resultUpdateTail

// validateResultStatus, hedef durumun processing'den geçilebilir olduğunu doğrular.
//
// Durum makinesi domain'de tek yerde duruyor; burada yeniden yorumlanmıyor, yalnızca
// soruluyor. Veritabanı gerektirmediği için birim testiyle kapsanır.
func validateResultStatus(status domain.Status) error {
	if !domain.StatusProcessing.CanTransitionTo(status) {
		return fmt.Errorf("%w: processing -> %s", ErrInvalidResultStatus, status)
	}

	return nil
}

// CompleteNotification, gönderim sonucunu ve deneme kaydını tek transaction'da yazar.
//
// Hatalar: hedef durum geçersizse ErrInvalidResultStatus, lease artık bizde değilse
// ErrLeaseLost.
func (s *Store) CompleteNotification(ctx context.Context, res NotificationResult) error {
	if err := validateResultStatus(res.Status); err != nil {
		return err
	}

	args := []any{
		res.ID, res.LeaseToken, nil,
		res.StatusCode, res.LastError, res.DurationMS,
	}

	var query string

	switch res.Status {
	case domain.StatusSent:
		query = sentQuery
		args[2] = res.ProviderMessageID

	case domain.StatusFailed:
		query = failedQuery
		args[2] = res.LastError

	case domain.StatusRetrying:
		query = retryingQuery
		args[2] = res.LastError
		args = append(args, res.RetryAfter)

	default:
		// validateResultStatus bu dala düşmeyi engelliyor; yine de sessiz kalmayalım.
		return fmt.Errorf("%w: %s", ErrInvalidResultStatus, res.Status)
	}

	tag, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("gönderim sonucu yazılamadı: %w", err)
	}

	// INSERT ... SELECT upd olduğu için etkilenen satır sayısı, UPDATE'in eşleşip
	// eşleşmediğini birebir yansıtır: 0 ise lease bizde değildi.
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}

	return nil
}
