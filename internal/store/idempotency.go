package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Idempotency kapsamları: aynı anahtar farklı uçlarda çarpışmasın.
const (
	ScopeNotifications      = "notifications"
	ScopeNotificationsBatch = "notifications_batch"
)

// ErrIdempotencyKeyReused, aynı anahtarın farklı bir gövdeyle kullanıldığını bildirir.
// Çağıran bunu 409'a çevirir.
var ErrIdempotencyKeyReused = errors.New("idempotency anahtarı farklı bir gövdeyle kullanılmış")

// IdempotencyRequest, tekrar gönderim koruması parametreleri.
// Key boşsa koruma hiç uygulanmaz.
type IdempotencyRequest struct {
	// Scope, anahtarın hangi uca ait olduğu (ScopeNotifications | ScopeNotificationsBatch).
	Scope string
	Key   string
	// Hash, ham istek gövdesinin SHA-256'sı. Aynı key + farklı hash → 409.
	Hash []byte
	// TTL, saklanan cevabın geçerlilik süresi. Süresi dolmuş anahtar yeniden kullanılabilir.
	TTL time.Duration
}

// enabled, bu istek için koruma uygulanıp uygulanmayacağını söyler.
func (r IdempotencyRequest) enabled() bool {
	return r.Key != ""
}

// Outcome, idempotent bir işlemin sonucu.
type Outcome struct {
	// Replayed true ise iş hiç yapılmadı; Status ve Body saklanan cevaptır.
	Replayed bool
	Status   int
	Body     json.RawMessage
}

// claimPlaceholderStatus, cevap henüz üretilmemiş bir kaydı işaretler.
// Aynı transaction içinde gerçek değerle güncellenir; rollback hâlinde satır hiç kalmaz.
const claimPlaceholderStatus = 0

// claimQuery, anahtarı talep eder.
//
// Tek ifadede hem "yeni anahtar" hem "süresi dolmuş anahtarı yeniden kullan" durumunu
// çözer. ON CONFLICT DO UPDATE, çakışan satırı WHERE değerlendirilmeden önce KİLİTLER;
// bu yüzden eşzamanlı iki aynı istek sıraya girer ve ikincisi birincinin commit'ini
// bekler. Ayrı SELECT + INSERT yarış durumuna açık olurdu.
//
//   - 1 satır döner → anahtar bizim (yeni, ya da süresi dolmuş olduğu için devraldık)
//   - 0 satır döner → TAZE bir kayıt var (DO UPDATE'in WHERE'i tutmadı)
const claimQuery = `
	INSERT INTO idempotency_keys (scope, key, request_hash, response_status, response_body)
	VALUES ($1, $2, $3, $4, '{}'::jsonb)
	ON CONFLICT (scope, key) DO UPDATE
	SET request_hash    = EXCLUDED.request_hash,
	    response_status = EXCLUDED.response_status,
	    response_body   = EXCLUDED.response_body,
	    created_at      = now()
	WHERE idempotency_keys.created_at <= now() - $5::interval
	RETURNING 1`

// storedResponseQuery, taze bir kaydın saklanan cevabını okur.
const storedResponseQuery = `
	SELECT request_hash, response_status, response_body
	FROM idempotency_keys
	WHERE scope = $1 AND key = $2`

// claimIdempotencyKey, anahtarı talep eder; başarılıysa true döner.
func claimIdempotencyKey(ctx context.Context, tx pgx.Tx, idem IdempotencyRequest) (bool, error) {
	var claimed int

	err := tx.QueryRow(ctx, claimQuery,
		idem.Scope, idem.Key, idem.Hash, claimPlaceholderStatus, idem.TTL,
	).Scan(&claimed)

	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("idempotency anahtarı talep edilemedi: %w", err)
	}

	return true, nil
}

// readStoredResponse, taze kaydın cevabını okur ve hash'i karşılaştırır.
//
// Hash farklıysa ErrIdempotencyKeyReused döner: istemci aynı anahtarı başka bir istek
// için kullanmış ve sessizce eski cevabı vermek yanlış olurdu.
func readStoredResponse(
	ctx context.Context, tx pgx.Tx, idem IdempotencyRequest,
) (Outcome, error) {
	var (
		storedHash   []byte
		storedStatus int
		storedBody   []byte
	)

	err := tx.QueryRow(ctx, storedResponseQuery, idem.Scope, idem.Key).
		Scan(&storedHash, &storedStatus, &storedBody)
	if err != nil {
		return Outcome{}, fmt.Errorf("saklanan idempotency cevabı okunamadı: %w", err)
	}

	if !bytes.Equal(storedHash, idem.Hash) {
		return Outcome{}, ErrIdempotencyKeyReused
	}

	// Yer tutucu durum: ilk istek aynı anda sürüyor olamaz (talep kilidi serileştiriyor),
	// yani buraya düşmek beklenmeyen bir durumdur.
	if storedStatus == claimPlaceholderStatus {
		return Outcome{}, errors.New("idempotency kaydı yarı yazılmış durumda bulundu")
	}

	return Outcome{Replayed: true, Status: storedStatus, Body: storedBody}, nil
}

// completeQuery, talep edilen anahtarın cevabını tamamlar.
const completeQuery = `
	UPDATE idempotency_keys
	SET response_status = $1, response_body = $2
	WHERE scope = $3 AND key = $4`

// completeIdempotencyKey, üretilen cevabı kayda yazar.
func completeIdempotencyKey(
	ctx context.Context, tx pgx.Tx, idem IdempotencyRequest, status int, body any,
) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("idempotency cevabı serialize edilemedi: %w", err)
	}

	if _, err := tx.Exec(ctx, completeQuery, status, encoded, idem.Scope, idem.Key); err != nil {
		return fmt.Errorf("idempotency cevabı yazılamadı: %w", err)
	}

	return nil
}

// deleteExpiredQuery, süresi dolmuş anahtarları parça parça siler.
//
// Tek büyük DELETE tabloyu uzun süre kilitler ve WAL'i şişirir; LIMIT ile parçalamak
// temizliği normal trafiğin arasına sıkıştırılabilir hâle getirir.
const deleteExpiredQuery = `
	DELETE FROM idempotency_keys
	WHERE (scope, key) IN (
		SELECT scope, key FROM idempotency_keys
		WHERE created_at <= now() - $1::interval
		ORDER BY created_at
		LIMIT $2
	)`

// DeleteExpiredIdempotencyKeys, süresi dolmuş anahtarlardan en fazla batchSize kadarını
// siler ve silinen satır sayısını döner.
//
// Doğruluk bu temizliğe bağlı DEĞİL: lookup sorgusu süresi dolmuş anahtarları zaten yok
// sayıyor ve yeni bir istek onları devralabiliyor. Temizlik yalnızca tabloyu küçük tutmak
// için var; çağıran Adım 6'da worker'ın reaper döngüsünün yanına eklenecek.
func (s *Store) DeleteExpiredIdempotencyKeys(
	ctx context.Context, ttl time.Duration, batchSize int,
) (int64, error) {
	tag, err := s.pool.Exec(ctx, deleteExpiredQuery, ttl, batchSize)
	if err != nil {
		return 0, fmt.Errorf("süresi dolmuş idempotency anahtarları silinemedi: %w", err)
	}

	return tag.RowsAffected(), nil
}
