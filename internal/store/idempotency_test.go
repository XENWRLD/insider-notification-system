package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

const testTTL = 24 * time.Hour

// hashOf, gövde baytlarının SHA-256'sını üretir (handler ile aynı hesap).
func hashOf(body string) []byte {
	sum := sha256.Sum256([]byte(body))

	return sum[:]
}

// idemFor, verilen anahtar ve gövde için koruma parametreleri üretir.
func idemFor(key, body string) IdempotencyRequest {
	return IdempotencyRequest{
		Scope: ScopeNotifications,
		Key:   key,
		Hash:  hashOf(body),
		TTL:   testTTL,
	}
}

// countNotifications, tablodaki bildirim sayısını döner.
func countNotifications(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()

	var count int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM notifications`).Scan(&count))

	return count
}

// countIdempotencyKeys, tablodaki anahtar sayısını döner.
func countIdempotencyKeys(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()

	var count int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM idempotency_keys`).Scan(&count))

	return count
}

// renderFor, bildirimin kimliğini döndüren basit bir cevap üreticisi.
func renderFor(n *domain.Notification) func() any {
	return func() any {
		return map[string]any{"id": n.ID.String(), "status": string(n.Status)}
	}
}

// TestIdempotencySameKeySameBodyReplays, tekrar gönderimde yeni kayıt oluşmadığını ve
// İLK isteğin cevabının döndüğünü doğrular.
func TestIdempotencySameKeySameBodyReplays(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	const body = `{"channel":"sms","recipient":"+905551234567","content":"Merhaba"}`

	first := newTestNotification(t, "sms", "normal")
	outcome, err := store.CreateNotification(ctx, first, idemFor("k-1", body), renderFor(first))
	require.NoError(t, err)
	require.False(t, outcome.Replayed, "ilk istek iş yapmalıydı")
	require.Equal(t, 1, countNotifications(t, pool))

	// İkinci istek: aynı anahtar, aynı gövde.
	second := newTestNotification(t, "sms", "normal")
	outcome, err = store.CreateNotification(ctx, second, idemFor("k-1", body), renderFor(second))
	require.NoError(t, err)

	require.True(t, outcome.Replayed, "ikinci istek saklanan cevabı döndürmeliydi")
	require.Equal(t, 202, outcome.Status)

	// Kritik: ikinci bildirim OLUŞMADI.
	require.Equal(t, 1, countNotifications(t, pool), "tekrar gönderim yeni kayıt oluşturmamalı")

	// Dönen cevap İLK isteğin kimliğini taşıyor, ikincinin değil.
	var replayed map[string]any
	require.NoError(t, json.Unmarshal(outcome.Body, &replayed))
	require.Equal(t, first.ID.String(), replayed["id"])
	require.NotEqual(t, second.ID.String(), replayed["id"])
}

// TestIdempotencySameKeyDifferentBodyConflicts, anahtarın başka bir istek için
// kullanılmasının reddedildiğini doğrular.
func TestIdempotencySameKeyDifferentBodyConflicts(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	first := newTestNotification(t, "sms", "normal")
	_, err := store.CreateNotification(ctx, first, idemFor("k-1", `{"a":1}`), renderFor(first))
	require.NoError(t, err)

	second := newTestNotification(t, "sms", "normal")
	_, err = store.CreateNotification(ctx, second, idemFor("k-1", `{"a":2}`), renderFor(second))

	require.ErrorIs(t, err, ErrIdempotencyKeyReused)
	require.Equal(t, 1, countNotifications(t, pool), "çakışmada yeni kayıt oluşmamalı")
}

// TestIdempotencyScopesDoNotCollide, aynı anahtarın tekil ve batch uçlarında
// çarpışmadığını doğrular.
func TestIdempotencyScopesDoNotCollide(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	const body = `{"x":1}`

	single := newTestNotification(t, "sms", "normal")
	_, err := store.CreateNotification(ctx, single, IdempotencyRequest{
		Scope: ScopeNotifications, Key: "shared", Hash: hashOf(body), TTL: testTTL,
	}, renderFor(single))
	require.NoError(t, err)

	// Aynı anahtar, farklı scope: batch ucu etkilenmemeli.
	batch, batchID, err := domain.NewNotificationBatch([]domain.NewNotificationInput{
		{Channel: "sms", Recipient: "+905559999999", Content: "Toplu"},
	})
	require.NoError(t, err)
	batch[0].CorrelationID = uuid.NewString()

	_, err = store.CreateBatch(ctx, batch, IdempotencyRequest{
		Scope: ScopeNotificationsBatch, Key: "shared", Hash: hashOf(body), TTL: testTTL,
	}, func() any { return map[string]any{"batch_id": batchID.String()} })
	require.NoError(t, err)

	require.Equal(t, 2, countNotifications(t, pool))
	require.Equal(t, 2, countIdempotencyKeys(t, pool))
}

// TestIdempotencyDisabledWithoutKey, anahtar verilmediğinde korumanın atlandığını doğrular.
func TestIdempotencyDisabledWithoutKey(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	none := IdempotencyRequest{Scope: ScopeNotifications, TTL: testTTL}

	for range 2 {
		notification := newTestNotification(t, "sms", "normal")
		outcome, err := store.CreateNotification(ctx, notification, none, renderFor(notification))
		require.NoError(t, err)
		require.False(t, outcome.Replayed)
	}

	require.Equal(t, 2, countNotifications(t, pool), "koruma yoksa iki kayıt oluşmalı")
	require.Zero(t, countIdempotencyKeys(t, pool), "anahtar kaydı hiç yazılmamalı")
}

// TestIdempotencyExpiredKeyIsReclaimed, süresi dolmuş anahtarın yeniden kullanılabildiğini
// doğrular.
//
// Doğruluğun temizlik işine bağlı olmaması bu davranışa dayanıyor: TTL geçtiyse lookup
// eski kaydı yok sayar ve yeni istek onu aynı transaction'da devralır.
func TestIdempotencyExpiredKeyIsReclaimed(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	const body = `{"channel":"sms"}`

	first := newTestNotification(t, "sms", "normal")
	_, err := store.CreateNotification(ctx, first, idemFor("k-exp", body), renderFor(first))
	require.NoError(t, err)

	// Kaydı TTL'in ötesine taşı.
	_, err = pool.Exec(ctx,
		`UPDATE idempotency_keys SET created_at = now() - $1::interval`, testTTL+time.Hour)
	require.NoError(t, err)

	second := newTestNotification(t, "sms", "normal")
	outcome, err := store.CreateNotification(ctx, second, idemFor("k-exp", body), renderFor(second))
	require.NoError(t, err)

	require.False(t, outcome.Replayed, "süresi dolmuş anahtar replay etmemeli")
	require.Equal(t, 2, countNotifications(t, pool), "yeni bildirim oluşmalıydı")

	// Eski satır güncellendi, ikinci bir satır eklenmedi.
	require.Equal(t, 1, countIdempotencyKeys(t, pool))

	var storedID string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT response_body->>'id' FROM idempotency_keys WHERE key = 'k-exp'`).Scan(&storedID))
	require.Equal(t, second.ID.String(), storedID, "saklanan cevap yeni isteğin olmalı")
}

// TestDeleteExpiredIdempotencyKeys, temizliğin yalnızca süresi dolmuşları sildiğini ve
// parça parça çalıştığını doğrular.
func TestDeleteExpiredIdempotencyKeys(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()

	insert := `
		INSERT INTO idempotency_keys
			(scope, key, request_hash, response_status, response_body, created_at)
		VALUES ($1, $2, $3, 202, '{}'::jsonb, $4)`

	// 5 süresi dolmuş, 2 taze kayıt.
	for i := range 5 {
		_, err := pool.Exec(ctx, insert, ScopeNotifications,
			"old-"+uuid.NewString(), hashOf("x"), time.Now().Add(-48*time.Hour))
		require.NoError(t, err, "kayıt %d", i)
	}
	for i := range 2 {
		_, err := pool.Exec(ctx, insert, ScopeNotifications,
			"fresh-"+uuid.NewString(), hashOf("x"), time.Now())
		require.NoError(t, err, "kayıt %d", i)
	}

	// batchSize=2 → parça parça silme.
	deleted, err := store.DeleteExpiredIdempotencyKeys(ctx, testTTL, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, deleted, "LIMIT'e uyulmalı")
	require.Equal(t, 5, countIdempotencyKeys(t, pool))

	// Kalanları temizle.
	deleted, err = store.DeleteExpiredIdempotencyKeys(ctx, testTTL, 100)
	require.NoError(t, err)
	require.EqualValues(t, 3, deleted)

	// Taze kayıtlar hayatta.
	require.Equal(t, 2, countIdempotencyKeys(t, pool))

	// Silinecek bir şey kalmadığında 0 döner, hata değil.
	deleted, err = store.DeleteExpiredIdempotencyKeys(ctx, testTTL, 100)
	require.NoError(t, err)
	require.Zero(t, deleted)
}

// TestStoredResponseIsValueIdenticalNotByteIdentical, jsonb'nin anahtar sırasını
// normalize ettiğini belgeler.
//
// Bu bir kusur değil, bilinçli bir takas: jsonb geçersiz JSON saklanmasını engelliyor
// ve cevap üzerinde sorgu yapılabilmesini sağlıyor. JSON'da anahtar sırası anlamsız
// (RFC 8259), bu yüzden istemci açısından fark yok. Byte düzeyinde aynılık gerekirse
// kolon tipi json (jsonb değil) olmalıydı.
func TestStoredResponseIsValueIdenticalNotByteIdentical(t *testing.T) {
	pool := sharedPool(t)
	store := New(pool)

	ctx := context.Background()
	const body = `{"x":1}`

	// Alfabetik olmayan sırada, boşluksuz bir cevap saklanıyor.
	original := `{"z":"son","a":"ilk","m":"orta"}`

	first := newTestNotification(t, "sms", "normal")
	_, err := store.CreateNotification(ctx, first, idemFor("k-order", body), func() any {
		return json.RawMessage(original)
	})
	require.NoError(t, err)

	second := newTestNotification(t, "sms", "normal")
	outcome, err := store.CreateNotification(ctx, second, idemFor("k-order", body), func() any {
		return json.RawMessage(original)
	})
	require.NoError(t, err)
	require.True(t, outcome.Replayed)

	// Değerler aynı.
	require.JSONEq(t, original, string(outcome.Body))

	// Baytlar aynı DEĞİL: jsonb anahtarları sıralamış ve boşluk eklemiş.
	require.NotEqual(t, original, string(outcome.Body),
		"jsonb normalizasyonu bekleniyordu; davranış değiştiyse bu test güncellenmeli")
	require.Equal(t, `{"a": "ilk", "m": "orta", "z": "son"}`, string(outcome.Body))
}
