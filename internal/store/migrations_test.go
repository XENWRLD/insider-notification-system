// Şemanın davranış testleri: generated kolon, CHECK kısıtları, trigger ve index'ler.
//
// Neden sahte (mock) değil? Test edilen şey SQL'in kendisi; bu özelliklerin davranışı
// yalnızca gerçek Postgres'te doğrulanabilir.
package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// schemaTables, migration'lar uygulandığında beklenen tablolar.
var schemaTables = []string{"idempotency_keys", "notification_attempts", "notifications"}

// TestMigrationsUpDownUp, down dosyalarının gerçekten çalıştığını kanıtlar.
//
// Bu test olmadan down migration'lar yalnızca "yazılmış" olur; bozuk bir down, ancak
// production'da geri alma gerektiğinde — yani en kötü anda — fark edilirdi.
// Şemayı değiştirdiği için kendi container'ında koşar.
func TestMigrationsUpDownUp(t *testing.T) {
	migrator, pool := dedicatedPostgres(t)

	require.NoError(t, migrator.Up())
	require.Equal(t, schemaTables, tableNames(t, pool))

	// Tüm migration'lar geri alınır: schema_migrations dışında tablo kalmamalı.
	require.NoError(t, migrator.Down())
	require.Empty(t, tableNames(t, pool))

	// Trigger'ın fonksiyonu da düşmeli; aksi hâlde ikinci Up "already exists" ile patlar.
	require.NoError(t, migrator.Up())
	require.Equal(t, schemaTables, tableNames(t, pool))
}

// TestExpectedIndexesExist, sıcak yol index'lerinin varlığını sabitler.
//
// Index'ler performans sözleşmesinin parçası; birinin sessizce düşmesi testle değil
// ancak yük testiyle fark edilirdi.
func TestExpectedIndexesExist(t *testing.T) {
	pool := sharedPool(t)

	for _, index := range []string{
		"notifications_claim_idx",
		"notifications_lease_expiry_idx",
		"notifications_batch_idx",
		"notifications_cursor_idx",
		"idempotency_keys_created_at_idx",
	} {
		var exists bool
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1)`,
			index,
		).Scan(&exists))

		require.Truef(t, exists, "%s index'i bulunamadı", index)
	}
}

// insertRawNotification, şema testleri için doğrudan SQL ile satır ekler.
func insertRawNotification(t *testing.T, pool *pgxpool.Pool, channel, priority string) uuid.UUID {
	t.Helper()

	id := uuid.New()

	_, err := pool.Exec(context.Background(), `
		INSERT INTO notifications (id, channel, recipient, content, priority, correlation_id)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		id, channel, "+905551234567", "deneme", priority, uuid.NewString())
	require.NoError(t, err)

	return id
}

// TestPriorityRankOrdering, planın gizli sıralama hatasına karşı regresyon testi.
//
// priority metin olarak sıralanırsa alfabetik sıra 'high' < 'low' < 'normal' olur ve
// düşük öncelik normali geçer. priority_rank bunu düzeltiyor; test de bunu sabitliyor.
func TestPriorityRankOrdering(t *testing.T) {
	pool := sharedPool(t)

	// Ekleme sırası bilerek karışık: sıralamayı rank sağlamalı, insert sırası değil.
	for _, priority := range []string{"low", "high", "normal"} {
		insertRawNotification(t, pool, "sms", priority)
	}

	rows, err := pool.Query(context.Background(),
		`SELECT priority, priority_rank FROM notifications ORDER BY priority_rank`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		priority string
		rank     int16
	}

	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.priority, &r.rank))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())

	require.Equal(t, []row{
		{priority: "high", rank: 1},
		{priority: "normal", rank: 2},
		{priority: "low", rank: 3},
	}, got)
}

// TestConstraintsRejectInvalidRows, CHECK kısıtlarının gerçekten koruduğunu doğrular.
func TestConstraintsRejectInvalidRows(t *testing.T) {
	pool := sharedPool(t)

	ctx := context.Background()
	correlationID := uuid.NewString()

	tests := []struct {
		name    string
		channel string
		status  string
	}{
		{name: "geçersiz kanal", channel: "fax", status: "pending"},
		{name: "geçersiz durum", channel: "sms", status: "unknown"},
		// processing durumu lease'siz olamaz: reaper bu değişmeze güveniyor.
		{name: "lease'siz processing", channel: "sms", status: "processing"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, `
				INSERT INTO notifications (id, channel, recipient, content, status, correlation_id)
				VALUES ($1, $2, $3, $4, $5, $6)`,
				uuid.New(), test.channel, "+905551234567", "deneme", test.status, correlationID)

			require.Error(t, err)
		})
	}

	// Lease bir çifttir: yalnızca token yazmak reddedilmeli.
	_, err := pool.Exec(ctx, `
		INSERT INTO notifications (id, channel, recipient, content, correlation_id, lease_token)
		VALUES ($1, 'sms', '+905551234567', 'deneme', $2, $3)`,
		uuid.New(), correlationID, uuid.New())
	require.Error(t, err)
}

// TestUpdatedAtTrigger, trigger'ın her UPDATE'te updated_at'i ilerlettiğini doğrular.
func TestUpdatedAtTrigger(t *testing.T) {
	pool := sharedPool(t)

	ctx := context.Background()
	id := insertRawNotification(t, pool, "email", "normal")

	var before time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT updated_at FROM notifications WHERE id = $1`, id).Scan(&before))

	// now() transaction başlangıç zamanıdır; ayrı bir transaction'da güncellemek için
	// kısa bir bekleme yeterli ve ölçülebilir bir fark üretir.
	time.Sleep(10 * time.Millisecond)

	// UPDATE sorgusunda updated_at bilerek yazılmıyor: trigger'ın işini test ediyoruz.
	_, err := pool.Exec(ctx,
		`UPDATE notifications SET status = 'failed', last_error = 'deneme' WHERE id = $1`, id)
	require.NoError(t, err)

	var after time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT updated_at FROM notifications WHERE id = $1`, id).Scan(&after))

	require.True(t, after.After(before), "updated_at ilerlememiş: %s → %s", before, after)
}

// TestAttemptsCascadeAndUniqueness, deneme kayıtlarının bütünlüğünü doğrular.
func TestAttemptsCascadeAndUniqueness(t *testing.T) {
	pool := sharedPool(t)

	ctx := context.Background()
	id := insertRawNotification(t, pool, "push", "high")

	insertAttempt := func(attemptNo int) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO notification_attempts (notification_id, attempt_no, status_code, duration_ms)
			VALUES ($1, $2, 202, 42)`, id, attemptNo)

		return err
	}

	require.NoError(t, insertAttempt(1))
	// Aynı deneme numarası ikinci kez yazılamaz (at-least-once teslimatın yan etkisi).
	require.Error(t, insertAttempt(1))
	require.NoError(t, insertAttempt(2))

	// ON DELETE CASCADE: bildirim silinince deneme geçmişi de gitmeli, yetim satır kalmamalı.
	_, err := pool.Exec(ctx, `DELETE FROM notifications WHERE id = $1`, id)
	require.NoError(t, err)

	var remaining int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM notification_attempts WHERE notification_id = $1`, id).Scan(&remaining))
	require.Zero(t, remaining)
}

// TestIdempotencyKeyConflict, bileşik PK'nın atomik "daha önce gördüm mü?" kontrolünü
// sağladığını doğrular.
func TestIdempotencyKeyConflict(t *testing.T) {
	pool := sharedPool(t)

	ctx := context.Background()
	insert := `
		INSERT INTO idempotency_keys (scope, key, request_hash, response_status, response_body)
		VALUES ($1, $2, $3, 202, '{"id":"x"}'::jsonb)
		ON CONFLICT (scope, key) DO NOTHING`

	hash := make([]byte, 32)

	tag, err := pool.Exec(ctx, insert, "notifications", "key-1", hash)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected(), "ilk yazım satır eklemeliydi")

	// Aynı scope + key: çakışma sessizce yutulur, etkilenen satır 0 → "daha önce gördüm".
	tag, err = pool.Exec(ctx, insert, "notifications", "key-1", hash)
	require.NoError(t, err)
	require.EqualValues(t, 0, tag.RowsAffected(), "çakışmada satır eklenmemeliydi")

	// Farklı scope aynı key: ayrı uçlar çarpışmamalı.
	tag, err = pool.Exec(ctx, insert, "notifications_batch", "key-1", hash)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected(), "farklı scope ayrı kayıt olmalıydı")
}
