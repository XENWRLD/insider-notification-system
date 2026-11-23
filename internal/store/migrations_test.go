// Migration testleri gerçek bir PostgreSQL 18 container'ında koşar.
//
// Neden sahte (mock) değil? Test edilen şey SQL'in kendisi: generated kolon, CHECK
// kısıtları, trigger ve index'ler. Bunların davranışı yalnızca gerçek Postgres'te
// doğrulanabilir. Testler `go test -short` ile atlanır, böylece Docker'sız makinede
// (ör. hızlı lint döngüsünde) test paketi yine derlenir.
package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// migrationsSource, migrations klasörünün bu test dosyasına göre konumu.
// Klasör yalnızca SQL içersin diye test burada duruyor (CLAUDE.md: DB kodu internal/store).
const migrationsSource = "file://../../migrations"

// startPostgres, tek seferlik bir Postgres container'ı başlatır ve DSN'ini döner.
func startPostgres(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("testcontainers Docker gerektirir; -short ile atlandı")
	}

	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("notifications_test"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		// Postgres ilk açılışta bir kez başlayıp kapanır; "ready" satırını iki kez
		// beklemek, henüz hazır olmayan sunucuya bağlanmayı önler.
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(container))
	})

	// pgx5:// şeması bilinçli: golang-migrate'in varsayılan postgres sürücüsü lib/pq
	// çekerdi, böylece projede tek bir Postgres sürücüsü (pgx) kalıyor.
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	return dsn
}

// newMigrator, verilen DSN için migrate örneği kurar.
func newMigrator(t *testing.T, dsn string) *migrate.Migrate {
	t.Helper()

	m, err := migrate.New(migrationsSource, strings.Replace(dsn, "postgres://", "pgx5://", 1))
	require.NoError(t, err)

	t.Cleanup(func() {
		sourceErr, dbErr := m.Close()
		require.NoError(t, sourceErr)
		require.NoError(t, dbErr)
	})

	return m
}

// connect, şema doğrulaması için bir bağlantı havuzu açar.
func connect(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, pool.Ping(ctx))

	return pool
}

// tableNames, schema_migrations dışındaki tabloları alfabetik döner.
func tableNames(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()

	rows, err := pool.Query(context.Background(), `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_type = 'BASE TABLE'
		  AND table_name <> 'schema_migrations'
		ORDER BY table_name`)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	return names
}

// TestMigrationsUpDownUp, down dosyalarının gerçekten çalıştığını kanıtlar.
//
// Bu test olmadan down migration'lar yalnızca "yazılmış" olur; bozuk bir down,
// ancak production'da geri alma gerektiğinde fark edilirdi.
func TestMigrationsUpDownUp(t *testing.T) {
	dsn := startPostgres(t)
	m := newMigrator(t, dsn)
	pool := connect(t, dsn)

	expected := []string{"idempotency_keys", "notification_attempts", "notifications"}

	require.NoError(t, m.Up())
	require.Equal(t, expected, tableNames(t, pool))

	// Tüm migration'lar geri alınır: şemada schema_migrations dışında tablo kalmamalı.
	require.NoError(t, m.Down())
	require.Empty(t, tableNames(t, pool))

	// Trigger'ın fonksiyonu da düşmeli; aksi halde ikinci Up "already exists" ile patlar.
	require.NoError(t, m.Up())
	require.Equal(t, expected, tableNames(t, pool))
}

// TestExpectedIndexesExist, sıcak yol index'lerinin varlığını sabitler.
//
// Index'ler performans sözleşmesinin parçası; birinin sessizce düşmesi testle değil
// ancak yük testiyle fark edilirdi.
func TestExpectedIndexesExist(t *testing.T) {
	dsn := startPostgres(t)
	m := newMigrator(t, dsn)
	pool := connect(t, dsn)

	require.NoError(t, m.Up())

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

// insertNotification, testler için geçerli bir bildirim satırı ekler.
func insertNotification(t *testing.T, pool *pgxpool.Pool, channel, priority string) uuid.UUID {
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
	dsn := startPostgres(t)
	m := newMigrator(t, dsn)
	pool := connect(t, dsn)

	require.NoError(t, m.Up())

	// Ekleme sırası bilerek karışık: sıralamayı index/rank sağlamalı, insert sırası değil.
	for _, priority := range []string{"low", "high", "normal"} {
		insertNotification(t, pool, "sms", priority)
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
	dsn := startPostgres(t)
	m := newMigrator(t, dsn)
	pool := connect(t, dsn)

	require.NoError(t, m.Up())

	ctx := context.Background()
	correlationID := uuid.NewString()

	tests := []struct {
		name    string
		channel string
		status  string
		extra   string
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
	dsn := startPostgres(t)
	m := newMigrator(t, dsn)
	pool := connect(t, dsn)

	require.NoError(t, m.Up())

	ctx := context.Background()
	id := insertNotification(t, pool, "email", "normal")

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
	dsn := startPostgres(t)
	m := newMigrator(t, dsn)
	pool := connect(t, dsn)

	require.NoError(t, m.Up())

	ctx := context.Background()
	id := insertNotification(t, pool, "push", "high")

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
	dsn := startPostgres(t)
	m := newMigrator(t, dsn)
	pool := connect(t, dsn)

	require.NoError(t, m.Up())

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
