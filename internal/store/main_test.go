// Store testlerinin ortak altyapısı.
//
// Her test kendi Postgres container'ını başlatırsa süre test sayısıyla doğrusal büyür
// (Adım 4-8 boyunca store sorguları artacak). Bu yüzden TestMain tek bir container
// başlatır, migration'ları bir kez uygular ve testler bu veritabanını paylaşır; her
// test kendi verisini sonunda temizler.
//
// ÖNEMLİ: paylaşılan veritabanı kullanan testler t.Parallel() ÇAĞIRMAZ — aynı tablolara
// yazıp birbirinin verisini temizlerlerdi.
package store

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// migrationsSource, migrations klasörünün bu test dosyasına göre konumu.
// Klasör yalnızca SQL içersin diye testler burada duruyor (CLAUDE.md: DB kodu internal/store).
const migrationsSource = "file://../../migrations"

// sharedMigratedPool, TestMain tarafından kurulan paylaşılan havuz.
// -short modunda nil kalır; sharedPool bu durumda testi atlar.
var sharedMigratedPool *pgxpool.Pool

func TestMain(m *testing.M) {
	// testing.Short() ancak bayraklar çözümlendikten sonra okunabilir.
	flag.Parse()

	if testing.Short() {
		os.Exit(m.Run())
	}

	code, err := runWithPostgres(m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "store testleri için Postgres kurulamadı: %v\n", err)
		os.Exit(1)
	}

	os.Exit(code)
}

// runWithPostgres, bir container başlatıp migration'ları uygular, testleri koşturur
// ve çıkış kodunu döner. defer'ler çalışsın diye os.Exit burada çağrılmaz.
func runWithPostgres(m *testing.M) (int, error) {
	ctx := context.Background()

	container, dsn, err := newPostgresContainer(ctx)
	if err != nil {
		return 0, err
	}

	defer func() {
		if terminateErr := testcontainers.TerminateContainer(container); terminateErr != nil {
			fmt.Fprintf(os.Stderr, "container kapatılamadı: %v\n", terminateErr)
		}
	}()

	migrator, err := migrate.New(migrationsSource, pgxDSN(dsn))
	if err != nil {
		return 0, fmt.Errorf("migrate kurulamadı: %w", err)
	}

	if err := migrator.Up(); err != nil {
		return 0, fmt.Errorf("migration uygulanamadı: %w", err)
	}

	if sourceErr, dbErr := migrator.Close(); sourceErr != nil || dbErr != nil {
		return 0, fmt.Errorf("migrate kapatılamadı: %w", errJoin(sourceErr, dbErr))
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("havuz kurulamadı: %w", err)
	}
	defer pool.Close()

	sharedMigratedPool = pool

	return m.Run(), nil
}

// errJoin, iki hatadan boş olmayan ilkini döner.
func errJoin(first, second error) error {
	if first != nil {
		return first
	}

	return second
}

// newPostgresContainer, tek seferlik bir Postgres 18 container'ı başlatır.
func newPostgresContainer(ctx context.Context) (*tcpostgres.PostgresContainer, string, error) {
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
	if err != nil {
		return nil, "", fmt.Errorf("container başlatılamadı: %w", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", fmt.Errorf("bağlantı adresi alınamadı: %w", err)
	}

	return container, dsn, nil
}

// pgxDSN, golang-migrate'in pgx/v5 sürücüsünün beklediği şemayı uygular.
//
// Varsayılan "postgres://" şeması golang-migrate'te lib/pq sürücüsüne gider; pgx5://
// ile projede tek bir Postgres sürücüsü kalıyor.
func pgxDSN(dsn string) string {
	return strings.Replace(dsn, "postgres://", "pgx5://", 1)
}

// sharedPool, migration'ları uygulanmış paylaşılan veritabanını döner.
//
// Test bitiminde tablolar temizlenir, böylece testler birbirinin verisini görmez.
// Docker yoksa (-short) test atlanır.
func sharedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	if testing.Short() {
		t.Skip("testcontainers Docker gerektirir; -short ile atlandı")
	}

	require.NotNil(t, sharedMigratedPool, "paylaşılan havuz kurulmamış")

	t.Cleanup(func() {
		_, err := sharedMigratedPool.Exec(context.Background(),
			`TRUNCATE notifications, notification_attempts, idempotency_keys
			 RESTART IDENTITY CASCADE`)
		require.NoError(t, err)
	})

	return sharedMigratedPool
}

// dedicatedPostgres, şemayı değiştiren testler için ayrı bir container başlatır.
//
// Paylaşılan veritabanında migration'ı geri almak diğer testlerin altından tabloları
// çekerdi; bu yüzden up/down testi kendi container'ında koşar.
func dedicatedPostgres(t *testing.T) (*migrate.Migrate, *pgxpool.Pool) {
	t.Helper()

	if testing.Short() {
		t.Skip("testcontainers Docker gerektirir; -short ile atlandı")
	}

	ctx := context.Background()

	container, dsn, err := newPostgresContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(container))
	})

	migrator, err := migrate.New(migrationsSource, pgxDSN(dsn))
	require.NoError(t, err)

	t.Cleanup(func() {
		sourceErr, dbErr := migrator.Close()
		require.NoError(t, sourceErr)
		require.NoError(t, dbErr)
	})

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, pool.Ping(ctx))

	return migrator, pool
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
