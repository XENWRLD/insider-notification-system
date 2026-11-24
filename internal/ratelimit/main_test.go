// Bu paketin entegrasyon testleri gerçek bir Redis'e ihtiyaç duyar: redis_rate'in
// GCRA mantığı sunucu tarafında bir Lua script'i olarak koşuyor, dolayısıyla taklit
// bir istemciyle doğrulanamaz.
//
// Container TestMain'de bir kez kurulur (internal/store'daki kalıbın aynısı).
// Testler FLUSHDB YAPMAZ; her test kendi anahtar önekini kullanıyor (keyPrefixFor),
// böylece birbirinin kovasını tüketmez ve t.Parallel() ile koşabilirler.
package ratelimit

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// sharedRedis, TestMain tarafından kurulan paylaşılan istemci.
// -short modunda nil kalır; sharedClient bu durumda testi atlar.
var sharedRedis *redis.Client

func TestMain(m *testing.M) {
	// testing.Short() ancak bayraklar çözümlendikten sonra okunabilir.
	flag.Parse()

	if testing.Short() {
		os.Exit(m.Run())
	}

	code, err := runWithRedis(m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ratelimit testleri için Redis kurulamadı: %v\n", err)
		os.Exit(1)
	}

	os.Exit(code)
}

// runWithRedis, bir container başlatır, testleri koşturur ve çıkış kodunu döner.
// defer'ler çalışsın diye os.Exit burada çağrılmaz.
func runWithRedis(m *testing.M) (int, error) {
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:8-alpine")
	if err != nil {
		return 0, fmt.Errorf("container başlatılamadı: %w", err)
	}

	defer func() {
		if terminateErr := testcontainers.TerminateContainer(container); terminateErr != nil {
			fmt.Fprintf(os.Stderr, "container kapatılamadı: %v\n", terminateErr)
		}
	}()

	url, err := container.ConnectionString(ctx)
	if err != nil {
		return 0, fmt.Errorf("bağlantı adresi alınamadı: %w", err)
	}

	options, err := redis.ParseURL(url)
	if err != nil {
		return 0, fmt.Errorf("bağlantı adresi çözümlenemedi: %w", err)
	}

	client := redis.NewClient(options)
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "redis istemcisi kapatılamadı: %v\n", closeErr)
		}
	}()

	sharedRedis = client

	return m.Run(), nil
}

// sharedClient, paylaşılan Redis istemcisini döner. Docker yoksa (-short) test atlanır.
func sharedClient(t *testing.T) *redis.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("testcontainers Docker gerektirir; -short ile atlandı")
	}

	require.NotNil(t, sharedRedis, "paylaşılan Redis istemcisi kurulmamış")

	return sharedRedis
}

// keyPrefixFor, teste özel bir anahtar öneki üretir.
//
// Testler aynı container'ı paylaşıyor; ortak bir önek kullanılsa biri diğerinin
// kovasını tüketir ve sonuçlar koşma sırasına bağlı hâle gelirdi.
func keyPrefixFor(t *testing.T) string {
	t.Helper()

	return "test:" + t.Name()
}

// newLimiter, teste özel kovası olan bir limiter kurar.
func newLimiter(t *testing.T, perSecond int) *RedisLimiter {
	t.Helper()

	limiter, err := New(sharedClient(t), perSecond, keyPrefixFor(t))
	require.NoError(t, err)

	return limiter
}

// drainBucket, kovayı boşaltır ve kaç izin alındığını döner.
//
// Uyku yok: reddedilene kadar istenir. attempts bir güvenlik sınırı, beklenen değil.
func drainBucket(
	t *testing.T, limiter *RedisLimiter, channel domain.Channel, attempts int,
) int {
	t.Helper()

	granted := 0

	for range attempts {
		decision, err := limiter.Allow(context.Background(), channel)
		require.NoError(t, err)

		if !decision.OK {
			return granted
		}

		granted++
	}

	t.Fatalf("kova %d denemede boşalmadı", attempts)

	return granted
}

// rateFor, saniyedeki izin sayısı için emisyon aralığını verir.
func rateFor(perSecond int) time.Duration {
	return time.Second / time.Duration(perSecond)
}
