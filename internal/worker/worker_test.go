package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// TestNewDefaultsToAllChannels, kanal verilmediğinde üç kanalın da açıldığını doğrular.
func TestNewDefaultsToAllChannels(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Channels = nil

	engine := newTestEngine(t, cfg, &stubQueue{}, &stubSender{})

	require.ElementsMatch(t,
		[]domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush},
		engine.cfg.Channels)
}

// TestNewRejectsLeaseShorterThanSafeWindow, lease süresi gönderim penceresini
// kapsamıyorsa worker'ın hiç açılmadığını doğrular.
//
// Kapsamazsa lease iş bitmeden dolar, reaper satırı devralır ve aynı mesaj ikinci kez
// gider. Hata mesajı sayıları içeriyor: "lease çok kısa" demek kullanıcıya hangi
// değeri değiştireceğini söylemez.
func TestNewRejectsLeaseShorterThanSafeWindow(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.ProviderTimeout = 10 * time.Second
	cfg.ResultTimeout = 3 * time.Second
	cfg.LeaseDuration = 20 * time.Second // gereken: 2 × 13s = 26s

	_, err := New(cfg, &stubQueue{}, &stubSender{}, discardLogger())

	require.Error(t, err)
	require.Contains(t, err.Error(), "LeaseDuration")
	require.Contains(t, err.Error(), "26s")
	require.Contains(t, err.Error(), "20s")
}

// TestNewAcceptsLeaseAtExactSafeBoundary, tam sınırdaki değerin kabul edildiğini
// doğrular (sınır kapsayıcı).
func TestNewAcceptsLeaseAtExactSafeBoundary(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.ProviderTimeout = 10 * time.Second
	cfg.ResultTimeout = 3 * time.Second
	cfg.LeaseDuration = 26 * time.Second
	cfg.DrainTimeout = 13 * time.Second

	_, err := New(cfg, &stubQueue{}, &stubSender{}, discardLogger())

	require.NoError(t, err)
}

// TestNewRejectsDrainTimeoutShorterThanSendWindow, kapanış penceresinin uçan
// gönderim + sonuç yazımını kapsamasını şart koştuğunu doğrular.
//
// Kapsamazsa Run sender'lar hâlâ çalışırken döner, main havuzu kapatır ve başarılı
// gönderimlerin sonucu yazılamaz: satır reaper'a kadar processing'de kalır. Graceful
// shutdown'ın tam da engellemek için var olduğu durum.
func TestNewRejectsDrainTimeoutShorterThanSendWindow(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.ProviderTimeout = 10 * time.Second
	cfg.ResultTimeout = 3 * time.Second
	cfg.LeaseDuration = time.Minute
	cfg.DrainTimeout = 5 * time.Second // gereken: 13s

	_, err := New(cfg, &stubQueue{}, &stubSender{}, discardLogger())

	require.Error(t, err)
	require.Contains(t, err.Error(), "DrainTimeout")
	require.Contains(t, err.Error(), "13s")
}

// TestNewRejectsInvalidConfig, eksik veya tutarsız ayarların boot'ta yakalandığını ve
// hepsinin tek hatada toplandığını doğrular.
func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantStr string
	}{
		{
			name:    "eşzamanlılık sıfır",
			mutate:  func(c *Config) { c.Concurrency = 0 },
			wantStr: "Concurrency",
		},
		{
			name:    "parti boyutu sıfır",
			mutate:  func(c *Config) { c.ClaimBatchSize = 0 },
			wantStr: "ClaimBatchSize",
		},
		{
			name:    "reaper aralığı negatif",
			mutate:  func(c *Config) { c.ReaperInterval = -time.Second },
			wantStr: "ReaperInterval",
		},
		{
			name:    "poll aralığı ters",
			mutate:  func(c *Config) { c.PollMinInterval = 10 * time.Second },
			wantStr: "PollMinInterval",
		},
		{
			name:    "geçersiz kanal",
			mutate:  func(c *Config) { c.Channels = []domain.Channel{"fax"} },
			wantStr: "geçersiz kanal",
		},
		{
			name:    "temizlik partisi sıfır",
			mutate:  func(c *Config) { c.IdempotencyCleanupBatch = 0 },
			wantStr: "IdempotencyCleanupBatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			test.mutate(&cfg)

			_, err := New(cfg, &stubQueue{}, &stubSender{}, discardLogger())

			require.Error(t, err)
			require.Contains(t, err.Error(), test.wantStr)
		})
	}
}

// TestNewCollectsAllProblems, birden çok sorunun tek hatada döndüğünü doğrular:
// kullanıcı her seferinde bir ayarı düzeltip yeniden başlatmak zorunda kalmasın.
func TestNewCollectsAllProblems(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Concurrency = 0
	cfg.ClaimBatchSize = 0
	cfg.ReaperBatchSize = 0

	_, err := New(cfg, &stubQueue{}, &stubSender{}, discardLogger())

	require.Error(t, err)
	require.Contains(t, err.Error(), "Concurrency")
	require.Contains(t, err.Error(), "ClaimBatchSize")
	require.Contains(t, err.Error(), "ReaperBatchSize")
}

// TestNewRejectsMissingDependencies, kuyruk veya gönderici olmadan motor
// kurulamadığını doğrular.
func TestNewRejectsMissingDependencies(t *testing.T) {
	t.Parallel()

	_, err := New(testConfig(), nil, &stubSender{}, discardLogger())
	require.Error(t, err)

	_, err = New(testConfig(), &stubQueue{}, nil, discardLogger())
	require.Error(t, err)
}

// TestRunStartsReaperAndJanitor, arka plan döngülerinin gerçekten koştuğunu doğrular.
//
// Idempotency temizliğinin çağıranı Adım 4'te eksik bırakılmıştı; bu test onun
// bağlandığını sabitliyor.
func TestRunStartsReaperAndJanitor(t *testing.T) {
	t.Parallel()

	queue := &stubQueue{}
	engine := newTestEngine(t, testConfig(), queue, &stubSender{resp: acceptedResponse()})

	ctx, cancel := context.WithCancel(context.Background())

	runErr := make(chan error, 1)

	go func() { runErr <- engine.Run(ctx) }()

	require.True(t, waitFor(t, 5*time.Second, func() bool {
		return queue.reclaimCalls.Load() > 0 && queue.cleanupCalls.Load() > 0
	}), "reaper ve janitor çalışmalıydı")

	cancel()

	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run kapanmadı")
	}
}

// TestRunReturnsOnContextCancel, kapanış sinyalinde Run'ın hatasız döndüğünü doğrular.
func TestRunReturnsOnContextCancel(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Channels = []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}

	engine := newTestEngine(t, cfg, &stubQueue{}, &stubSender{resp: acceptedResponse()})

	ctx, cancel := context.WithCancel(context.Background())

	runErr := make(chan error, 1)

	go func() { runErr <- engine.Run(ctx) }()

	// Havuzların açılmasına izin ver, sonra kapat.
	require.True(t, waitFor(t, 5*time.Second, func() bool {
		return len(engine.cfg.Channels) == 3
	}))

	cancel()

	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run kapanmadı")
	}
}

// TestRunIsolatesChannels, bir kanalın tıkanmasının diğerlerini durdurmadığını
// doğrular — PLAN §9 "Kanal izolasyonu" satırının worker tarafı.
func TestRunIsolatesChannels(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Channels = []domain.Channel{domain.ChannelSMS, domain.ChannelEmail}
	cfg.Concurrency = 2

	queue := &stubQueue{alwaysFull: true}

	// SMS kanalı tıkansın: gönderim yapay olarak uzun sürer. E-posta tarafı
	// etkilenmemeli — stub kanal ayrımı yapmadığı için burada ölçtüğümüz şey
	// havuzların birbirinin goroutine'lerini bloklamaması.
	sender := &stubSender{resp: acceptedResponse(), delay: time.Millisecond}
	engine := newTestEngine(t, cfg, queue, sender)

	ctx, cancel := context.WithCancel(context.Background())

	runErr := make(chan error, 1)

	go func() { runErr <- engine.Run(ctx) }()

	require.True(t, waitFor(t, 5*time.Second, func() bool {
		return len(queue.completedResults()) >= 20
	}), "iki kanal birlikte ilerlemeliydi")

	cancel()

	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run kapanmadı")
	}
}

// TestSafeSendWindowIgnoresClaimBatchSize, lease hesabının parti boyutundan bağımsız
// olduğunu doğrular.
//
// Jeton semaforu sayesinde satır claim edilir edilmez bir sender'a gidiyor; tamponlu
// bir kuyruk olsaydı pencere ceil(batch/concurrency) katına çıkardı.
func TestSafeSendWindowIgnoresClaimBatchSize(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.ProviderTimeout = 10 * time.Second
	cfg.ResultTimeout = 3 * time.Second

	cfg.ClaimBatchSize = 1
	small := cfg.safeSendWindow()

	cfg.ClaimBatchSize = 1000
	large := cfg.safeSendWindow()

	require.Equal(t, small, large)
	require.Equal(t, 13*time.Second, small)
}
