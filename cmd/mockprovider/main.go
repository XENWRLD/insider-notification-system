// Command mockprovider, ayarlanabilir hata oranı ve gecikmesi olan sahte bir
// bildirim sağlayıcısıdır.
//
// Neden var: webhook.site ücretsiz URL'i 100 istek / 7 gün ile sınırlı, yani tek bir
// yük testi kotayı bitirir. Retry, rate limit ve kanal izolasyonu senaryolarını
// ölçmek için davranışı çalışma anında değiştirilebilen yerel bir sağlayıcı gerekiyor.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/config"
	"github.com/XENWRLD/insider-notification-system/internal/observability"
)

const (
	// serviceName, logların kaynağını ayırt etmek için kullanılır.
	serviceName = "mockprovider"

	// HTTP sunucu zaman aşımları açıkça verilir (cmd/api ile aynı gerekçe).
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 60 * time.Second

	// maxConfigBody, /config gövdesinin üst sınırı. Ayar gövdesi birkaç yüz byte.
	maxConfigBody = 4 << 10

	// shutdownTimeout, kapanışta süren isteklere tanınan süre. Mock'un en uzun işi
	// ayarlanabilir gecikme kadar; 10 saniye rahat bir üst sınır.
	shutdownTimeout = 10 * time.Second

	// rateLimitRetryAfterSeconds, 429 yanıtında bildirilen bekleme süresi.
	// Worker'ın Retry-After'a uyduğunu demoda göstermek için sabit ve kısa.
	rateLimitRetryAfterSeconds = 1
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "mockprovider başlatılamadı: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadMockProvider()
	if err != nil {
		return err
	}

	logger := observability.NewLogger(serviceName, cfg.LogLevel)

	provider := newMockProvider(cfg, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /send", provider.send)
	mux.HandleFunc("GET /health/live", provider.live)
	mux.HandleFunc("GET /stats", provider.stats)
	mux.HandleFunc("POST /config", provider.updateConfig)

	server := &http.Server{
		Addr:              net.JoinHostPort("", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	// SIGINT/SIGTERM geldiğinde bu context iptal olur (cmd/api ile aynı kalıp).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErrors := make(chan error, 1)

	go func() {
		logger.Info("sahte sağlayıcı dinlemeye başladı",
			slog.String("addr", server.Addr),
			slog.Float64("failure_rate", cfg.FailureRate),
			slog.Float64("rate_limit_rate", cfg.RateLimitRate),
			slog.Float64("bad_request_rate", cfg.BadRequestRate),
			slog.String("latency", cfg.Latency.String()),
		)

		if listenErr := server.ListenAndServe(); listenErr != nil &&
			!errors.Is(listenErr, http.ErrServerClosed) {
			serverErrors <- listenErr

			return
		}

		serverErrors <- nil
	}()

	select {
	case err := <-serverErrors:
		if err != nil {
			return fmt.Errorf("sahte sağlayıcı durdu: %w", err)
		}

		return nil

	case <-ctx.Done():
		logger.Info("kapanış sinyali alındı")

		// Taze context: iptal edilmiş olanı kullanmak shutdown'ı anında keserdi.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("sahte sağlayıcı düzgün kapanamadı: %w", err)
		}

		return nil
	}
}

// behaviour, sağlayıcının çalışma anında değiştirilebilen davranışı.
type behaviour struct {
	FailureRate    float64
	RateLimitRate  float64
	BadRequestRate float64
	Latency        time.Duration
	LatencyJitter  time.Duration
}

// MarshalJSON, davranışı /config'in KABUL ETTİĞİ biçimde yazar.
//
// Varsayılan serileştirme Go alan adlarını (FailureRate) ve süreleri nanosaniye
// tamsayısı olarak verirdi; yani /stats çıktısı doğrudan /config'e geri verilemezdi.
// Aynı sözleşmeyi iki yönde kullanmak demoyu basitleştiriyor.
func (b behaviour) MarshalJSON() ([]byte, error) {
	payload, err := json.Marshal(struct {
		FailureRate    float64 `json:"failure_rate"`
		RateLimitRate  float64 `json:"rate_limit_rate"`
		BadRequestRate float64 `json:"bad_request_rate"`
		Latency        string  `json:"latency"`
		LatencyJitter  string  `json:"latency_jitter"`
	}{
		FailureRate:    b.FailureRate,
		RateLimitRate:  b.RateLimitRate,
		BadRequestRate: b.BadRequestRate,
		Latency:        b.Latency.String(),
		LatencyJitter:  b.LatencyJitter.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("davranış kodlanamadı: %w", err)
	}

	return payload, nil
}

// mockProvider, istekleri karşılar ve sayaç tutar.
//
// Durum RWMutex arkasında: /config çalışma anında davranışı değiştirebiliyor ve
// yük testi sırasında onlarca eşzamanlı gönderim aynı değerleri okuyor.
type mockProvider struct {
	logger *slog.Logger

	mu       sync.RWMutex
	current  behaviour
	byStatus map[int]int
	byChan   map[string]int
}

func newMockProvider(cfg config.MockProviderConfig, logger *slog.Logger) *mockProvider {
	return &mockProvider{
		logger: logger,
		current: behaviour{
			FailureRate:    cfg.FailureRate,
			RateLimitRate:  cfg.RateLimitRate,
			BadRequestRate: cfg.BadRequestRate,
			Latency:        cfg.Latency,
			LatencyJitter:  cfg.LatencyJitter,
		},
		byStatus: make(map[int]int),
		byChan:   make(map[string]int),
	}
}

// sendRequest, worker'ın gönderdiği gövde (docs/PLAN.md §5 sözleşmesi).
type sendRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// send, bir gönderim isteğini karşılar.
func (p *mockProvider) send(w http.ResponseWriter, r *http.Request) {
	var req sendRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxConfigBody)).Decode(&req); err != nil {
		p.record(http.StatusBadRequest, "")
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "gövde çözülemedi"})

		return
	}

	current := p.behaviourSnapshot()

	// Gecikme sağlayıcının gerçek yanıt süresini taklit eder; worker'ın eşzamanlılık
	// hesabı bu bekleme üzerinden doğrulanır.
	if delay := current.delay(); delay > 0 {
		time.Sleep(delay)
	}

	// Zar: #nosec G404 -- benzetim amaçlı rastgelelik, güvenlik kararı değil.
	roll := rand.Float64() //nolint:gosec // benzetim; kriptografik rastgelelik gereksiz

	status := http.StatusAccepted

	switch {
	case roll < current.BadRequestRate:
		status = http.StatusBadRequest
	case roll < current.BadRequestRate+current.RateLimitRate:
		status = http.StatusTooManyRequests
	case roll < current.BadRequestRate+current.RateLimitRate+current.FailureRate:
		status = http.StatusInternalServerError
	}

	p.record(status, req.Channel)

	p.logger.DebugContext(r.Context(), "gönderim isteği karşılandı",
		slog.String("correlation_id", r.Header.Get("X-Correlation-ID")),
		slog.String("idempotency_key", r.Header.Get("Idempotency-Key")),
		slog.String("channel", req.Channel),
		slog.Int("status_code", status),
	)

	switch status {
	case http.StatusAccepted:
		writeJSON(w, status, map[string]string{
			"messageId": uuid.NewString(),
			"status":    "accepted",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})

	case http.StatusTooManyRequests:
		w.Header().Set("Retry-After", fmt.Sprint(rateLimitRetryAfterSeconds))
		writeJSON(w, status, map[string]string{"error": "rate limit aşıldı"})

	case http.StatusBadRequest:
		writeJSON(w, status, map[string]string{"error": "alıcı reddedildi"})

	default:
		writeJSON(w, status, map[string]string{"error": "sağlayıcı geçici olarak kullanılamıyor"})
	}
}

// live, compose healthcheck'inin çağırdığı uç.
func (p *mockProvider) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// stats, durum kodu ve kanal bazında sayaçları döner.
//
// "%30 hata oranında retry görülüyor" iddiası böylece göz kararı değil, sayılabilir
// bir sonuç olur.
func (p *mockProvider) stats(w http.ResponseWriter, _ *http.Request) {
	p.mu.RLock()

	byStatus := make(map[string]int, len(p.byStatus))
	for status, count := range p.byStatus {
		byStatus[fmt.Sprint(status)] = count
	}

	byChannel := make(map[string]int, len(p.byChan))
	for channel, count := range p.byChan {
		byChannel[channel] = count
	}

	current := p.current

	p.mu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"by_status":  byStatus,
		"by_channel": byChannel,
		"behaviour":  current,
	})
}

// configPatch, /config gövdesi. Alanlar işaretçi: verilmeyen ayar değişmez.
type configPatch struct {
	FailureRate    *float64 `json:"failure_rate"`
	RateLimitRate  *float64 `json:"rate_limit_rate"`
	BadRequestRate *float64 `json:"bad_request_rate"`
	Latency        *string  `json:"latency"`
	LatencyJitter  *string  `json:"latency_jitter"`
	ResetStats     bool     `json:"reset_stats"`
}

// updateConfig, davranışı çalışma anında değiştirir.
//
// Demo adımı "mock provider hata oranını artır, retry'ları izle" böylece container
// yeniden başlatmadan, tek curl ile yapılabiliyor.
func (p *mockProvider) updateConfig(w http.ResponseWriter, r *http.Request) {
	var patch configPatch
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxConfigBody)).Decode(&patch); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "gövde çözülemedi"})

		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	next := p.current

	for _, rate := range []struct {
		name   string
		value  *float64
		target *float64
	}{
		{"failure_rate", patch.FailureRate, &next.FailureRate},
		{"rate_limit_rate", patch.RateLimitRate, &next.RateLimitRate},
		{"bad_request_rate", patch.BadRequestRate, &next.BadRequestRate},
	} {
		if rate.value == nil {
			continue
		}

		if *rate.value < 0 || *rate.value > 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": rate.name + " 0 ile 1 arasında olmalı",
			})

			return
		}

		*rate.target = *rate.value
	}

	for _, field := range []struct {
		name   string
		value  *string
		target *time.Duration
	}{
		{"latency", patch.Latency, &next.Latency},
		{"latency_jitter", patch.LatencyJitter, &next.LatencyJitter},
	} {
		if field.value == nil {
			continue
		}

		parsed, err := time.ParseDuration(*field.value)
		if err != nil || parsed < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": field.name + " süre olarak okunamadı (örn. 50ms)",
			})

			return
		}

		*field.target = parsed
	}

	// Tüm alanlar doğrulandıktan sonra tek seferde uygulanır: yarı uygulanmış bir
	// ayar seti yük testinin ortasında anlamsız davranış üretirdi.
	p.current = next

	if patch.ResetStats {
		p.byStatus = make(map[int]int)
		p.byChan = make(map[string]int)
	}

	p.logger.InfoContext(r.Context(), "sahte sağlayıcı davranışı güncellendi",
		slog.Float64("failure_rate", next.FailureRate),
		slog.Float64("rate_limit_rate", next.RateLimitRate),
		slog.Float64("bad_request_rate", next.BadRequestRate),
		slog.String("latency", next.Latency.String()),
	)

	writeJSON(w, http.StatusOK, next)
}

// behaviourSnapshot, davranışın o anki kopyasını verir.
//
// Kopya alınıyor ki gecikme beklerken kilit tutulmasın: 500ms'lik bir gecikmede
// kilidi tutmak /config'i ve diğer gönderimleri bloklardı.
func (p *mockProvider) behaviourSnapshot() behaviour {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.current
}

// delay, bu istek için beklenecek süreyi hesaplar.
func (b behaviour) delay() time.Duration {
	if b.LatencyJitter <= 0 {
		return b.Latency
	}

	//nolint:gosec // benzetim; kriptografik rastgelelik gereksiz
	return b.Latency + time.Duration(rand.Int64N(int64(b.LatencyJitter)))
}

// record, sayaçları günceller.
func (p *mockProvider) record(status int, channel string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.byStatus[status]++

	if channel != "" {
		p.byChan[channel]++
	}
}

// writeJSON, JSON yanıt yazar.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		// Yanıt başlığı gönderildi, yapılacak bir şey yok; sessiz kalmamak için loglanır.
		slog.Error("sahte sağlayıcı yanıtı yazılamadı", slog.String("error", err.Error()))
	}
}
