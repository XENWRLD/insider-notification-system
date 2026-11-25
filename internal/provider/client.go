// Package provider, dış bildirim sağlayıcısına HTTP isteği atar.
//
// Bu paket yalnızca "ne oldu" bilgisini üretir: 2xx geldi, şu kodla reddedildi, hiç
// yanıt gelmedi. "Yeniden denenmeli mi, kaç saniye sonra" kararı worker'a aittir —
// böylece retry politikası (Adım 8) client'a dokunmadan değişebilir.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// Zaman aşımları açıkça verilir: http.Client'ın varsayılanı sınırsızdır ve
	// takılı tek bir istek lease süresini aşıp işin reaper tarafından yeniden
	// dağıtılmasına yol açar.

	// dialTimeout, TCP bağlantısı kurma süresi. Sağlayıcı erişilemezse hızlı başarısız
	// olmak, yavaş başarısız olmaktan iyidir: iş retrying'e alınıp sıraya döner.
	dialTimeout = 2 * time.Second

	// tlsHandshakeTimeout, HTTPS el sıkışması (webhook.site gerçek sağlayıcı).
	tlsHandshakeTimeout = 5 * time.Second

	// responseHeaderTimeout, istek gönderildikten sonra ilk byte'a kadar beklenen süre.
	// Client.Timeout'tan kısa: gövdeyi hiç göndermeyen bir sağlayıcı daha erken kesilir.
	responseHeaderTimeout = 8 * time.Second

	// idleConnTimeout, kullanılmayan bağlantının havuzda kalma süresi.
	idleConnTimeout = 90 * time.Second

	// expectContinueTimeout, 100-continue beklemesi.
	expectContinueTimeout = time.Second

	// maxResponseBody, okunan yanıt gövdesinin üst sınırı.
	//
	// Beklenen zarf birkaç yüz byte; sağlayıcı bir HTML hata sayfası dönerse onu
	// tamamen bellekte tutmanın anlamı yok ve hata mesajına da sığmaz.
	maxResponseBody = 8 << 10

	// maxDrainBody, bağlantıyı yeniden kullanmak için boşaltılan gövde üst sınırı.
	// Okunan kısmın (maxResponseBody) ötesinde kalan artığı temizlemeye yeter.
	maxDrainBody = 64 << 10

	// errorBodyExcerpt, hata mesajına konan gövde parçasının uzunluğu.
	errorBodyExcerpt = 512
)

// Config, client ayarları.
type Config struct {
	// URL, bildirimlerin POST edileceği adres.
	URL string

	// Timeout, tek bir gönderimin uçtan uca üst sınırı (bağlantı + yazma + okuma).
	// Worker'ın lease hesabı bu değere dayanır.
	Timeout time.Duration

	// MaxIdleConnsPerHost, host başına havuzda tutulan bağlantı sayısı.
	//
	// Go'nun varsayılanı 2: saniyede 100 gönderimde neredeyse her istek yeni bir
	// TCP (ve HTTPS'te TLS) el sıkışmasına girer. Worker bunu eşzamanlı gönderim
	// sayısına eşitler, böylece bağlantılar yeniden kullanılır.
	MaxIdleConnsPerHost int
}

// Client, sağlayıcıya gönderim yapar. Eşzamanlı kullanıma uygundur.
type Client struct {
	url    string
	http   *http.Client
	logger *slog.Logger
}

// New, verilen ayarlarla bir client kurar.
func New(cfg Config, logger *slog.Logger) (*Client, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("sağlayıcı adresi boş olamaz")
	}

	if cfg.Timeout <= 0 {
		return nil, fmt.Errorf("sağlayıcı zaman aşımı pozitif olmalı: %s", cfg.Timeout)
	}

	maxIdle := cfg.MaxIdleConnsPerHost
	if maxIdle <= 0 {
		maxIdle = http.DefaultMaxIdleConnsPerHost
	}

	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: dialTimeout}).DialContext,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
		IdleConnTimeout:       idleConnTimeout,
		ExpectContinueTimeout: expectContinueTimeout,
		MaxIdleConnsPerHost:   maxIdle,
		MaxIdleConns:          maxIdle * 2,
		ForceAttemptHTTP2:     true,
	}

	return &Client{
		url: cfg.URL,
		http: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: transport,

			// YÖNLENDİRME TAKİP EDİLMİYOR.
			//
			// Go'nun varsayılanı 10 hop'a kadar takip etmek ve 301/302/303'te isteği
			// GÖVDESİZ GET'e dönüştürmek. Sonuç: PROVIDER_URL yönlendirme dönerse
			// mesaj hiç iletilmiyor ama hedefteki 2xx "başarı" sayılıp satır sent
			// yazılıyordu — classify bunu göremiyor çünkü Send başarı bildiriyor.
			//
			// İkinci sorun: hop hedefi keyfi (link-local metadata, cluster içi
			// servisler) ve Go özel header'ları (Idempotency-Key, istemci
			// kontrolündeki X-Correlation-ID) oraya da taşıyor; hedefin hata gövdesi
			// de last_error üzerinden kimlik doğrulamasız okuma uçlarında görünüyor.
			//
			// ErrUseLastResponse ile 3xx olduğu gibi dönüyor ve StatusError'a
			// çevriliyor: sınıflandırma onu kanal arızası sayıyor, yani devre açılıyor
			// ve mesajlar ölmek yerine yapılandırma düzeltilene kadar bekliyor.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		logger: logger,
	}, nil
}

// Send, bildirimi sağlayıcıya gönderir.
//
// Hatalar: *StatusError (2xx dışı yanıt), *TransportError (yanıt yok),
// *DecodeError (2xx ama zarf okunamadı). Çağıran errors.As ile ayırır.
func (c *Client) Send(ctx context.Context, req SendRequest) (SendResponse, error) {
	body, err := json.Marshal(wireRequest{
		To:      req.To,
		Channel: req.Channel,
		Content: req.Content,
	})
	if err != nil {
		return SendResponse{}, fmt.Errorf("sağlayıcı isteği kodlanamadı: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return SendResponse{}, fmt.Errorf("sağlayıcı isteği kurulamadı: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	if req.IdempotencyKey != "" {
		httpReq.Header.Set("Idempotency-Key", req.IdempotencyKey)
	}

	if req.CorrelationID != "" {
		httpReq.Header.Set("X-Correlation-ID", req.CorrelationID)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		// Yanıt hiç gelmedi: mesajın ulaşıp ulaşmadığı bilinemez.
		return SendResponse{}, &TransportError{Err: err}
	}

	defer func() {
		// Gövde sonuna kadar okunup kapatılmazsa bağlantı havuza geri dönmez ve
		// keep-alive kazanımı kaybolur. Boşaltma sınırlı: sağlayıcı megabaytlık bir
		// hata sayfası dönerse onu okumak için beklemek, bağlantıyı yeniden
		// kullanmaktan daha pahalıya gelir — sınır aşılırsa Go bağlantıyı kapatır.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBody))

		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.WarnContext(ctx, "sağlayıcı yanıt gövdesi kapatılamadı",
				slog.String("error", closeErr.Error()))
		}
	}()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return SendResponse{}, &TransportError{Err: fmt.Errorf("yanıt gövdesi okunamadı: %w", err)}
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return SendResponse{}, &StatusError{
			StatusCode: resp.StatusCode,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
			Body:       excerpt(raw),
		}
	}

	return c.decodeSuccess(ctx, resp.StatusCode, raw)
}

// decodeSuccess, 2xx yanıtı çözer.
//
// Başarı ölçütü 202 değil "herhangi bir 2xx": webhook.site 200 döndürüyor ve katı bir
// 202 kuralı gerçek sağlayıcı demosunun tamamını retrying yapardı (docs/PLAN.md §6
// tablosu mock ve gerçek sağlayıcılar için geçerli kalıyor).
//
// Gövdesi boş 2xx hata sayılmaz: mesaj kabul edildi, yalnızca kimliği öğrenemedik.
// provider_message_id NULL kalır ve durum uyarıyla loglanır.
func (c *Client) decodeSuccess(ctx context.Context, statusCode int, raw []byte) (SendResponse, error) {
	result := SendResponse{StatusCode: statusCode}

	if len(bytes.TrimSpace(raw)) == 0 {
		c.logger.WarnContext(ctx, "sağlayıcı gövdesiz 2xx döndü, mesaj kimliği alınamadı",
			slog.Int("status_code", statusCode))

		return result, nil
	}

	var decoded wireResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return SendResponse{StatusCode: statusCode}, &DecodeError{Err: err, Body: excerpt(raw)}
	}

	result.MessageID = decoded.MessageID
	result.Status = decoded.Status
	result.Timestamp = decoded.Timestamp

	if result.MessageID == "" {
		c.logger.WarnContext(ctx, "sağlayıcı yanıtında messageId yok",
			slog.Int("status_code", statusCode))
	}

	return result, nil
}

// excerpt, gövdenin hata mesajına konacak parçasını verir.
//
// Metin VERİTABANINA yazılabilir olmak zorunda: buradan çıkan parça StatusError /
// DecodeError mesajına, oradan worker'ın classify'ına, oradan notifications.last_error
// ve notification_attempts.error kolonlarına gidiyor. Postgres text kolonu geçersiz
// UTF-8 dizisini ve NUL baytını REDDEDER — tek bozuk sağlayıcı gövdesi bütün sonuç
// yazımını düşürür, satır da lease süresi boyunca processing'de kalıp reaper tarafından
// yeniden gönderilirdi. Bu yüzden iki koruma var:
//
//  1. Geçersiz UTF-8 ve NUL ayıklanır (ToValidUTF8 + NUL temizliği).
//  2. Kesme RUNE sınırında yapılır: bayt dilimi (raw[:512]) çok baytlı bir karakteri
//     ortadan böler ve tam bu geçersiz diziyi üretir. Türkçe veya emoji içeren bir
//     sağlayıcı hata sayfasında 512. bayt neredeyse hiç rune sınırına denk gelmez.
func excerpt(raw []byte) string {
	cleaned := strings.ToValidUTF8(strings.TrimSpace(string(raw)), "")
	cleaned = strings.ReplaceAll(cleaned, "\x00", "")

	if len(cleaned) <= errorBodyExcerpt {
		return cleaned
	}

	// Kesim noktasındaki bayt bir rune'un BAŞI olana kadar geriye gidilir; böylece
	// cleaned[:cut] her zaman tam rune'lardan oluşur. Rune en fazla 4 bayt olduğu için
	// döngü en çok 3 adım atar.
	cut := errorBodyExcerpt
	for cut > 0 && !utf8.RuneStart(cleaned[cut]) {
		cut--
	}

	return cleaned[:cut] + "…"
}
