package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

// Swagger UI varlıkları CDN'den SABİT SÜRÜM + SRI ile çekilir.
//
// Neden vendor edilmedi: swagger-ui-dist'in iki dosyası ~2 MB ve repoya girmesi,
// değerlendiricinin okuyacağı diff'i hiç ilgisi olmayan minified JS ile şişirirdi.
// Neden integrity: sürümü sabitlemek yalnızca ETİKETİ sabitler; içeriğin de
// değişmediğini garanti eden şey hash. Hash eşleşmezse tarayıcı dosyayı hiç
// çalıştırmaz (sayfa boş kalır) — sessizce değiştirilmiş bir bundle çalıştırmaktan iyi.
//
// Sürüm yükseltirken hash'ler şöyle yenilenir (uydurulmaz, hesaplanır):
//
//	curl -sfL https://cdn.jsdelivr.net/npm/swagger-ui-dist@<sürüm>/swagger-ui.css |
//	  openssl dgst -sha384 -binary | openssl base64 -A
const (
	swaggerUIVersion = "5.36.0"

	swaggerUICSSIntegrity = "sha384-z/AtSaOHGn5MJ5auAHXUypohywlIWLBZo1jeGmhdpVIbCnFhxAH92cwdV41oyINc"
	swaggerUIJSIntegrity  = "sha384-fUpRgC19yPykTqzdrUbdAg8NTiu16NEKf9U12J+IJWCg8/1mEI8f2+PEIz60Wyd+"

	// specPath, UI'ın spec'i çektiği yol. Mutlak değil göreli de olabilirdi ama
	// mutlak yol, sayfanın ters proxy altında farklı bir alt yola monte edilmesi
	// hâlinde de tek doğru davranışı verir: aynı host'tan oku.
	specPath = "/openapi.yaml"
)

// docsPageTemplate, Swagger UI'ı ayağa kaldıran tek sayfa.
//
// Kendi elimizle yazıldı; swagger-ui-dist'in hazır index.html'i petstore örneğine
// ve ./swagger-ui.css gibi GÖRELİ yollara bağlı, yani ayrıca düzenlenmesi gerekirdi.
// deepLinking: UI'daki uca verilen bağlantı (örn. #/Notifications/createNotification)
// paylaşılabilir olsun.
const docsPageTemplate = `<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Insider Notification System API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@%[1]s/swagger-ui.css"
      integrity="%[2]s" crossorigin="anonymous">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@%[1]s/swagger-ui-bundle.js"
        integrity="%[3]s" crossorigin="anonymous"></script>
<script>
  window.onload = function () {
    window.ui = SwaggerUIBundle({
      url: %[4]q,
      dom_id: "#swagger-ui",
      deepLinking: true,
      displayRequestDuration: true,
      tryItOutEnabled: true
    });
  };
</script>
</body>
</html>
`

// docsPage, şablonun doldurulmuş hâli. Sayfa isteğe göre değişmediği için bir kez
// üretilir.
var docsPage = fmt.Sprintf(docsPageTemplate,
	swaggerUIVersion, swaggerUICSSIntegrity, swaggerUIJSIntegrity, specPath)

// docsHandler, OpenAPI dokümanını ve Swagger UI sayfasını sunar.
type docsHandler struct {
	spec []byte
	// etag, spec içeriğinden türetilir: dosya gömülü olduğu için gövde process
	// ömrü boyunca sabit ve hash bir kez hesaplanır.
	etag string
}

// newDocsHandler, gömülü spec için handler kurar.
func newDocsHandler(spec []byte) docsHandler {
	sum := sha256.Sum256(spec)

	// İlk 16 bayt yeterli: ETag kriptografik bir iddia değil, "aynı içerik mi?"
	// sorusunun cevabı. Çift tırnak RFC 9110 §8.8.3'ün istediği biçim (güçlü
	// doğrulayıcı); tırnaksız değer bazı ara sunucular tarafından yok sayılıyor.
	return docsHandler{
		spec: spec,
		etag: `"` + hex.EncodeToString(sum[:16]) + `"`,
	}
}

// serveSpec, OpenAPI dokümanını ham olarak yazar.
func (h docsHandler) serveSpec(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("ETag", h.etag)
	// no-cache = "önbellekleyebilirsin ama her kullanımda doğrula". Doküman
	// dağıtımla birlikte değişiyor; uzun bir max-age, yeni sürüm yayınlandıktan
	// sonra eski spec'i gösteren bir UI bırakırdı.
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	if matchesETag(r.Header.Get("If-None-Match"), h.etag) {
		w.WriteHeader(http.StatusNotModified)

		return
	}

	// text/yaml değil application/yaml: RFC 9512 ile kayıtlı olan bu.
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	// HEAD isteğinde net/http gövdeyi kendisi atar; ayrı bir dal gerekmiyor.
	if _, err := w.Write(h.spec); err != nil {
		slog.Error("openapi dokümanı yazılamadı", slog.String("error", err.Error()))
	}
}

// serveUI, Swagger UI sayfasını yazar.
func (h docsHandler) serveUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write([]byte(docsPage)); err != nil {
		slog.Error("swagger ui sayfası yazılamadı", slog.String("error", err.Error()))
	}
}

// matchesETag, If-None-Match başlığının verilen etiketi kapsayıp kapsamadığını söyler.
//
// Başlık virgülle ayrılmış bir liste olabilir ve "*" her şeyle eşleşir (RFC 9110
// §13.1.2). W/ öneki (zayıf doğrulayıcı) kırpılıyor: bizim tek bir temsilimiz var,
// zayıf-güçlü ayrımı bu uçta bir fark üretmiyor.
func matchesETag(header, etag string) bool {
	header = strings.TrimSpace(header)
	if header == "" {
		return false
	}

	if header == "*" {
		return true
	}

	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		candidate = strings.TrimPrefix(candidate, "W/")

		if candidate == etag {
			return true
		}
	}

	return false
}
