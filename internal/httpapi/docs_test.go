package httpapi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/XENWRLD/insider-notification-system/api/openapi"
)

// openAPIDocument, spec'in bu testlerin ilgilendiği kadarı.
//
// Tam bir OpenAPI modeli kurulmuyor: amaç dokümanı DOĞRULAMAK değil (onu Swagger UI
// ve harici araçlar yapıyor), kod ile dokümanın SAPMADIĞINI görmek.
type openAPIDocument struct {
	Paths      map[string]map[string]any `yaml:"paths"`
	Components struct {
		Schemas map[string]struct {
			Properties map[string]any `yaml:"properties"`
		} `yaml:"schemas"`
	} `yaml:"components"`
}

// loadSpec, gömülü dokümanı çözer.
//
// Diskten değil openapi.Spec üzerinden okunuyor: testin doğruladığı şey, binary'nin
// gerçekten sunduğu baytlar olmalı.
func loadSpec(t *testing.T) openAPIDocument {
	t.Helper()

	var document openAPIDocument
	require.NoError(t, yaml.Unmarshal(openapi.Spec, &document))

	return document
}

// newDocsRouter, dokümantasyon uçları kayıtlı bir router kurar.
func newDocsRouter() http.Handler {
	return NewRouter(Dependencies{
		Logger:      discardLogger(),
		OpenAPISpec: openapi.Spec,
	})
}

func TestSpecEndpointServesEmbeddedSpec(t *testing.T) {
	t.Parallel()

	router := newDocsRouter()

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, specPath, nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "application/yaml; charset=utf-8", recorder.Header().Get("Content-Type"))
	require.Equal(t, string(openapi.Spec), recorder.Body.String())

	etag := recorder.Header().Get("ETag")
	require.NotEmpty(t, etag)

	// İkinci istek aynı etiketle gelirse gövde tekrar gönderilmez.
	conditional := httptest.NewRequestWithContext(t.Context(), http.MethodGet, specPath, nil)
	conditional.Header.Set("If-None-Match", etag)

	cached := httptest.NewRecorder()
	router.ServeHTTP(cached, conditional)

	require.Equal(t, http.StatusNotModified, cached.Code)
	require.Empty(t, cached.Body.String())
}

func TestSpecEndpointAnswersHead(t *testing.T) {
	t.Parallel()

	// compose healthcheck'i ve bazı istemciler HEAD atıyor; chi GET rotasını HEAD'e
	// otomatik eşlemiyor (bkz. router.go'daki /health/live notu).
	recorder := httptest.NewRecorder()
	newDocsRouter().ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodHead, specPath, nil))

	require.Equal(t, http.StatusOK, recorder.Code)
}

func TestDocsEndpointsAbsentWithoutSpec(t *testing.T) {
	t.Parallel()

	// Spec verilmediğinde uçlar hiç kaydedilmiyor: boş bir doküman yayınlamak yerine
	// 404 dönmek dürüst (MetricsCollector ile aynı kalıp).
	router := NewRouter(Dependencies{Logger: discardLogger()})

	for _, path := range []string{specPath, "/docs"} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder,
			httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))

		require.Equal(t, http.StatusNotFound, recorder.Code, path)
	}
}

func TestDocsPageReferencesPinnedAssetsWithIntegrity(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	newDocsRouter().ServeHTTP(recorder,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/docs", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "text/html; charset=utf-8", recorder.Header().Get("Content-Type"))

	page := recorder.Body.String()

	// Sürüm sabit: "@latest" veya sürümsüz bir URL, CDN'in gönderdiği her yeni
	// sürümü doğrudan sayfaya alırdı.
	require.Contains(t, page, "swagger-ui-dist@"+swaggerUIVersion+"/swagger-ui.css")
	require.Contains(t, page, "swagger-ui-dist@"+swaggerUIVersion+"/swagger-ui-bundle.js")
	require.NotContains(t, page, "@latest")

	// İçerik de sabit: hash'siz bir script etiketi, sürüm sabitken bile CDN'e
	// koşulsuz güven demek olurdu.
	require.Contains(t, page, `integrity="`+swaggerUICSSIntegrity+`"`)
	require.Contains(t, page, `integrity="`+swaggerUIJSIntegrity+`"`)
	require.Equal(t, 2, strings.Count(page, `crossorigin="anonymous"`),
		"integrity, crossorigin olmadan sessizce çalışmaz")

	// UI spec'i kendi sunucumuzdan okur; CDN'deki petstore örneğinden değil.
	require.Contains(t, page, strconv.Quote(specPath))
}

func TestSpecDocumentsEveryRoute(t *testing.T) {
	t.Parallel()

	document := loadSpec(t)

	documented := make(map[string]struct{})

	for path, operations := range document.Paths {
		for key := range operations {
			if method, ok := httpMethodFor(key); ok {
				documented[method+" "+path] = struct{}{}
			}
		}
	}

	// Tüm yüzeyi gören router: /metrics yalnızca toplayıcı verildiğinde kaydedilir
	// ve dokümanda yazılı olduğu için burada da kayıtlı olmalı.
	full := NewRouter(Dependencies{
		Logger:           discardLogger(),
		Health:           map[string]Pinger{"postgres": stubPinger{}},
		MetricsCollector: newStubCollector(),
		OpenAPISpec:      openapi.Spec,
	})

	routes, ok := full.(chi.Routes)
	require.True(t, ok)

	registered := make(map[string]struct{})

	require.NoError(t, chi.Walk(routes,
		func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
			// HEAD atlanıyor: dokümanda GET ile aynı şeyi anlatır ve her GET ucunun
			// yanına ikinci bir operasyon yazmak dokümanı iki katına çıkarırdı.
			if method == http.MethodHead {
				return nil
			}

			// chi, kök dışındaki kalıpları "/*" ile de kaydedebiliyor; sondaki slash
			// normalize ediliyor ki "/metrics/" ile "/metrics" aynı sayılsın.
			registered[method+" "+strings.TrimSuffix(route, "/")] = struct{}{}

			return nil
		}))

	for route := range registered {
		require.Contains(t, documented, route,
			"uç kayıtlı ama api/openapi/openapi.yaml'da yok: %s", route)
	}

	for route := range documented {
		require.Contains(t, registered, route,
			"doküman var olmayan bir ucu anlatıyor: %s", route)
	}
}

// TestSpecErrorEnumMatchesErrorCodes, dokümandaki hata kodu listesini kodun tek
// kaynağıyla (errorCodes) eşitler.
func TestSpecErrorEnumMatchesErrorCodes(t *testing.T) {
	t.Parallel()

	document := loadSpec(t)

	envelope, ok := document.Components.Schemas["ErrorEnvelope"]
	require.True(t, ok, "ErrorEnvelope şeması bulunamadı")

	errorProperty, ok := envelope.Properties["error"].(map[string]any)
	require.True(t, ok, "ErrorEnvelope.error nesnesi bulunamadı")

	rawEnum, ok := errorProperty["enum"].([]any)
	require.True(t, ok, "ErrorEnvelope.error.enum bulunamadı")

	documented := make([]string, 0, len(rawEnum))
	for _, value := range rawEnum {
		code, isString := value.(string)
		require.True(t, isString, "enum değerleri string olmalı: %v", value)

		documented = append(documented, code)
	}

	expected := make([]string, 0, len(errorCodes))
	for _, code := range errorCodes {
		expected = append(expected, string(code))
	}

	require.ElementsMatch(t, expected, documented)
}

// TestErrorCodeConstantsAreAllListed, errorCodes dizisinin sabitlerin TAMAMINI
// içerdiğini doğrular.
//
// Olmasa: yeni bir sabit eklenip diziye yazılmadığında doküman testi de
// (TestSpecErrorEnumMatchesErrorCodes) kör kalırdı — dizi ile enum tutarlı görünür,
// kodda ise listelenmemiş bir kod dolaşırdı. Tek bir dosya ayrıştırıldığı için
// maliyeti de ihmal edilebilir.
func TestErrorCodeConstantsAreAllListed(t *testing.T) {
	t.Parallel()

	file, err := parser.ParseFile(token.NewFileSet(), "errors.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)

	declared := make([]string, 0, len(errorCodes))

	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}

		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}

			if typeName, ok := valueSpec.Type.(*ast.Ident); !ok || typeName.Name != "ErrorCode" {
				continue
			}

			for _, value := range valueSpec.Values {
				literal, ok := value.(*ast.BasicLit)
				require.True(t, ok, "ErrorCode sabitleri düz string literal olmalı")

				unquoted, unquoteErr := strconv.Unquote(literal.Value)
				require.NoError(t, unquoteErr)

				declared = append(declared, unquoted)
			}
		}
	}

	require.NotEmpty(t, declared, "errors.go içinde ErrorCode sabiti bulunamadı")

	listed := make([]string, 0, len(errorCodes))
	for _, code := range errorCodes {
		listed = append(listed, string(code))
	}

	require.ElementsMatch(t, declared, listed,
		"errors.go'daki ErrorCode sabitleri ile errorCodes dizisi aynı olmalı")
}

// TestSpecSchemasMatchResponseDTOs, şema alanlarını DTO'ların json etiketleriyle
// eşitler.
//
// Uç listesini kontrol etmek yeni ALANLARI yakalamıyor: bir yanıta alan eklendiğinde
// (ör. zamanlanmış bildirim bonusunun send_at'i) uçlar aynı kalır, doküman sessizce
// eksilir. Örnek yanıtı şemaya doğrulamak da olurdu ama JSON Schema doğrulayıcısı yeni
// bir bağımlılık demekti; etiket kümesi hem bedava hem daha kesin.
func TestSpecSchemasMatchResponseDTOs(t *testing.T) {
	t.Parallel()

	document := loadSpec(t)

	cases := map[string]any{
		"Notification":              notificationResponse{},
		"Attempt":                   attemptResponse{},
		"CreateNotificationRequest": createNotificationRequest{},
		"BatchCreateRequest":        batchRequest{},
		"BatchCreateResponse":       batchResponse{},
		"NotificationList":          listResponse{},
		"BatchSummary":              batchSummaryResponse{},
		"ErrorEnvelope":             errorResponse{},
	}

	for name, dto := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			schema, ok := document.Components.Schemas[name]
			require.True(t, ok, "%s şeması bulunamadı", name)

			documented := make([]string, 0, len(schema.Properties))
			for field := range schema.Properties {
				documented = append(documented, field)
			}

			require.ElementsMatch(t, jsonFieldNames(reflect.TypeOf(dto)), documented)
		})
	}
}

// jsonFieldNames, bir struct'ın serialize edilen alan adlarını döner.
func jsonFieldNames(structType reflect.Type) []string {
	names := make([]string, 0, structType.NumField())

	for i := range structType.NumField() {
		tag := structType.Field(i).Tag.Get("json")
		if tag == "-" {
			continue
		}

		name, _, _ := strings.Cut(tag, ",")
		if name == "" {
			continue
		}

		names = append(names, name)
	}

	return names
}

// httpMethodFor, OpenAPI'deki operasyon anahtarını HTTP metoduna çevirir.
//
// paths altında operasyonların yanında "parameters", "summary" gibi anahtarlar da
// durabiliyor; yalnızca tanıdığımız metotlar sayılıyor.
func httpMethodFor(key string) (string, bool) {
	switch key {
	case "get":
		return http.MethodGet, true
	case "post":
		return http.MethodPost, true
	case "put":
		return http.MethodPut, true
	case "patch":
		return http.MethodPatch, true
	case "delete":
		return http.MethodDelete, true
	case "options":
		return http.MethodOptions, true
	default:
		return "", false
	}
}
