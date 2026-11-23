# Adım 01 — İskelet: Config, Log, Router, Health, Docker

Commit: `ae32622` · `chore: initialize project structure`

---

## 1. Bu adımda ne yaptık?

Hiç bildirim göndermedik. Bunun yerine **binanın temelini ve tesisatını** döktük:
elektrik (yapılandırma), aydınlatma (loglama), kapı-koridor düzeni (HTTP router),
"bina kullanıma hazır mı?" panosu (health uçları) ve binayı tek düğmeyle kurup
çalıştıran vinç (Docker + compose + Makefile).

Benzetme: bir restoran açıyoruz. Bu adımda tek bir yemek pişirmedik; mutfağa gaz ve su
hattını çektik, ışıkları taktık, "açık / kapalı" tabelasını astık ve mutfağın anahtar
teslim kurulabilmesini sağladık. Yemek tarifleri (bildirim gönderme) Adım 3'ten sonra
geliyor.

Sonuç: `docker compose up` deyince PostgreSQL + Redis + API ayağa kalkıyor ve
`curl localhost:8080/health/ready` bize
`{"checks":{"postgres":"ok","redis":"ok"},"status":"ready"}` diyor. Yani "iskelet ayakta
ve bağımlılıklarıyla konuşabiliyor" kanıtlanmış oluyor.

---

## 2. Değişen dosyalar (okuma sırasıyla: önce temel, sonra kat)

| # | Dosya | Ne değişti | Neden gerekli |
|---|---|---|---|
| 1 | `go.mod` / `go.sum` | Modül yolu `github.com/XENWRLD/insider-notification-system`, Go 1.27 ve 5 doğrudan bağımlılık (chi, uuid, pgx, go-redis, testify) | Modül dosyası projenin kimliği ve bağımlılık kilidi (dependency lock). `go.sum` sürümlerin parmak izini tutar; aynı kod her makinede aynı derlenir |
| 2 | `.env.example` | 26 satır örnek ortam değişkeni (DB, Redis, provider URL, port, log seviyesi) | Gerçek `.env` gitignore'da. Örnek dosya "hangi değişkenler gerekiyor" sorusunun tek cevabı; sırlar (secrets) repoya girmez |
| 3 | `internal/config/config.go` | `Config` struct'ı + `Load()`: ortam değişkenlerini okur, doğrular, **tüm hataları tek seferde** döner | Yarı yapılandırılmış servis trafiğe açılmasın. Hata ilk istekte değil deploy anında görünsün (fail-fast) |
| 4 | `internal/config/config_test.go` | Varsayılanlar, eksik zorunlu alan, geçersiz süre / log seviyesi testleri | Yapılandırma doğrulaması dış altyapı istemez; en ucuz ve en değerli birim test (unit test) burada |
| 5 | `internal/observability/logger.go` | `NewLogger(service, level)`: stdout'a **JSON** yazan `slog.Logger`; `service` alanı sabitlenir | Loglar container stdout'undan toplanıp alan bazlı sorgulanacak (Loki/ELK). Aynı fonksiyon `api` ve `worker` binary'lerinde kullanılır, `service` ile ayrışır |
| 6 | `internal/httpapi/middleware.go` | `correlationIDMiddleware`, `requestLogger`, `CorrelationID(ctx)` / `ContextWithCorrelationID` | Her isteğe izlenebilir bir kimlik (correlation ID). Müşteri "şu istek patladı" dediğinde log'u tek kimlikle buluyoruz |
| 7 | `internal/httpapi/health.go` | `Pinger` arayüzü, `healthHandler.live`, `healthHandler.ready`, `writeJSON` | Orchestrator'ın (compose / Kubernetes) "bu kopyaya istek yönlendirebilir miyim?" sorusuna makine-okunur cevap |
| 8 | `internal/httpapi/router.go` | `Dependencies` struct'ı ve `NewRouter`: chi router + middleware zinciri + `/health/*` + boş `/api/v1` | Bağımlılıklar dışarıdan verilir (dependency injection), global değişken yok → test edilebilirlik |
| 9 | `internal/httpapi/health_test.go` | `stubPinger` ile senaryolar: live her zaman 200, hepsi sağlıklıysa ready 200, biri bozuksa 503, correlation ID header'ı | `Pinger` bir arayüz olduğu için gerçek Postgres/Redis olmadan test edilebiliyor — arayüz kararının somut getirisi |
| 10 | `cmd/api/main.go` | `main` → `run` → `openPostgres` / `openRedis` → `waitForDependency` → `serve`; graceful shutdown ve açık HTTP timeout'ları | Process'in yaşam döngüsü (lifecycle): bağımlılığı bekle, dinle, sinyal gelince düzgün kapan |
| 11 | `Dockerfile` | İki aşamalı (multi-stage) build; `CMD` build arg'ı ile **tek dosyadan üç binary**; non-root `app` kullanıcısı; `CGO_ENABLED=0` statik derleme | Build ayarları üç servis arasında kopyalanmaz. Küçük, statik, root olmayan imaj = daha güvenli ve hızlı |
| 12 | `docker-compose.yml` | `postgres:18-alpine` + `redis:8-alpine` + `api`; healthcheck'li sıralı başlatma, portlar yalnızca `127.0.0.1`'e bağlı | Tek komutla çalışan demo ortamı. `depends_on: service_healthy` sayesinde API, DB hazır olmadan başlamaz |
| 13 | `Makefile` | `help, env, up, up-build, down, logs, ps, build, run, test, test-race, lint, fmt, tidy, clean` | Komutlar ezberlenmez, `make help` der geçilir. Değerlendiren kişi de aynı komutları kullanır |
| 14 | `.golangci.yml` | golangci-lint v2: standart set + `revive, gosec, misspell, bodyclose, errorlint, noctx, sqlclosecheck` | Lint (statik analiz) hataları insan gözünden önce yakalar. `bodyclose` / `noctx` özellikle worker–provider HTTP çağrıları için kritik |
| 15 | `CLAUDE.md` | Proje kuralları + "Durum" bölümü | Yeni bir oturum/geliştirici kodu baştan taramadan devam edebilsin |
| 16 | `docs/PLAN.md` | Adım 1 kutucuğu işaretlendi (satır 195) | Yol haritası tek doğruluk kaynağı (single source of truth) |
| 17 | 10 adet `.gitkeep` | `internal/{domain,store,worker,provider,ratelimit}`, `cmd/{worker,mockprovider}`, `migrations`, `api`, `deploy`, `loadtest` | Git boş klasör tutmaz. Hedef mimari klasör yapısından okunabilir olsun |

---

## 3. Akış — bir istek bu kodun içinden nasıl geçiyor?

### A) Process başlangıcı (`cmd/api/main.go`)

```
main()
 └─ run()
     ├─ config.Load()                          # eksik env varsa burada ölür, port dinlemez
     ├─ observability.NewLogger("api", cfg.LogLevel)
     ├─ signal.NotifyContext(SIGINT, SIGTERM)  # kapanış sinyali bu ctx'i iptal eder
     ├─ openPostgres()  ─▶ pgxpool.New()     ─▶ waitForDependency("postgres", pool.Ping)
     ├─ openRedis()     ─▶ redis.NewClient() ─▶ waitForDependency("redis", pinger.Ping)
     ├─ httpapi.NewRouter(Dependencies{Logger, Health: {postgres, redis}})
     ├─ &http.Server{ ...dört adet açık timeout... }
     └─ serve()  ─▶ goroutine: ListenAndServe()
                 └─ select { server hatası | ctx.Done() ─▶ server.Shutdown(timeout) }
```

### B) Bir HTTP isteği (`GET /health/ready`)

```
istemci
  │ GET /health/ready   (X-Correlation-ID göndermedi)
  ▼
correlationIDMiddleware   → uuid üret, ctx'e koy, yanıt header'ına yaz
  ▼
requestLogger             → saati başlat, wrapped response writer ile sarmala
  ▼
middleware.Recoverer      → panik olursa yanıtı 500'e çevir
  ▼
chi route: /health/ready → healthHandler.ready
  │  ctx'e 2 saniye timeout ekle
  │  her bağımlılık için Ping(ctx):  ok → "ok",  hata → "error: ..." + WARN log
  │  hepsi ok → 200 {"status":"ready"}   biri bozuk → 503 {"status":"unavailable"}
  ▼
writeJSON                 → Content-Type + status kodu + JSON gövde
  ▼
requestLogger (dönüşte)   → tek satır JSON log: correlation_id, method, path, status,
                            bytes, duration_ms, remote_addr
                            (/health* yolları DEBUG seviyesinde → log kirliliği yok)
```

Dikkat: `requestLogger` isteği **tamamlandıktan sonra** loglar, çünkü status kodu ve süre
ancak handler bittiğinde biliniyor.

---

## 4. Önemli kod parçaları

### 4.1 Tüm yapılandırma hatalarını tek seferde toplamak — `internal/config/config.go:49`

```go
for _, required := range []struct {
    name  string
    value string
}{
    {"DATABASE_URL", cfg.DatabaseURL},
    {"REDIS_URL", cfg.RedisURL},
    {"PROVIDER_URL", cfg.ProviderURL},
} {
    if strings.TrimSpace(required.value) == "" {
        problems = append(problems, required.name+" zorunlu")
    }
}
```

- `problems` bir string listesi; her eksik alan buraya eklenir.
- İlk eksik alanda `return` **edilmiyor** — döngü sonuna kadar devam ediyor.
- `config.go:72`'de liste boş değilse hepsi `; ` ile birleştirilip tek hata olur.
- Fayda: kullanıcı `DATABASE_URL zorunlu; REDIS_URL zorunlu` görür; üç kez
  başlat–hata–düzelt döngüsüne girmez.

### 4.2 `Pinger` arayüzü — test edilebilirliğin anahtarı — `internal/httpapi/health.go:18`

```go
type Pinger interface {
    Ping(ctx context.Context) error
}
```

- `pgxpool.Pool` zaten `Ping(ctx) error` metoduna sahip → arayüzü **kendiliğinden**
  karşılar (Go'da implicit interface satisfaction; `implements` yazmak gerekmez).
- `go-redis`'in `Ping`'i `*StatusCmd` döndürdüğü için uymuyor; `cmd/api/main.go:216`'daki
  `redisPinger` küçük bir adaptör (adapter) olarak araya giriyor.
- Testte `stubPinger{err: errors.New("down")}` koyuyoruz (`health_test.go:17`) → Docker,
  Postgres, Redis olmadan 503 senaryosu test edilebiliyor.

### 4.3 Liveness ile readiness'ın bilinçli farkı — `internal/httpapi/health.go:33`

```go
func (h healthHandler) live(w http.ResponseWriter, _ *http.Request) {
    writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
```

- `live` **hiçbir bağımlılığa dokunmuyor**; sadece "process ayakta" diyor.
- Neden? Liveness başarısız olursa orchestrator container'ı **yeniden başlatır**. Postgres
  geçici düştüyse API'yi restart etmek sorunu çözmez, sadece kesintiyi büyütür.
- `ready` ise bağımlılıkları kontrol eder; başarısızsa 503 → load balancer bu kopyaya
  istek yöneltmeyi bırakır ama container yaşamaya devam eder ve DB dönünce kendiliğinden
  trafiğe geri girer.

### 4.4 Graceful shutdown — `cmd/api/main.go:132`

```go
case <-ctx.Done():
    logger.Info("kapanış sinyali alındı, graceful shutdown başlıyor", ...)

    shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
    defer cancel()

    if err := server.Shutdown(shutdownCtx); err != nil {
        return fmt.Errorf("graceful shutdown tamamlanamadı: %w", err)
    }
```

- `ctx.Done()`: SIGTERM/SIGINT geldi (compose stop ve Kubernetes ikisi de SIGTERM yollar).
- Yeni bir `context.Background()` kullanılıyor — iptal olmuş `ctx`'i verirsek `Shutdown`
  anında iptal olur ve devam eden isteklere hiç süre tanınmaz.
- `server.Shutdown`: yeni bağlantıları reddeder, devam eden isteklerin bitmesini bekler.
- `%w` ile sarma: üst katman `errors.Is` / `errors.As` ile hatayı sınıflandırabilsin.

---

## 5. Yeni kavramlar

| Kavram | Tanım | Projede nerede |
|---|---|---|
| **Fail-fast yapılandırma** | Eksik/yanlış ayarda process hiç başlamaz, ilk istekte patlamaz | `config.Load()`, `main.go:56` |
| **Structured logging** | Serbest metin yerine alanlı (key-value) JSON log; sorgulanabilir | `observability/logger.go`, `requestLogger` |
| **Correlation ID** | Bir isteği uçtan uca izlemeye yarayan tek kimlik; istemci gönderirse korunur, yoksa UUID üretilir | `middleware.go:42`. Adım 3'te DB'ye yazılıp worker loglarına taşınacak |
| **Middleware** | Handler'ı saran, her istekte zincir halinde çalışan katman | `router.go:33-35`; sıra önemli |
| **Context + timeout** | Her dış çağrı iptal edilebilir ve süre sınırlı olmalı | `readinessTimeout = 2s`, `waitForDependency` |
| **Liveness / Readiness** | "Ayakta mı?" (restart kararı) vs "trafik alabilir mi?" (routing kararı) | `/health/live`, `/health/ready` |
| **Dependency injection** | Bağımlılıklar global değil, parametre olarak verilir | `httpapi.Dependencies` |
| **Implicit interface** | Go'da tip, metod imzası uyuyorsa arayüzü otomatik karşılar | `pgxpool.Pool` → `Pinger` |
| **Adapter** | Uymayan bir imzayı istenen arayüze çeviren ince sarmalayıcı | `redisPinger`, `main.go:216` |
| **Graceful shutdown** | Kapanırken devam eden isteklere süre tanımak | `serve()`, compose'da `stop_grace_period: 20s` |
| **Multi-stage Docker build** | Derleme ayrı aşamada; runtime imajına yalnızca binary kopyalanır | `Dockerfile` build → runtime |
| **Build arg ile tek Dockerfile** | `CMD=api\|worker\|mockprovider` ile aynı dosyadan üç imaj | `Dockerfile:19`, `docker-compose.yml:48` |
| **Healthcheck + depends_on** | Container "sağlıklı" olana kadar bağımlı servis başlatılmaz | `docker-compose.yml:21` ve `:57` |
| **Connection pool** | Her sorguda yeni bağlantı açmak yerine havuzdan ödünç almak | `pgxpool.New`, `main.go:152` |
| **Lint / statik analiz** | Kod çalışmadan hata ve kötü kokuları yakalamak | `.golangci.yml`, `make lint` |

---

## 6. Verdiğimiz kararlar ve alternatifler

| Karar | Alternatif | Neden bunu seçtik |
|---|---|---|
| Config'i ortam değişkeninden okumak | YAML/TOML dosyası, Viper | 12-factor app; container ve compose ile doğal uyum. Harici bağımlılık yok, test `t.Setenv` ile kolay |
| Tüm config hatalarını biriktirip tek hata dönmek | İlk hatada dönmek | Kullanıcı tüm eksikleri tek seferde görür |
| `log/slog` (standart kütüphane) | zap, logrus, zerolog | Go 1.21+ ile standart; bağımlılık yok, JSON handler hazır. Performans bu yük için yeterli |
| chi router | `net/http` ServeMux, gin, echo, fiber | `net/http` uyumlu, middleware modeli sade ve idiomatic. gin/echo kendi context tipini getirir |
| chi'nin `RealIP` middleware'ini **kullanmamak** | Kullanmak | `X-Forwarded-For`'a güvenip `r.RemoteAddr`'i eziyor; ters proxy arkasında değilsek istemci IP'si taklit edilebilir (GHSA-3fxj-6jh8-hvhx) |
| chi'nin `Logger`'ı yerine kendi `requestLogger` | `middleware.Logger` | chi'nin logger'ı düz metin yazar; biz alan bazlı JSON istiyoruz (`correlation_id`, `duration_ms`) |
| Health kontrolünü `Pinger` arayüzünden almak | Doğrudan `*pgxpool.Pool` ve `*redis.Client` almak | Altyapı olmadan test edilebilirlik; yeni bağımlılık eklemek tek satır |
| `live` hiçbir şeyi kontrol etmez | Her iki uç da DB'yi kontrol eder | Liveness restart tetikler; DB arızasında restart zarar verir, fayda getirmez |
| Readiness yanıtında bağımlılık bazında durum | Sadece 200/503 | Teşhis için log'a bakmak gerekmez; `{"postgres":"ok","redis":"error: ..."}` tek bakışta yeter |
| Health loglarını DEBUG seviyesine düşürmek | INFO'da bırakmak | Healthcheck saniyeler arayla çağrılır; INFO'da gerçek istekleri log içinde boğar |
| HTTP timeout'larını açıkça vermek | Go varsayılanı | Go'nun varsayılanı **sınırsız**; yavaş istemci bağlantıyı süresiz tutabilir (slowloris) |
| Bağımlılığı sınırlı sayıda beklemek (10 × 2s) | Süresiz beklemek / hiç beklememek | Süresiz beklemek hatalı yapılandırmayı gizler; hiç beklememek geçici ağ gecikmesinde boşa restart üretir |
| `pgx` (native driver) | `database/sql` + `lib/pq` | Postgres'e özel özellikler (`SKIP LOCKED`, `LISTEN/NOTIFY`, batch) ve daha iyi performans. Adım 2+ için gerekli |
| ORM yok, SQL elle yazılır | GORM, sqlc, ent | Case'in değerlendirdiği şey SQL ve eşzamanlılık kararları; ORM bunları gizler |
| Tek Dockerfile + build arg | Her binary için ayrı Dockerfile | Üç yerde tekrar eden güvenlik/build ayarı bakım yükü demek |
| Non-root kullanıcı + `CGO_ENABLED=0` | Root ve dinamik binary | Container ele geçse bile etki alanı daralır; statik binary libc bağımlılığı istemez |
| Alpine runtime | `gcr.io/distroless/static` | `wget` healthcheck'i ve `tzdata` gerekiyordu; distroless'ta shell/araç yok |
| Postgres/Redis portunu `127.0.0.1`'e bağlamak | `0.0.0.0` (varsayılan) | Ağdaki herkese açık bir veritabanı kalmasın |
| Redis'te kalıcılığı kapatmak (`--save "" --appendonly no`) | AOF/RDB açık | Redis yalnızca rate-limit jetonu ve pub/sub için; kaybı tolere edilebilir. Kalıcı veri Postgres'te |
| Paketleri `internal/` altına koymak | Public paketler | Bu bir uygulama, kütüphane değil; `internal/` dışarıdan import'u derleyici düzeyinde engeller |

---

## 7. Kendim nasıl denerim?

### Testler ve lint (altyapı gerekmez)

```bash
make test
# ok  github.com/XENWRLD/insider-notification-system/internal/config    0.0xs
# ok  github.com/XENWRLD/insider-notification-system/internal/httpapi   0.0xs

make lint
# (çıktı yok = temiz)
```

### Yığını kaldır ve health uçlarını kontrol et

```bash
make up-build
make ps          # üç servis de healthy / running olmalı

curl -s localhost:8080/health/live
# {"status":"ok"}

curl -s localhost:8080/health/ready
# {"checks":{"postgres":"ok","redis":"ok"},"status":"ready"}
```

### Correlation ID'nin korunduğunu gör

```bash
curl -s -D - -o /dev/null -H "X-Correlation-ID: deneme-123" localhost:8080/health/live
# X-Correlation-Id: deneme-123      <- gönderdiğimiz kimlik aynen döndü

curl -s -D - -o /dev/null localhost:8080/health/live
# X-Correlation-Id: 9f3c...         <- göndermedik, UUID üretildi
```

### Readiness'ın gerçekten 503 döndüğünü gör

```bash
docker compose stop redis
curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/health/ready   # 503
curl -s localhost:8080/health/ready
# {"checks":{"postgres":"ok","redis":"error: redis ping başarısız: ..."},"status":"unavailable"}

docker compose start redis
curl -s localhost:8080/health/ready   # tekrar ready — container restart gerekmedi
```

### Fail-fast'i gör

```bash
DATABASE_URL= REDIS_URL= PROVIDER_URL= go run ./cmd/api
# api başlatılamadı: geçersiz yapılandırma: DATABASE_URL zorunlu; REDIS_URL zorunlu; PROVIDER_URL zorunlu
```

### Graceful shutdown'ı logda gör

```bash
docker compose logs -f api     # ayrı bir terminalde
docker compose stop api
# {"level":"INFO","msg":"kapanış sinyali alındı, graceful shutdown başlıyor","timeout":"15s",...}
# {"level":"INFO","msg":"http sunucusu düzgün şekilde kapandı",...}
```

---

## 8. Mülakatta gelebilecek sorular

**S1 — Liveness ve readiness arasındaki fark nedir, neden `live` hiçbir şeyi kontrol etmiyor?**

Liveness "process ayakta mı" sorusudur ve başarısız olduğunda orchestrator container'ı
yeniden başlatır. Readiness "bu kopyaya trafik yönlendirebilir miyim" sorusudur ve
başarısız olduğunda load balancer o kopyayı havuzdan çıkarır, container'a dokunmaz.
Bu yüzden `live` bilerek hiçbir bağımlılığa dokunmaz: Postgres geçici düştüğünde API'yi
restart etmek arızayı çözmez, yalnızca kesintiyi büyütür ve restart fırtınası (crash loop)
üretir. `ready` ise 503 dönüp trafiği keser; DB geri gelince kopya kendiliğinden trafiğe
döner.

**S2 — Correlation ID'yi neden context'te taşıyorsunuz; global değişken veya parametre olmaz mıydı?**

Global değişken eşzamanlı isteklerde birbirine karışır, çünkü her istek ayrı goroutine'de
çalışır. Her fonksiyona ekstra parametre geçirmek ise imzaları kirletir ve zincirin
ortasında bir yeri unuttuğunuzda kimlik sessizce kaybolur. `context.Context` Go'da zaten
istek kapsamlı (request-scoped) veri taşımak için var ve DB/HTTP çağrılarına nasılsa
geçiyoruz. Anahtar olarak dışa kapalı bir tip (`correlationIDKey struct{}`) kullanıyoruz;
string anahtar kullansak başka bir paket aynı stringi seçip değeri ezebilirdi. API ve
worker ayrı process olduğu için bu kimlik Adım 3'te bildirim kaydıyla DB'ye yazılacak ve
worker logları aynı kimliği oradan okuyacak — izleme zinciri process sınırını da aşıyor.

**S3 — Config'de neden ilk hatada dönmeyip tüm hataları topluyorsunuz?**

Çünkü yapılandırma hataları genelde toplu gelir: yeni bir ortama deploy ederken üç
değişkeni birden unutursunuz. İlk hatada dönerseniz kullanıcı "düzelt–başlat–yeni hata gör"
döngüsünü üç kez yaşar ve her tur bir deploy demektir. Hepsini `problems` listesinde
biriktirip tek hatada birleştirmek bu turu bire indirir. Aynı mantıkla `Load()` process
başlangıcında bir kez çalışır ve hata varsa process hiç port dinlemez — yarı
yapılandırılmış bir servis trafik almaz, hata ilk müşteri isteğinde değil deploy anında
görünür.

**S4 — `Pinger` arayüzü yerine doğrudan `*pgxpool.Pool` alsaydınız ne kaybederdiniz?**

Üç şey. Birincisi test edilebilirlik: health handler'ını test etmek için gerçek bir
Postgres ayağa kaldırmak gerekirdi; şimdi `stubPinger` ile hem sağlıklı hem bozuk senaryoyu
milisaniyede test ediyoruz. İkincisi genişletilebilirlik: yeni bir bağımlılık (örneğin
provider) eklemek `Health` map'ine tek satır; handler'a hiç dokunmuyoruz. Üçüncüsü
bağımlılık yönü: `httpapi` paketi `pgx` veya `go-redis` import etmiyor, yani HTTP katmanı
altyapı seçimlerinden habersiz. `pgxpool.Pool` zaten `Ping(ctx) error` imzasına sahip
olduğu için arayüzü kendiliğinden karşılıyor; go-redis uymadığı için `main.go`'da küçük
bir adaptör yazdık — adaptörün doğru yeri de orası, çünkü altyapı seçimini `main` bilir.

**S5 — HTTP sunucusunda timeout'ları neden elle verdiniz?**

Çünkü `http.Server`'ın varsayılanları sınırsızdır. `ReadHeaderTimeout` olmadan bir istemci
bağlantıyı açıp header'ı çok yavaş göndererek goroutine ve dosya tanıtıcısı (file
descriptor) tutabilir — klasik slowloris saldırısı. Dört timeout'u ayrı ayrı veriyoruz:
`ReadHeaderTimeout` 5s (header hızlı gelmeli), `ReadTimeout` 15s (gövde okuma),
`WriteTimeout` 30s (yanıt yazma), `IdleTimeout` 60s (keep-alive bağlantısının boşta kalma
süresi). Aynı prensip `http.Client` için de geçerli; onu provider çağrılarında Adım 6'da
uygulayacağız ve `noctx` lint kuralı context'siz çağrıyı şimdiden engelliyor.

**S6 — Middleware sırası neden önemli; `router.go`'daki sırayı nasıl seçtiniz?**

Middleware'ler soğan kabuğu gibi; dıştaki her isteği önce görür, en son bırakır. Sıramız
`correlationIDMiddleware` → `requestLogger` → `Recoverer`. Correlation ID en dışta, çünkü
logger'ın loglayacağı alanı onun hazırlaması gerekiyor; tersi olsa loglarda kimlik boş
çıkardı. `Recoverer` logger'ın **içinde**, çünkü handler panikleyip Recoverer bunu 500'e
çevirdiğinde isteğin yine loglanmasını istiyoruz — Recoverer en dışta olsa panikleyen
istek log'a hiç düşmezdi.

**S7 — Neden iş kuyruğu için RabbitMQ/Kafka yerine PostgreSQL seçtiniz?**

Bu adımda kuyruk henüz yok ama karar `docs/PLAN.md`'de verildi. Bildirim kaydı ile kuyruğa
eklenmesi aynı transaction'da olabiliyor; ayrı bir broker kullansaydık "DB'ye yazdım ama
broker'a yollayamadım" (veya tersi) durumu oluşur ve bu dual-write problemi için outbox
pattern gibi ek karmaşıklık gerekirdi. `SELECT ... FOR UPDATE SKIP LOCKED` Postgres'te
çoklu worker'ın aynı işi iki kez çekmesini engelliyor. Bu ölçekte tek bir kalıcı sistem
hem veriyi hem kuyruğu taşıyabilir; Kafka'nın getirisi (çok yüksek throughput, replay,
çoklu tüketici grubu) bu case'in gereksinimini aşıyor ve işletme maliyeti getiriyor.
