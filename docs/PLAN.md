# Insider One — Event-Driven Notification System
## Proje Planı (3 gün)

> Kaynak: `docs/assessment/Software Engineer Assessment - Golang.pdf`
> (şirkete ait belge; `.gitignore` ile repo dışında tutulur, yalnızca yerelde bulunur).
> Bu belge mimari kararları, adım adım yol haritasını ve kabul kriterlerini içerir.
> Her adım bitince ilgili kutucuk işaretlenir.

---

## 1. Hedef

Go ile SMS / Email / Push kanallarından bildirim gönderen, yüksek trafiği (flash sale, son dakika haberi)
kaldırabilen, hataları akıllıca yeniden deneyen ve her bildirimin durumunu takip edilebilir kılan bir sistem.

Değerlendirmede asıl bakılacak şey: **her teknik kararın gerekçesi** (kuyruk, rate limit, idempotency,
retry, gözlemlenebilirlik).

---

## 2. Final mimari

```
 Client ──HTTP──▶ API (Go) ──tx──▶ PostgreSQL  (veri + kalıcı iş kuyruğu)
                                        ▲
                                        │ SKIP LOCKED ile iş çekme (kanal başına)
                     ┌──────────────────┴──────────────────┐
                     │  Worker (Go)                        │
                     │   SMS havuzu │ Email havuzu │ Push havuzu
                     │   lease + reaper + retry + circuit breaker
                     └──────┬─────────────────────┬────────┘
                            │ jeton iste          │ HTTP POST
                        Redis (rate limit,     Provider (webhook.site / mock-provider)
                        pub/sub → WebSocket)
```

| Bileşen | Görev |
|---|---|
| **API** (`cmd/api`) | İstek alır, doğrular, idempotency kontrol eder, DB'ye `pending` yazar, `202` döner |
| **PostgreSQL** | Kalıcı veri **ve** iş kuyruğu. Kayıt ile kuyruk aynı transaction'da → "kayıt var ama kuyrukta yok" sorunu yok |
| **Worker** (`cmd/worker`) | Kanal başına ayrı goroutine havuzu; iş çeker, gönderir, sonucu yazar |
| **Redis** | Kanal başına **global** 100 msg/sn jeton kovası (tüm worker kopyaları paylaşır); WebSocket için pub/sub |
| **mock-provider** (`cmd/mockprovider`) | Ayarlanabilir hata oranı/gecikme; yük, retry ve rate limit testleri için |
| **webhook.site** | Gerçek dış entegrasyon demosu (ücretsiz URL: maks. 100 istek, 7 gün geçerli → sadece demo) |

### Temel kararlar ve gerekçeleri

| Karar | Gerekçe |
|---|---|
| Kuyruk = PostgreSQL (`FOR UPDATE SKIP LOCKED`) | Dual-write/outbox problemi yok, daha az altyapı, transactional. Ölçek gerekirse RabbitMQ/Kafka'ya geçiş README'de anlatılır |
| Kanal başına worker havuzu | Head-of-line blocking önlenir: SMS tıkanınca email etkilenmez |
| Redis rate limiter (`go-redis/redis_rate`) | `docker compose up --scale worker=3` olsa bile limit kanal başına 100/sn kalır |
| Lease + lease_token + reaper | Çöken worker'ın işi takılı kalmaz; geç kalan eski worker sonucu ezemez |
| Tek `available_at` alanı | Normal gönderim, retry zamanı ve zamanlanmış bildirim aynı mekanizmayla çalışır |
| Idempotency tablosu (key + request hash + saklı cevap) | Aynı key+payload → eski cevap; aynı key farklı payload → `409` |
| At-least-once teslimat | Exactly-once dış sağlayıcıda garanti edilemez; provider'a notification ID idempotency anahtarı olarak gider, risk README'de belgelenir |
| Cursor pagination | Büyük veride OFFSET yavaşlar |
| Prometheus `/metrics` | Sektör standardı, Grafana ile görselleştirilebilir |
| Correlation ID DB'de saklanır | API ve worker ayrı process; ID worker loglarına DB üzerinden taşınır |

---

## 3. Teknoloji yığını

| Araç | Kullanım |
|---|---|
| Go (güncel kararlı sürüm, 1.2x) | API, worker, mock provider |
| chi | HTTP router + middleware |
| pgx/v5 (pgxpool) | PostgreSQL erişimi (elle yazılmış SQL) |
| golang-migrate | Versiyonlu migration'lar (compose'da ayrı `migrate` servisi) |
| go-redis + redis_rate | Dağıtık rate limit, pub/sub |
| log/slog | JSON structured logging |
| prometheus/client_golang | Metrikler |
| sony/gobreaker | Kanal başına circuit breaker |
| google/uuid | ID üretimi |
| testify, httptest, testcontainers-go | Unit + entegrasyon testleri |
| golangci-lint | Lint |
| Docker + Docker Compose | Tek komutla kurulum |
| OpenAPI 3 + Swagger UI | API dokümantasyonu |
| GitHub Actions | CI (test + lint) |
| k6 | Yük testi |
| Prometheus + Grafana (opsiyonel) | Metrik paneli |
| OpenTelemetry + Jaeger (opsiyonel) | Distributed tracing |

---

## 4. Veri modeli (özet)

**notifications**
`id (uuid)`, `batch_id (uuid, null)`, `channel (sms|email|push)`, `recipient`, `content`,
`priority (high|normal|low)`, `status`, `attempt_count`, `max_attempts`, `available_at`,
`lease_token (uuid, null)`, `lease_expires_at`, `provider_message_id`, `last_error`,
`correlation_id`, `template_id (null)`, `created_at`, `updated_at`

Ek kolon: `priority_rank (smallint, generated)` — `priority` metin olarak index'lenirse sıralama
alfabetik olur (`high` < `low` < `normal`) ve düşük öncelik normali geçerdi.

Index (Adım 2'de kurulan hali):

| Index | Sorgu |
|---|---|
| `(channel, priority_rank, available_at, id) WHERE status IN ('pending','retrying')` | Worker claim — kısmi index, `sent` satırlarıyla büyümez |
| `(lease_expires_at) WHERE status = 'processing'` | Reaper |
| `(batch_id, status) WHERE batch_id IS NOT NULL` | Batch özeti |
| `(created_at DESC, id DESC)` | Cursor pagination |

`status` / `channel` filtre index'leri Adım 5'te gerçek sorgu + `EXPLAIN` ile kararlaştırılır.

**notification_attempts** — her denemenin kaydı: `notification_id`, `attempt_no`, `status_code`,
`error`, `duration_ms`, `created_at`

**idempotency_keys** — `key`, `scope`, `request_hash`, `response_status`, `response_body`, `created_at`

**templates** (bonus) — `id`, `name`, `channel`, `body`, `created_at`

### Durum makinesi

```
pending ──▶ processing ──▶ sent
   │            ├──▶ retrying ──▶ processing (available_at gelince)
   │            └──▶ failed   (kalıcı hata veya max deneme)
   ▼
cancelled   (yalnızca pending / retrying iken)
```

`sent` = sağlayıcı kabul etti (202). Gerçek teslimat onayı değildir.

---

## 5. API

| Method | Path | İşlev |
|---|---|---|
| POST | `/api/v1/notifications` | Tekil oluştur (Idempotency-Key destekli) |
| POST | `/api/v1/notifications/batch` | 1–1000 bildirim, tek transaction; bir öğe hatalıysa tümü reddedilir, hatalı satırlar listelenir |
| GET | `/api/v1/notifications/{id}` | Durum + deneme geçmişi |
| GET | `/api/v1/batches/{id}` | Batch özeti (status bazında sayılar) |
| GET | `/api/v1/notifications` | Filtre: `status`, `channel`, `from`, `to`; `cursor`, `limit` (maks. 100) |
| POST | `/api/v1/notifications/{id}/cancel` | Atomik iptal; başlamışsa `409` |
| GET | `/metrics` | Prometheus metrikleri |
| GET | `/health/live`, `/health/ready` | Liveness / readiness (DB + Redis) |
| GET | `/ws` (bonus) | WebSocket durum güncellemeleri |
| CRUD | `/api/v1/templates` (bonus) | Template yönetimi |

### Validation kuralları (varsayım — README'de gerekçelendirilir)

| Kanal | Recipient | Content |
|---|---|---|
| sms | E.164 (`+905551234567`) | GSM-7 ≤ 160, Unicode ≤ 70 karakter |
| email | Geçerli e-posta adresi | 1–10.000 karakter |
| push | Boş olmayan device token | 1–512 karakter |

### Provider isteği

```
POST {PROVIDER_URL}
{ "to": "+905551234567", "channel": "sms", "content": "Your message" }
→ 202 { "messageId": "uuid", "status": "accepted", "timestamp": "ISO8601" }
```

---

## 6. Retry politikası

| Sonuç | Davranış |
|---|---|
| 202 | `sent` |
| 400 / 422 | Hemen `failed` |
| 429 | Retry, `Retry-After` dikkate alınır |
| 5xx, timeout, bağlantı hatası | Retry (timeout'ta duplicate riski belgelenir) |
| Max deneme (5) aşıldı | `failed` |

Bekleme: `min(2s × 2^(n-1), 5dk)` + full jitter. Worker beklemez; `status=retrying`, `available_at=now+gecikme`.
Circuit breaker: kanal başına; açıkken o kanaldan iş çekilmez.

---

## 7. Klasör yapısı

```
cmd/api  cmd/worker  cmd/mockprovider
internal/
  config/         ayarlar (.env)
  domain/         modeller, durumlar, validation
  httpapi/        handler, middleware, router
  store/          PostgreSQL sorguları
  worker/         havuzlar, claim, retry, reaper
  provider/       HTTP client
  ratelimit/      Redis jeton kovası
  observability/  metrikler, logger
migrations/   api/openapi/ (openapi.yaml + embed.go)   deploy/ (grafana, prometheus)   loadtest/ (k6)
docs/PLAN.md  CLAUDE.md  README.md  Makefile  Dockerfile  docker-compose.yml  .env.example
```

Go konvansiyonu: `_test.go` dosyaları test ettikleri kodun yanında durur.

Spec `api/openapi.yaml` değil `api/openapi/openapi.yaml`: dosya `go:embed` ile binary'e
gömülüyor ve embed kendi klasörünün dışına çıkamadığı için yanında küçük bir Go dosyası
duruyor; alt klasör paket adı (`openapi`) ile import yolunu hizalıyor.

---

## 8. Yol haritası

### 1. Gün — Temel ve API
- [x] **Adım 0 — Ortam:** Go 1.27.4, Git 2.53, Docker 29.9 + Compose v5.7, GNU Make 4.4.1,
      golangci-lint 2.15, gh CLI 2.102, Claude Code; `git init -b main`, `.gitignore`,
      `.gitattributes` (`eol=lf`); GitHub reposu `insider-notification-system` (private)
  - [x] webhook.site URL'i alındı (`docs/webhook unique url.txt`; `.env.example` içinde
        `PROVIDER_URL` olarak kullanılıyor)
- [x] **Adım 1 — İskelet:** klasörler, `CLAUDE.md`, config, slog, compose (postgres + redis), `/health` → `chore: initialize project structure`
- [x] **Adım 2 — Veritabanı:** migration'lar, index'ler, migrate servisi → `feat: add database schema and migrations`
  - [x] `priority` metin sıralaması hatası düzeltildi: `priority_rank` generated kolonu
        (alfabetik sırada `low` < `normal` çıkıyordu)
  - [x] `templates` tablosu bonus adıma bırakıldı; `template_id` FK'si o migration'da eklenecek
- [x] **Adım 3 — Tekil API:** create + validation + correlation ID → `feat: implement notification creation with validation`
  - [x] SMS uzunluğu gerçek GSM 03.38 tablosuyla: küçük `ç` ve `ş ğ ı İ` tabloda **yok**,
        büyük `Ç` (0x09) ve `ö ü Ö Ü` **var** → aynı metnin büyük/küçük hâli farklı sınıra tabi
  - [x] Unicode sınırı rune değil **UTF-16 kod birimi** (emoji vekil çift = 2 birim)
  - [x] API sözleşmesi sabitlendi: hata zarfı + `ErrorCode` enum'u + DTO (iç alanlar gizli)
- [x] **Adım 4 — Batch + idempotency** → `feat: add batch creation and idempotency`
  - [x] `request_hash` = **ham gövde baytlarının** SHA-256'sı; gövde bir kez okunup hem
        hash hem JSON çözme aynı baytlardan yapılır
  - [x] Anahtar **işten önce** talep edilir (claim-first): 1000 satır yazıp çöpe atmak yok
  - [x] Doğruluk TTL temizliğine bağlı değil: süresi dolmuş anahtar aynı transaction'da
        devralınır (`ON CONFLICT ... DO UPDATE ... WHERE süresi dolmuş`)
  - [x] Batch yazımı `pgx.CopyFrom`; zaman kolonları DB varsayılanına bırakıldı
  - [x] Uç başına gövde sınırı (`BODY_LIMIT_SINGLE` / `BODY_LIMIT_BATCH`)
- [x] **Adım 5 — Okuma + iptal:** get, batch özeti, cursor listeleme, atomik iptal → `feat: add query, listing and cancellation endpoints`
  - [x] Cursor opak `base64.RawURLEncoding` + RFC3339Nano; `limit+1` ile `next_cursor`
  - [x] Tarih aralığı **yarı açık** `[from, to)`; tarih-only reddedilir (saat dilimi zorunlu)
  - [x] İptal tek `UPDATE ... WHERE status IN (...)` ile atomik; worker claim'i ile yarış
        testle kanıtlandı (claim kazanırsa iptal `409`, rollback olursa iptal başarılı)
  - [x] `attempts` her zaman dizi ve **listede de gerçek veri** (tek sorguyla, N+1 yok)
  - [x] Listeleme `WHERE`'i Go'da dinamik kuruluyor — `$n IS NULL OR` kalıbı generic
        plan'da cursor koşulunu `Index Cond` yerine `Filter`'a düşürüyordu
  - [x] **Adım 2'den ertelenen index kararı verildi:** `000004` migration'ı ile
        `(status, created_at DESC, id DESC)`. Ölçüm (100k satır, 20'si `failed`):
        `Seq Scan` 13.77 ms → `Index Scan` 0.13 ms. Kanal için ayrı index eklenmedi;
        gerekçe migration dosyasında

**Gün sonu kontrolü:** `docker compose up` çalışıyor; curl ile oluştur / listele / iptal et.

### 2. Gün — İşleme motoru
- [x] **Adım 6 — Worker motoru:** kanal havuzları, SKIP LOCKED claim (20'li), lease + token, reaper, provider client, mock provider, graceful shutdown → `feat: implement worker engine with leases and recovery`
      *(adım dört commit'e bölündü: `refactor: extract bootstrap helpers for api and worker`,
      `feat: add claim, result and reaper queries`, `feat: add provider client and mock provider`,
      `feat: implement worker engine with leases and recovery`)*
  - [x] Idempotency TTL temizliğini reaper'ın yanına ekle:
        `store.DeleteExpiredIdempotencyKeys` Adım 4'te yazıldı ve test edildi,
        yalnızca periyodik çağıran eksikti (`internal/worker/janitor.go`, API değil worker koşturuyor)
  - [x] **Yeni migration yok:** Adım 2'nin `notifications_claim_idx`,
        `notifications_lease_expiry_idx` ve lease CHECK kısıtları olduğu gibi kullanıldı
  - [x] **Claimer boştaki sender kadar iş çeker** (jeton semaforu): tamponlu dağıtım
        kuyruğu 40 satırın lease'ini tutup en kötü pencereyi 52 sn'ye çıkarıyordu
        (lease 60 sn) → çift gönderim riski. Jetonla sınır `WORKER_CONCURRENCY`,
        pencere tek dalga (provider + sonuç yazımı). `ClaimBatchSize` lease hesabına girmiyor
  - [x] **Lease yenileme yok:** en kötü pencere kontrolümüzde; `worker.New` açılışta
        `lease >= 2 × (provider + result)` ve `drain >= provider + result` eşitsizliklerini zorluyor
  - [x] **Reaper hakkı tükenen satırı `failed` yapar:** claim'deki
        `attempt_count < max_attempts` koşuluyla birlikte zorunlu — biri olmadan satır
        sonsuz döner, ikisi eksik olunca sonsuza kadar `retrying`'de gömülü kalır
  - [x] **Kapanışa dayanıklı context** (`context.WithoutCancel`): uçan gönderim ve
        sonuç yazımı SIGTERM ile kesilmez; kesilse gönderilmiş mesajın satırı
        `processing`'de kalıp reaper tarafından yeniden gönderilirdi
  - [x] **Geçici hatalar errgroup'a döndürülmez:** loglanıp backoff ile yeniden denenir,
        aksi hâlde tek saniyelik bir DB kopması worker'ı crash-loop'a sokardı
  - [x] **2xx + `DecodeError` → `sent`:** `DecodeError` yalnızca başarılı durum
        kodlarında üretiliyor, yani sağlayıcı kabul etmiş. `retrying` yapmak çift
        gönderim olurdu (gövdesi JSON olmayan sağlayıcıda her mesaj 5 kez giderdi)
  - [x] **Başarı = her 2xx, yalnızca 202 değil:** webhook.site 200 dönüyor
- [x] **Adım 7 — Rate limit + öncelik:** Redis jeton kovası, `--scale worker=3` demosu → `feat: add distributed rate limiting and priority ordering`
      *(adım üç commit'e bölündü: `feat: add redis token bucket rate limiter`,
      `refactor: move lease hand-back to the engine`, `feat: add distributed rate
      limiting and priority ordering`)*
  - [x] **Açık soru cevaplandı: izin SENDER'da, `Send`'den hemen önce alınıyor.**
        Bekleme lease penceresinin içinde geçiyor ama `WORKER_RATE_WAIT_MAX` (10s) ile
        sınırlı ve lease formülüne dâhil: `lease >= 2 × (rate_wait + provider + result)`
        = `2 × 23s` = 46s <= 60s. Claim'den önce izin almak REDDEDİLDİ: izinler kaç
        satır olduğu bilinmeden alındığı için claim az satır döndürdüğünde boşa yanıyor
        (hesap: 5 msg/sn'lik trafik 100/sn limitinde throttle olurdu). Sender'da
        **1 izin = 1 gerçek gönderim**, israf sıfır
  - [x] **`RATE_LIMIT_PER_SECOND` = "herhangi bir 1 sn'lik pencerede en fazla".**
        GCRA'da pencere `Rate + (Burst-1)` olduğu için iç hız geri hesaplanıyor:
        `internalRate = perSecond - (burst-1)` → 100 ve burst 5 için 96, en kötü
        pencere tam 100. `Burst` ayarlanabilir (`RATE_LIMIT_BURST`, varsayılan 5) ve
        ÖLÇÜMLE seçildi: `PerSecond(n)` (burst=n) ilk saniyede ~2n geçirirdi;
        `burst=1` toleransı sıfırladığı için kaçan yuvalar geri gelmiyor ve hız %20
        düşüyordu (14.0 sn); `burst=5` + iç hız 96 ile hem sınır korunuyor hem kayıp %4
  - [x] **Fail closed:** Redis'e ulaşılamazsa gönderim YAPILMAZ; hata reddedilmiş
        izinle aynı muameleyi görüyor (sınırlı süre yeniden denenir, sonra satır
        kuyruğa döner). "Hata hâlinde lease'i hemen geri ver" denseydi Redis düştüğünde
        claim → hata → release döngüsü Postgres'i döverdi; beklemek jetonu sender'da
        tuttuğu için backpressure'ı semafor sağlıyor
  - [x] **Tek izin sorgusuna kendi zaman aşımı** (`rateAllowTimeout`, 1s) — canlı
        ölçümde yakalandı: Redis container'ı durdurulduğunda go-redis çağrısı istemci
        içinde bloklanıyor ve tek çağrı bütün bekleme bütçesini yiyordu (ne hata
        loglanıyor ne yeniden deneniyor). Regresyon testi: `TestPermitWaitBoundsEachLimiterCall`
  - [x] **Bekleme katlanıyor** (taban 10ms → tavan 250ms, full jitter): GCRA'nın
        `RetryAfter`'ı "bir sonraki izne kadar" (~10ms) ve bekleyen herkes aynı değeri
        alıyor; doğrudan beklemek kova boşken saniyede binlerce sorgu üretirdi
  - [x] **İki ayrı pencere:** `leaseWindow = rate_wait + provider + result` (bekleme
        lease altında geçiyor), `drainWindow = provider + result` (bekleme kapanışta
        iptal edilebiliyor, satır kuyruğa dönüyor)
  - [x] **Starvation uygulanmadı, belgelendi:** `TestClaimLeavesLowPriorityBehindWhileHighPriorityArrives`
        davranışı sabitliyor (yüksek öncelik akışı sürdükçe düşük öncelik beklemede
        kalır), README'de gerekçe ve aging çözüm yolu yazılı. PLAN §11 soru 7'nin cevabı
  - [x] **Ölçümler:** 1000 SMS → 1 worker 12.1 sn (maks 98/sn) / 3 worker 10.9 sn
        (her tam saniyede tam 96, maks 96) — hız ARTMIYOR ve **sınır hiç aşılmıyor**.
        Toplam sağlayıcı isteği 1000 (çift gönderim yok). Kanal izolasyonu: 562 SMS
        kuyrukta beklerken 5 e-posta 1.09 sn'de gönderildi
- [x] **Adım 8 — Retry + hata yönetimi:** sınıflandırma, backoff + jitter, attempts tablosu, circuit breaker → `feat: implement retry policy and circuit breaker`
      *(adım dört commit'e bölündü: `feat: add per-channel circuit breaker`,
      `feat: add deferred lease hand-back to the store`,
      `feat: implement retry policy and circuit breaker`,
      `fix: shield the claim query from shutdown cancellation`)*
  - [x] **"Yalnızca `classify.go` değişecek" varsayımı YANLIŞTI** ve düzeltildi:
        devre açıkken mesajların deneme hakkı ve rate limit izni harcamaması için
        `channel.go`'daki claim döngüsünün de durması zorunlu. PLAN §6'nın "açıkken o
        kanaldan iş çekilmez" ifadesi doğru olan
  - [x] 400/422 ve tanınmayan 4xx → hemen `failed`; 401/403/404 → `failed` DEĞİL
        (yapılandırma hatası, mesajlar geçerli → devre açılır, operatör düzeltince
        birikmiş kuyruk akar); 429 → `Retry-After`'a uyar, deneme HARCAMAZ, devrede
        "excluded"; 5xx/bağlantı → devre kapalıysa normal retry, devraldıysa denemesiz
        requeue; timeout → her durumda deneme sayılır (belirsiz)
  - [x] Backoff `jitter(min(2s × 2^(n-1), 5dk))`; `WORKER_RETRY_DELAY` kaldırıldı,
        yerine `WORKER_RETRY_BASE_DELAY` / `WORKER_RETRY_MAX_DELAY`
  - [x] **Zehirli mesaj açığı:** denemeyi koşulsuz saymamak, tek bir mesaja hep 500
        dönen sağlayıcıda sonsuz döngü üretiyordu (araya giren başarılar ardışık
        sayacı sıfırladığı için devre hiç açılmıyor). Muafiyet "breaker devraldıysa"
        durumuna daraltıldı
  - [x] **Erteleme `2 × BreakerOpenTimeout`:** tek kat yetmiyor çünkü claim sırası
        `ORDER BY priority_rank, available_at` — öncelik zamandan ÖNCE geliyor, yani
        yüksek öncelikli zehirli mesaj bir sonraki yarı açık sondada yine seçilirdi
  - [x] **Yarı açıkta uçuşta iş varken claim yok:** olmadan, sonda uçuşurken her
        poll turu bir satır daha çekiyor ve paylaşılan Redis bütçesi saniyeler içinde
        tükeniyordu
  - [x] **Açık soru cevaplandı: claim sorgusu kapanıştan MUAF** (`context.WithoutCancel`
        + `WORKER_CLAIM_TIMEOUT=3s`). ctx doğrudan geçildiğinde SIGTERM sorguyu ortada
        kesiyor ve commit olmuş bir `UPDATE`'in satırları öğrenilmeden kalıyordu
        (kopya başına 60 satıra kadar, lease dolana dek görünmez). Lease süresini bu
        pencereye göre büyütmek REDDEDİLDİ: belirsizliği yok etmiyor, yalnızca
        erteliyordu. Kapanış ≈ `3 + 20 = 23 sn < stop_grace_period 30 sn`
  - [x] **Worker tarafı lease token'ı üretme seçeneği REDDEDİLDİ:** claim partisinin
        tek token'ı paylaşması pencereyi tamamen kapatırdı ama Adım 6'nın "token'ı
        worker uydurmaz, tek kaynak DB" kararını ve claim SQL'ini değiştiriyor;
        kalan risk (nadir, satır kaybı yok, en kötü 60 sn gecikme) bunu hak etmiyor
- [x] **Adım 9 — Gözlemlenebilirlik:** `/metrics`, uçtan uca correlation ID, `/health/ready` → `feat: add metrics, structured logging and health checks`
  - [x] **Açık soru cevaplandı: tek `/metrics` ucu API'de, worker'a port AÇILMADI.**
        Değerler veritabanından türetiliyor (`notifications` + `notification_attempts`).
        Gerekçe: `--scale worker=3` ile üç kopyanın kendi portunu expose etmesi hem
        yüzey hem kopya toplama demekti; DB zaten tek doğruluk kaynağı. Bedeli,
        değerlerin counter değil GAUGE olması — adlandırma bunu söylüyor
  - [x] `queue_depth` yalnızca AKTİF durumları sayıyor (Adım 2'nin kısmi index'leri);
        maliyet tablo boyutundan değil aktif satır sayısından geliyor,
        `TestQueueDepthQueryAvoidsSeqScan` EXPLAIN ile sabitliyor
  - [x] **Kümülatif terminal sayımı ÖLÇÜMLE reddedildi:** WHERE koşulu index'in kısmi
        yordamıyla birebir aynı olduğu için planlayıcı Seq Scan seçiyor (15.500 satırda
        317 buffer) ve maliyet tablo boyutuyla büyüyordu → pencereli
        `notifications_terminal_recent`
  - [x] **İki gecikme birlikte:** uçtan uca (`created_at→updated_at`, kuyrukta bekleme
        dâhil) ve sağlayıcı çağrısının kendi süresi (`notification_attempts.duration_ms`).
        İkisi birlikte "yavaşlık kuyrukta mı sağlayıcıda mı" sorusunu cevaplıyor —
        ölçümde p50 1.65 sn ↔ 0.021 sn, yani fark kuyruk/hız sınırı
  - [x] **Devre kesici durumu Redis üzerinden** (`internal/breakerstate`): anahtar
        KOPYA BAŞINA (`<kanal>:<hostname>`), TTL periyodun üç katı ve worker durumları
        periyodik yeniden yazıyor (yalnızca değişimde yazmak TTL'i tazelemezdi).
        Ölçümde iki kopyanın devreleri farklı düşünüyor — yerel breaker'ın belgelenen
        sonucu metrikte görünüyor
  - [x] **`/metrics` kimliksiz olduğu için korumalı:** 5 sn önbellek + singleflight
        (eşzamanlı scrape'ler tek sorgu turu paylaşıyor), sorgu başına 2 sn zaman aşımı,
        sorgular sırayla koşuyor. Bir sorgu düşse uç yine dönüyor ve
        `collect_errors` bunu bildiriyor
  - [x] `migrations/000005`: iki metrik index'i, ikisi de EXPLAIN ile doğrulandı
- [ ] **Motor testleri:** çift claim yok, lease recovery, retry → `test: add worker integration tests`

**Gün sonu kontrolü:** 1000 SMS ≈ 10 sn; worker restart'ta iş kaybı yok; %30 hata oranında retry görülüyor; webhook.site'ta 3 kanal doğru JSON.

### 3. Gün — Bonuslar, kalite, teslim
- [ ] **Testler + CI:** kalan testler, `make test`, GitHub Actions → `ci: add GitHub Actions pipeline`
- [x] **Bonus — Scheduled:** `send_at` → `feat: support scheduled notifications`
  - [x] **Yeni migration YOK:** §2'nin "tek `available_at`" kararı bunu zaten
        karşılıyor — zamanlanmış gönderim = `available_at = send_at`, claim sorgusu
        (`available_at <= now()`) değişmedi. Satır `pending` beklediği için zamanı
        gelmeden **iptal edilebiliyor**
  - [x] `send_at` ayrıştırması **domain'de** (httpapi'de değil): diğer alanlarla aynı
        yol ve batch ucunda hata anahtarının `notifications[1].send_at` olarak
        öneklenmesi bedavaya geliyor
  - [x] Sınırlar: saat dilimli RFC3339 zorunlu, en fazla **30 gün** ileri (sınırsız
        bırakmak claim index'inde kalıcı birikme demekti), geçmiş an reddedilir ama
        **1 dakika tolerans** var (istemci-sunucu saat farkı geçerli bir isteği
        kırmasın; tolerans içindeki değer "hemen" olarak işlenir)
  - [x] **F12 düzeltildi (aynı kodu elliyordu):** zamanlanmamış kayıtta `available_at`
        artık uygulama saatiyle değil `COALESCE($10, now())` + `RETURNING` ile
        DB tarafından damgalanıyor — tekil ve batch yolu aynı saat kaynağını kullanıyor.
        Ölçüm: `available_at == created_at` (canlı)
  - [x] **Batch'te COPY + tek UPDATE:** COPY satır başına "varsayılanı uygula"
        diyemiyor (NULL, NOT NULL kısıtını ihlal eder), bu yüzden karışık batch'te
        yalnızca zamanlanmış satırlar aynı transaction'da `unnest(...)` ile tek
        UPDATE'le düzeltiliyor; satır başına UPDATE, COPY'nin kazancını geri verirdi
  - [x] **`queue_depth` ikiye ayrıldı:** `notifications_queue_depth` yalnızca ŞİMDİ
        gönderilebilir işi sayıyor, zamanı gelmemiş satırlar
        `notifications_deferred_depth` altında. Sebep scheduled'dan eski: retry
        backoff'u da `available_at`'i ileri taşıyor. Tek sorgu
        (`GROUP BY status, channel, available_at > now()`), EXPLAIN ile doğrulandı —
        20.000 terminal + 50 aktif satırda `Index Scan`, 18 buffer
  - [x] **Ölçüm (canlı):** 30 sn ileriye kurulan bildirim zamanı gelmeden `pending`
        ve `/metrics`te `deferred_depth`; hedef andan **153 ms** sonra `sent`.
        Karışık batch: biri `sent`, zamanlanmış olan `pending`
- [ ] **Bonus — Template:** `text/template`, değişken yerleştirme → `feat: add template system`
- [ ] **Bonus — WebSocket:** Redis pub/sub → `/ws` → `feat: add websocket status updates`
- [ ] **Kanıt:** k6 yük testi, Prometheus + Grafana paneli → `chore: add load test and dashboards`
- [x] **Dokümantasyon:** OpenAPI + Swagger UI, README → `docs: add OpenAPI spec and README`
      *(iki commit: `docs: add OpenAPI spec and Swagger UI` ve `docs: add README and design notes`)*
  - [x] **README İngilizce** (`README.md`, ~460 satır) + uzun gerekçeler
        `docs/DESIGN.md`'de (12 bölüm). Gerekçe: değerlendirme belgesi İngilizce ve ekip
        uluslararası olabilir; kod yorumları, OpenAPI açıklamaları ve API hata
        mesajları Türkçe kalıyor, README bunu bir notla söylüyor
  - [x] **İlk ekran kaydırmasız:** ne olduğu (tek paragraf) + Quick start
        (compose → curl → `/docs`, `/metrics`, `/health`) → 2 dakikada çalışır
  - [x] **Mermaid mimari diyagramı** (GitHub'da render olur) + sistemi ayakta tutan
        dört değişmez
  - [x] **PDF gereksinim eşleme tablosu**: dört fonksiyonel başlık, teslim listesi ve
        bonuslar madde madde "nerede karşılandı" ile. PDF metni
        `docs/assessment/*.pdf`'ten çıkarılıp birebir kullanıldı
  - [x] **Tasarım kararları** karar → neden → takas biçiminde 12 satırlık tablo;
        uzun hâlleri DESIGN'da
  - [x] **Ölçümler tablosu**: 1000 SMS 12.1 sn / 3 kopyada 10.9 sn (96/sn),
        kabul B (200 yerine 8 istek, 0 failed), kabul C (2150 istekte 0 failed,
        0 deneme), iki gecikme (1.65 sn ↔ 0.021 sn), index ölçümleri, zamanlanmış
        gönderim 153 ms, kapanışta 0 ERROR. Örnek `/metrics` çıktısı **canlı
        yığından** alındı (uydurulmadı)
  - [x] **webhook.site bölümü**: kendi URL'ini alma, 202 + JSON yanıtı ayarlama,
        `.env`'e yazma (`.env.example`'a DEĞİL) ve varsayılanın neden mock olduğu
        (100 istek / 7 gün kotası)
  - [x] **OpenAPI 3.0.3 speci** `api/openapi/openapi.yaml`, elle yazıldı (koddan
        üretilmedi): 11 uç, kanal bazlı validation kuralları, idempotency/cursor/
        tarih aralığı sözleşmeleri ve her hata durumu örnekli. Redocly ile doğrulandı
        (yalnızca "security tanımlı değil" uyarıları — API bilinçli olarak kimliksiz,
        kök `security: []` ile bu makine-okunur hâle getirildi)
  - [x] **Spec binary'e gömülü** (`api/openapi/embed.go`, `go:embed`) ve `/openapi.yaml`
        olarak sunuluyor (ETag + `If-None-Match` → 304). Dosyanın imaja ayrıca
        kopyalanmış olmasına bağlı kalmak "compose up sonrası /docs çalışır" sözünü
        kırılgan yapardı
  - [x] **Swagger UI `/docs`**: varlıklar CDN'den SABİT sürümle (5.36.0) **ve SRI
        `integrity` hash'iyle** geliyor — sürümü sabitlemek yalnızca etiketi sabitler,
        içeriği sabitleyen şey hash. İnternet yoksa sayfa boş kalır ama `/openapi.yaml`
        erişilebilir kalır (README'ye yazılacak). Hash'ler indirilip hesaplandı,
        yenileme komutu `docs.go`'da yazılı
  - [x] **Sapma testleri** (`internal/httpapi/docs_test.go`, Docker istemez):
        (1) `chi.Walk` ile kayıtlı uçlar ↔ spec'in `paths`'i, (2) yanıt DTO'larının
        json etiketleri ↔ şema `properties`'leri — yeni bir ALAN (ör. Scheduled'ın
        `send_at`'i) dokümana yazılmazsa kırılır, (3) `errorCodes` dizisi ↔
        `ErrorEnvelope.error.enum`, (4) `go/parser` ile `errors.go`'nun `ErrorCode`
        sabitleri ↔ `errorCodes` dizisi (dizinin kendisi de kopya olmaktan çıkıyor)
  - [x] OpenAPI'de `error` kodu enum'unu listele — tek kaynak: `internal/httpapi/errors.go`
  - [x] README'de RFC 9457 (`problem+json`) yerine düz hata zarfının neden seçildiğini
        bir cümleyle açıkla
  - [x] README'de SMS uzunluk kuralını (GSM-7 160 / Unicode 70) ve Türkçe karakterin
        maliyet etkisini belgele
  - [x] README'de idempotency hash tercihi: "Semantik olarak aynı ama byte olarak farklı
        istekler 409 alır; tekrar denemeler aynı baytları gönderdiği için bu bilinçli ve
        güvenli bir tercihtir."
  - [x] README'de batch sınırları: "1000 kayıt ve 16 MB sınırı birlikte uygulanır;
        Türkçe/emoji içerikli çok uzun e-posta batch'leri byte sınırına önce takılabilir."
  - [x] README'de starvation: yüksek öncelikli mesaj akışı sürdükçe düşük öncelikli
        mesaj hiç gönderilmeyebilir. Kuyruk sırası tek bir `ORDER BY priority_rank,
        available_at, id` ile belirleniyor ve bu, claim index'inin kolon sırasıyla
        birebir aynı. Aging (yaşlanan işin öncelik kazanması) bilinçli olarak
        uygulanmadı: sıra anahtarına yaş terimi eklemek
        (örn. `priority_rank - floor(age/threshold)`) index'i ve üzerine kurulu bütün
        plan kararlarını değiştirir — yük altında ölçmeden yapılacak bir değişiklik
        değil. Davranış `TestClaimLeavesLowPriorityBehindWhileHighPriorityArrives` ile
        sabitlendi.
  - [x] README'de rate limit: izin gönderimden hemen önce alınıyor (1 izin = 1 gerçek
        gönderim). Bedeli: izin bekleyen satır `processing` olduğu için o süre boyunca
        iptal edilemez (tipik 0.6 sn, en kötü `WORKER_RATE_WAIT_MAX`) ve öncelik sırası
        bir dalga boyunca donar. Claim öncesi izinde bu iki sorun olmazdı ama izinler
        boşa yanardı.
  - [x] README'de `WORKER_CONCURRENCY` sizing: limite göre aşırı büyük tutulmamalı.
        Throttle altında sender'lar izin beklerken lease tutuyor; `rate=1/sn` +
        `concurrency=20` olsaydı 20 satır 20 saniye rehin kalırdı. Sağlıklı ilişki
        `concurrency ≈ rate × (sağlayıcı gecikmesi + sonuç yazımı)` + pay.
        "izin beklemesi aşıldı" uyarıları çoğalıyorsa bu ayar yanlış.
  - [x] README'de kapanışta geri verilen iş: `ReleaseLeases` ile kuyruğa dönen hiç
        denenmemiş satır `retrying` + `attempt_count = 0` görünür. `processing → pending`
        durum makinesinde yasak olduğu için satır `retrying`'e düşer, ama denemesi
        sayılmadığı için sayaç sıfıra geri alınır.
  - [x] README'de cursor sözleşmesi: imleç **opaktır**, yalnızca önceki yanıttan gelen
        `next_cursor` geri verilmelidir; elle kurulmamalı (iç yapı değişebilir)
  - [x] README'de tarih aralığı: `[from, to)` yarı açık; `to` hariç olduğu için ardışık
        pencereler sınır kaydını iki kez saymaz. Saat dilimi zorunlu
- [x] **Polish (review bulguları):** F06, F08, F09, F13, F14, F19, F42 → `fix: close review polish findings`
  - [x] **F08 — kendi recoverer'ımız:** chi'nin hazır `Recoverer`'ı istemciye GÖVDESİZ
        bir 500 dönüyor (hata zarfı ve `correlation_id` yok) ve yığını ANSI renkli,
        çok satırlı olarak doğrudan stderr'e basıyordu — iki belgelenmiş değişmez
        birden kırılıyordu. `http.ErrAbortHandler` yeniden paniklendiriliyor
  - [x] **F13 — JSON hata metni çevriliyor:** `encoding/json`'un mesajı iç DTO
        adlarını (`createNotificationRequest`, `[]httpapi....`) sızdırıyor ve onları
        fiilî sözleşmeye çeviriyordu. Artık alan adı + JSON tipi:
        `"channel" alanı için beklenen tip string, gelen number`
  - [x] **F09 — `batchTimeout` 30s → 20s:** `WriteTimeout` ile eşit olmak kuralın tek
        istisnasıydı ve yorumun engellediğini söylediği sonucu üretiyordu (write
        deadline header'la başlıyor, 500 zarfı flush edilemiyor)
  - [x] **F14 — sayaçlara üst sınır** (`config.MaxCountValue`, 1.000.000):
        `WORKER_DB_MAX_CONNS=4294967296` → `int32(0)` → havuz sessizce pgx varsayılanına
        düşerken log 4294967296 yazıyordu. Artık açılışta hata; `MaxCountValue`'nun
        int32'ye sığdığı da testle sabit
  - [x] **F06 — idle bağlantı havuzu `len(domain.AllChannels()) × concurrency`:** üç
        kanal havuzu aynı `provider.Client`'ı paylaşıyor, uçan istek 3 × 20 = 60'tı;
        idle 20 kalınca yanıtların 2/3'ü bağlantıyı kapatıyor ve alanın kaldırmak için
        eklendiği el sıkışma maliyeti geri geliyordu. `domain.AllChannels()` tek kaynak
  - [x] **F19 + F42 — kapanış gürültüsü:** kapanışta `select`'in rastgele dal seçimi
        yüzünden izin sorgusu ve reaper/janitor sorguları ölü context'le gidiyor,
        normal bir kapanış gerçek arızayla aynı ERROR'u basıyordu. Reaper ve janitor
        döngüsü ortak `tickLoop`'a çıkarıldı (F21'in kopyası da kapandı) ve tick
        kanalını dışarıdan aldığı için yarış **determinist** test edilebiliyor:
        guard'sız 200 turun 93'ü ölü context'le çalışıyor, guard'la 0.
        İzin hatası kapanışta DEBUG, gerçek arızada ERROR kalıyor
  - [x] **Ölçüm (canlı):** 300'lük batch uçuşurken `docker compose stop worker` →
        kapanış loglarında **0 ERROR**, yalnızca INFO; restart sonrası 303 `sent`,
        takılı `processing` satırı 0
  - [x] **Bilinen sınırlar bölümü** (davranış + operasyonel): starvation, izin
        beklerken iptal edilemezlik, at-least-once, zamanlanmış hedef anın retry
        sonrası kaybı, kapanışta `retrying`+`attempt_count=0`, F41 (lease geri
        verme hatasının deneme maliyeti), sürekli 429'un churn bedeli, replika
        başına breaker, `WORKER_CONCURRENCY` sizing, **F10** (16 MiB gövde 15 sn
        `ReadTimeout` ile ulaşılamaz — kodda DÜZELTİLMEDİ: `WriteTimeout` sayacı
        header'da başladığı için yalnızca `ReadTimeout`'u büyütmek yetmiyor, ikisini
        birden açmak yavaş istemci korumasını zayıflatıyor), gauge'ler,
        kimliksiz `/metrics`, Swagger UI'ın CDN bağımlılığı, batch'in iki sınırı,
        cursor opaklığı
- [x] **Son prova:** sıfırdan klon + demo senaryosu (§10'un on adımı, temiz klonda)
  - [x] Klon GitHub'dan alındı, `cp .env.example .env` + `docker compose up --build`:
        beş servis sağlıklı, `migrate` `Exited (0)`, `/health/ready` iki bağımlılığı da
        `ok`. Klonda `docs/review`, `docs/assessment`, `docs/learning` **yok** (gitignore
        doğru çalışıyor); `make test-unit` ve `make help` klonda sorunsuz
  - [x] Üç kanalda bildirim → 3'ü de `sent`, deneme kaydı dolu (202, ~51 ms)
  - [x] 1000'lik batch + aynı `Idempotency-Key` ile tekrar → `Idempotent-Replay: true`,
        aynı `batch_id` ve aynı 1000 kimlik, **DB'de 1000 satır** (2000 değil);
        aynı key + farklı gövde → `409 conflict`
  - [x] Öncelik (600 satır, karışık): high ortalama **1.01 sn**, normal **3.08 sn**,
        low **5.15 sn**; bloklar yalnızca bir dalga kadar örtüşüyor. 60 satırla ölçüm
        aldatıcıydı (hepsi 1 sn'de bitiyor) — büyük kuyrukla tekrarlandı
  - [x] %30 hata oranı: 68 satır ilk denemede, 20'si 2, 7'si 3, 5'i 4 denemede gitti;
        `failed` **0**, deneme geçmişinde 500 kodu ve hata metni görünüyor
  - [x] İptal: `pending` → `cancelled`; tekrar iptal → `409`; `sent` kaydın iptali → `409`
  - [x] Worker restart (800 satır uçuşurken, 150'si gitmişken): sonrası **800/800 sent**,
        takılı `processing` **0**, sağlayıcıya giden toplam istek **800** (çift gönderim yok)
  - [x] `--scale worker=3` + 1000 SMS: **12 sn**, maks **96/sn**, 100'ü aşan saniye **0**,
        toplam sağlayıcı isteği **1000**. `/metrics` üç kopyanın devresini ayrı ayrı
        gösteriyor (kopya başına anahtar)
  - [x] `/health/live`, `/health/ready`, `/docs` (200, 907 B), `/openapi.yaml`
        (200, 35.9 KB), `/metrics` (200) — hepsi temiz klonda
  - [x] **webhook.site gerçek entegrasyon:** yeni URL `.env`'e alındı (eski, sızmış URL
        dosyadan silindi). Üç kanal gönderildi, webhook.site'ta **3 istek** PDF'in
        istediği gövdeyle göründü (`{to, channel, content}` + `Idempotency-Key` =
        bildirim kimliği + `X-Correlation-ID`). Yanıt webhook.site'ın API'siyle
        **202 + `{messageId, status, timestamp}`** olarak ayarlandı ve ikinci turda
        `provider_message_id` doldu, kod **202**. Kotadan 6 istek kullanıldı (100 üzerinden)
- [ ] **Mülakat provası**
- [ ] *(Zaman kalırsa)* **Tracing:** OpenTelemetry + Jaeger

---

## 9. Test listesi

| Test | Beklenen |
|---|---|
| Geçerli bildirim | 202 + kayıt |
| Eksik recipient / geçersiz channel / uzun içerik | 400 + alan bazlı hata |
| 1000'lik batch / 1001'lik batch | Başarılı / 400 |
| Aynı key + aynı payload | Yeni kayıt yok, aynı cevap |
| Aynı key + farklı payload | 409 |
| High ve low aynı anda | High önce çekilir |
| İki worker aynı anda claim | Her iş tek worker'da |
| Rate limit | Kanal başına ≤ 100/sn (3 worker kopyasında da) |
| Kanal izolasyonu | SMS yoğunluğu email gecikmesine yol açmaz |
| Provider 202 / 500 / 400 / 429 | sent / retrying / failed / retrying (Retry-After) |
| Max deneme aşımı | failed |
| Pending iptal / processing iptal | cancelled / 409 |
| Lease süresi dolar | Reaper işi geri koyar |
| Eski lease token ile sonuç yazma | Reddedilir |
| Filtre + cursor pagination | Doğru ve deterministik sonuç |
| Scheduled | Zamanı gelmeden çekilmez |

---

## 10. Demo senaryosu (teslim öncesi)

1. `docker compose up --build`
2. 3 kanalda birkaç bildirim oluştur
3. 1000'lik batch oluştur, aynı isteği tekrar gönder (idempotency)
4. High priority'nin öne geçtiğini göster
5. Mock provider hata oranını artır, retry'ları izle
6. Pending bir bildirimi iptal et
7. Worker'ı yeniden başlat, iş kaybı olmadığını göster
8. `--scale worker=3` ile rate limit'in korunduğunu göster
9. `/metrics`, `/health/ready`, Swagger UI, WebSocket
10. webhook.site'ta gelen JSON'ları göster

---

## 11. Mülakat soruları (prova)

1. Neden RabbitMQ/Kafka değil de PostgreSQL kuyruğu?
2. Worker gönderdi ama DB'ye yazamadan çöktü — ne olur?
3. 3 worker kopyasında rate limit nasıl 100/sn kalıyor?
4. SMS tıkanınca email neden etkilenmiyor?
5. İptal ile worker claim'i aynı anda gelirse?
6. Günde 10 milyon bildirimde ilk darboğaz nerede, ne değiştirirsin?
7. Low priority mesajlar hiç gönderilmeyebilir mi? (starvation → aging)
