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
migrations/   api/openapi.yaml   deploy/ (grafana, prometheus)   loadtest/ (k6)
docs/PLAN.md  CLAUDE.md  README.md  Makefile  Dockerfile  docker-compose.yml  .env.example
```

Go konvansiyonu: `_test.go` dosyaları test ettikleri kodun yanında durur.

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
- [ ] **Adım 4 — Batch + idempotency** → `feat: add batch creation and idempotency`
- [ ] **Adım 5 — Okuma + iptal:** get, batch özeti, cursor listeleme, atomik iptal → `feat: add query, listing and cancellation endpoints`

**Gün sonu kontrolü:** `docker compose up` çalışıyor; curl ile oluştur / listele / iptal et.

### 2. Gün — İşleme motoru
- [ ] **Adım 6 — Worker motoru:** kanal havuzları, SKIP LOCKED claim (20'li), lease + token, reaper, provider client, mock provider, graceful shutdown → `feat: implement worker engine with leases and recovery`
- [ ] **Adım 7 — Rate limit + öncelik:** Redis jeton kovası, `--scale worker=3` demosu → `feat: add distributed rate limiting and priority ordering`
- [ ] **Adım 8 — Retry + hata yönetimi:** sınıflandırma, backoff + jitter, attempts tablosu, circuit breaker → `feat: implement retry policy and circuit breaker`
- [ ] **Adım 9 — Gözlemlenebilirlik:** `/metrics`, uçtan uca correlation ID, `/health/ready` → `feat: add metrics, structured logging and health checks`
- [ ] **Motor testleri:** çift claim yok, lease recovery, retry → `test: add worker integration tests`

**Gün sonu kontrolü:** 1000 SMS ≈ 10 sn; worker restart'ta iş kaybı yok; %30 hata oranında retry görülüyor; webhook.site'ta 3 kanal doğru JSON.

### 3. Gün — Bonuslar, kalite, teslim
- [ ] **Testler + CI:** kalan testler, `make test`, GitHub Actions → `ci: add GitHub Actions pipeline`
- [ ] **Bonus — Scheduled:** `send_at` → `feat: support scheduled notifications`
- [ ] **Bonus — Template:** `text/template`, değişken yerleştirme → `feat: add template system`
- [ ] **Bonus — WebSocket:** Redis pub/sub → `/ws` → `feat: add websocket status updates`
- [ ] **Kanıt:** k6 yük testi, Prometheus + Grafana paneli → `chore: add load test and dashboards`
- [ ] **Dokümantasyon:** OpenAPI + Swagger UI, README → `docs: add OpenAPI spec and README`
  - [ ] OpenAPI'de `error` kodu enum'unu listele — tek kaynak: `internal/httpapi/errors.go`
  - [ ] README'de RFC 9457 (`problem+json`) yerine düz hata zarfının neden seçildiğini
        bir cümleyle açıkla
  - [ ] README'de SMS uzunluk kuralını (GSM-7 160 / Unicode 70) ve Türkçe karakterin
        maliyet etkisini belgele
- [ ] **Son prova:** sıfırdan klon + demo senaryosu
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
