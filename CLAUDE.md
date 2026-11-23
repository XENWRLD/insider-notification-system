# CLAUDE.md

Insider One case study: Go ile event-driven bildirim sistemi (SMS / Email / Push).
Mimari kararlar, yol haritası ve kabul kriterleri **`docs/PLAN.md`** içinde — her oturumda
önce o dosyaya bak, adımları sırayla uygula ve bitirdiğin adımın kutucuğunu işaretle.

## Durum

> Bu bölüm her adımın commit'inde güncellenir. Yeni bir oturumda yalnızca bu dosyayı ve
> `docs/PLAN.md` §8'i okumak yeterli olmalı; kodun tamamını taramaya gerek yok.

- **Tamamlanan:** Adım 0 (ortam), Adım 1 (iskelet: config, slog, chi router, correlation ID,
  `/health/live`, `/health/ready`, Dockerfile, compose = postgres 18 + redis 8 + api),
  Adım 2 (şema: 3 migration, 4 index, `migrate` servisi, testcontainers testleri)
- **Sıradaki:** Adım 3 — tekil `POST /api/v1/notifications`: `internal/domain` (model, durum
  makinesi, kanal bazlı validation), `internal/store` (elle yazılmış SQL), handler + testler
- **Henüz boş** (`.gitkeep`): `internal/{domain,worker,provider,ratelimit}`, `cmd/worker`,
  `cmd/mockprovider`, `api`, `deploy`, `loadtest`
- **Doğrulama:** `make lint && make test && make up-build`, ardından
  `curl localhost:8080/health/ready` → `{"checks":{"postgres":"ok","redis":"ok"},"status":"ready"}`
  ve `docker compose ps -a` → `migrate` servisi `Exited (0)`, `api` `Up (healthy)`

### Şema notları (Adım 2)

- Tablolar: `notifications`, `notification_attempts`, `idempotency_keys`.
  `templates` **yok** — 3. gün bonusunda kendi migration'ı ile gelir, `template_id` FK'si de orada.
- `priority` text olarak kalır ama sıralama `priority_rank` (generated, STORED) üzerinden
  yapılır: metin index'i alfabetik sıralayıp `low`'u `normal`'ın önüne geçiriyordu.
- Sabit küme alanları ENUM değil `text` + `CHECK`; yeni değer eklemek tek satır ALTER
  ve pgx'te tip kaydı gerekmiyor.
- `updated_at` trigger (`set_updated_at`) ile yazılır — store sorgularında elle yazılmaz.
- `status = 'processing'` satırı lease'siz olamaz (CHECK); reaper bu değişmeze güvenir.
- Claim index'i **kısmi**: yalnızca `pending`/`retrying` satırlarını tutar, `sent` kayıtları
  milyonlara çıksa da büyümez.

## Modül ve klasörler

Modül yolu: `github.com/XENWRLD/insider-notification-system`

| Klasör | Sorumluluk |
|---|---|
| `cmd/api` | HTTP API process'i |
| `cmd/worker` | Kanal havuzları, claim/retry/reaper (Adım 6) |
| `cmd/mockprovider` | Ayarlanabilir hata oranlı sahte sağlayıcı (Adım 6) |
| `internal/config` | Ortam değişkenlerinden yapılandırma |
| `internal/domain` | Modeller, durum makinesi, validation |
| `internal/httpapi` | Router, middleware, handler'lar |
| `internal/store` | PostgreSQL sorguları (elle yazılmış SQL) |
| `internal/worker` | Havuzlar, claim, retry, reaper |
| `internal/provider` | Dış sağlayıcı HTTP client'ı |
| `internal/ratelimit` | Redis jeton kovası |
| `internal/observability` | Logger, metrikler |
| `migrations` | golang-migrate dosyaları |

`internal/` dışına public paket koyma; bu bir uygulama, kütüphane değil.

## Kurallar

- **SQL elle yazılır**, ORM yok. Sorgular `internal/store` içinde kalır; handler ve worker
  doğrudan SQL çalıştırmaz.
- **Hata sarma:** `fmt.Errorf("bağlam: %w", err)`. Hata yutulmaz; `errors.Is/As` ile sınıflandırılır.
- **Loglama:** yalnızca `log/slog`, JSON. Her log satırında mümkün olduğunda `correlation_id`
  bulunur (`httpapi.CorrelationID(ctx)`); API ve worker ayrı process olduğu için bu kimlik
  bildirim kaydıyla birlikte veritabanına yazılır ve worker loglarına oradan taşınır.
- **Context:** her dış çağrı (DB, Redis, HTTP) context ile ve zaman aşımlı yapılır.
  `context.Background()` yalnızca `main` içinde.
- **Zaman aşımları açıkça verilir** — `http.Server` ve `http.Client` varsayılanları sınırsızdır.
- **Testler kodun yanında:** `foo.go` → `foo_test.go`. Dış altyapı gerektiren testler
  testcontainers ile; birim testleri arayüz stub'ları ile (`httpapi.Pinger` örneği).
- **Yorumlar Türkçe** ve *neden*'i anlatır, *ne*'yi değil. `.golangci.yml` içinde revive'in
  `exported` / `package-comments` kuralları bu yüzden kapalı.
- **Satır sonu LF** (`.gitattributes`). Windows'ta CRLF ile checkout edilen script'ler
  Linux container'da çalışmaz.
- **Sırlar repoya girmez:** `.env` gitignore'da, örnek değerler `.env.example` içinde.
- **Commit mesajları** conventional commits; her adımın mesajı `docs/PLAN.md` §8'de yazılı.
  Adım bitmeden commit atma, adımı ikiye bölme.
- **Adım bitirme akışı** (sırayı bozma):
  1. Testler + lint geçer.
  2. **COMMIT ATMADAN DUR.** `git diff --stat` ve dosya başına 1 cümlelik açıklama göster,
     onay bekle.
  3. Onay gelince `/adim-ozeti` talimatına göre öğrenme notunu yaz.
  4. `docs/PLAN.md` kutucuğunu işaretle ve bu dosyadaki **Durum** bölümünü güncelle —
     sonraki oturumun tek giriş noktası orası.
  5. Commit.

## Komutlar

```bash
make help        # tüm hedefler
make env         # .env yoksa .env.example'dan üretir
make up-build    # compose yığınını kurar ve başlatır
make test        # go test ./...  (testcontainers testleri Docker ister)
make test-unit   # go test -short ./...  (Docker gerektirmez)
make lint        # golangci-lint run

make migrate-up        # bekleyen migration'lari uygular
make migrate-down N=1  # son N migration'i geri alir
make migrate-version   # mevcut sema surumu
make migrate-create NAME=add_templates  # yeni migration cifti
```

`make up` zaten compose'daki `migrate` servisini çalıştırır; `migrate-*` hedefleri elle
müdahale içindir. Hepsi compose üzerinden koşar, yerelde golang-migrate CLI'si gerekmez.

## Ortam notu (Windows)

Araçlar kurulu ama bir session'ın PATH'i eski kalmışsa doğrudan şu yollar kullanılabilir:

| Araç | Yol |
|---|---|
| go 1.27.4 | `C:\Program Files\Go\bin\go.exe` |
| GNU Make 4.4.1 | `%LOCALAPPDATA%\Microsoft\WinGet\Packages\ezwinports.make_*\bin\make.exe` |
| golangci-lint 2.15.0 | `%LOCALAPPDATA%\Microsoft\WinGet\Packages\GolangCI.golangci-lint_*\golangci-lint-2.15.0-windows-amd64\golangci-lint.exe` |

`docs/assessment/` şirkete ait belgedir; gitignore'da, repoya girmez.
