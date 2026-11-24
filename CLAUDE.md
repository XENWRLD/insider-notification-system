# CLAUDE.md

Insider One case study: Go ile event-driven bildirim sistemi (SMS / Email / Push).
Mimari kararlar, yol haritası ve kabul kriterleri **`docs/PLAN.md`** içinde — her oturumda
önce o dosyaya bak, adımları sırayla uygula ve bitirdiğin adımın kutucuğunu işaretle.

## Durum

> Bu bölüm her adımın commit'inde güncellenir. Yeni bir oturumda yalnızca bu dosyayı ve
> `docs/PLAN.md` §8'i okumak yeterli olmalı; kodun tamamını taramaya gerek yok.

- **Tamamlanan:** Adım 0 (ortam), Adım 1 (iskelet: config, slog, chi router, correlation ID,
  `/health/live`, `/health/ready`, Dockerfile, compose = postgres 18 + redis 8 + api),
  Adım 2 (şema: 3 migration, 4 index, `migrate` servisi, testcontainers testleri),
  Adım 3 (domain + store + `POST /api/v1/notifications`, API sözleşmesi),
  Adım 4 (batch + idempotency: CopyFrom, claim-first transaction, uç başına gövde sınırı),
  Adım 5 (okuma uçları: `GET {id}`, cursor listeleme, batch özeti, atomik iptal),
  Adım 6 (worker motoru: `internal/bootstrap`, claim/result/reaper sorguları,
  provider client + mock provider, kanal havuzları + jeton semaforu, reaper, janitor,
  `cmd/worker`; dört commit)
- **Sıradaki:** Adım 7 — rate limit + öncelik: Redis jeton kovası (`internal/ratelimit`),
  kanal başına global 100 msg/sn, `--scale worker=3` demosu, starvation/aging.
  PLAN §8'de Adım 7'ye yazılan açık soru: rate limit beklemesi lease süresi içinde
  geçiyor; jeton claim'den önce mi alınacak?
- **Henüz boş** (`.gitkeep`): `internal/ratelimit`, `api`, `deploy`, `loadtest`
- **Doğrulama:** `make lint && make test && make up-build`, ardından
  `curl localhost:8080/health/ready` → `{"checks":{"postgres":"ok","redis":"ok"},"status":"ready"}`,
  `curl localhost:8081/health/live` → `{"status":"alive"}` ve `docker compose ps -a` →
  `migrate` `Exited (0)`, `api`/`mockprovider` `Up (healthy)`, `worker` `Up`.
  Uçtan uca: bildirim oluştur → 2 sn sonra `status=sent`, `attempts[0]` dolu,
  `docker compose logs worker | grep <correlation-id>` satırı bulur

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
- `000004` (Adım 5): `(status, created_at DESC, id DESC)`. Adım 2'de ertelenip Adım 5'te
  **ölçülerek** eklendi — nadir durum filtresinde `Seq Scan` 13.77 ms → `Index Scan`
  0.13 ms. Gerekçe ve kanal index'inin neden eklenmediği migration dosyasında yazılı.
- **Index ölçümü uygulamanın ürettiği SQL ile yapılır**: `explainQuery` yardımcısı
  `buildListQuery`'nin metnini ve argümanlarını aynen `EXPLAIN`'e verir.

### API sözleşmesi (Adım 3'te sabitlendi)

- **Hata zarfı** (tüm uçlar, 404/405 dahil): `{error, message, fields?, correlation_id}`.
  `correlation_id` **her** hata yanıtında bulunur. RFC 9457 yerine bu biçim seçildi.
- **`error` kodları tek kaynaktan**: `internal/httpapi/errors.go` içindeki `ErrorCode`
  sabitleri. Yeni kod eklenince OpenAPI de güncellenir.
- **`fields`** alan adı → mesaj haritası. Batch uçlarında anahtar `notifications[3].recipient`
  biçiminde olur (`writeValidationError`'a önek geçilir).
- **Tüm alan hataları tek yanıtta** döner. İstisna: `channel` geçersizse alıcı/içerik
  **biçimi** doğrulanmaz (hangi kuralın geçerli olduğu bilinemez), yalnızca boşluk kontrolü yapılır.
- **Yanıt DTO'su ayrıdır** (`notificationResponse`): `domain.Notification` doğrudan
  serialize **edilmez**. `lease_token`, `lease_expires_at`, `priority_rank`, `template_id`
  dışarı çıkmaz. `GET /notifications/{id}` (Adım 5) aynı şemayı kullanacak.
- **Oluşturma** `202` + `Location` header döner (201 değil: kayıt oluştu, gönderim olmadı).
- **SMS uzunluğu** GSM 03.38 tablosuna göre: kodlanabiliyorsa 160 septet, aksi hâlde
  70 **UTF-16 kod birimi** (emoji = 2 birim). Küçük `ç` ve `ş ğ ı İ` tabloda yok;
  büyük `Ç` ve `ö ü Ö Ü` var.
- **Validation domain'de**, veritabanı CHECK kısıtları ikinci savunma hattı.

### Idempotency ve batch (Adım 4'te sabitlendi)

- **`Idempotency-Key`** isteğe bağlı, en fazla 255 karakter. Verilmezse koruma kapalı.
- **`request_hash` = ham gövde baytlarının SHA-256'sı.** Gövde bir kez okunur
  (`decodeJSON` baytları da döner); hash ve JSON çözme aynı baytlardan yapılır.
  Sonuç: fazladan boşluk veya farklı alan sırası **409** alır — bilinçli katılık,
  gerçek tekrar denemeleri aynı baytları gönderir.
- **Tekrar gönderim** `202` + `Idempotent-Replay: true` + saklanan gövde döner.
  Gövde `jsonb`'den geldiği için anahtar sırası normalize olur: değer olarak aynı,
  **bayt olarak farklı** olabilir (JSON'da anahtar sırası anlamsız).
- **Anahtar işten ÖNCE talep edilir** (claim-first): 1000 satır yazıp tekrar gönderim
  fark edildiğinde çöpe atmak yerine erken çıkılır. Yer tutucu cevap aynı transaction'da
  güncellenir, rollback hâlinde satır kalmaz.
- **Süresi dolmuş anahtar** aynı transaction'da devralınır; doğruluk TTL temizliğine
  **bağlı değil**. Temizlik (`store.DeleteExpiredIdempotencyKeys`, `LIMIT`'li) Adım 6'da
  worker'da çağrılacak.
- **Batch**: 1–1000 öğe, tek transaction, `pgx.CopyFrom`. `available_at`/`created_at`/
  `updated_at` COPY listesinde **yok** → DB varsayılanı (tek saat kaynağı DB).
- **Batch hep ya hiç**: bir öğe geçersizse hiçbiri yazılmaz; hatalı satırların **tümü**
  tek yanıtta, `notifications[3].recipient` biçiminde anahtarlarla döner.
- **Batch yanıtı özet**: `batch_id`, `accepted_count`, `notification_ids`,
  `correlation_id`. `Location` header'ı **yok** (`/batches/{id}` ucu Adım 5'te geliyor).
- **Gövde sınırı uç başına**: `BODY_LIMIT_SINGLE` (128 KiB) / `BODY_LIMIT_BATCH` (16 MiB).
  Batch'in 413 mesajı sınırı ve "batch'i bölün" öğüdünü içerir.
- **Batch yanıtı `Location: /api/v1/batches/{id}`** döner (uç Adım 5'te geldi).

### Okuma uçları (Adım 5'te sabitlendi)

- **`attempts` HER ZAMAN dizi**: oluşturma yanıtında ve denemesi olmayan kayıtta `[]`.
  `omitempty` yok, `null` yok. Go'da `nil` slice `null` serialize ettiği için
  `newAttemptResponses` daima `make(..., 0, n)` kullanır.
- **Listede de gerçek deneme verisi** döner. Boş bırakmak "denenmedi" demek olurdu.
  Sayfadaki kimliklerin denemeleri `attemptsFor` ile **tek sorguda** çekilir (N+1 yok);
  `ORDER BY notification_id, attempt_no` Adım 2'deki unique index'in sırasıyla aynı.
- **Cursor opaktır**: `base64.RawURLEncoding(created_at RFC3339Nano + " " + id)`.
  RFC3339Nano zorunlu — mikrosaniye kaybolursa aynı saniyedeki kayıtlarda sayfalama
  atlama/tekrar üretir. Bozuk imleç → `400 invalid_cursor`.
- **Sonraki sayfa `limit+1`** kayıt çekilerek tespit edilir; son sayfada
  `next_cursor: null`.
- **Filtre mantığı**: aynı parametrenin değerleri **VEYA**, farklı parametreler **VE**.
  `from`/`to` saat dilimli RFC3339 olmalı (tarih-only reddedilir), aralık **yarı açık**
  `[from, to)`. `limit` varsayılan 20, en fazla 100.
- **Geçersiz UUID → `404`** (400 değil): çözümlenemeyen bir kimlik zaten var olamaz.
- **İptal**: tek `UPDATE ... WHERE status IN ('pending','retrying')` ile atomik.
  Güncelleme olmazsa 404/409 ayrımı için ikinci bir varlık sorgusu atılır (yalnızca
  hata yolunda). Worker claim'i ile yarışta claim kazanırsa iptal `409` alır.
- **Listeleme `WHERE`'i Go'da dinamik kurulur** (`buildListQuery`): `$n IS NULL OR`
  kalıbı pgx statement cache + generic plan altında cursor koşulunu `Index Cond` yerine
  `Filter`'a düşürüyor. Plan `TestListQueryUsesCursorIndex` ile testte doğrulanıyor.

### Worker motoru (Adım 6'da sabitlendi)

- **Kanal başına havuz:** 1 claimer + `WORKER_CONCURRENCY` sender + jeton semaforu.
  SMS tıkanınca e-posta etkilenmez; izolasyon yapısal.
- **Claimer boştaki sender kadar iş çeker.** Jeton semaforu (`chan struct{}`, önceden
  dolu; `len(sem)` = boşta sender) claim limitini belirler. Sonuç: tutulan lease
  **her zaman ≤ `WORKER_CONCURRENCY`** ve bir satırın claim→sonuç penceresi **tek dalga**
  (`ProviderTimeout + ResultTimeout`). `ClaimBatchSize` lease hesabına **girmez**.
- **Dağıtım kanalı tamponsuz:** bir satır ya claimer'ın elinde ya bir sender'da; üçüncü
  yer yok. Kapanışta hangi satırın geri verileceği bu yüzden kesin.
- **Jeton muhasebesi:** dağıtılan jetonu sender (sonuç yazımından **sonra**) bırakır,
  dağıtılmayanı claimer. Kullanılmayan jetonlar ve hata hâlinde tümü **backoff
  beklemesinden ÖNCE** geri verilir — aksi hâlde tur başına sızar ve worker sessizce durur.
- **Lease sabit 60 sn, yenileme yok.** `worker.New` açılışta iki eşitsizliği zorlar:
  `LeaseDuration >= 2 × (ProviderTimeout + ResultTimeout)` ve
  `DrainTimeout >= ProviderTimeout + ResultTimeout`. Hata mesajı gereken sayıyı söyler.
- **`context.WithoutCancel`:** uçan sağlayıcı çağrısı ve sonuç yazımı SIGTERM ile
  kesilmez. Sonuç yazımını iptal etmek, gönderilmiş mesajın satırını `processing`'de
  bırakıp reaper'a yeniden göndertmek olurdu. `pool.Close()` `Run` döndükten sonra çalışır.
- **Geçici hatalar errgroup'a döndürülmez** (claim/sonuç/reaper/janitor): loglanır ve
  backoff ile yeniden denenir. Döndürülse tek saniyelik bir DB kopması crash-loop yapardı.
- **`attempt_count` claim anında artar**, yalnızca `ReleaseLeases` azaltır (sağlayıcı hiç
  çağrılmadı). Deneme kaydını **yalnızca** `CompleteNotification` yazar; bu değişmez
  sayesinde `attempt_no` çakışması imkânsız.
- **Sonuç + deneme kaydı tek statement** (`WITH upd AS (UPDATE ... RETURNING) INSERT ...`),
  `lease_token` koşullu. Lease kaybedilmişse CTE boş kalır, deneme kaydı da yazılmaz →
  zombi worker iz bırakmaz. `withTx` yok; gerekçe `result.go` yorumunda.
- **Reaper:** lease dolan satırı `retrying`'e döndürür, `attempt_count >= max_attempts`
  ise `failed` yapar. Claim'deki `attempt_count < max_attempts` koşuluyla birlikte
  zorunlu — biri eksikse satır ya sonsuz döner ya sonsuza kadar gömülü kalır.
- **`classify.go` Adım 8 dikişi.** Adım 6 kuralı: 2xx → `sent`; 2xx + `DecodeError` →
  `sent` + `provider_message_id` NULL (sağlayıcı kabul etti, retrying çift gönderim
  olurdu); diğer her hata → hak varsa `retrying` + sabit 5 sn, yoksa `failed`.
  Adım 8'de yalnızca bu dosyanın gövdesi değişecek.
- **Başarı = her 2xx** (yalnızca 202 değil): webhook.site 200 dönüyor.
- **Worker HTTP dinlemez, Redis açmaz.** Metrik kararı Adım 9, Redis Adım 7.
- **Provider client politika kurmaz**, olgu bildirir: `StatusError` (kod + çözülmüş
  `Retry-After`), `TransportError` (+`Unwrap`), `DecodeError`. Adım 8 `errors.As` ile
  sınıflandırır, client'a dokunmaz.
- **`MaxIdleConnsPerHost = WORKER_CONCURRENCY`:** Go'nun varsayılanı host başına 2 idle
  bağlantı; 100/s'te neredeyse her istek yeni TCP (ve TLS) el sıkışması olurdu.
- **Ölçüm:** 1000 SMS → 7.7 sn (mock 50 ms gecikme); restart'ta iş kaybı yok, takılı
  `processing` satırı 0.

## Modül ve klasörler

Modül yolu: `github.com/XENWRLD/insider-notification-system`

| Klasör | Sorumluluk |
|---|---|
| `cmd/api` | HTTP API process'i |
| `cmd/worker` | Kanal havuzları, claim/retry/reaper (Adım 6) |
| `cmd/mockprovider` | Ayarlanabilir hata oranlı sahte sağlayıcı (Adım 6) |
| `internal/bootstrap` | Ortak açılış: havuzlar + bağımlılık bekleme (api ve worker) |
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

make run-worker        # worker'i host'ta calistirir
make run-mock          # sahte saglayiciyi host'ta calistirir
make logs-worker       # worker loglarini izler
make scale-worker N=3  # worker kopyasi sayisini ayarlar

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

### Git Bash'te UTF-8 gövde göndermek

`curl -d '...'` ile **Türkçe karakter veya emoji gönderilemez**: Git Bash, argümanı
`curl.exe`'ye geçirirken Windows ANSI kod sayfasına çevirir ve `ü` → `�`, `🎉` → `??`
olur. Sunucu bozuk veri alır, test sonucu yanıltıcı çıkar. Gövdeyi dosyaya yazıp
`--data-binary @dosya` kullan — borular ve yönlendirmeler baytı korur:

```bash
printf '{"channel":"sms","recipient":"+905551234567","content":"%s"}' "$(printf 'ş%.0s' $(seq 1 71))" > /tmp/body.json
curl -s -X POST localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' --data-binary @/tmp/body.json
```

(Aynı sınıf sorun Makefile'daki `MSYS_NO_PATHCONV` ihtiyacının da kaynağı.)

`docs/assessment/` şirkete ait belgedir; gitignore'da, repoya girmez.
