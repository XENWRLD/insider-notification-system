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
  `cmd/worker`; dört commit),
  Adım 7 (dağıtık rate limit: `internal/ratelimit` GCRA kovası, izin sender'da,
  fail closed, starvation belgelendi; üç commit)
  Adım 8 (retry politikası + kanal başına circuit breaker: `internal/breaker`,
  `store.DeferLeases`, `classify.go` yeniden yazımı, iki breaker kapısı + claimer
  kapısı, claim sorgusunun kapanıştan muafiyeti; dört commit — onaylı plan
  `docs/plans/adim-08-plan.md`)
  Adım 9 (gözlemlenebilirlik: API'de tek `/metrics`, değerler DB'den — kuyruk
  derinliği, son 15 dk terminal sayımı, uçtan uca ve sağlayıcı gecikmesi yüzdelikleri,
  kopya başına devre kesici durumu; `internal/breakerstate`, `migrations/000005`),
  CI (GitHub Actions: lint + `go test -race` + testcontainers),
  **OpenAPI + Swagger UI** (`api/openapi/openapi.yaml` gömülü, `/openapi.yaml` + `/docs`,
  dört sapma testi)
  **Scheduled bonusu** (`send_at`, migration'sız; F12 düzeltmesi ve `queue_depth`
  ayrımı dâhil)
  ve **polish** (review bulguları F06, F08, F09, F13, F14, F19, F42)
  ve **README** (`README.md` İngilizce + `docs/DESIGN.md` uzun gerekçeler)
  ve **son prova** (temiz klonda §10 demo senaryosunun on adımı; webhook.site'a
  gerçek gönderim 202 + `messageId` ile doğrulandı)
- **Teslime hazır.** Kapsam dışı: WebSocket, Template, tracing, k6, Grafana.
  Paylaşmadan önce tek açık iş: webhook.site URL'inin git GEÇMİŞİNDE duran ESKİ
  kopyası (`.env` temiz, `.env.example` yer tutucu) — repo public yapılacaksa önce
  yeni URL'in rotasyonu. Kapsam dışı bırakılanlar: WebSocket, Template,
  tracing, k6, Grafana.
- **Review:** Adım 8 öncesi beş açılı tam tarama yapıldı (52 bulgu). Düzeltilenler:
  A grubu (`fix: close review defects blocking step 8` — `excerpt` rune kesmesi,
  mock gövde sınırı, izin bekleme bütçesi, lease geri verme yorumu, sağlık uçlarının
  HEAD'i), B grubu (`fix: close review group B defects` — sağlayıcı yönlendirmesi,
  `/health/ready` sızıntısı, compose `HTTP_PORT`, replay `Location`, 405 `Allow`,
  `make run`'ın `.env`'i), ve `.env.example`'daki canlı webhook URL'i.
  **Kalan bulgular, düzeltme sırası, README'ye girecek maddeler ve teslim durumu
  `docs/review/findings.md` içinde** (gitignore'da; dosyanın başındaki **DEVİR**
  bölümü bir sonraki oturumun giriş noktasıdır).
- **Henüz boş** (`.gitkeep`): `deploy`, `loadtest`
- **Doğrulama:** `make lint && make test && make up-build`, ardından
  `curl localhost:8080/health/ready` → `{"checks":{"postgres":"ok","redis":"ok"},"status":"ready"}`,
  `curl localhost:8081/health/live` → `{"status":"alive"}` ve `docker compose ps -a` →
  `migrate` `Exited (0)`, `api`/`mockprovider` `Up (healthy)`, `worker` `Up`.
  Uçtan uca: bildirim oluştur → 2 sn sonra `status=sent`, `attempts[0]` dolu,
  `docker compose logs worker | grep <correlation-id>` satırı bulur.
  Rate limit: 1000'lik SMS batch'i 1 worker'da 12.1 sn, `make scale-worker N=3` ile
  10.9 sn (hız artmıyor); saniyelik histogram `notification_attempts.created_at`
  üzerinden maks 96-98, sınır (100) hiç aşılmıyor (bkz. Adım 7 notları)

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

### Rate limit (Adım 7'de sabitlendi)

- **Kanal başına global 100 msg/sn**, Redis'te paylaşılan GCRA kovası
  (`internal/ratelimit`). Sınır process'e değil kovaya ait: `--scale worker=3` ile üç
  kopya da aynı anahtarı (`notifications:rate:<kanal>`) tüketir.
  `TestAllowSharesBucketAcrossLimiterInstances` bunu sabitliyor.
- **İzin SENDER'da, `Send`'den hemen önce alınır** → **1 izin = 1 gerçek gönderim**.
  Claim'den önce almak reddedildi: izinler kaç satır olduğu bilinmeden alınır ve claim
  az satır döndürdüğünde boşa yanar (5 msg/sn'lik trafik 100/sn limitinde throttle
  olurdu). Bedeli: izin bekleyen satır `processing` olduğu için o süre boyunca iptal
  edilemez ve öncelik sırası bir dalga donar — bilinçli takas.
- **`RATE_LIMIT_PER_SECOND` = "herhangi bir 1 sn'lik pencerede en fazla".** GCRA'da
  pencere `Rate + (Burst-1)` olduğu için iç hız geri hesaplanıyor:
  `internalRate = perSecond - (burst-1)` (100 ve burst 5 için **96**), en kötü pencere
  tam 100. `Burst` ayarlanabilir (`RATE_LIMIT_BURST`, varsayılan 5) ve ÖLÇÜMLE seçildi:
  `PerSecond(n)` (burst=n) ilk saniyede ~2n geçirirdi; `burst=1` toleransı sıfırladığı
  için kaçan yuvalar geri gelmiyor ve hız %20 düşüyordu (14.0 sn). `burst=5` + iç hız 96
  ile kayıp %4.
- **Fail closed:** Redis'e ulaşılamazsa gönderim YAPILMAZ. Hata reddedilmiş izinle aynı
  muameleyi görür (sınırlı süre yeniden denenir, sonra satır kuyruğa döner). "Hata
  hâlinde lease'i hemen geri ver" denseydi claim → hata → release döngüsü Postgres'i
  döverdi; beklemek jetonu sender'da tuttuğu için **backpressure'ı semafor sağlıyor**.
- **Tek izin sorgusunun kendi zaman aşımı var** (`rateAllowTimeout`, 1 sn). Canlı
  ölçümde yakalandı: Redis container'ı durdurulduğunda go-redis çağrısı istemci içinde
  bloklanıyor ve tek çağrı bütün bekleme bütçesini yiyordu — ne hata loglanıyor ne
  yeniden deneniyor.
- **Bekleme katlanıyor:** taban 10 ms → tavan 250 ms, full jitter. GCRA'nın
  `RetryAfter`'ı "bir sonraki izne kadar" (~10 ms) ve **bekleyen herkes aynı değeri
  alıyor**; doğrudan beklemek kova boşken saniyede binlerce sorgu üretirdi.
- **İki ayrı pencere** (`worker.Config`): `leaseWindow = RateWaitMax + Provider +
  Result` (bekleme lease altında geçiyor), `drainWindow = Provider + Result` (bekleme
  kapanışta iptal edilebiliyor). `validate` ikisini de boot'ta zorluyor:
  `lease >= 2 × 23s = 46s <= 60s` ✓.
- **`WORKER_CONCURRENCY` limite göre aşırı büyük olmamalı:** throttle altında sender'lar
  izin beklerken lease tutuyor. `concurrency ≈ rate × (sağlayıcı gecikmesi + sonuç
  yazımı)` + pay. "izin beklemesi aşıldı" uyarıları çoğalıyorsa ayar yanlış.
- **Starvation UYGULANMADI, belgelendi.** Yüksek öncelik akışı sürdükçe düşük öncelikli
  satır beklemede kalır; `TestClaimLeavesLowPriorityBehindWhileHighPriorityArrives`
  davranışı sabitliyor. Aging, sıra anahtarına yaş terimi eklemek demek ve claim
  index'ini değiştirir — ölçmeden yapılmaz.
- **Ölçüm:** 1000 SMS → 1 worker 12.1 sn (maks 98/sn), 3 worker 10.9 sn (her tam
  saniyede tam 96, maks 96) — sınır hiç aşılmıyor; sağlayıcı isteği toplam 1000 (çift
  gönderim yok). Kanal izolasyonu: 562 SMS kuyrukta beklerken 5 e-posta 1.09 sn'de gitti.

### Dokümantasyon dili (README adımında sabitlendi)

- **README ve `docs/DESIGN.md` İNGİLİZCE**, geri kalan her şey Türkçe (kod yorumları,
  OpenAPI açıklamaları, API hata mesajları, commit gövdeleri, `docs/PLAN.md`).
  Gerekçe: değerlendirme belgesi İngilizce, ekip uluslararası olabilir. README bu
  ayrımı bir notla açıkça söylüyor — okuyucu Türkçe yorumlara rastladığında şaşırmasın.
- **İki dosya, üç değil:** README değerlendiricinin okuyacağı tek şey olabileceği için
  kendi başına yeterli; uzun gerekçeler DESIGN'a taşındı ve README oradan link veriyor.
- **Ölçüm sayıları uydurulmaz:** README'deki `/metrics` örneği canlı yığından
  alındı (400'lük batch akarken), gereksinim tablosu da PDF metninden çıkarıldı.

### Polish kararları (review bulguları)

- **Panik yanıtı da hata zarfı kullanır:** chi'nin `Recoverer`'ı yerine
  `httpapi.recoverer` — hazırı gövdesiz 500 dönüyor ve yığını ANSI'li olarak stderr'e
  basıyordu. `http.ErrAbortHandler` bilinçli olarak yeniden paniklendirilir.
- **JSON çözümleme hatası çevrilir, aktarılmaz** (`decodeErrorMessage`): ham metin iç
  DTO adlarını sızdırıyor ve onları fiilî sözleşmeye çeviriyordu.
- **`batchTimeout` < `WriteTimeout`** (20s < 30s): write deadline istek header'ıyla
  başlıyor, eşitlik hâlinde 500 zarfı flush edilemiyordu.
- **Sayaç ayarlarının üst sınırı var** (`config.MaxCountValue`): değerler aşağı akışta
  int32'ye dönüşüyor ve Go'da dönüşüm sessizce taşıyor. Yanlış ayar artık açılışta
  durduruyor.
- **Sağlayıcı idle havuzu `len(domain.AllChannels()) × WORKER_CONCURRENCY`:** üç kanal
  havuzu aynı client'ı paylaşıyor. `domain.AllChannels()` bu iki yerin tek kaynağı.
- **Kapanış sessiz olmalı:** reaper/janitor tek `tickLoop`'tan koşuyor ve tick dalının
  içinde `ctx.Err()` kontrolü var; izin hatası kapanışta DEBUG, gerçek arızada ERROR.
  `tickLoop` tick kanalını dışarıdan alıyor — yarışı determinist test etmenin tek yolu
  bu (gerçek `time.Ticker` ile tick select'e varılmadan hazır olmuyor ve test yarışı
  hiç görmüyordu).

### Zamanlanmış gönderim (Scheduled bonusunda sabitlendi)

- **Yeni kolon/migration yok:** `send_at` → `available_at`. Normal gönderim, retry
  gecikmesi ve zamanlanmış gönderim tek mekanizma; claim sorgusu değişmedi.
- **Zamanlanmış kayıt `pending` bekler**, ayrı durum yok → zamanı gelmeden iptal
  edilebilir.
- **Ayrıştırma domain'de** (`validateSendAt`): batch hata anahtarının
  `notifications[1].send_at` olarak öneklenmesi bu sayede bedava.
- **Sınırlar:** dilimli RFC3339 zorunlu, maks 30 gün ileri (`ScheduleMaxHorizon`),
  geçmiş reddedilir ama 1 dk tolerans (`ScheduleClockSkew`) — tolerans içindeki değer
  "hemen" sayılır.
- **`AvailableAt` sıfır = "damgayı DB bassın".** Tekil INSERT
  `COALESCE($10::timestamptz, now())` + `RETURNING available_at`; eskiden uygulama
  saati yazılıyordu ve claim DB saatine baktığı için iki saat farklıysa satır bir süre
  görünmez kalıyordu (review bulgusu F12). Artık tekil ve batch aynı saat kaynağında.
- **Batch:** COPY satır başına varsayılan uygulayamadığı için zamanlanmış satırlar
  COPY'den sonra aynı transaction'da `unnest(uuid[], timestamptz[])` ile TEK UPDATE
  ile düzeltiliyor; hiç zamanlanmış satır yoksa sorgu hiç çalışmıyor.
- **`queue_depth` ikiye ayrıldı:** `notifications_queue_depth` = şimdi gönderilebilir
  iş (alarm buna kurulur), `notifications_deferred_depth` = zamanı gelmemiş iş
  (zamanlanmış veya retry backoff'u). Tek sorgu, `GROUP BY ... available_at > now()`;
  partial index korunuyor (EXPLAIN: `Index Scan`, 18 buffer).
- **Orijinal hedef an retry'dan sonra kaybolur** (`available_at` backoff'a kayar).
  İkinci bir kolon açmak iki saat kaynağı yaratacağı için YAPILMADI; README'de bilinen
  sınır olarak yazılacak. İlk gönderim anı `notification_attempts`'te duruyor.

### API dokümantasyonu (OpenAPI adımında sabitlendi)

- **Spec elle yazılır, koddan üretilmez** (`api/openapi/openapi.yaml`). Üretici araç
  eklemek Go tiplerini sözleşmenin kaynağı yapardı; oysa sözleşme DTO'lardan daha
  fazlasını söylüyor (202'nin gerekçesi, cursor'ın opaklığı, `[from, to)`).
- **Spec `go:embed` ile binary'de** ve `/openapi.yaml`'dan sunuluyor (ETag → 304).
  Paket `internal/` dışında çünkü embed kendi klasörünün dışına çıkamıyor; dışa açılan
  tek şey bir `[]byte`. Klasör `api/openapi` olduğu için paket adı = import yolunun
  son parçası.
- **Swagger UI `/docs`: CDN + sabit sürüm + SRI.** Sürüm etiketi içeriği sabitlemiyor,
  hash sabitliyor; eşleşmezse tarayıcı dosyayı hiç çalıştırmıyor. Bedeli: internetsiz
  ortamda sayfa boş kalır — `/openapi.yaml` yine erişilebilir olduğu için kabul edildi.
  Hash'ler uydurulmaz, `docs.go`'daki `openssl` komutuyla hesaplanır.
- **Doküman sapması testle kapalı** (`internal/httpapi/docs_test.go`): uç listesi
  `chi.Walk` ile, yanıt DTO'larının json etiketleri şema `properties`'leriyle,
  hata kodları `errorCodes` dizisiyle, dizi de `go/parser` ile okunan `ErrorCode`
  sabitleriyle karşılaştırılıyor. Uç eklemek YETMEZ: yeni bir alan da dokümana
  yazılmadan testi geçmez.
- **Uçlar sürümlü tabanın dışında** (`/docs`, `/openapi.yaml`): doküman API'nin bir
  kaynağı değil, API'nin kendisinin açıklaması.
- Dokümandaki `/health/live` gövdesi API'nin kendi yanıtı: `{"status":"ok"}`.
  Yukarıdaki doğrulama satırındaki `{"status":"alive"}` 8081'deki **mockprovider**'a
  ait; ikisi farklı servis.

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
| `internal/ratelimit` | Redis GCRA kovası: kanal başına global hız sınırı |
| `internal/breaker` | Kanal başına devre kesici (sony/gobreaker sarmalayıcısı) |
| `api/openapi` | OpenAPI dokümanı + `go:embed` sarmalayıcısı (`internal/` dışındaki tek paket) |
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
