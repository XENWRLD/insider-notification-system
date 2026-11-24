# Adım 8 — Retry politikası + kanal başına circuit breaker

## 0. Bu adımda geçen yeni Go kavramları

| Kavram | Nerede | Bir cümle |
|---|---|---|
| **Jenerik tip** (`CircuitBreaker[T]`) | `internal/breaker` | gobreaker tipi `Execute(func() (T, error))` alıyor; biz `T = any` seçip kendi sade arayüzümüzün (`Do(func() error) error`) altına saklıyoruz. Testlerdeki `ptr[T any]`'den farkı: orada jenerik **fonksiyon**, burada jenerik **tip** |
| **Closure** (kapanış) | `brk.Do(func() error { ... })` | İçteki anonim fonksiyon dıştaki `resp`, `elapsed`, `sendErr` değişkenlerine **yazıyor**. Breaker'a yalnızca `error` dönüyoruz; gönderim sonucunu closure dışarı taşıyor |
| **Fonksiyon değer olarak** | `ReadyToTrip func(Counts) bool`, `OnStateChange func(...)` | Davranışı veri gibi geçirmek: "kaç hatada aç" kararını kütüphaneye fonksiyon olarak veriyoruz |
| **Değişmez map, kilitsiz** | `Engine.breakers map[domain.Channel]Breaker` | `New` içinde bir kez kurulup bir daha yazılmıyor → eşzamanlı okuma güvenli, mutex gerekmez. (Yazılsaydı `sync.Map` ya da mutex şart olurdu) |
| **`iota` ile sayısal enum** | `type action int` | `type Status string`'i gördük; `iota` ardışık sabit üretir ve dışa sızmayan iç kararlar için uygun |
| **Sentinel hata ↔ tipli hata** | `errors.Is(err, breaker.ErrOpen)` ↔ `errors.As(err, &statusErr)` | `Is` **değer** arar (tek bir paylaşılan hata nesnesi), `As` **tip** arar (içinden alan okunacaksa). İkisi bu adımda yan yana kullanılıyor |
| **`net.Error` arayüzü** | `isTimeout(err)` | Hataya "sen zaman aşımı mısın?" diye soran arayüz: `var ne net.Error; errors.As(err, &ne) && ne.Timeout()` |
| **Durum makinesi** | breaker | Aşağıdaki diyagram |

### Breaker durum makinesi

```
                    ardışık hata >= eşik (5)
         ┌───────────────────────────────────────────┐
         │                                           ▼
   ┌───────────┐                              ┌───────────┐
   │  CLOSED   │                              │   OPEN    │
   │ her istek │                              │  HİÇBİR   │
   │   geçer   │                              │istek geçmez│
   └───────────┘                              └───────────┘
         ▲                                           │
         │ sonda BAŞARILI                            │ OpenTimeout (30 sn)
         │                                           ▼
         │                                    ┌─────────────┐
         └────────────────────────────────────│  HALF-OPEN  │
                                              │ TEK sonda   │
              sonda BAŞARISIZ ───────────────▶│   geçer     │
              (tekrar OPEN)                   └─────────────┘
```

**Bizim eklediğimiz kritik kural:** breaker `CLOSED` değilken **claimer o kanaldan iş
çekmeyi bırakır**. Yani `OPEN` durumunda mesajlar ne deneme hakkı ne rate limit izni
harcar — satır `pending`/`retrying` olarak kuyrukta, iptal edilebilir hâlde bekler.
`HALF-OPEN`'da claimer **tek satır** çeker (sonda).

```
  Claimer                          Sender
  ───────                          ──────
  state == OPEN?                   state == OPEN? ──▶ kuyruğa dön (izin HARCAMADAN)
    evet → claim YOK, backoff           │
    HALF-OPEN → limit 1                 ▼
    CLOSED → limit = boş sender    awaitPermit (Adım 7)
                                        │
                                        ▼
                                   brk.Do(send)  ──▶ ErrOpen ise kuyruğa dön
                                        │
                                        ▼
                                   classify → sonuç yaz VEYA kuyruğa dön
```

---

## Context

Adım 6'da `classify.go`'ya bilinçli bir **dikiş** bırakıldı: 2xx dışı her sonuç aynı
muameleyi görüyor (sabit 5 sn gecikmeyle `retrying`, hak tükenince `failed`). Dosyanın
başındaki "ADIM 8 DİKİŞİ" yorumu bu adımda ne olacağını yazıyor.

Adım 8 iki şey ekliyor:

1. **Gerçek sınıflandırma:** hatanın mesaja mı, kanala mı, yoksa sağlayıcının anlık
   yoğunluğuna mı dair olduğunu ayırmak ve her birine farklı davranmak.
2. **Kanal başına circuit breaker:** sağlayıcı çökmüşken kanalı tamamen durdurmak, ve
   —en önemlisi— bunu yaparken **deneme hakkı ve rate limit izni harcamamak**.

`internal/breaker` paketi yok, `sony/gobreaker` bağımlılığı `docs/PLAN.md §3`'te planlı
ama henüz eklenmemiş. Repoda hiçbir yerde breaker kodu yok.

**Keşifte çıkan çelişki:** `docs/PLAN.md:174` "Circuit breaker: kanal başına; açıkken o
kanaldan iş çekilmez" diyor (→ `claimLoop` değişir), ama `classify.go:19-25` ve
`CLAUDE.md:25` "yalnızca `classify.go` gövdesi değişecek" diyor. Bu adımın kuralı
birincisini doğruluyor: izin ve deneme harcamamak için claim'in durması **zorunlu**.
O iki yorum bu adımda düzeltilecek.

---

## 1. Sınıflandırma tablosu (kararlaştırıldı)

Üç eksen var ve bunlar **bağımsız**: (a) sonuç nedir, (b) deneme hakkı harcanır mı,
(c) breaker tetiklenir mi.

| Sağlayıcı sonucu | Sonuç | Deneme sayılır? | `available_at` | Breaker |
|---|---|---|---|---|
| 2xx (+ `DecodeError`) | `sent` | evet (claim'de arttı) | — | **başarı** (sayaç sıfırlanır) |
| **400, 422** ve diğer tanınmayan 4xx | **`failed`** (hemen) | evet | — | hayır |
| **429** | kuyruğa dön | **HAYIR** | `now + rateLimitDelay` (aşağıda) | **sayılmaz** (excluded) |
| **timeout** (belirsiz) — devre **CLOSED** | `retrying` | **EVET** | `now + backoff` | **EVET** |
| **timeout** (belirsiz) — devre **OPEN/HALF-OPEN** | `retrying` | **EVET** | `now + max(backoff, 2 × BreakerOpenTimeout)` | **EVET** |
| **5xx / 401,403,404 / bağlantı hatası** — çağrıdan sonra devre **CLOSED** | `retrying` | **EVET** | `now + backoff` | **EVET** |
| **5xx / 401,403,404 / bağlantı hatası** — çağrıdan sonra devre **OPEN veya HALF-OPEN** | kuyruğa dön | **HAYIR** | `now + 2 × BreakerOpenTimeout` | **EVET** |
| `brk.Do` → `ErrOpen` (çağrı hiç yapılmadı) | kuyruğa dön | **HAYIR** | `now + 2 × BreakerOpenTimeout` | — |
| `max_attempts` tükendi | `failed` | — | — | — |

**Son iki satır tek bir kuralın iki yüzü:** "denemeyi saymama" muafiyeti yalnızca
**breaker devraldığında** geçerli. Bu ayrımın neden zorunlu olduğu aşağıda
("zehirli mesaj") yazılı.

### Gerekçeler

**400 / 422 → hemen `failed`.** Mesajın kendisi hatalı; aynı baytları 5 kez göndermek
sağlayıcıyı meşgul etmekten başka bir şey yapmaz. Tanınmayan diğer 4xx'ler de buraya
düşüyor: 4xx'in anlamı "isteğin yanlış", ve bilinmeyen bir kodu kanal arızası saymak
tek bir tuhaf yanıtın bütün kanalı durdurmasına yol açardı.

**401 / 403 / 404 → kuyruğa dön + breaker (önerim, gerekçesiyle).** Bunlar **mesajın
değil yapılandırmanın** hatası: yanlış/süresi geçmiş API anahtarı, IP izin listesinde
olmama, yanlış `PROVIDER_URL`. İki naif seçeneğin ikisi de kötü:

- **`failed`** deseydik: anahtar rotasyonu sırasında kuyruktaki her mesaj kalıcı olarak
  ölürdü. Mesajlar geçerliydi, **sistem** bozuktu — ve `failed` terminal olduğu için
  anahtar düzeltildikten sonra onları gönderen bir şey yok.
- **Sadece `retrying`** deseydik: bozuk anahtarla her mesaj 5 deneme harcayıp yine
  `failed` olurdu, sağlayıcı 5× dövülürdü.

Doğru cevap ikisi değil, **breaker**: hata kanalın bütününe dair olduğu için breaker
~5 ardışık 401'de açılır, claimer o kanaldan çekmeyi bırakır, **deneme harcanmaz**,
mesajlar kuyrukta bekler. Operatör anahtarı düzeltince yarı açık sonda başarılı olur ve
birikmiş kuyruk olduğu gibi akar. Breaker'ın var olma sebebi tam olarak budur.

**429 → kuyruğa dön, deneme SAYILMAZ, breaker'da SAYILMAZ (excluded).** 429 "kesin
kabul edilmedi" demek (çift gönderim riski yok) ve sürekli 429 görmek **bizim ayar
hatamız**:
Adım 7'deki `RATE_LIMIT_PER_SECOND` sağlayıcının sınırından yüksek. Mesajın suçu
olmadığı için deneme harcatmak yanlış olurdu — harcasaydık, limitimiz yüksek olduğunda
mesajlar 5 kez 429 alıp **kalıcı `failed`** olurdu. Breaker'a da dokunmuyor: 429 açık
bir `Retry-After` rehberi veren anlık bir throttle sinyali, sağlayıcı çöküşü değil;
`Retry-After` 1 sn derken kanalı 30 sn kapatmak aşırı tepki olurdu. Sürekli 429'un
doğru düzeltmesi README'de: **`RATE_LIMIT_PER_SECOND`'ı düşür.**

Kritik ayrıntı: 429 breaker'da **başarı sayılmıyor, hiç sayılmıyor** (`IsExcluded`).
Başarı sayılsaydı ardışık hata sayacını **sıfırlardı** ve karışık bir kesintide
(sağlayıcı kâh 503 kâh 429 dönüyor) devre çok geç açılırdı — 429'lar araya girdikçe
5xx'ler hiç üst üste gelmezdi. Dışlanınca devre 5xx sayısına göre açılıyor, 429'lar
kararı ne hızlandırıyor ne geciktiriyor.

**5xx / 401-403-404 / bağlantı hatası → devrenin durumuna bağlı.** Hepsi "kesin
ulaşmadı" (sağlayıcı hata yanıtı verdi ya da bağlantı hiç kurulamadı), yani çift
gönderim riski yok. Ama denemeyi *koşulsuz* saymamak **zehirli mesaj açığı** yaratıyor:

> Sağlayıcı yalnızca **tek bir mesaja** hep 500 dönüyor (ör. onun alıcısını kendi
> doğrulayıcısı reddediyor ama 400 yerine 500 veriyor), diğer mesajlar 202 alıyor.
> `ReadyToTrip` **ardışık** hataya baktığı için araya giren başarılar sayacı sıfırlar
> → **breaker hiç açılmaz**. O mesaj `delay 0` ile hemen yeniden çekilebilir olur,
> `attempt_count` hiç artmadığı için `max_attempts`'a asla ulaşmaz ve **sonsuza kadar**
> döner — her turda bir rate limit izni yiyerek.

Bu yüzden muafiyet, tam olarak tasarlandığı duruma daraltılıyor: **breaker devraldıysa**
deneme sayılmaz, devre hâlâ kapalıysa bu sıradan bir geçici hatadır ve normal retry
uygulanır (deneme sayılır, backoff uygulanır, deneme kaydı yazılır, `max_attempts`'ta
`failed`).

Ayrım tek bir kontrolle yapılıyor: **`brk.Do`'dan SONRA okunan devre durumu**.
İki durum da aynı koşula indiriyor —
*(a)* çağrı yarı açık sondaysa ve başarısız olduysa durum `open`'a geçmiş olur;
*(b)* kapalı durumdaki hata eşiği doldurup devreyi açtıysa durum yine `open`'dır.
Dolayısıyla `postState != StateClosed` ikisini birden kapsıyor.

**İkinci açık: tek mesaj kanalı kilitleyebiliyor.** Requeue gecikmesi 0 bırakılsaydı
(satır kuyruk sırasını korusun diye) trafik azken şu olurdu:

> Kuyrukta **yalnızca** zehirli mesaj var; araya başarı girmediği için 5 ardışık 500
> devreyi açar. `DeferLeases(0)` `available_at`'i koruduğundan satır kuyruğun başında
> kalır → her yarı açık sondada **yine aynı mesaj** seçilir → sonda hep başarısız →
> **devre hiç kapanmaz.** Sabah sağlıklı mesajlar gelse bile tek bir mesaj kanalı
> süresiz durdurur.

**Gecikme neden `BreakerOpenTimeout` değil `2 ×`:** tek katlık erteleme yetmiyor ve
sebebi sıralama anahtarının **ilk** terimi. `claim.go:38` sırası
`ORDER BY priority_rank, available_at, id` — yani öncelik `available_at`'ten **önce**
geliyor:

> Devre `R` anında açılır, satır `R+δ+30s`'e ertelenir (δ = milisaniyeler), yarı açık
> `R+30s`'te başlar. Claimer poll gecikmesiyle (≤ `PollMaxInterval`) baktığında satır
> **çoktan çekilebilir** durumda. Zehirli mesaj `high` ve sağlıklılar `normal`/`low`
> ise `priority_rank` onu yine en öne koyar → **sonda yine aynı mesajla yapılır.**
> "Sonradan gelenlerin `available_at`'i daha eski" argümanı yalnızca aynı öncelik
> sınıfı içinde geçerli; önceliği atlıyordu.

`2 × BreakerOpenTimeout` satırın bir sonraki yarı açık pencereyi **kesin kaçırmasını**
sağlıyor: `R+60s`'te çekilebilir olurken sonda `R+30s`'te başka bir satırla yapılıyor.
Sağlıklı bir sonda devreyi kapatıyor, ardından zehirli mesaj **kapalı** devrede
sayılıp `max_attempts`'ta `failed` oluyor.

**Aynı taban timeout yolunda da geçerli.** Timeout "belirsiz" olduğu için denemeyi
saymaya devam ediyor, ama devre devraldığında gecikmesi
`max(backoff, 2 × BreakerOpenTimeout)` oluyor. Yoksa şu olurdu: `high` öncelikli bir
satır timeout alır, devre açılır, satıra 2 sn backoff yazılır; yarı açıkta satır
çoktan çekilebilir ve önceliği yüzünden **yine sondaya** çıkar → her döngüde aynı satır
sondalanır ve 5 denemesi bitene kadar (≈5 × `BreakerOpenTimeout`) kanalın sonda yuvası
tek satır tarafından tutulur. Devre açıkken daha kısa bir gecikmenin hiçbir faydası
yok: satır o sırada zaten çekilemiyor.

Tam kesintide maliyet aynı: sondalar deneme harcamıyor, yalnızca devre açılana kadar
eşik-1 (≈4) mesaj birer deneme harcıyor. İki bedel var ve ikisi de kabul edilebilir:
*(a)* sağlayıcı toparlandıktan sonra en son ertelenen satırlar en çok
`2 × BreakerOpenTimeout` kadar daha bekler; *(b)* kuyrukta ertelenmiş satırdan başka
hiçbir şey yoksa ilk sonda o pencerenin sonuna kadar gecikir — ama o durumda zaten
bekleyen başka iş yok.

### 429 gecikmesi

`attempt_count` geri alındığı için `backoffWindow(attempt)` **büyümüyor**; attempt 1'de
pencere `RetryBaseDelay` (2 sn) ve `jitter` onu `(0, 2s]` aralığına indirip **sıfıra
yakın** bir değer üretebilir → 429 fırtınasında satır neredeyse beklemeden dönerdi.

```
Retry-After VAR  →  gecikme = min(RetryAfter, RetryMaxDelay)     // sağlayıcıya uy, ama tavanla
Retry-After YOK  →  gecikme = RetryBaseDelay + jitter(RetryBaseDelay)   // [2s, 4s]
```
Taban `RetryBaseDelay`: sıfıra yakın gecikme olamıyor. Tavan `RetryMaxDelay`: sağlayıcı
`Retry-After: 86400` derse satır bir gün kaybolmuyor.

**timeout → `retrying`, deneme SAYILIR, backoff uygulanır.** Burada tek fark bu ve
sebebi kritik: timeout'ta **mesajın gidip gitmediği bilinmiyor** (yanıt gelmedi ama SMS
gönderilmiş olabilir). Denemeyi saymazsak aynı satır her yarı açık sondada yeniden
seçilip **sınırsız** gönderilebilir (30 sn'lik breaker timeout'uyla saatte ~120 kez).
Deneme sayılınca satır `max_attempts`'ta durur: at-least-once'ın üst sınırı korunur.

> **Bilinçli muhafazakârlık:** dial/TLS zaman aşımları da "timeout" sayılıyor, oysa
> orada istek hiç gönderilmedi (çift gönderim riski yok) ve aslında saymamak
> gerekirdi. Ayrımı yapmak hata zincirinde `*net.OpError{Op:"dial"}` aramak demek;
> kazanç marjinal (breaker zaten açılıyor, sonda başına 1 deneme), karmaşıklık gerçek.
> Hata yönü güvenli tarafa düşüyor, bu yüzden basit kural seçildi ve belgelendi.

### Deneme sayılmayan yolların bedeli

`attempt_count` geri alındığı için o denemenin `notification_attempts` kaydı
**yazılamaz**: kayıt `attempt_no = attempt_count` ile yazılıyor, geri alınan değere
kayıt yazılsa bir sonraki claim aynı `attempt_no`'yu üretip
`UNIQUE (notification_id, attempt_no)`'yu ihlal ederdi.

Telafi: bu yollar `WarnContext` ile loglanır (`correlation_id`, `notification_id`,
`channel`, `status_code`, `error`, `requeue_delay`) **ve** `last_error` satıra yazılır
— yani `GET /notifications/{id}` "neden beklediğini" gösterir, yalnızca deneme
tutanağı oluşmaz.

### Backoff

```
backoffWindow(attempt) = min(RetryBaseDelay × 2^(attempt-1), RetryMaxDelay)
gecikme                = jitter(backoffWindow(attempt))        // (0, pencere]
```
Varsayılanlar `2s` ve `5m`: deneme 1→2s, 2→4s, 3→8s, 4→16s, 5→32s.
`backoffWindow` **saf ve tam değerle** test edilir; jitter uygulanan gecikme için
yalnızca aralık iddia edilir (projedeki anti-flake kuralı).

---

## 2. `internal/breaker` paketi (yeni)

`internal/ratelimit`'in kalıbının aynısı: dış kütüphaneyi kendi arayüzümüzün ve kendi
tiplerimizin altına saklıyoruz, **politika çağırana kalıyor**.

| Dosya | Sorumluluk |
|---|---|
| `breaker.go` | `State`, `ErrOpen`, `Breaker` arayüzü, `Config`, `AlwaysClosed()` |
| `gobreaker.go` | `New(Config) (Breaker, error)` → `sony/gobreaker/v2` sarmalayıcısı |
| `breaker_test.go` | Birim testler (`-short`, Docker'sız) |

```go
type State string

const (
    StateClosed   State = "closed"
    StateHalfOpen State = "half-open"
    StateOpen     State = "open"
)

// ErrOpen, çağrının devre kesici tarafından reddedildiğini bildirir.
// Sentinel: çağıran errors.Is ile tanır.
var ErrOpen = errors.New("devre kesici açık: sağlayıcıya gidilmedi")

type Breaker interface {
    // Do, fn'i devre kesicinin altında çalıştırır. Devre açıksa fn HİÇ çağrılmaz
    // ve ErrOpen döner. fn'in döndürdüğü hata "başarısızlık" sayılır.
    Do(fn func() error) error

    // State, claimer'ın o kanaldan iş çekip çekmeyeceğine karar vermesi için.
    State() State
}

type Config struct {
    Name             string        // log/metrik adı (kanal)
    FailureThreshold int           // kaç ardışık hatada açılsın
    OpenTimeout      time.Duration // açık → yarı açık
    OnStateChange    func(name string, from, to State)

    // IsFailure, hatanın devreyi ilgilendirip ilgilendirmediğini söyler.
    //
    // Üç sonuç var, ikisi değil:
    //   nil hata            → BAŞARI    (ardışık hata sayacı sıfırlanır)
    //   IsFailure true      → HATA      (sayaç artar, eşikte devre açılır)
    //   IsFailure false     → SAYILMAZ  (sayaca hiç dokunulmaz)
    IsFailure func(error) bool
}
```

Kararlar:

- **`Do(fn func() error) error`**, gobreaker'ın jenerik `Execute`'ı değil: dönen değeri
  closure dışarı taşıyor, böylece `internal/breaker` `provider` tiplerini hiç bilmiyor.
- **Üçüncü sonuç (`SAYILMAZ`) gobreaker'ın `IsExcluded`'ına eşleniyor**; `IsSuccessful`
  ise `err == nil` olarak kalıyor:
  ```go
  IsSuccessful: func(err error) bool { return err == nil },
  IsExcluded:   func(err error) bool { return err != nil && !cfg.IsFailure(err) },
  ```
  Closure, kanalın **sağlıklı olduğunu kanıtlayan** sonuçlarda `nil`, diğerlerinde
  gerçek hatayı döndürüyor; üç yollu karar `internal/worker`'daki
  `breakerOutcomeFor` fonksiyonunda (§4.1).

  > **gobreaker v2 doğrulaması (kaynaktan okundu):** `Settings`'te `IsExcluded` var ve
  > `afterRequest` dışlanan hatada `onExclusion`'a girip **`onSuccess`/`onFailure`'ın
  > ikisini de atlıyor** — `ConsecutiveFailures`/`ConsecutiveSuccesses` değişmiyor ve
  > durum geçişi olmuyor. Yarı açık kotası `counts.validRequests() >= maxRequests` ile
  > kontrol ediliyor (`Requests` değil), yani dışlanan bir sonda kotayı **tüketmiyor** —
  > aksi hâlde 429 dönen tek bir sonda devreyi yarı açıkta kilitlerdi.
  > İkisi de okumaya bırakılmıyor, testle sabitleniyor:
  > `TestExcludedErrorDoesNotResetFailureCounter`,
  > `TestExcludedProbeDoesNotConsumeHalfOpenSlot`.
- **`MaxRequests = 1` sabit** (gobreaker'da 0 da 1 demek, açıkça 1 yazılacak): yarı
  açıkta **tek sonda** tasarımın parçası, ayar yapılmıyor.
- **`Interval = 0`**: kapalı durumda sayaçları periyodik temizlemeye gerek yok, çünkü
  `ReadyToTrip` **ardışık** hataya bakıyor ve tek başarı sayacı sıfırlıyor.
- **`AlwaysClosed()`**: Adım 6/7'nin mevcut testleri breaker kurmak zorunda kalmasın
  (`ratelimit.Unlimited()` ile aynı rol).

### 2.1 Breaker YEREL: her kopyada ayrı durum

Rate limiter Redis'te paylaşılıyor ama breaker **process içinde** duruyor; üç worker
kopyası üç ayrı durum makinesi işletiyor. Sonuçları açıkça:

1. **Kesinti başında her kopya kendi eşiğini doldurur.** Varsayılanlarla kopya başına
   eşik-1 ≈ 4 mesaj birer deneme harcar → 3 kopyada **~12 mesaj 1 deneme kaybeder**.
   Bu sayı kopya sayısıyla doğrusal artıyor (README'ye yazılacak).
2. **Sondalar 3× sık.** Her kopya `BreakerOpenTimeout`'ta bir sonda atıyor → kesintiden
   çıkış daha hızlı fark ediliyor, karşılığında sağlayıcı pencere başına 1 yerine 3
   sonda alıyor. Üçü de deneme harcamadığı için mesaj kaybı yok.
3. **Kopyalar birbirinden farklı düşünebilir** (biri açık, ikisi kapalı). Örneğin tek
   bir kopyanın ağ yolu bozuksa yalnızca o kopya kendini kapatır — ki doğrusu bu:
   her kopya sağlayıcıyı **kendisinden** korur.

**Neden paylaşılmıyor:** rate limiter'ın durumu bir **sayaç** — değişmeli (commutative),
kim hangi sırayla artırırsa aynı sonuç, bu yüzden tek bir Redis anahtarı yeterli ve
paylaşmak **zorunlu** (aşım kabul edilemez). Breaker'ın durumu ise **sıraya duyarlı bir
durum makinesi**: "ardışık 5 hata" ifadesi kopyalar arası bir olay sıralaması
gerektiriyor; bunu paylaşmak dağıtık uzlaşı problemi. Karşılığında kazanç küçük, çünkü
**fazla korumak zararsız, az korumak ise `eşik × kopya` ile sınırlı.**

---

## 3. `internal/store` — gecikmeli, deneme saymayan geri verme

Bugün `ReleaseLeases(ctx, holds)` var: `status='retrying'`, `attempt_count-1`, lease
NULL, **`available_at` korunur**, `last_error` yazılmaz. Kapanış yolu bunu kullanıyor
ve **değişmiyor** (satır kuyruk sırasını kaybetmemeli).

Yeni method:

```go
// DeferLeases, denemeyi saymadan işi kuyruğa geri verir, sebebini kaydeder ve
// istenirse ileri tarihler.
//
// delay 0 ise available_at KORUNUR (satır kuyruk sırasını kaybetmez); breaker
// zaten bekletiyor. 429'da delay = Retry-After.
func (s *Store) DeferLeases(
    ctx context.Context, holds []LeaseHold, delay time.Duration, reason string,
) (int64, error)
```

```sql
UPDATE notifications
SET status = 'retrying',
    attempt_count = attempt_count - 1,
    last_error = $3,
    -- delay 0 iken available_at'e dokunulmuyor: satır sıradaki yerini korusun.
    available_at = CASE WHEN $4::interval > interval '0'
                        THEN now() + $4::interval
                        ELSE available_at END,
    lease_token = NULL, lease_expires_at = NULL
WHERE (id, lease_token) IN (SELECT * FROM unnest($1::uuid[], $2::uuid[]))
  AND status = 'processing'
```

- `attempt_count`'un azaldığı **ikinci** yer (ilki `ReleaseLeases`). `CHECK
  (attempt_count >= 0)` güvende: claim artırmıştı ve lease hâlâ bizde.
- Deneme kaydı **yazılmıyor** — gerekçe §1'de.
- `Queue` arayüzüne bir satır ekleniyor; `stubQueue` ve `cmd/worker` zaten `*store.Store`
  geçtiği için başka değişiklik yok.

---

## 4. `internal/worker` değişiklikleri

### 4.1 `classify.go` — dikiş dolduruluyor

`classify` artık yalnızca "sonuç" değil **eylem** döndürüyor, çünkü bazı yollarda
veritabanına sonuç yazılmıyor:

```go
type action int

const (
    actionWriteResult action = iota // sent | failed | retrying yaz
    actionRequeue                   // denemeyi saymadan kuyruğa dön
)

type decision struct {
    action action

    result store.NotificationResult // actionWriteResult

    requeueDelay  time.Duration     // actionRequeue (0 = available_at korunur)
    requeueReason string            // actionRequeue → last_error

    // breakerFailure, hatanın KANALA dair olup olmadığı. Her iki action'da anlamlı.
    breakerFailure bool
}

func classify(
    notification *domain.Notification,
    resp provider.SendResponse,
    sendErr error,
    cfg retryPolicy,                // RetryBaseDelay, RetryMaxDelay
    elapsed time.Duration,
    breakerState breaker.State,     // brk.Do'dan SONRAKİ durum
) decision
```

`breakerState` parametresi zehirli mesaj açığını kapatıyor: "kesin ulaşmadı" sınıfı
yalnızca devre `closed` DEĞİLKEN denemeyi atlıyor (bkz. §1). Fonksiyon saf kalıyor —
durumu okumak değil, parametre olarak almak.

Yardımcılar (hepsi saf, tek tek test edilir):

```go
func backoffWindow(attempt int, base, max time.Duration) time.Duration
func isTimeout(err error) bool                  // net.Error.Timeout() || DeadlineExceeded
func statusClass(code int) ...                  // sent | permanent | config | throttle | transient

// breakerOutcome, bir gönderim sonucunun DEVRE açısından ne ifade ettiği.
// Üç değer var, iki değil — "hata" ile "kanal arızalı" aynı şey değil.
type breakerOutcome int

const (
    breakerHealthy breakerOutcome = iota // kanal sağlıklı: sayaç sıfırlanır, sonda devreyi KAPATIR
    breakerIgnored                       // kanala dair bilgi yok: sayaçlara dokunulmaz
    breakerFailed                        // kanal arızalı: ardışık hata sayacı artar
)

func breakerOutcomeFor(sendErr error) breakerOutcome
```

`breakerOutcomeFor` eşlemesi:

| Sonuç | Outcome | Neden |
|---|---|---|
| `nil` (2xx) | `breakerHealthy` | mesaj kabul edildi |
| **`DecodeError`** (2xx, zarf okunamadı) | **`breakerHealthy`** | mesaj **kabul edildi**; yalnızca yanıtı çözemedik — kanal sağlıklı |
| 400 / 422 / tanınmayan 4xx | `breakerHealthy` | sağlayıcı ayakta ve düzgün bir ret üretti: kanalın sağlığının kanıtı |
| **429** | `breakerIgnored` | ne sağlık ne arıza kanıtı; sayacı sıfırlamamalı (§1) |
| 5xx / 401 / 403 / 404 / `TransportError` | `breakerFailed` | kanala dair arıza |

> **`DecodeError` neden ayrıca vurgulanıyor:** closure'ın "gerçek hatayı döndür"
> biçimi `DecodeError`'ı da hataya çevirdiği için `IsFailure` false → **excluded**
> olurdu. Sonuç: mesaj gönderilmiş olmasına rağmen yarı açık sonda devreyi
> **kapatmaz**, kapalı durumda da ardışık hata sayacını sıfırlamaz. Gövdesi JSON
> olmayan bir sağlayıcıda (webhook.site HTML döndürebiliyor) devre süresiz yarı açık
> kalırdı. Bu yüzden closure **`breakerHealthy` olan her sonuçta `nil` döndürüyor.**
> Test: `TestDecodeErrorProbeClosesBreaker`.

`isTimeout` **varsayıma değil ölçüme dayanacak**: gerçek `httptest` sunucusu + gerçek
`provider.Client` ile üretilen timeout hatası üzerinde test edilecek (`http.Client.Timeout`
ile `context.DeadlineExceeded` zincirinin nasıl sarıldığına dair tahmin yürütmek yerine).

### 4.2 `send.go` — iki breaker kapısı

```
handle(ctx, n):
  1. lease token kontrolü                                   (mevcut)
  2. ERKEN KAPI: brk.State() == Open  →  DeferLeases(breakerDelay) + return
     (izin HENÜZ alınmadı: breaker açıkken rate limit izni harcanmıyor)
     HalfOpen GEÇER: sonda bu kapıdan geçmek zorunda.
  3. awaitPermit                                            (mevcut, Adım 7)
  4. brkErr := brk.Do(func() error {
         resp, elapsed, sendErr = e.send(ctx, n)   // closure dışarı yazıyor
         if breakerOutcomeFor(sendErr) == breakerHealthy {
             return nil        // 2xx, DecodeError, 400/422 → devre için BAŞARI
         }
         return sendErr        // 429 → excluded, 5xx/auth/transport → failure
     })
     errors.Is(brkErr, breaker.ErrOpen) → DeferLeases(breakerDelay) + return
     (yarış: izin beklerken devre açılmış ya da sonda kotası dolmuş.
      İzin yandı, satır kaybolmadı.)

     breakerDelay = 2 × BreakerOpenTimeout — classify'ın breaker yolundakiyle AYNI.
     İlk taslakta bu yol 0 gecikme kullanıyordu; aynı satırın bir sonraki sondada
     yeniden seçilmesine yol açtığı için birleştirildi.
  5. state := brk.State()        ← çağrıdan SONRAKİ durum
  6. decision := classify(n, resp, sendErr, policy, elapsed, state)
  7. logOutcome(...)
  8. switch decision.action:
       actionWriteResult → writeResult(...)        (mevcut)
       actionRequeue     → deferLease(...)         ← YENİ
```

5. adım şart: devre `closed` kaldıysa 500 alan mesaj sıradan bir geçici hata gibi
sayılmalı (zehirli mesaj açığı, §1).

İki kapı neden gerekli: 2. adım **izin harcanmasını** engelliyor (asıl kural),
4. adımdaki `ErrOpen` ise izin beklerken durumun değişmesi yarışını yakalıyor.

### 4.3 `channel.go` — claimer kapısı (asıl kural)

```go
tokens := p.acquireTokens(ctx)
if tokens == 0 { return }

limit := tokens

switch state := p.engine.breakerFor(p.channel).State(); state {
case breaker.StateOpen:
    // Devre açık: claim YOK. Satırlar kuyrukta kalır → ne deneme ne izin harcanır,
    // ve POST /cancel çalışmaya devam eder (satır processing'e geçmiyor).
    p.releaseTokens(tokens)            // backoff BEKLEMESİNDEN ÖNCE (Adım 6 kuralı)
    if !poll.wait(ctx) { return }
    continue

case breaker.StateHalfOpen:
    // Yarı açıkta TEK sonda. Uçuşta iş varsa HİÇ claim etmiyoruz.
    //
    // Neden: başarılı claim'den sonra bu döngü poll.reset() + continue ile
    // BEKLEMEDEN dönüyor. Sonda uçuştayken (≤ ProviderTimeout) her turda bir
    // satır daha çekilse, sender erken kapıyı geçer (durum Open değil, HalfOpen),
    // izin alır ve brk.Do'da ErrTooManyRequests→ErrOpen yer. Tur ~3 ms sürdüğü
    // için saniyede yüzlerce kez: PAYLAŞILAN Redis bütçesinin tamamı (diğer
    // kopyaların gönderimleri dahil) ve ~600 UPDATE/sn boşa gider.
    if tokens+len(p.sem) < p.engine.cfg.Concurrency {
        // Kanalda uçuşta iş var (sonda ya da devre açılmadan önce başlamış
        // gönderimler). Sonda bitene kadar bekle.
        p.releaseTokens(tokens)
        if !poll.wait(ctx) { return }
        continue
    }

    if limit > 1 {
        p.releaseTokens(limit - 1)
        limit = 1
    }
}

claimed, err := p.engine.queue.ClaimNotifications(claimCtx, p.channel, limit, lease)
```

`tokens + len(p.sem) < Concurrency` koşulu "kanalda uçuşta iş var" demek: `tokens`
az önce semafordan alındı, `len(p.sem)` kalan boşlar; ikisinin toplamı `Concurrency`'den
küçükse fark sender'ların elindedir. Okuma yarışa açık ama **hata yönü güvenli**: eski
bir düşük değer bir claim'i atlar (zararsız), yüksek değer en fazla bir fazla satır
çeker ve o satır bir kez `ErrOpen` yolundan geri döner — döngü kurulamıyor çünkü
sonrasında `poll.wait` var.

Jeton muhasebesi Adım 6'nın kuralını aynen koruyor: her jetonun tek sahibi var ve geri
verme **backoff beklemesinden önce** yapılıyor. `runPoolUntil`'in "kapanışta
`len(sem) == Concurrency`" iddiası bozulmuyor.

Durum logu: her pollda loglamak yerine `OnStateChange` callback'i kullanılıyor
(geçiş başına bir `Warn`/`Info`); pollda yalnızca `Debug`.

### 4.4 `Engine` + `Config`

```go
type Engine struct {
    cfg      Config
    queue    Queue
    sender   Sender
    limiter  ratelimit.Limiter
    breakers map[domain.Channel]breaker.Breaker  // New'de kurulur, bir daha yazılmaz
    logger   *slog.Logger
}

func New(cfg Config, queue Queue, sender Sender, limiter ratelimit.Limiter,
    newBreaker func(domain.Channel) (breaker.Breaker, error),
    logger *slog.Logger) (*Engine, error)
```

`newBreaker` bir **fabrika fonksiyonu** olarak alınıyor: üretimde `breaker.New(...)`
geçilir, testte açık/yarı açık durumları determinist kuran bir stub. Alternatif olarak
hazır `map` geçirmek testte kanal eklemeyi zorlaştırırdı.

`Config` değişiklikleri:

| Alan | Durum |
|---|---|
| `FixedRetryDelay` | **kaldırılıyor** (Adım 6 yer tutucusu); `validate` tablosundan da çıkar |
| `RetryBaseDelay` | yeni, `2s` |
| `RetryMaxDelay` | yeni, `5m` |
| `ClaimTimeout` | yeni, `3s` (bkz. §5) |
| `BreakerFailureThreshold` | yeni, `5` |
| `BreakerOpenTimeout` | yeni, `30s` |

`validate` eklemeleri: dördü de pozitif; `RetryMaxDelay >= RetryBaseDelay`.
**Lease/drain formülleri değişmiyor** — breaker beklemesi lease altında geçmiyor
(claim hiç yapılmıyor), izin beklemesi zaten `leaseWindow`'da.

### 4.5 `cmd/worker/main.go`

`breaker.New` fabrikası kuruluyor; `OnStateChange` log'u `observability` logger'ına
bağlanıyor (kanal adı + eski/yeni durum, `Warn` açılışta, `Info` kapanışta).

---

## 5. Açık soru: claim sorgusunun kapanışta iptal edilmesi

**Sorun:** `claimLoop` bugün `ClaimNotifications`'a iptal edilebilir `ctx` geçiyor.
SIGTERM claim'in ortasında gelirse pgx sorguyu iptal eder; ama `UPDATE` sunucuda
**commit olmuş olabilir** (iptal isteği commit ile yarışıyor). Olmuşsa satırlar
`processing`'de, lease'li; worker kimliklerini **hiç öğrenmediği** için kuyruğa geri
veremez → reaper'a kadar (lease süresi, 60 sn) beklerler.

**Uygulanan çözüm (senin önerin):** claim sorgusu kapanıştan muaf ve kısa süreli.

```go
// claimCtx, kapanış sinyalinden MUAF ve kısa süreli.
//
// ctx doğrudan geçilse SIGTERM sorguyu ortada kesebilir ve commit olmuş bir
// UPDATE'in satırları öğrenilmeden kalırdı. WithoutCancel ile sorgu tamamlanır,
// satırlar döner; ctx zaten iptal olduğu için dispatchAll hiçbirini dağıtmaz ve
// claimLoop hepsini anında kuyruğa geri verir.
claimCtx, cancel := context.WithTimeout(
    context.WithoutCancel(ctx), p.engine.cfg.ClaimTimeout)
defer cancel()
```

Ek kod gerekmiyor: `dispatchAll` zaten `ctx.Done()`'da hepsini döndürüyor ve
`claimLoop` `releaseTokens` + `releaseLeases` ile geri veriyor.

**Kapattığı ve kapatmadığı:** SIGTERM'in claim ortasına denk geldiği yaygın durumu
tamamen kapatıyor. Artık kalan tek pencere, claim'in **kendi** 3 sn'lik zaman aşımına
takılması (indeksli, 20 satırlık bir sorgu için nadir) — o durumda satırlar yine
reaper'ı bekler. Yani belirsizlik yok edilmiyor, **bizim seçtiğimiz bir süreyle
sınırlandırılıyor**; R4 (sonuç yazımı başarısız) ile aynı sınıfa iniyor.

Kapanış süresi ≈ `ClaimTimeout` + `DrainTimeout` = 3 + 20 = 23 sn < compose'daki
`stop_grace_period: 30s` ✓ (compose yorumuna yazılacak).

**Değerlendirip reddettiğim daha güçlü seçenek:** lease token'ını **worker** üretsin
(bugün `gen_random_uuid()` ile DB üretiyor). Claim partisinin tamamı tek token
paylaşırsa, cevabı kaybeden worker `WHERE lease_token = $token AND status='processing'`
ile kendi yarım kalan claim'ini bulup geri verebilir → pencere **tamamen** kapanır,
üstelik DB bağlantısının kopması gibi başka bir arıza sınıfını da kapsar. Bedeli: claim
SQL'i ve Adım 6'nın "token'ı worker uydurmaz, tek kaynak DB" kararı değişir, kurtarma
sorgusu + testleri gelir (~40 satır). Artık kalan risk (nadir, satır kaybı yok, en kötü
60 sn gecikme) bunu hak etmiyor; `docs/PLAN.md`'ye gelecek seçenek olarak not düşülecek.

---

## 6. Test planı

### `internal/breaker/breaker_test.go` (`-short`)
`TestClosedBreakerRunsRequest`, `TestBreakerOpensAfterThresholdFailures`,
`TestOpenBreakerDoesNotCallRequest` (fn hiç çalışmadı + `ErrOpen`),
`TestSuccessResetsConsecutiveFailures`, `TestBreakerHalfOpensAfterTimeout`,
`TestHalfOpenAllowsSingleProbe` (ikinci eşzamanlı istek `ErrOpen`),
`TestHalfOpenClosesOnSuccessfulProbe`, `TestHalfOpenReopensOnFailedProbe`,
`TestOnStateChangeReportsTransitions`, `TestAlwaysClosedNeverBlocks`,
`TestNewRejectsInvalidConfig`,
**`TestExcludedErrorDoesNotResetFailureCounter`** (dışlanan hatadan sonra bir hata daha
gelince eşik hâlâ dolar — yani sayaç sıfırlanmadı),
**`TestExcludedProbeDoesNotConsumeHalfOpenSlot`** (yarı açıkta dışlanan bir sondadan
sonra ikinci sonda hâlâ geçebiliyor; `validRequests()` davranışını okumaya değil teste
bağlıyor).
Zamanlama: `OpenTimeout` testlerde 50 ms; iddialar yön üzerine, tam süre üzerine değil.

### `internal/worker/classify_test.go` (yeniden yazılıyor)
Korunan: sent / bodyless 2xx / `DecodeError` testleri.
**Kırılacağı baştan yazılmış iki test** (`TestClassifyRetriesWhenAttemptsRemain`,
`TestClassifyAppliesStep6RuleToEveryFailureKind`) yerine:

| Test | İddia |
|---|---|
| `TestClassifyFailsImmediatelyOnBadRequest` | 400, 422 → `failed`, hak kalsa bile; breaker tetiklenmez |
| `TestClassifyFailsOnUnrecognizedClientError` | 409, 413 → `failed` (tanınmayan 4xx varsayılanı) |
| `TestClassifyCountsServerErrorWhileBreakerClosed` | 500, 503 + `StateClosed` → `retrying`, deneme **sayılır**, backoff; `breakerFailure` |
| `TestClassifyRequeuesServerErrorWhenBreakerTookOver` | 500, 503 + `StateOpen`/`StateHalfOpen` → `actionRequeue`, gecikme **`2 × BreakerOpenTimeout`**, deneme sayılmaz |
| `TestClassifyCountsConfigErrorWhileBreakerClosed` | 401, 403, 404 + `StateClosed` → `retrying`, deneme sayılır |
| `TestClassifyRequeuesConfigErrorWhenBreakerTookOver` | 401, 403, 404 + `StateOpen` → `actionRequeue`, deneme sayılmaz |
| `TestClassifyRequeuesRateLimitWithRetryAfter` | 429 + `Retry-After: 30s` → gecikme 30s, **`breakerFailure` false** |
| `TestClassifyFloorsRateLimitDelayWhenHeaderMissing` | 429, `RetryAfter` 0 → gecikme `[RetryBaseDelay, 2×RetryBaseDelay]`; **asla sıfıra yakın değil** |
| `TestClassifyCapsRetryAfterAtMaxDelay` | 429 + `Retry-After: 24h` → gecikme `RetryMaxDelay` |
| `TestClassifyConnectionErrorFollowsBreakerState` | bağlantı reddi: `StateClosed` → sayılır; `StateOpen` → `actionRequeue` |
| `TestClassifyCountsTimeoutRegardlessOfBreakerState` | timeout → her üç durumda da `retrying` + **sayılır** (belirsizlik kuralı devre durumundan bağımsız) |
| `TestClassifyRaisesTimeoutDelayWhenBreakerTookOver` | timeout + `StateOpen` → gecikme `>= 2 × BreakerOpenTimeout`; `StateClosed` → yalnızca backoff penceresi |
| `TestBreakerOutcomeForMapsEachErrorKind` | tablo: `nil`/`DecodeError`/400/422 → `breakerHealthy`, 429 → `breakerIgnored`, 5xx/401/403/404/`TransportError` → `breakerFailed` |
| `TestClassifyFailsWhenAttemptsExhausted` | timeout + `attempt_count == max` → `failed` |
| `TestBackoffWindowGrowsExponentiallyAndCaps` | **tam değer**: 2s, 4s, 8s, 16s, 32s, … 5m'de durur |
| `TestBreakerWorthyCoversChannelLevelErrorsOnly` | tablo: hangi hata breaker'ı tetikler |
| `TestIsTimeoutRecognizesRealClientTimeout` | gerçek `httptest` + `provider.Client` ile üretilmiş timeout hatası tanınıyor |

### Harness eklemesi
`stubSender` bugün her çağrıda **tek sabit** `resp`/`err` döndürüyor. Zehirli mesaj
testi isteğe göre farklı yanıt gerektiriyor, bu yüzden isteğe bağlı bir alan ekleniyor:

```go
// respondTo, isteğe göre yanıt üretir (nil ise sabit resp/err kullanılır).
respondTo func(provider.SendRequest) (provider.SendResponse, error)
```

### `internal/worker/breaker_test.go` (yeni, motor düzeyi, stub breaker)
| Test | İddia |
|---|---|
| **`TestOpenBreakerConsumesNoAttemptsOrPermits`** | **EN ÖNEMLİ KURAL**: devre açık → `Send` 0, limiter çağrısı 0, `CompleteNotification` 0; satır `DeferLeases` ile geri verilmiş |
| `TestClaimerStopsClaimingWhenBreakerOpen` | `claimLimits()` büyümeyi durduruyor |
| `TestClaimerClaimsSingleRowWhenHalfOpen` | son claim limiti **1** |
| **`TestHalfOpenDoesNotClaimWhileProbeInFlight`** | *(senin istediğin test)* yavaş sonda (gecikmeli stub sender) sürerken: `Send` çağrısı **1**, limiter çağrısı **1**, `DeferLeases` **0**. Uçuş kontrolü olmasa bu sayılar yüzlere çıkardı |
| `TestClaimerResumesWhenBreakerCloses` | devre kapanınca claim limiti yeniden `Concurrency`'ye çıkıyor |
| `TestHandleRequeuesWhenBreakerOpensDuringPermitWait` | izin beklerken açılma yarışı → `ErrOpen` yolu, satır geri veriliyor |
| `TestBreakerIsPerChannel` | SMS devresi açıkken e-posta gönderime devam ediyor |
| **`TestTimeoutProbesConsumeAttemptsAndEventuallyFail`** | *(senin istediğin test)* timeout veren sağlayıcı + aç/yarı aç döngüsü → aynı satırın `attempt_count`'u artıyor ve `max_attempts`'ta `failed` |
| **`TestServerErrorProbesDoNotConsumeAttempts`** | 503 veren sağlayıcı (tam kesinti → devre açılır) → sonda turlarında `attempt_count` artmıyor, satır asla `failed` olmuyor. **Düzeltmeden sonra da aynen geçmeli** |
| **`TestPoisonMessageFailsWhileBreakerStaysClosed`** | *(senin istediğin test)* stub sağlayıcı TEK bir recipient'a hep 500, diğerlerine 202 → araya giren başarılar sayacı sıfırladığı için **devre kapalı kalır**; zehirli mesaj backoff'la `max_attempts`'ta **`failed`** olur, diğer mesajlar `sent` olur |
| **`TestPoisonMessageAloneDoesNotBlockChannel`** | *(senin istediğin test)* kuyrukta önce YALNIZCA zehirli mesaj → devre açılır; sonra sağlıklı mesajlar eklenir. Beklenen: sağlıklı mesajlar birkaç sonda turunda `sent`, devre **kapanmış**, zehirli mesaj sonunda `failed`. Erteleme olmasa test sonsuza kadar beklerdi |
| **`TestPoisonHighPriorityMessageDoesNotBlockChannel`** | *(senin istediğin varyant)* zehirli mesaj **`high`**, sağlıklılar `normal`/`low` → `ORDER BY priority_rank` zehirliyi öne aldığı için **tek katlık** erteleme yetmez; `2 × BreakerOpenTimeout` ile sonda sağlıklı satıra düşüyor ve devre kapanıyor |
| **`TestSustainedRateLimitNeverFailsMessage`** | *(senin istediğin test)* sürekli 429 → `max_attempts`'tan sonra bile `failed` DEĞİL; `DeferLeases` gecikmesi `Retry-After` kadar |
| `TestBreakerTripsOnServerErrorsNotOnBadRequests` | 500 akışı devreyi açıyor, 400 akışı açmıyor |
| **`TestDecodeErrorProbeClosesBreaker`** | *(senin istediğin test)* devre yarı açık, sonda 2xx + çözülemeyen gövde alıyor → devre **KAPANIYOR** (mesaj kabul edildi); dışlanmış sayılsa süresiz yarı açık kalırdı |

### `internal/store` (testcontainers)
`TestDeferLeasesReturnsWorkWithoutCountingAttempt`,
`TestDeferLeasesDelaysAvailableAt`,
`TestDeferLeasesPreservesAvailableAtWhenNoDelay`,
`TestDeferLeasesWritesReason`,
`TestDeferLeasesIgnoresForeignToken`,
`TestDeferredRowDoesNotCollideOnAttemptNo` (defer → claim → complete → `attempt_no` sorunsuz).

### `internal/worker/channel_test.go`
`TestClaimSurvivesShutdownCancellation` — stub, aldığı ctx'in `Done` **olmadığını**
doğrular (parent iptal edilmişken bile) ve claim edilen satırların `ReleaseLeases` ile
geri verildiğini gösterir.

### `internal/config/config_test.go`
Yeni varsayılanlar (`2s`, `5m`, `3s`, `5`, `30s`), `WORKER_RETRY_DELAY`'in artık
okunmadığı, geçersiz değer reddi, `RetryMaxDelay >= RetryBaseDelay` kontrolü.

---

## 7. Doğrulama

```bash
make fmt && make tidy && make lint
make test-unit && make test
docker run --rm -v "//c/Users/Admin/Desktop/Insider Case Study://src" -w //src \
  golang:1.27-alpine sh -c "apk add --no-cache gcc musl-dev && go test -race -short ./..."
make up-build
```

**Kabul A — 400 hemen `failed`:**
```bash
curl -s -X POST localhost:8081/config -d '{"reset_stats":true,"bad_request_rate":1}'
# tek bildirim → attempt_count 1, status failed (5 deneme YAPILMADI)
```

**Kabul B — 5xx breaker'ı açıyor ve sonrası deneme harcanmıyor.**
Önce **tek** worker ile ölç (sayılar yorumlanabilir olsun):
```bash
make scale-worker N=1
curl -s -X POST localhost:8081/config -d '{"reset_stats":true,"failure_rate":1}'
# 200'lük batch gönder, 60 sn izle:
#   worker logunda "devre kesici açıldı" (kanal başına bir kez)
#   psql: attempt_count=1 olan satır sayısı ≈ eşik-1 (4) + devre açıldığı anda
#         UÇUŞTA olan istekler (≤ WORKER_CONCURRENCY) — yani ≤ 24, geri kalan 0
#   psql: failed = 0 (hepsi bekliyor)
#   mock /stats: istek ≈ eşik + uçuştaki istekler + sonda sayısı
#                — BATCH BOYUTU (200) KADAR DEĞİL
curl -s -X POST localhost:8081/config -d '{"failure_rate":0}'
# devre kapanıyor, birikmiş kuyruk akıyor, toplam sent = 200
```
Üç kopyada aynı ölçüm **kopya başına** tekrarlanır (§2.1): `attempt_count=1` olan satır
sayısı ve mock istek sayısı yaklaşık 3× çıkar. Bu bir hata değil, yerel breaker'ın
bilinen sonucu.

**Kabul C — sürekli 429 mesajı öldürmüyor:**
```bash
curl -s -X POST localhost:8081/config -d '{"reset_stats":true,"rate_limit_rate":1}'
# 60 sn sonra: failed = 0, satırlar retrying, available_at ileride, last_error "429"
docker compose logs worker | grep "hız sınırı"
```

**Kabul D — timeout denemeyi harcıyor:**
```bash
curl -s -X POST localhost:8081/config -d '{"reset_stats":true,"latency":"30s"}'
# sağlayıcı zaman aşımı 10 sn → timeout; satır retrying + backoff, attempt_count artıyor
# yeterince beklenince failed (max_attempts) — 429/5xx'in aksine
```

**Kabul E — kapanışta claim kaybı yok:**
```bash
# 1000'lik batch + docker compose restart worker (claim ortasında)
docker compose exec -T postgres psql -U insider -d notifications \
  -c "select count(*) from notifications where status='processing'"   # 0
```

**Kabul F — Adım 7 ölçümü bozulmadı:** 1000 SMS, 3 worker ≈ 11 sn, maks ≤ 100/sn.

---

## 8. Commit akışı (dört commit, her birinde durup onay)

1. **`feat: add per-channel circuit breaker`** — `internal/breaker` + `sony/gobreaker/v2`
   bağımlılığı + testleri. Worker'a dokunmaz.
2. **`feat: add deferred lease hand-back to the store`** — `DeferLeases` SQL + `Queue`
   arayüzü satırı + store testleri.
3. **`feat: implement retry policy and circuit breaker`** *(PLAN'daki mesaj)* —
   `classify.go` yeniden yazımı, iki breaker kapısı, claimer kapısı, `Config`/`New`,
   `cmd/worker`, config/compose/env.
4. **`fix: shield the claim query from shutdown cancellation`** — `claimCtx` +
   `ClaimTimeout` + testi, ardından `docs/PLAN.md` kutucukları ve açık sorunun cevabı,
   `classify.go`/`CLAUDE.md`'deki "yalnızca classify.go değişir" çelişkisinin
   düzeltilmesi, `CLAUDE.md` **Durum**, öğrenme notu (`adim-08-retry-breaker.md`).

### 3. gün README maddeleri (PLAN'a eklenecek)

- **Breaker replika başına ayrı:** kesinti başında her kopyada ~eşik-1 mesaj 1 deneme
  kaybeder (3 kopya × 4 ≈ 12 mesaj); sondalar deneme harcamaz. Neden paylaşılmadığı
  (sayaç ↔ sıraya duyarlı durum makinesi ayrımı) §2.1'de.
- **Sürekli 429:** mesajın suçu değil, `RATE_LIMIT_PER_SECOND` sağlayıcının sınırından
  yüksek. 429 deneme harcamadığı için mesaj ölmez ama kuyruk büyür; doğru düzeltme
  limiti düşürmek.
- **429 breaker'da sayılmıyor (excluded), başarı DEĞİL.** Başarı sayılsaydı ardışık hata
  sayacını sıfırlardı ve kâh 503 kâh 429 dönen karışık bir kesintide devre çok geç
  açılırdı. Dışlanınca karar yalnızca 5xx/timeout/bağlantı hatası sayısına dayanıyor.
- **Zehirli mesaj:** sağlayıcı tek bir mesaja hep 5xx dönerse devre kapalı kaldığı için
  o mesaj normal backoff'la `max_attempts`'ta `failed` olur ve kanalı durdurmaz.
- **Timeout belirsizdir:** deneme sayılır, çünkü mesaj gitmiş olabilir; 5xx ve bağlantı
  hatasında "kesin ulaşmadı" bilindiği için breaker devraldığında sayılmaz.

---

## 9. `docs/PLAN.md`'den sapmalar

1. **Claimer değişiyor** — `classify.go:19` ve `CLAUDE.md:25` "yalnızca classify.go
   gövdesi değişecek" diyor; izin ve deneme harcamamak için claim'in durması zorunlu.
   PLAN §2'nin "açıkken o kanaldan iş çekilmez" ifadesi doğru olan; iki yorum düzeltilecek.
2. **5xx / 401 / 403 / 404: deneme muhasebesi devrenin durumuna bağlı** — PLAN §6
   tablosu "5xx → retry" diyor ama deneme muhasebesinden söz etmiyor. Devre kapalıysa
   normal retry (sayılır), breaker devraldıysa sayılmaz. Koşulsuz saymamak zehirli
   mesaj açığı yaratıyordu (gerekçe §1).
   **429 koşulsuz sayılmıyor**, timeout koşulsuz sayılıyor.
3. **429 `Retry-After`'a uyuyor ama deneme harcamıyor** — PLAN §6 "Retry, Retry-After
   dikkate alınır" diyor; eksik kalan kısım tamamlanıyor.
4. **401/403/404 PLAN'da yok** — §1'de gerekçesiyle eklendi.
5. **Yeni paket `internal/breaker`** — PLAN §7 ağacında yok (`internal/ratelimit` ile
   aynı gerekçe: dış kütüphaneyi kendi arayüzümüzün altına saklamak). `CLAUDE.md`
   klasör tablosu güncellenecek.
6. **Yeni store methodu `DeferLeases`** — PLAN'da yok; deneme saymayan geri verme için
   gerekli.
7. **`WORKER_RETRY_DELAY` kaldırılıyor**, yerine `WORKER_RETRY_BASE_DELAY` /
   `WORKER_RETRY_MAX_DELAY`.
8. **Dial/TLS timeout'ları muhafazakâr biçimde "belirsiz" sayılıyor** (§1'deki not).
8b. **Yarı açıkta uçuşta iş varken claim yok** — PLAN'da yok; olmadan sonda uçuşurken
   claimer paylaşılan rate limit bütçesini saniyeler içinde tüketiyordu (§4.3).
8c. **Breaker kaynaklı requeue gecikmesi `2 × BreakerOpenTimeout`** — tek kat
   yetmiyordu çünkü `ORDER BY priority_rank` önceliği `available_at`'ten önce
   değerlendiriyor (§1).
9. **Worker tarafı lease token'ı üretme seçeneği reddedildi** (§5); PLAN'a gelecek
   seçenek olarak not düşülecek.

---

## 10. Onaylanan ek düzeltmeler (Commit 3'te uygulanacak)

Bu dört madde planın beşinci gözden geçirme turunda onaylandı. Yukarıdaki bölümler
(§1, §4.1, §4.2, §6) bunları zaten içeriyor; burada **uygulama sırasında tek tek
işaretlenebilsin** diye ayrıca listeleniyor.

1. **`2xx + DecodeError`'da closure `nil` dönsün.** `brk.Do`'ya geçirilen closure
   "gerçek hatayı döndür" biçiminde yazılırsa `DecodeError` de hataya çevrilir;
   `IsFailure` onu false döndürdüğü için sonuç **excluded** olur. O zaman mesaj
   gönderilmiş olmasına rağmen yarı açık sonda devreyi **kapatmaz** ve kapalı durumda
   ardışık hata sayacını **sıfırlamaz** — gövdesi JSON olmayan bir sağlayıcıda
   (webhook.site HTML döndürebiliyor) devre süresiz yarı açık kalırdı.
   Kural: closure, `breakerOutcomeFor(sendErr) == breakerHealthy` olan **her** sonuçta
   `nil` döner. Test: `TestDecodeErrorProbeClosesBreaker`.

2. **`TestClassifyRequeuesServerErrorWhenBreakerTookOver` beklentisi
   `2 × BreakerOpenTimeout`.** Test tablosu ilk taslaktan kalma tek katlık değeri
   taşıyordu; §1'deki gerekçe (`ORDER BY priority_rank` önceliği `available_at`'ten
   önce değerlendiriyor, bu yüzden tek kat erteleme satırın bir sonraki yarı açık
   pencerede yine sondaya çıkmasını engellemiyor) iki katı zorunlu kılıyor.

3. **`postState != closed` iken timeout da yükseltilmiş gecikme alır.** Timeout
   "belirsiz" olduğu için **denemeyi saymaya devam ediyor** (mesaj gitmiş olabilir),
   ama gecikmesi `max(backoff, 2 × BreakerOpenTimeout)` oluyor. Yoksa `high` öncelikli
   bir satır timeout alıp devreyi açtığında kendisine 2 sn backoff yazılır, yarı açıkta
   önceliği yüzünden **yine sondaya** çıkar ve 5 denemesi bitene kadar
   (≈5 × `BreakerOpenTimeout`) kanalın tek sonda yuvasını tek başına tutar. Devre
   açıkken daha kısa bir gecikmenin faydası yok: satır o sırada zaten çekilemiyor.
   Test: `TestClassifyRaisesTimeoutDelayWhenBreakerTookOver`.

4. **README'nin zehirli mesaj maddesine iki senaryo eklenecek:**
   - **Düşük trafik:** kuyrukta yalnızca zehirli mesaj varsa araya başarı girmediği
     için devre açılır; erteleme olmasa satır kuyruğun başında kalır, her yarı açık
     sondada yine o seçilir ve **devre hiç kapanmaz**. `2 × BreakerOpenTimeout`
     ertelemesi satırın bir sonraki sonda penceresini kaçırmasını sağlıyor.
   - **Yüksek öncelik:** zehirli mesaj `high`, sağlıklılar `normal`/`low` ise
     `priority_rank` onu `available_at`'e bakılmadan öne alır. "Sonradan gelenlerin
     `available_at`'i daha eski" argümanı yalnızca **aynı öncelik sınıfı içinde**
     geçerli — erteleme hesabının önceliği de kapsaması bu yüzden gerekti.
     Test: `TestPoisonHighPriorityMessageDoesNotBlockChannel`.
