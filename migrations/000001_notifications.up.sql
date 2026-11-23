-- notifications: hem kalıcı veri hem iş kuyruğu.
--
-- Ayrı bir broker (RabbitMQ/Kafka) yerine bu tablo kullanılıyor: bildirim kaydı ile
-- kuyruğa eklenmesi aynı transaction'da olur, böylece "kayıt var ama kuyrukta yok"
-- (dual-write) problemi ve onu çözmek için gereken outbox pattern karmaşıklığı doğmaz.

CREATE TABLE notifications (
    id                  uuid        PRIMARY KEY,
    -- Toplu (batch) oluşturmada aynı isteğin tüm satırlarını bağlar; tekil oluşturmada NULL.
    batch_id            uuid,
    channel             text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    -- Uzunluk sınırları kanal bazında değişiyor (E.164 / e-posta / device token); burada
    -- yalnızca üst sınır var, kanal bazlı doğrulama API katmanında (Adım 3) yapılır.
    recipient           text        NOT NULL CHECK (length(recipient) BETWEEN 1 AND 512),
    content             text        NOT NULL CHECK (length(content) BETWEEN 1 AND 10000),
    priority            text        NOT NULL DEFAULT 'normal'
                                    CHECK (priority IN ('high', 'normal', 'low')),
    -- priority metin olarak index'lenirse sıralama alfabetik olur ve 'low' < 'normal'
    -- çıkar, yani düşük öncelik normali geçer. Bu türetilmiş kolon doğru sıralamayı
    -- veritabanı düzeyinde garanti eder; STORED olduğu için index'lenebilir.
    priority_rank       smallint    NOT NULL GENERATED ALWAYS AS (
                                        CASE priority
                                            WHEN 'high'   THEN 1
                                            WHEN 'normal' THEN 2
                                            ELSE 3
                                        END
                                    ) STORED,
    status              text        NOT NULL DEFAULT 'pending'
                                    CHECK (status IN ('pending', 'processing', 'sent',
                                                      'retrying', 'failed', 'cancelled')),
    attempt_count       integer     NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts        integer     NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    -- Tek alan üç işi birden taşır: normal gönderim (now), retry (now + backoff) ve
    -- zamanlanmış bildirim (send_at). Worker yalnızca available_at <= now() olanı çeker.
    available_at        timestamptz NOT NULL DEFAULT now(),
    -- Lease: işi çeken worker kendi token'ını yazar. Çöken worker'ın işini reaper geri
    -- alır; geç kalan eski worker ise token eşleşmediği için sonucu yazamaz.
    lease_token         uuid,
    lease_expires_at    timestamptz,
    provider_message_id text,
    last_error          text,
    -- API ve worker ayrı process olduğu için izleme kimliği kayıtla birlikte saklanır;
    -- worker logları correlation_id'yi buradan okur.
    correlation_id      text        NOT NULL,
    -- templates tablosu bonus adımda geleceği için foreign key şimdi tanımlanmıyor;
    -- FK o migration'da ALTER TABLE ile eklenecek.
    template_id         uuid,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),

    -- Lease bir çifttir: ya ikisi birlikte dolu ya ikisi birlikte boş.
    CONSTRAINT notifications_lease_pair
        CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
    -- processing durumu lease'siz olamaz; reaper'ın dayanağı bu değişmez (invariant).
    CONSTRAINT notifications_processing_needs_lease
        CHECK (status <> 'processing' OR lease_token IS NOT NULL)
);

-- Worker'ın iş çekme sorgusu (Adım 6) sistemin en sıcak sorgusu: saniyede yüzlerce kez,
-- kanal başına çalışır. Kısmi (partial) index yalnızca çekilebilir satırları tutar;
-- sent/failed kayıtları milyonlara çıksa da index küçük ve önbellekte kalır.
-- Kolon sırası sorgunun şeklini izler: eşitlik (channel) → sıralama (rank, available_at, id).
CREATE INDEX notifications_claim_idx
    ON notifications (channel, priority_rank, available_at, id)
    WHERE status IN ('pending', 'retrying');

-- Reaper (Adım 6): süresi dolmuş lease'leri bulup işi kuyruğa geri koyar.
CREATE INDEX notifications_lease_expiry_idx
    ON notifications (lease_expires_at)
    WHERE status = 'processing';

-- Batch özeti (GET /api/v1/batches/{id}): status bazında sayım bu index üzerinden gider.
CREATE INDEX notifications_batch_idx
    ON notifications (batch_id, status)
    WHERE batch_id IS NOT NULL;

-- Cursor pagination (GET /api/v1/notifications):
--   WHERE (created_at, id) < ($1, $2) ORDER BY created_at DESC, id DESC
-- id'nin ikinci kolon olması, aynı created_at'e sahip satırlarda sıralamayı
-- deterministik yapar; OFFSET'in büyük veride yavaşlaması böylece yaşanmaz.
CREATE INDEX notifications_cursor_idx
    ON notifications (created_at DESC, id DESC);

-- updated_at'i uygulama koduna bırakmak yerine trigger ile yazıyoruz: worker ve API'de
-- onlarca UPDATE sorgusu olacak ve birinde unutulması sessiz bir hata olurdu.
CREATE FUNCTION set_updated_at() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER notifications_set_updated_at
    BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
