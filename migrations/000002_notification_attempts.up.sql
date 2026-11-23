-- notification_attempts: her gönderim denemesinin kaydı.
--
-- Neden ayrı tablo? notifications satırı yalnızca son durumu taşır (last_error,
-- attempt_count). "Hangi denemede ne oldu, ne kadar sürdü" sorusu teşhis için gerekli
-- ve bu veri tek satıra sığmaz. Ayrıca bu tablo append-only olduğundan notifications'ın
-- sık güncellenen satırlarını şişirmez.

CREATE TABLE notification_attempts (
    -- uuid değil bigint identity: bu bir kayıt defteri, satırlar sırayla ekleniyor.
    -- Artan anahtar B-tree'nin sağ ucuna yazar, index parçalanması (fragmentation)
    -- olmaz ve 16 byte yerine 8 byte yer tutar.
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id uuid        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_no      integer     NOT NULL CHECK (attempt_no > 0),
    -- Timeout veya bağlantı hatasında HTTP yanıtı hiç gelmez, bu yüzden NULL olabilir.
    status_code     integer,
    error           text,
    duration_ms     integer     NOT NULL CHECK (duration_ms >= 0),
    created_at      timestamptz NOT NULL DEFAULT now(),

    -- At-least-once teslimatta aynı deneme iki kez yazılmaya çalışılabilir; bu kısıt
    -- çift kaydı engeller. Ayrıca notification_id öncü kolon olduğu için
    -- "bir bildirimin deneme geçmişi" sorgusuna da hizmet eder — ek index gerekmez.
    CONSTRAINT notification_attempts_unique_attempt UNIQUE (notification_id, attempt_no)
);
