-- idempotency_keys: aynı isteğin iki kez işlenmesini engeller (Adım 4).
--
-- İstemci ağ hatası sonrası isteği tekrar gönderdiğinde ikinci bildirim oluşmasın diye
-- Idempotency-Key header'ı ile gelen istekler burada kaydedilir. Kayıt, bildirimle aynı
-- transaction'da yazılır; "bildirim oluştu ama anahtar kaydedilemedi" durumu olamaz.

CREATE TABLE idempotency_keys (
    -- scope: anahtarın hangi uç için verildiği ("notifications" | "notifications_batch").
    -- Aynı Idempotency-Key farklı uçlarda kullanıldığında çarpışmasın.
    scope           text        NOT NULL,
    key             text        NOT NULL,
    -- İstek gövdesinin SHA-256'sı, ham hali (32 byte); hex metinden hem küçük hem hızlı.
    -- Aynı key + aynı hash → saklı cevap döner; aynı key + farklı hash → 409.
    request_hash    bytea       NOT NULL,
    -- İlk isteğin cevabı birebir saklanır ve tekrarda aynen döndürülür.
    response_status integer     NOT NULL,
    response_body   jsonb       NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),

    -- Bileşik birincil anahtar: "INSERT ... ON CONFLICT DO NOTHING" ile tek sorguda
    -- atomik "bu isteği daha önce gördüm mü?" kontrolü yapılabilir. Ayrı bir SELECT +
    -- INSERT yarış durumuna (race condition) açık olurdu.
    PRIMARY KEY (scope, key)
);

-- Saklanan cevaplar sonsuza kadar durmamalı; süresi geçmiş kayıtları silen temizlik
-- işi (Adım 4) bu index üzerinden tarar.
CREATE INDEX idempotency_keys_created_at_idx ON idempotency_keys (created_at);
