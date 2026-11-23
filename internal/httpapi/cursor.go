package httpapi

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// cursorSeparator, kodlanmış imleçte zaman ile kimliği ayırır.
// UUID ve RFC3339Nano biçimlerinin ikisi de boşluk içermediği için güvenli.
const cursorSeparator = " "

// errInvalidCursor, imlecin çözülemediğini bildirir.
var errInvalidCursor = errors.New("imleç çözümlenemedi")

// encodeCursor, (created_at, id) çiftini opak bir dizgeye çevirir.
//
// Opak tutmanın sebebi: istemci imleci elle kurmaya çalışmasın ve ileride sıralama
// kolonlarını değiştirirsek API sözleşmesi bozulmasın.
//
// RFC3339Nano kullanılıyor: PostgreSQL timestamptz mikrosaniye hassasiyeti tutar ve
// RFC3339 (saniye) ile kodlasaydık aynı saniyedeki kayıtlar arasında konum kaybolurdu.
//
// base64.RawURLEncoding: URL'de güvenli karakter kümesi, dolgu (=) yok.
func encodeCursor(cursor domain.Cursor) string {
	raw := cursor.CreatedAt.UTC().Format(time.RFC3339Nano) + cursorSeparator + cursor.ID.String()

	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor, kodlanmış imleci çözer.
func decodeCursor(encoded string) (domain.Cursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return domain.Cursor{}, fmt.Errorf("%w: base64 değil", errInvalidCursor)
	}

	parts := strings.Split(string(decoded), cursorSeparator)
	if len(parts) != 2 {
		return domain.Cursor{}, fmt.Errorf("%w: beklenen iki parça", errInvalidCursor)
	}

	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return domain.Cursor{}, fmt.Errorf("%w: zaman damgası okunamadı", errInvalidCursor)
	}

	id, err := uuid.Parse(parts[1])
	if err != nil {
		return domain.Cursor{}, fmt.Errorf("%w: kimlik okunamadı", errInvalidCursor)
	}

	return domain.Cursor{CreatedAt: createdAt, ID: id}, nil
}
