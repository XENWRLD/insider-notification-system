package httpapi

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDecodeErrorDoesNotLeakInternalTypeNames, JSON hata mesajının iç tip adlarını
// sızdırmadığını doğrular.
//
// encoding/json'un ham metni "...Go struct field createNotificationRequest.channel of
// type string" diyordu; batch ucunda "[]httpapi.createNotificationRequest". Bu hem
// paket içi adları dışarı veriyor hem de o adları fiilî API sözleşmesine çeviriyordu:
// bir struct yeniden adlandırıldığında mesaja bakan istemci kırılırdı (F13).
func TestDecodeErrorDoesNotLeakInternalTypeNames(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		body    string
		expects string
	}{
		"yanlış tip": {
			body:    `{"channel":123,"recipient":"+905551234567","content":"x"}`,
			expects: `"channel" alanı için beklenen tip string, gelen number`,
		},
		"bozuk JSON": {
			body:    `{"channel":`,
			expects: "gövde çözümlenemedi",
		},
		"bilinmeyen alan": {
			body:    `{"channel":"sms","recipient":"+905551234567","content":"x","prioritiy":"high"}`,
			expects: `bilinmeyen alan: "prioritiy"`,
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}), test.body)

			require.Equal(t, http.StatusBadRequest, recorder.Code)

			body := decodeError(t, recorder)
			require.Equal(t, CodeInvalidJSON, body.Error)
			require.Equal(t, test.expects, body.Message)

			// Hiçbir durumda Go/paket adı geçmemeli.
			require.NotContains(t, recorder.Body.String(), "httpapi")
			require.NotContains(t, recorder.Body.String(), "Go struct field")
			require.NotContains(t, recorder.Body.String(), "createNotificationRequest")
		})
	}
}

// TestBatchDecodeErrorDoesNotLeakSliceType, batch ucunda dilim tipinin sızmadığını
// doğrular: ham metin orada "[]httpapi.createNotificationRequest" diyordu.
func TestBatchDecodeErrorDoesNotLeakSliceType(t *testing.T) {
	t.Parallel()

	recorder := postBatch(t, newNotificationRouter(&stubNotificationStore{}),
		`{"notifications":"dizi değil"}`, nil)

	require.Equal(t, http.StatusBadRequest, recorder.Code)

	body := decodeError(t, recorder)
	require.Equal(t, CodeInvalidJSON, body.Error)
	require.Equal(t, `"notifications" alanı için beklenen tip dizi, gelen string`, body.Message)
	require.NotContains(t, recorder.Body.String(), "httpapi")
}

// TestEmptyBodyMessageIsExplicit, boş gövdenin anlaşılır bir mesaj aldığını doğrular.
func TestEmptyBodyMessageIsExplicit(t *testing.T) {
	t.Parallel()

	recorder := postJSON(t, newNotificationRouter(&stubNotificationStore{}), "")

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Equal(t, "istek gövdesi boş", decodeError(t, recorder).Message)
}
