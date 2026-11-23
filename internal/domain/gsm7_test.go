package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGSM7Septets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		septets   int
		encodable bool
	}{
		{name: "boş metin", input: "", septets: 0, encodable: true},
		{name: "ascii", input: "Merhaba dunya", septets: 13, encodable: true},

		// Türkçe: tabloda büyük Ç var, küçük ç yok. Büyük/küçük harf farkı
		// mesajın maliyetini ikiye katlıyor.
		{name: "büyük Ç tabloda var", input: "Çok", septets: 3, encodable: true},
		{name: "küçük ç tabloda yok", input: "çok", encodable: false},
		{name: "ö ü Ö Ü dördü de var", input: "ü ö Ü Ö", septets: 7, encodable: true},
		{name: "ş ğ ı İ hiçbiri yok", input: "ş ğ ı İ", encodable: false},
		{name: "tek ş yeter", input: "Merhaba dünyaş", encodable: false},
		{name: "ü içeren metin GSM-7", input: "Merhaba dünya", septets: 13, encodable: true},

		// Uzatma tablosu: ESC öneki aldıkları için 2 septet.
		{name: "euro 2 septet", input: "100€", septets: 5, encodable: true},
		{name: "köşeli parantez 2 septet", input: "[x]", septets: 5, encodable: true},
		{name: "ters eğik çizgi", input: "\\", septets: 2, encodable: true},

		// Varsayılan tablodaki diğer özel karakterler.
		{name: "yunan harfleri", input: "ΔΦΓΛΩ", septets: 5, encodable: true},
		{name: "alt tire", input: "a_b", septets: 3, encodable: true},
		{name: "satır sonu", input: "a\nb", septets: 3, encodable: true},

		// Tabloda olmayanlar.
		{name: "emoji", input: "🎉", encodable: false},
		{name: "kiril", input: "Привет", encodable: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			septets, encodable := GSM7Septets(test.input)

			require.Equal(t, test.encodable, encodable)
			if test.encodable {
				require.Equal(t, test.septets, septets)
			}
		})
	}
}

// TestGSM7SeptetLimit, 160 septet sınırının tam olarak nerede bittiğini sabitler.
func TestGSM7SeptetLimit(t *testing.T) {
	t.Parallel()

	septets, encodable := GSM7Septets(strings.Repeat("a", SMSGSM7MaxSeptets))
	require.True(t, encodable)
	require.Equal(t, SMSGSM7MaxSeptets, septets)

	// Uzatma karakteri 2 septet saydığı için 80 adet '€' tam sınırı doldurur.
	septets, encodable = GSM7Septets(strings.Repeat("€", 80))
	require.True(t, encodable)
	require.Equal(t, SMSGSM7MaxSeptets, septets)

	// 81 adet '€' sınırı aşar: karakter sayısı 81 ama septet 162.
	septets, _ = GSM7Septets(strings.Repeat("€", 81))
	require.Greater(t, septets, SMSGSM7MaxSeptets)
}

func TestUTF16Units(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		units int
	}{
		{name: "boş metin", input: "", units: 0},
		{name: "ascii", input: "abc", units: 3},
		// BMP içindeki karakterler tek birim.
		{name: "türkçe", input: "şğıİ", units: 4},
		// BMP dışı: vekil çift, karakter başına 2 birim.
		{name: "tek emoji", input: "🎉", units: 2},
		{name: "üç emoji", input: "🎉🎉🎉", units: 6},
		{name: "karışık", input: "a🎉b", units: 4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.units, UTF16Units(test.input))
		})
	}
}

// TestUTF16UnitLimit, emoji sınırını sabitler: 35 geçer, 36 geçmez.
//
// Rune sayılsaydı 70 emoji'ye izin verilirdi ve operatör bunu 2 SMS olarak
// faturalandırırdı; bu test o hataya karşı regresyon.
func TestUTF16UnitLimit(t *testing.T) {
	t.Parallel()

	require.Equal(t, SMSUnicodeMaxUnits, UTF16Units(strings.Repeat("🎉", 35)),
		"35 emoji tam olarak 70 kod birimi olmalı")

	require.Greater(t, UTF16Units(strings.Repeat("🎉", 36)), SMSUnicodeMaxUnits,
		"36 emoji sınırı aşmalı")

	// Karşılaştırma: 70 Türkçe karakter sınıra tam oturur.
	require.Equal(t, SMSUnicodeMaxUnits, UTF16Units(strings.Repeat("ş", 70)))
}
