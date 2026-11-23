package domain

import (
	"strings"
	"unicode/utf16"
)

// SMS uzunluk sınırları.
//
// Bir SMS 140 bayt yük taşır. GSM-7 ile her karakter 7 bit tuttuğu için 160 karakter
// sığar; UCS-2'ye düşüldüğünde her kod birimi 2 bayt tuttuğu için yalnızca 70 birim.
const (
	SMSGSM7MaxSeptets  = 160
	SMSUnicodeMaxUnits = 70
)

// gsm7Default, GSM 03.38 varsayılan 7-bit alfabesi — 3GPP TS 23.038 Tablo 6.2.1'deki
// kod sırasıyla yazılmıştır (0x00..0x7F, ESC 0x1B hariç). Sıra yalnızca üyelik testi
// için gerekli değil ama kaynakla satır satır karşılaştırılabilsin diye korunuyor.
//
// Türkçe açısından kritik: tabloda büyük 'Ç' (0x09) VAR, küçük 'ç' YOK. Aynı şekilde
// 'ş Ş ğ Ğ ı İ' de yok. Bu harflerden biri geçtiğinde mesaj UCS-2'ye düşer, sınır
// 160'tan 70'e iner ve operatör faturası iki katına çıkar. 'ö ü Ö Ü' ise tabloda var.
const gsm7Default = "@£$¥èéùìòÇ\nØø\rÅå" + // 0x00-0x0F
	"Δ_ΦΓΛΩΠΨΣΘΞ" + // 0x10-0x1A  (0x1B = ESC, uzatma öneki)
	"ÆæßÉ" + // 0x1C-0x1F
	" !\"#¤%&'()*+,-./0123456789:;<=>?" + // 0x20-0x3F
	"¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§" + // 0x40-0x5F
	"¿abcdefghijklmnopqrstuvwxyzäöñüà" // 0x60-0x7F

// gsm7Extended, uzatma tablosu (3GPP TS 23.038 Tablo 6.2.1.1). Her biri ESC (0x1B)
// öneki ile kodlandığı için 2 septet tutar.
const gsm7Extended = "\f^{}\\[~]|€"

// GSM7Septets, metnin GSM-7 ile kodlanması hâlinde kaç septet tutacağını döner.
//
// encodable=false ise metinde GSM-7'ye girmeyen en az bir karakter var; bu durumda
// septet sayısı anlamsızdır ve çağıran UTF16Units ile UCS-2 sınırına bakmalıdır.
func GSM7Septets(s string) (septets int, encodable bool) {
	for _, r := range s {
		switch {
		case strings.ContainsRune(gsm7Default, r):
			septets++
		case strings.ContainsRune(gsm7Extended, r):
			septets += 2
		default:
			return 0, false
		}
	}

	return septets, true
}

// UTF16Units, metnin UCS-2/UTF-16 ile kodlanması hâlinde kaç kod birimi tutacağını döner.
//
// Rune sayısı yanlış sonuç verir: BMP dışındaki karakterler (emoji, bazı CJK) vekil çift
// (surrogate pair) olarak kodlanır ve 2 birim tutar. Rune saysaydık 70 emoji'ye izin
// verirdik, oysa operatör bunu 2 SMS olarak faturalandırırdı.
func UTF16Units(s string) int {
	return len(utf16.Encode([]rune(s)))
}
