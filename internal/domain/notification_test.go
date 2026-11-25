package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAllChannelsCoversEveryValidChannel, listenin tek kaynak olma iddiasını
// doğrular.
//
// Liste iki yerde kullanılıyor: worker havuzlarının varsayılanı ve sağlayıcı bağlantı
// havuzunun boyutu (kanal × concurrency). Yeni bir kanal eklenip listeye yazılmazsa
// idle havuz yine küçük kalır ve her gönderim yeni TCP+TLS el sıkışması öder
// (review bulgusu F06).
func TestAllChannelsCoversEveryValidChannel(t *testing.T) {
	t.Parallel()

	channels := AllChannels()

	require.Len(t, channels, 3)

	for _, channel := range channels {
		require.True(t, channel.Valid(), "listedeki kanal geçerli olmalı: %s", channel)
	}

	// Ters yön: bilinen her kanal listede olmalı.
	for _, channel := range []Channel{ChannelSMS, ChannelEmail, ChannelPush} {
		require.Contains(t, channels, channel)
	}
}

// TestAllChannelsReturnsIndependentSlice, çağıranın listeyi bozamayacağını doğrular:
// paket düzeyinde paylaşılan bir dilim olsaydı tek bir yanlış atama worker'ın kanal
// kümesini değiştirirdi.
func TestAllChannelsReturnsIndependentSlice(t *testing.T) {
	t.Parallel()

	first := AllChannels()
	first[0] = "bozuk"

	require.Equal(t, ChannelSMS, AllChannels()[0])
}
