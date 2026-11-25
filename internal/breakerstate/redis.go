// Package breakerstate, kanal başına devre kesici durumunu process'ler arasında
// taşır.
//
// Neden gerekli: devre kesici worker process'inin İÇİNDE ve worker HTTP dinlemiyor
// (metrik kararı: tek /metrics ucu API'de). Ama bir olay anında en çok sorulan şey
// "devre açık mı" ve bunun cevabı veritabanında YOK — devre durumu hiçbir tabloya
// yazılmıyor. Worker durumu Redis'e yazıyor, API /metrics'te okuyor.
//
// Neden kopya başına anahtar: devre YEREL, yani `--scale worker=3` ile üç kopya üç
// ayrı durum makinesi işletiyor ve biri açıkken diğerleri kapalı olabilir (bu doğru
// olan: her kopya sağlayıcıyı kendisinden korur). Tek anahtar olsaydı son yazan
// kazanırdı ve metrik yanıltıcı olurdu.
//
// Neden TTL: worker ölürse "open" sonsuza kadar asılı kalmamalı. Yazıcı durumları
// periyodik olarak YENİDEN yazıyor; yalnızca durum değişiminde yazmak TTL'i
// tazelemezdi ve sağlıklı bir kanalın metriği de kaybolurdu.
package breakerstate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/XENWRLD/insider-notification-system/internal/breaker"
	"github.com/XENWRLD/insider-notification-system/internal/domain"
)

// DefaultKeyPrefix, üretimde kullanılan anahtar öneki.
//
// Anahtar biçimi: <önek>:<kanal>:<worker_id>
const DefaultKeyPrefix = "notifications:breaker"

// Entry, tek bir kopyanın tek bir kanaldaki devre durumu.
type Entry struct {
	Channel domain.Channel
	Worker  string
	State   breaker.State
}

// Store, devre durumlarını Redis üzerinde tutar.
type Store struct {
	client redis.UniversalClient
	prefix string
}

// New, verilen istemci ve önekle bir store kurar.
//
// keyPrefix parametre: testler birbirinin anahtarlarını görmesin (ratelimit
// paketindeki kalıbın aynısı).
func New(client redis.UniversalClient, keyPrefix string) (*Store, error) {
	if client == nil {
		return nil, errors.New("devre durumu deposu Redis istemcisi olmadan kurulamaz")
	}

	if strings.TrimSpace(keyPrefix) == "" {
		return nil, errors.New("devre durumu anahtar öneki boş olamaz")
	}

	return &Store{client: client, prefix: keyPrefix}, nil
}

// Publish, bir kopyanın kanal durumunu TTL ile yazar.
//
// TTL, çağıranın yazma periyodunun katı olacak şekilde seçilir (bkz. worker): birkaç
// kaçırılmış yazıma tolerans bırakıp ölü kopyanın durumunu da süresi dolunca düşürür.
func (s *Store) Publish(
	ctx context.Context,
	channel domain.Channel,
	workerID string,
	state breaker.State,
	ttl time.Duration,
) error {
	if err := s.client.Set(ctx, s.key(channel, workerID), string(state), ttl).Err(); err != nil {
		return fmt.Errorf("devre durumu yazılamadı (%s/%s): %w", channel, workerID, err)
	}

	return nil
}

// Read, bütün kopyaların bütün kanal durumlarını döner.
//
// SCAN kullanılıyor, KEYS değil: anahtar sayısı küçük (kanal × kopya) ama KEYS tüm
// anahtar uzayını tarayıp Redis'i bloklar ve üretimde kaçınılması gereken bir
// çağrıdır. Anahtar sayısı az olduğu için tek tur yetiyor.
func (s *Store) Read(ctx context.Context) ([]Entry, error) {
	pattern := s.prefix + ":*"

	keys, err := s.scan(ctx, pattern)
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, nil
	}

	values, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("devre durumları okunamadı: %w", err)
	}

	entries := make([]Entry, 0, len(keys))

	for i, key := range keys {
		// Anahtar SCAN ile MGET arasında süresi dolmuş olabilir: nil atlanır.
		raw, ok := values[i].(string)
		if !ok {
			continue
		}

		channel, worker, ok := s.parseKey(key)
		if !ok {
			continue
		}

		entries = append(entries, Entry{
			Channel: channel,
			Worker:  worker,
			State:   breaker.State(raw),
		})
	}

	return entries, nil
}

// scan, önekle eşleşen anahtarları toplar.
func (s *Store) scan(ctx context.Context, pattern string) ([]string, error) {
	var (
		keys   []string
		cursor uint64
	)

	for {
		page, next, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("devre durumu anahtarları taranamadı: %w", err)
		}

		keys = append(keys, page...)

		if next == 0 {
			return keys, nil
		}

		cursor = next
	}
}

// key, kanal ve kopya için anahtarı üretir.
func (s *Store) key(channel domain.Channel, workerID string) string {
	return s.prefix + ":" + string(channel) + ":" + workerID
}

// parseKey, anahtardan kanal ve kopya kimliğini çıkarır.
//
// Kopya kimliği hostname olduğu için ":" içerebilir mi? İçermez (DNS adı), ama yine de
// yalnızca İLK iki parça ayrıştırılıp kalanı kopya kimliği sayılıyor: beklenmeyen bir
// anahtar sessizce yanlış etiketlenmesin.
func (s *Store) parseKey(key string) (domain.Channel, string, bool) {
	rest, ok := strings.CutPrefix(key, s.prefix+":")
	if !ok {
		return "", "", false
	}

	channel, worker, ok := strings.Cut(rest, ":")
	if !ok || channel == "" || worker == "" {
		return "", "", false
	}

	return domain.Channel(channel), worker, true
}
