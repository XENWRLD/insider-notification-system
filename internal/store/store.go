// Package store, PostgreSQL erişimini barındırır.
//
// SQL elle yazılır, ORM kullanılmaz: bu projenin kritik sorguları (SKIP LOCKED ile iş
// çekme, atomik iptal, lease yazma) ORM'in soyutlayamayacağı Postgres özelliklerine
// dayanıyor. Sorgular yalnızca bu pakette durur; handler ve worker doğrudan SQL çalıştırmaz.
package store

import "github.com/jackc/pgx/v5/pgxpool"

// Store, bildirim verisine erişimi sağlar.
type Store struct {
	pool *pgxpool.Pool
}

// New, verilen bağlantı havuzu üzerinde çalışan bir Store döner.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}
