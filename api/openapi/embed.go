// Package openapi, API sözleşmesinin OpenAPI dokümanını binary'e gömer.
//
// Paket neden internal/ dışında: go:embed yalnızca KENDİ klasörünü ve alt
// klasörlerini gömebilir ("..", yani üst dizine çıkan desenler derleme hatası).
// Spec dosyasının yeri docs/PLAN.md §7'de api/ altında belirlenmiş ve araçların
// (Swagger Editor, openapi-generator) alışık olduğu yer de orası; dosyayı
// internal/ altına taşımak bu yolu kırardı. Dışa açılan tek şey bir []byte,
// yani "internal dışına public paket koyma" kuralının koruduğu şey — iş
// mantığının kütüphane gibi kullanılması — burada söz konusu değil.
//
// Klasör api/openapi: paket adı ile import yolunun son parçası böylece aynı
// oluyor ve kullanım yerinde adlandırılmış import gerekmiyor.
package openapi

import _ "embed"

// Spec, api/openapi/openapi.yaml dosyasının ham içeriği.
//
// Diskten okunmuyor: API tek statik binary olarak dağıtılıyor ve dokümanın
// çalışma dizinine veya imaja ayrıca kopyalanmış olmasına bağlı olması,
// "docker compose up sonrası /docs çalışır" sözünü kırılgan yapardı.
//
//go:embed openapi.yaml
var Spec []byte
