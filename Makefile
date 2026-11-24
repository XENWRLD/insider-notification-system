# Geliştirme ve demo komutları. Windows'ta Git Bash veya GNU Make (ezwinports) ile çalışır.

GO      ?= go
COMPOSE ?= docker compose

# Migration'lar compose'daki migrate servisi uzerinden kosar; yerel makinede
# golang-migrate CLI'sinin kurulu olmasi gerekmez. DSN compose icindeki ile ayni:
# host adi "postgres", cunku komut container agi icinde calisir.
MIGRATE_DSN ?= postgres://insider:insider@postgres:5432/notifications?sslmode=disable

# Windows'ta Git Bash, "/migrations" gibi argumanlari otomatik Windows yoluna cevirir
# ("C:/Program Files/Git/migrations") ve container icindeki yol bozulur. Bu iki degisken
# donusumu kapatir; Linux/macOS'ta tanimsiz olmalari zararsizdir.
NOPATHCONV := MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL=*

MIGRATE ?= $(NOPATHCONV) $(COMPOSE) run --rm migrate -path=/migrations -database="$(MIGRATE_DSN)"

# migrate-down kac surum geri alsin (varsayilan: 1). migrate-force hangi surume sabitlesin.
N ?= 1
V ?=
NAME ?=

.DEFAULT_GOAL := help
.PHONY: help env up up-build down logs ps build run run-mock test test-unit test-race lint fmt tidy clean \
        migrate-up migrate-down migrate-version migrate-create migrate-force

help: ## Komut listesini gösterir
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/' | column -t -s "$$(printf '\t')"

env: ## .env yoksa .env.example'dan oluşturur
	@test -f .env || (cp .env.example .env && echo ".env olusturuldu")

up: env ## Compose yiginini baslatir
	$(COMPOSE) up -d

up-build: env ## Imajlari yeniden kurup baslatir
	$(COMPOSE) up -d --build

down: ## Yigini durdurur (veri korunur)
	$(COMPOSE) down

logs: ## Tum servislerin loglarini izler
	$(COMPOSE) logs -f

ps: ## Servis durumlarini listeler
	$(COMPOSE) ps

build: ## Tum binary'leri bin/ altina derler
	$(GO) build -o bin/ ./...

run: env ## API'yi host'ta calistirir (postgres ve redis compose'da olmali)
	$(GO) run ./cmd/api

run-mock: ## Sahte saglayiciyi host'ta calistirir
	$(GO) run ./cmd/mockprovider

migrate-up: ## Bekleyen migration'lari uygular
	$(MIGRATE) up

migrate-down: ## Son N migration'i geri alir (N=1)
	$(MIGRATE) down $(N)

migrate-version: ## Mevcut sema surumunu gosterir
	$(MIGRATE) version

migrate-create: ## Yeni migration cifti olusturur (NAME=add_templates)
	@test -n "$(NAME)" || (echo "NAME gerekli: make migrate-create NAME=add_templates" && exit 1)
	$(NOPATHCONV) $(COMPOSE) run --rm --no-deps migrate create -ext sql -dir /migrations -seq $(NAME)

migrate-force: ## Dirty state'i verilen surume sabitler (V=3)
	@test -n "$(V)" || (echo "V gerekli: make migrate-force V=3" && exit 1)
	$(MIGRATE) force $(V)

test: ## Testleri kosar (testcontainers testleri Docker gerektirir)
	$(GO) test ./...

test-unit: ## Yalnizca Docker gerektirmeyen testleri kosar
	$(GO) test -short ./...

test-race: ## Testleri race detector ile kosar
	$(GO) test -race -count=1 ./...

lint: ## golangci-lint calistirir
	golangci-lint run

fmt: ## Kodu bicimlendirir
	$(GO) fmt ./...

tidy: ## go.mod/go.sum'i duzenler
	$(GO) mod tidy

clean: ## Derleme ciktilarini siler
	rm -rf bin
