# Geliştirme ve demo komutları. Windows'ta Git Bash veya GNU Make (ezwinports) ile çalışır.

GO      ?= go
COMPOSE ?= docker compose

.DEFAULT_GOAL := help
.PHONY: help env up up-build down logs ps build run test test-race lint fmt tidy clean

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

test: ## Testleri kosar
	$(GO) test ./...

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
