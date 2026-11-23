# Tek Dockerfile, üç binary'yi de üretir: CMD build arg'ı hangi cmd/ paketinin
# derleneceğini seçer (api | worker | mockprovider). Böylece build adımları,
# taban imajlar ve güvenlik ayarları üç servis arasında kopyalanmaz.
ARG GO_VERSION=1.27
ARG ALPINE_VERSION=3.22

# --- build aşaması ---
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src

# Bağımlılıklar ayrı katmanda indirilir: kaynak kod değiştiğinde bu katman
# cache'ten gelir ve build süresi kısalır.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG CMD=api
# CGO kapalı: statik binary, runtime imajında libc bağımlılığı yok.
# -trimpath ve -ldflags="-s -w": yol bilgisi sızmaz, binary küçülür.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

# --- runtime aşaması ---
FROM alpine:${ALPINE_VERSION}

# ca-certificates: dış sağlayıcıya (webhook.site) HTTPS isteği için zorunlu.
# wget: compose healthcheck'inin /health/live uçlarını çağırması için.
# tzdata: zamanlanmış bildirimlerde (bonus) doğru zaman dilimi.
RUN apk add --no-cache ca-certificates tzdata wget \
    && adduser -D -u 10001 app

COPY --from=build /out/app /usr/local/bin/app

# Root olmayan kullanıcı: container ele geçirilse bile host üzerindeki etki alanı daralır.
USER app

ENTRYPOINT ["/usr/local/bin/app"]
