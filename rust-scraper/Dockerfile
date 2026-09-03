# 1. ビルド用ステージ
FROM rust:1-slim-bookworm AS builder
WORKDIR /app

# OpenSSLのコンパイルに必要なパッケージを追加
RUN apt-get update && apt-get install -y pkg-config libssl-dev

COPY . .
RUN cargo build --release

# 2. 本番実行用ステージ
FROM debian:bookworm-slim
WORKDIR /app

# Chromium、yt-dlp、ffmpeg、日本語フォントをインストール
RUN apt-get update && apt-get install -y \
    chromium \
    ffmpeg \
    python3 \
    python3-pip \
    curl \
    ca-certificates \
    fonts-ipafont-gothic \
    fonts-vlgothic \
    && curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ビルドしたバイナリをコピー
COPY --from=builder /app/target/release/rust-scraper /app/rust-scraper

# ポート公開
ENV PORT=10000
EXPOSE 10000

CMD ["/app/rust-scraper"]
