# ----------------------------------------------------
# 1. ビルド用ステージ (Dockerキャッシュを活用して爆速化)
# ----------------------------------------------------
FROM rust:1-slim-bookworm AS builder
WORKDIR /app

# OpenSSLのコンパイルに必要なパッケージを追加
RUN apt-get update && apt-get install -y pkg-config libssl-dev

# 【超高速化の鍵】依存クレートの定義だけ先にコピーして事前ビルド
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# 実際のソースコードとHTMLをコピーして本番ビルド（依存クレートの再ビルドをスキップ！）
COPY . .
# タイムスタンプを更新して確実に再ビルド
RUN touch src/main.rs && cargo build --release

# ----------------------------------------------------
# 2. 本番実行用ステージ (不要なツールを削って軽量化)
# ----------------------------------------------------
FROM debian:bookworm-slim
WORKDIR /app

# Chromium、yt-dlp、ffmpeg、日本語フォントをインストール（不要なpipを削って軽量化）
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    ffmpeg \
    python3 \
    curl \
    ca-certificates \
    fonts-ipafont-gothic \
    fonts-vlgothic \
    && curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp \
    && apt-get purge -y curl \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Chromiumの実行パスを明示（エラー防止）
ENV CHROME_BIN=/usr/bin/chromium

# ビルドしたバイナリをコピー
COPY --from=builder /app/target/release/rust-scraper /app/rust-scraper

# ポート設定
ENV PORT=10000
EXPOSE 10000

CMD ["/app/rust-scraper"]