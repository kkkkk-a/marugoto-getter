# ----------------------------------------------------
# 1. ビルド用ステージ (Dockerキャッシュを活用して爆速化)
# ----------------------------------------------------
FROM rust:1-slim-bookworm AS builder
WORKDIR /app

# OpenSSLのコンパイルに必要なパッケージを追加
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 【超高速化の鍵】依存クレートの定義だけ先にコピーして事前ビルド
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/rust_scraper* target/release/rust-scraper*

# 実際のソースコードをコピーして本番ビルド
# ※ touch src/main.rs でタイムスタンプを更新し、Rustの再コンパイル漏れバグを確実に防ぎます
COPY . .
RUN touch src/main.rs && cargo build --release

# ----------------------------------------------------
# 2. 本番実行用ステージ (不要なツールを削って軽量化)
# ----------------------------------------------------
FROM debian:bookworm-slim
WORKDIR /app

# Chromium、yt-dlp、ffmpeg、日本語フォント（Noto CJK/絵文字含む）、OpenSSLランタイムをインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    ffmpeg \
    python3 \
    curl \
    ca-certificates \
    libssl3 \
    fonts-ipafont-gothic \
    fonts-vlgothic \
    fonts-noto-cjk \
    fonts-noto-color-emoji \
    && curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Chromiumの実行パスを明示
ENV CHROME_BIN=/usr/bin/chromium

# ビルドしたバイナリをコピー
COPY --from=builder /app/target/release/rust-scraper /app/rust-scraper

# ※ HTMLをバイナリ埋め込み（include_str!）ではなく実行時に外部ファイルとして読み込んでいる場合は、下の行のコメント(#)を解除してください
# COPY index.html /app/index.html

# ポート設定
ENV PORT=10000
EXPOSE 10000

CMD ["/app/rust-scraper"]