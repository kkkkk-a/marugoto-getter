use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html as HtmlResponse, IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use chromiumoxide::browser::{Browser, BrowserConfig};
use futures::StreamExt;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::process::Command;
use tower_http::cors::CorsLayer;
use url::Url;
use serde_json::Value;

// サーバー全体で共有する状態（HTTPクライアント ＋ 常駐ブラウザ ＋ ログインCookie）
#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    browser: Arc<Browser>,
    pixiv_phpsessid: Option<String>,
    twitter_auth_token: Option<String>,
    browser_semaphore: Arc<tokio::sync::Semaphore>,
    download_semaphore: Arc<tokio::sync::Semaphore>,
    rate_limiter: Arc<tokio::sync::Mutex<std::collections::HashMap<std::net::IpAddr, (u32, std::time::Instant)>>>,
}

impl AppState {
    async fn check_rate_limit(&self, ip: std::net::IpAddr) -> bool {
        let mut map = self.rate_limiter.lock().await;
        let now = std::time::Instant::now();
        // 5分以上古いエントリを自動クリーンアップ
        map.retain(|_, (_, time)| now.duration_since(*time) < Duration::from_secs(300));
        
        let entry = map.entry(ip).or_insert((0, now));
        if now.duration_since(entry.1) > Duration::from_secs(60) {
            *entry = (1, now);
            true
        } else {
            entry.0 += 1;
            entry.0 <= 30 // 1分間に最大30リクエストまで許可
        }
    }
}

#[derive(Deserialize)]
struct ScrapeParams {
    url: String,
    pixiv_cookie: Option<String>,
    twitter_cookie: Option<String>,
}

#[derive(Deserialize)]
struct ProxyParams {
    url: String,
    referer: Option<String>,
}

#[derive(Serialize)]
struct ScrapeResult {
    title: Option<String>,
    paragraphs: Vec<String>,
    images: Vec<String>,
    videos: Vec<String>,
    audios: Vec<String>,
}

// セレクタを1度だけコンパイルして使い回す
static TITLE_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("title").unwrap());
// 記事本文、まとめサイトのレスコンテナ、AAブロックを優先的に抽出するセレクタ
static TEXT_SELECTOR: LazyLock<Selector> = LazyLock::new(|| {
    Selector::parse(".t_b, .message, .post_text, dd, pre, .entry-content, article, blockquote, p").unwrap()
});
static META_DESC_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("meta[name='description'], meta[property='og:description']").unwrap());
// img に加え、picture 内の source や背景画像を持つ要素も捕捉
static IMG_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("img, picture source, [style*='background']").unwrap());
static META_IMG_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("meta[property='og:image'], meta[name='twitter:image']").unwrap());
static VIDEO_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("video, video source").unwrap());
static AUDIO_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("audio, audio source").unwrap());
static IFRAME_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("iframe[src]").unwrap());
static META_VIDEO_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("meta[property='og:video'], meta[property='og:video:url'], meta[property='og:video:secure_url']").unwrap());
static LINK_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("a[href]").unwrap());

#[tokio::main]
async fn main() {
    // 0. 過去に残ってしまった一時ダウンロードフォルダ(temp_downloads_*)を起動時に一括クリーンアップ
    if let Ok(mut entries) = tokio::fs::read_dir(".").await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("temp_downloads_") {
                        let _ = tokio::fs::remove_dir_all(&path).await;
                    }
                }
            }
        }
    }

    // 1. reqwest クライアント作成（接続プール拡大＋TCP最適化＋SSL許容）
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        .timeout(Duration::from_secs(20))
        .pool_max_idle_per_host(20) // 同一ホストへの並列コネクション維持数を拡大
        .tcp_nodelay(true)          // パケット遅延を最小化
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    // 2. ヘッドレストブラウザ（Chromium/Chrome）の常駐起動（macOS / Linux自動最適化）
    println!("🌐 ヘッドレストブラウザ (Chromium) を起動中...");
    let mut config_builder = BrowserConfig::builder();

    // Dockerfile等で CHROME_BIN が指定されている場合はそのパスを使用
    if let Ok(path) = std::env::var("CHROME_BIN") {
        config_builder = config_builder.chrome_executable(path);
    }

    // 普段使いのChromeとのプロファイル衝突・強制終了を防ぐため、独立した一時ディレクトリを割り当て
    let temp_profile = format!("/tmp/chrome_profile_{}", std::process::id());
    config_builder = config_builder.arg(format!("--user-data-dir={}", temp_profile));

    // Linux環境（RenderやDockerコンテナ等）でのみ必須となるフラグを付与（macOSでのクラッシュを防止）
    if cfg!(target_os = "linux") {
        config_builder = config_builder
            .no_sandbox()
            .arg("--disable-gpu")
            .arg("--disable-dev-shm-usage")
            .arg("--disable-setuid-sandbox");
    } else {
        // macOS (Chrome 120+) でのクラッシュ・フリーズを防ぐ最新ヘッドレスモード指定
        config_builder = config_builder.arg("--headless=new");
    }

    // Chromium起動引数に軽量化オプションを追加
    let (browser, mut handler) = Browser::launch(
        config_builder
            .arg("--disable-blink-features=AutomationControlled")
            .arg("--disable-background-timer-throttling")
            .arg("--disable-backgrounding-occluded-windows")
            .arg("--disable-renderer-backgrounding")
            .arg("--disable-extensions")
            .arg("--disable-component-extensions-with-background-pages")
            .arg("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
            .build()
            .expect("Chromiumの設定に失敗しました"),
    )
    .await
    .expect("Chrome/Chromiumが見つかりません。Google ChromeまたはChromiumをインストールしてください。");

    // ブラウザイベントのバックグラウンド駆動タスク（通信ポンプを常時稼働させる）
    tokio::spawn(async move {
        while let Some(_event) = handler.next().await {
            // chromiumoxideの内部メッセージをバックグラウンドで安全に処理・ディスパッチし続ける
        }
    });

    // 環境変数 PIXIV_PHPSESSID（Pixivログイン Cookie）を取得
    let pixiv_phpsessid = std::env::var("PIXIV_PHPSESSID").ok();
    if pixiv_phpsessid.is_some() {
        println!("🔑 Pixiv ログイン Cookieを検出しました（R-18・限定作品取得対応）");
    }

    // 環境変数 TWITTER_AUTH_TOKEN（X/Twitterログイン Cookie）を取得
    let twitter_auth_token = std::env::var("TWITTER_AUTH_TOKEN").ok();
    if twitter_auth_token.is_some() {
        println!("🔑 X (Twitter) ログイン Cookieを検出しました");
    }

    // yt-dlp の24時間ごとの自動アップデートタスク（仕様変更による停止を防止）
    tokio::spawn(async {
        loop {
            tokio::time::sleep(Duration::from_secs(86400)).await;
            let _ = Command::new("yt-dlp")
                .arg("-U")
                .output()
                .await;
        }
    });

    let state = AppState {
        client,
        browser: Arc::new(browser),
        pixiv_phpsessid,
        twitter_auth_token,
        browser_semaphore: Arc::new(tokio::sync::Semaphore::new(3)),  // Chromium同時最大3件
        download_semaphore: Arc::new(tokio::sync::Semaphore::new(2)), // ダウンロード同時最大2件
        rate_limiter: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/health", get(health_handler))
        .route("/scrape", post(scrape_handler).get(scrape_handler_get))
        .route("/download", get(download_handler))
        .route("/video-formats", get(video_formats_handler))
        .route("/proxy", get(proxy_handler))
        .route("/pdf", get(pdf_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    // Render の環境変数 PORT を取得（なければ 3000）
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3000);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Webアプリが起動しました 🚀 http://0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn index_handler() -> HtmlResponse<&'static str> {
    HtmlResponse(include_str!("../index.html"))
}

// クラウド死活監視用ヘルスチェックハンドラ（Chromiumプロセスの生存も確認）
async fn health_handler(
    State(state): State<AppState>,
) -> (StatusCode, &'static str) {
    if state.browser.version().await.is_ok() {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Browser Process Unhealthy")
    }
}

// スクレイピング処理 (POST: JSON Body から安全にパラメータを受け取る)
async fn scrape_handler(
    State(state): State<AppState>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<SocketAddr>,
    Json(params): Json<ScrapeParams>,
) -> Result<Json<ScrapeResult>, (StatusCode, String)> {
    if !state.check_rate_limit(addr.ip()).await {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "短時間にリクエストが多すぎます。1分ほど待ってから再試行してください。".to_string(),
        ));
    }

    // 同時実行制限：5秒以内にスロットが空かない場合は混雑エラーを返却
    let _permit = match tokio::time::timeout(Duration::from_secs(5), state.browser_semaphore.acquire()).await {
        Ok(Ok(permit)) => permit,
        _ => return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "現在アクセスが集中しています。恐れ入りますが、数十秒ほど待ってから再度お試しください。".to_string(),
        )),
    };

    // 30秒の全体タイムアウトを設定し、画面が「取得中...」のまま永久フリーズするのを防止
    let scrape_future = do_scrape(state.clone(), params);
    match tokio::time::timeout(Duration::from_secs(30), scrape_future).await {
        Ok(result) => result,
        Err(_) => Err((StatusCode::GATEWAY_TIMEOUT, "ページの読み込みがタイムアウトしました（30秒）。サイトが重いかブロックされている可能性があります。".to_string())),
    }
}

// 従来の GET リクエスト用のフォールバックハンドラ
async fn scrape_handler_get(
    State(state): State<AppState>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<SocketAddr>,
    Query(params): Query<ScrapeParams>,
) -> Result<Json<ScrapeResult>, (StatusCode, String)> {
    if !state.check_rate_limit(addr.ip()).await {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "短時間にリクエストが多すぎます。1分ほど待ってから再試行してください。".to_string(),
        ));
    }

    // 同時実行制限：5秒以内にスロットが空かない場合は混雑エラーを返却
    let _permit = match tokio::time::timeout(Duration::from_secs(5), state.browser_semaphore.acquire()).await {
        Ok(Ok(permit)) => permit,
        _ => return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "現在アクセスが集中しています。恐れ入りますが、数十秒ほど待ってから再度お試しください。".to_string(),
        )),
    };

    let scrape_future = do_scrape(state.clone(), params);
    match tokio::time::timeout(Duration::from_secs(30), scrape_future).await {
        Ok(result) => result,
        Err(_) => Err((StatusCode::GATEWAY_TIMEOUT, "ページの読み込みがタイムアウトしました（30秒）。サイトが重いかブロックされている可能性があります。".to_string())),
    }
}

async fn do_scrape(
    state: AppState,
    params: ScrapeParams,
) -> Result<Json<ScrapeResult>, (StatusCode, String)> {
    let base_url = Url::parse(&params.url).map_err(|_| {
        (StatusCode::BAD_REQUEST, "無効なURLです。".to_string())
    })?;

    // http または https 以外のスキームを拒否
    if base_url.scheme() != "http" && base_url.scheme() != "https" {
        return Err((StatusCode::BAD_REQUEST, "http または https のURLを指定してください。".to_string()));
    }

    let host = base_url.host_str().unwrap_or("");

    // --- Bluesky専用：公式公開APIを用いた超高速・直接取得（ブラウザ不要でRAM消費ゼロ） ---
    if host.contains("bsky.app") {
        let path = base_url.path();
        if path.contains("/profile/") && path.contains("/post/") {
            let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
            if parts.len() >= 4 && parts[0] == "profile" && parts[2] == "post" {
                let user_handle = parts[1];
                let post_rkey = parts[3];
                let at_uri = format!("at://{}/app.bsky.feed.post/{}", user_handle, post_rkey);
                let api_url = format!("https://public.api.bsky.app/xrpc/app.bsky.feed.getPostThread?uri={}&depth=0", at_uri);

                if let Ok(api_res) = state.client.get(&api_url).send().await {
                    if let Ok(json) = api_res.json::<Value>().await {
                        if let Some(post) = json.pointer("/thread/post") {
                            let author_name = post.pointer("/author/displayName")
                                .and_then(|v| v.as_str())
                                .unwrap_or(user_handle);
                            let title = Some(format!("Bluesky - {}", author_name));

                            let mut paragraphs = Vec::new();
                            if let Some(text) = post.pointer("/record/text").and_then(|v| v.as_str()) {
                                if !text.trim().is_empty() {
                                    paragraphs.push(text.trim().to_string());
                                }
                            }

                            let mut images = Vec::new();
                            if let Some(imgs) = post.pointer("/embed/images").and_then(|v| v.as_array()) {
                                for img in imgs {
                                    if let Some(full) = img.get("fullsize").and_then(|v| v.as_str()) {
                                        images.push(full.to_string());
                                    }
                                }
                            }

                            let mut videos = Vec::new();
                            if let Some(playlist) = post.pointer("/embed/playlist").and_then(|v| v.as_str()) {
                                videos.push(playlist.to_string());
                            }

                            return Ok(Json(ScrapeResult {
                                title,
                                paragraphs,
                                images,
                                videos,
                                audios: Vec::new(),
                            }));
                        }
                    }
                }
            }
        }
    }

    // --- Pixiv専用：個別作品 および 作者ユーザーページの原寸画像一覧取得 ---
    if host.contains("pixiv.net") {
        let mut illust_id: Option<String> = None;
        let mut user_id: Option<String> = None;
        let path = base_url.path();

        if path.contains("/artworks/") {
            if let Some(id_part) = path.split("/artworks/").nth(1) {
                let clean_id = id_part.split('/').next().unwrap_or("").split('?').next().unwrap_or("");
                if !clean_id.is_empty() && clean_id.chars().all(|c| c.is_ascii_digit()) {
                    illust_id = Some(clean_id.to_string());
                }
            }
        } else if path.contains("/users/") {
            if let Some(id_part) = path.split("/users/").nth(1) {
                let clean_id = id_part.split('/').next().unwrap_or("").split('?').next().unwrap_or("");
                if !clean_id.is_empty() && clean_id.chars().all(|c| c.is_ascii_digit()) {
                    user_id = Some(clean_id.to_string());
                }
            }
        } else if let Some((_, id)) = base_url.query_pairs().find(|(k, _)| k == "illust_id") {
            if !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()) {
                illust_id = Some(id.to_string());
            }
        } else if let Some((_, id)) = base_url.query_pairs().find(|(k, _)| k == "id") {
            if !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()) {
                user_id = Some(id.to_string());
            }
        }

        // 1. 個別作品ページの場合
        if let Some(id) = illust_id {
            let page = state
                .browser
                .new_page("about:blank")
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, format!("ページを開けませんでした: {}", e)))?;

            let effective_phpsessid = params.pixiv_cookie.as_ref().or(state.pixiv_phpsessid.as_ref());
            if let Some(phpsessid) = effective_phpsessid {
                let cookie_params = chromiumoxide::cdp::browser_protocol::network::SetCookieParams::builder()
                    .name("PHPSESSID")
                    .value(phpsessid.clone())
                    .domain(".pixiv.net")
                    .path("/")
                    .http_only(true)
                    .secure(true)
                    .build();
                if let Ok(p_cookie) = cookie_params {
                    let _ = page.execute(p_cookie).await;
                }
            }

            // Cookieセット後に目的のURLへ移動し、ロードを待機
            let _ = page.goto(base_url.as_str()).await;
            tokio::time::sleep(Duration::from_millis(1500)).await;

            let fetch_script = format!(r#"
                (async () => {{
                    try {{
                        const res = await fetch('/ajax/illust/{}/pages');
                        const data = await res.json();
                        if (!data.error && Array.isArray(data.body) && data.body.length > 0) {{
                            return data.body.map(item => item.urls.original);
                        }}
                    }} catch(e) {{}}

                    try {{
                        const res2 = await fetch('/ajax/illust/{}');
                        const data2 = await res2.json();
                        if (!data2.error && data2.body && data2.body.urls && data2.body.urls.original) {{
                            return [data2.body.urls.original];
                        }}
                    }} catch(e) {{}}

                    try {{
                        const meta = document.getElementById('meta-preload-data');
                        if (meta) {{
                            const json = JSON.parse(meta.content);
                            const illust = json.illust['{}'];
                            if (illust && illust.urls && illust.urls.original) {{
                                const pageCount = illust.pageCount || 1;
                                if (pageCount === 1) {{
                                    return [illust.urls.original];
                                }}
                                const origBase = illust.urls.original;
                                const results = [];
                                for (let p = 0; p < pageCount; p++) {{
                                    results.push(origBase.replace(/_p0\./, '_p' + p + '.'));
                                }}
                                return results;
                            }}
                        }}
                    }} catch(e) {{}}

                    return [];
                }})()
            "#, id, id, id);

            let pixiv_images: Vec<String> = page
                .evaluate(fetch_script)
                .await
                .ok()
                .and_then(|v| v.into_value::<Vec<String>>().ok())
                .unwrap_or_default();

            let title_script = "document.title";
            let page_title: Option<String> = page
                .evaluate(title_script)
                .await
                .ok()
                .and_then(|v| v.into_value::<String>().ok());

            // 終了時に確実にタブを閉じてメモリ解放
            let _ = page.close().await;

            if !pixiv_images.is_empty() {
                return Ok(Json(ScrapeResult {
                    title: page_title,
                    paragraphs: Vec::new(),
                    images: pixiv_images,
                    videos: Vec::new(),
                    audios: Vec::new(),
                }));
            } else {
                return Err((
                    StatusCode::FORBIDDEN,
                    "🔒 Pixivの作品を取得できませんでした。R-18作品、非公開作品、またはログインが必要な作品の可能性があります。".to_string(),
                ));
            }
        }

        // 2. 作者ユーザーページの場合（最新作品一覧を一括取得）
        if let Some(uid) = user_id {
            let page = state
                .browser
                .new_page("about:blank")
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, format!("ページを開けませんでした: {}", e)))?;

            let effective_phpsessid = params.pixiv_cookie.as_ref().or(state.pixiv_phpsessid.as_ref());
            if let Some(phpsessid) = effective_phpsessid {
                let cookie_params = chromiumoxide::cdp::browser_protocol::network::SetCookieParams::builder()
                    .name("PHPSESSID")
                    .value(phpsessid.clone())
                    .domain(".pixiv.net")
                    .path("/")
                    .http_only(true)
                    .secure(true)
                    .build();
                if let Ok(params) = cookie_params {
                    let _ = page.execute(params).await;
                }
            }

            // Cookieセット後に移動
            let _ = page.goto(base_url.as_str()).await;

            // 作者の投稿全作品IDリストから最新20件を抽出し、原寸URLを取得
            let user_script = format!(r#"
                (async () => {{
                    try {{
                        const res = await fetch('/ajax/user/{}/profile/all');
                        const data = await res.json();
                        if (data.error || !data.body) return [];

                        const illustIds = Object.keys(data.body.illusts || {{}});
                        const mangaIds = Object.keys(data.body.manga || {{}});
                        // 最新順にソートして最大20件を対象にする
                        const targetIds = [...illustIds, ...mangaIds]
                            .map(n => parseInt(n, 10))
                            .sort((a, b) => b - a)
                            .slice(0, 20);

                        // 20件の作品情報をブラウザ内で一気に並列取得して超高速化（波括弧を二重エスケープ）
                        const results = await Promise.all(targetIds.map(async (id) => {{
                            try {{
                                const dRes = await fetch('/ajax/illust/' + id);
                                const dData = await dRes.json();
                                if (!dData.error && dData.body && dData.body.urls && dData.body.urls.original) {{
                                    return dData.body.urls.original;
                                }}
                            }} catch(e) {{}}
                            return null;
                        }}));
                        return results.filter(Boolean);
                    }} catch(e) {{
                        return [];
                    }}
                }})()
            "#, uid);

            let user_images: Vec<String> = page
                .evaluate(user_script)
                .await
                .ok()
                .and_then(|v| v.into_value::<Vec<String>>().ok())
                .unwrap_or_default();

            let page_title: Option<String> = page
                .evaluate("document.title")
                .await
                .ok()
                .and_then(|v| v.into_value::<String>().ok());

            // 成功・失敗に関わらず確実にタブを閉じる
            let _ = page.close().await;

            if !user_images.is_empty() {
                return Ok(Json(ScrapeResult {
                    title: page_title,
                    paragraphs: vec![format!("🎨 作者 (ID: {}) の最新作品一覧（{}件）", uid, user_images.len())],
                    images: user_images,
                    videos: Vec::new(),
                    audios: Vec::new(),
                }));
            } else {
                return Err((
                    StatusCode::NOT_FOUND,
                    "🔒 作品が見つかりませんでした（作品が非公開、または R-18作品のみの場合は Cookie設定が必要です）。".to_string(),
                ));
            }
        }
    }

    // --- YouTube / ニコニコ動画 専用：チャンネル・プレイリスト・ユーザー一覧の一括取得 ---
    let url_lower = base_url.as_str().to_lowercase();
    let is_playlist_or_channel = url_lower.contains("youtube.com/@")
        || url_lower.contains("youtube.com/channel/")
        || url_lower.contains("youtube.com/c/")
        || url_lower.contains("youtube.com/user/")
        || url_lower.contains("youtube.com/playlist")
        || url_lower.contains("nicovideo.jp/user/")
        || url_lower.contains("nicovideo.jp/mylist/")
        || url_lower.contains("nicovideo.jp/series/");

    if is_playlist_or_channel {
        // yt-dlp で動画本体をダウンロードせず、最新30件のメタデータ一覧のみを高速JSON取得
        let mut cmd = Command::new("yt-dlp");
        cmd.arg("--flat-playlist")
            .arg("-J")
            .arg("--playlist-end")
            .arg("30")
            .arg("--no-warnings")
            .arg("--no-update")
            .arg("--")
            .arg(base_url.as_str());

        // 30秒の安全タイムアウトを設定してフリーズを防止
        let run_cmd = tokio::time::timeout(Duration::from_secs(30), cmd.output()).await;
        if let Ok(Ok(output)) = run_cmd {
            if output.status.success() {
                if let Ok(json) = serde_json::from_slice::<Value>(&output.stdout) {
                    let playlist_title = json["title"]
                        .as_str()
                        .or_else(|| json["channel"].as_str())
                        .map(|s| s.to_string());

                    let mut video_urls: Vec<String> = Vec::new();
                    let mut thumbnail_urls: Vec<String> = Vec::new();
                    let mut descriptions: Vec<String> = Vec::new();

                    if let Some(desc) = json["description"].as_str() {
                        if !desc.trim().is_empty() {
                            descriptions.push(desc.trim().to_string());
                        }
                    }

                    if let Some(entries) = json["entries"].as_array() {
                        for entry in entries {
                            // 各動画のURLを特定
                            let mut v_url = entry["url"].as_str().unwrap_or("").to_string();
                            if !v_url.starts_with("http") {
                                if let Some(id) = entry["id"].as_str() {
                                    if base_url.host_str().unwrap_or("").contains("nicovideo.jp") {
                                        v_url = format!("https://www.nicovideo.jp/watch/{}", id);
                                    } else {
                                        v_url = format!("https://www.youtube.com/watch?v={}", id);
                                    }
                                }
                            }

                            if !v_url.is_empty() && !video_urls.contains(&v_url) {
                                video_urls.push(v_url);
                            }

                            // サムネイル画像URLも取得
                            if let Some(thumbnails) = entry["thumbnails"].as_array() {
                                if let Some(last_thumb) = thumbnails.last().and_then(|t| t["url"].as_str()) {
                                    if !thumbnail_urls.contains(&last_thumb.to_string()) {
                                        thumbnail_urls.push(last_thumb.to_string());
                                    }
                                }
                            } else if let Some(thumb) = entry["thumbnail"].as_str() {
                                if !thumbnail_urls.contains(&thumb.to_string()) {
                                    thumbnail_urls.push(thumb.to_string());
                                }
                            }
                        }
                    }

                    if !video_urls.is_empty() {
                        return Ok(Json(ScrapeResult {
                            title: playlist_title,
                            paragraphs: descriptions,
                            images: thumbnail_urls,
                            videos: video_urls,
                            audios: Vec::new(),
                        }));
                    }
                }
            }
        }
    }

    // Twitter / X の場合、先に空ページでCookieを注入してから遷移
    let page_host = base_url.host_str().unwrap_or("");
    let is_twitter = page_host.contains("twitter.com") || page_host.contains("x.com");

    let page = if is_twitter {
        let page = state
            .browser
            .new_page("about:blank")
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("ページを開けませんでした: {}", e)))?;

        let effective_cookie = params.twitter_cookie.as_ref().or(state.twitter_auth_token.as_ref());
        if let Some(token) = effective_cookie {
            // .x.com と .twitter.com の両方に auth_token をセット
            for domain in [".x.com", ".twitter.com"] {
                let cookie_params = chromiumoxide::cdp::browser_protocol::network::SetCookieParams::builder()
                    .name("auth_token")
                    .value(token.clone())
                    .domain(domain)
                    .path("/")
                    .http_only(true)
                    .secure(true)
                    .build();
                if let Ok(p) = cookie_params {
                    let _ = page.execute(p).await;
                }
            }
        }
        let _ = page.goto(base_url.as_str()).await;
        // XのReactレンダリング待機
        tokio::time::sleep(Duration::from_millis(1500)).await;
        page
    } else {
        let page = state
            .browser
            .new_page(base_url.as_str())
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("ページを開けませんでした: {}", e)))?;
        // 動的サイト（SPA・React/Vue等）の初期DOMレンダリング待機
        tokio::time::sleep(Duration::from_millis(1500)).await;
        page
    };

    // 3. pixiv等の「すべて見る」・漫画サイトの「チャプターを見る」展開 ＆ 全自動深層スクロール
    let expand_and_scroll_script = r#"
        (async () => {
            // マウスイベント（mousedown/mouseup/click）をシミュレートして確実に発火
            const clickElement = (el) => {
                try {
                    el.scrollIntoView({ block: 'center', inline: 'center' });
                    const events = ['mouseenter', 'mouseover', 'mousedown', 'mouseup', 'click'];
                    for (const ev of events) {
                        el.dispatchEvent(new MouseEvent(ev, {
                            bubbles: true,
                            cancelable: true,
                            view: window,
                            buttons: 1
                        }));
                    }
                    if (typeof el.click === 'function') {
                        el.click();
                    }
                } catch(e) {}
            };

            // 最も深い子要素（親ラッパー要素への誤クリックを防止）を特定してクリック
            const candidateElements = Array.from(document.querySelectorAll('button, [role="button"], a, input[type="button"], input[type="submit"], div, span, p, .read-more, .load-more'));
            for (const el of candidateElements) {
                const text = (el.innerText || el.textContent || '').trim();
                // ページ全体のコンテナ誤爆を防ぐため長文要素は除外
                if (!text || text.length > 50) continue;

                const isTarget = [
                    'チャプターを見る',
                    'チャプター',
                    'すべて見る',
                    'もっと見る',
                    'See all',
                    '続きを読む',
                    'Load all',
                    'Read now',
                    'View all'
                ].some(keyword => text.includes(keyword));

                if (isTarget) {
                    // 直下に同一キーワードを持つ子要素がある場合は子要素側をクリックさせる
                    const hasChildTarget = Array.from(el.children).some(child => {
                        const childText = (child.innerText || child.textContent || '').trim();
                        return childText.includes('チャプター') || childText.includes('見る') || childText.includes('See') || childText.includes('Read');
                    });

                    if (!hasChildTarget) {
                        clickElement(el);
                    }
                }
            }

            // クリック後の非同期通信待ちを 1.2 秒に最適化
            await new Promise(r => setTimeout(r, 1200));

            // window だけでなく内部のスクロール可能コンテナも検出
            const scrollableElements = [window];
            document.querySelectorAll('div, section, main, article').forEach(elem => {
                if (elem.scrollHeight > elem.clientHeight + 100) {
                    const overflow = window.getComputedStyle(elem).overflowY;
                    if (overflow === 'auto' || overflow === 'scroll') {
                        scrollableElements.push(elem);
                    }
                }
            });

            // スクロール速度を大幅に高速化（間隔を40msに半減、移動距離を1500pxに拡大）
            await new Promise((resolve) => {
                let steps = 0;
                const maxSteps = 30;
                const distance = 1500;
                const timer = setInterval(() => {
                    for (const target of scrollableElements) {
                        if (target === window) {
                            window.scrollBy(0, distance);
                        } else {
                            target.scrollTop += distance;
                        }
                    }
                    steps++;

                    const scrollHeight = document.body.scrollHeight;
                    const currentScroll = window.scrollY + window.innerHeight;

                    if ((currentScroll >= scrollHeight && steps >= 6) || steps >= maxSteps) {
                        clearInterval(timer);
                        resolve();
                    }
                }, 40);
            });

            // DOM確定待機を 400ms に最適化
            await new Promise(r => setTimeout(r, 400));
        })()
    "#;
    let _ = page.evaluate(expand_and_scroll_script).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // 4. JavaScript実行・描画完了後の完全なDOM HTMLを取得（エラー時も確実にタブを閉じてメモリ解放）
    let html_content_result = page.content().await;
    let _ = page.close().await;

    let html_content = match html_content_result {
        Ok(content) => content,
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("HTMLの取得に失敗: {}", e))),
    };

    let document = Html::parse_document(&html_content);

    // タイトル取得
    let title = document
        .select(&TITLE_SELECTOR)
        .next()
        .map(|el| el.text().collect::<Vec<_>>().join(""));

    // 本文テキスト取得 (タグ本文 + メタタグ概要欄)
    let mut paragraphs: Vec<String> = Vec::new();
    let mut seen_texts: std::collections::HashSet<String> = std::collections::HashSet::new();

    // YouTube等の概要欄（meta description）を先頭に追加
    for element in document.select(&META_DESC_SELECTOR) {
        if let Some(content) = element.value().attr("content") {
            let trimmed = content.trim();
            if !trimmed.is_empty() && seen_texts.insert(trimmed.to_string()) {
                paragraphs.push(trimmed.to_string());
            }
        }
    }

    // 本文・AAテキスト取得（HTML構造の改行とレイアウトを維持）
    for element in document.select(&TEXT_SELECTOR) {
        // 親要素がすでに TEXT_SELECTOR に合致している場合は重複取得をスキップ
        let is_nested = element.ancestors().skip(1).any(|anc| {
            if let Some(anc_elem) = anc.value().as_element() {
                let name = anc_elem.name();
                let class_attr = anc_elem.attr("class").unwrap_or("");
                name == "dd" 
                    || name == "pre" 
                    || name == "blockquote" 
                    || name == "article" 
                    || name == "p" 
                    || class_attr.contains("t_b") 
                    || class_attr.contains("post_text") 
                    || class_attr.contains("message") 
                    || class_attr.contains("entry-content")
            } else {
                false
            }
        });

        if is_nested {
            continue;
        }

        let mut buf = String::new();
        for text in element.text() {
            buf.push_str(text);
        }

        // <br> を改行文字として反映したテキストを組み立て
        let mut formatted_text = String::new();
        for node in element.children() {
            if let Some(elem) = node.value().as_element() {
                if elem.name().eq_ignore_ascii_case("br") {
                    formatted_text.push('\n');
                } else if let Some(text_ref) = scraper::ElementRef::wrap(node) {
                    formatted_text.push_str(&text_ref.text().collect::<Vec<_>>().join(""));
                }
            } else if let Some(t) = node.value().as_text() {
                formatted_text.push_str(t);
            }
        }

        let final_text = if !formatted_text.is_empty() { formatted_text } else { buf };
        let trimmed = final_text.trim();
        if !trimmed.is_empty() && seen_texts.insert(trimmed.to_string()) {
            paragraphs.push(final_text);
        }
    }

    // 画像取得 (OGPサムネイル + <img> タグ)
    let mut images = Vec::new();

    // YouTube等のサムネイル（og:image）を最優先で追加
    for element in document.select(&META_IMG_SELECTOR) {
        if let Some(content) = element.value().attr("content") {
            if let Ok(absolute_url) = base_url.join(content) {
                let url_str = absolute_url.to_string();
                if !images.contains(&url_str) {
                    images.push(url_str);
                }
            }
        }
    }

    let img_attributes = [
        "data-original",
        "data-src",
        "data-lazy-src",
        "data-pagesrc",
        "data-full-url",
        "data-url",
        "data-cfsrc",
        "data-img",
        "data-origin",
        "data-source",
        "data-echo",
        "src",
    ];
    for element in document.select(&IMG_SELECTOR) {
        let mut found_for_element = false;

        // 1. 通常の属性 (data-src, src等) から取得（高解像度・原寸URLへ自動昇格）
    for attr in img_attributes {
        if let Some(src) = element.value().attr(attr) {
            if !src.starts_with("data:image") && !src.trim().is_empty() {
                if let Ok(absolute_url) = base_url.join(src.trim()) {
                    let mut url_str = absolute_url.to_string();

                    // pixiv (i.pximg.net) のサムネイルURLを高画質マスター画像に自動昇格
                    if url_str.contains("pximg.net") && url_str.contains("/c/") {
                        if let Some(pos) = url_str.find("/img-master/") {
                            url_str = format!("https://i.pximg.net{}", &url_str[pos..]);
                        } else if let Some(pos) = url_str.find("/img-original/") {
                            url_str = format!("https://i.pximg.net{}", &url_str[pos..]);
                        } else if let Some(pos) = url_str.find("/custom-thumb/") {
                            url_str = format!("https://i.pximg.net{}", &url_str[pos..]);
                        }
                    }

                    // Twitter / X (pbs.twimg.com) の画像を高解像度(orig)に自動昇格
                    if url_str.contains("pbs.twimg.com/media/") {
                        if let Ok(mut tw_url) = Url::parse(&url_str) {
                            let mut query_pairs: Vec<(String, String)> = tw_url.query_pairs().map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
                            for pair in &mut query_pairs {
                                if pair.0 == "name" {
                                    pair.1 = "orig".to_string();
                                }
                            }
                            tw_url.query_pairs_mut().clear().extend_pairs(query_pairs);
                            url_str = tw_url.to_string();
                        }
                    }

                    if !images.contains(&url_str) {
                        images.push(url_str);
                        found_for_element = true;
                        break;
                    }
                }
            }
        }
    }

        if found_for_element {
            continue;
        }

        // 2. srcset 属性 (レスポンシブ画像・高解像度画像) の解析 (例: "image-2x.jpg 2x, image-1x.jpg 1x")
        if let Some(srcset) = element.value().attr("srcset") {
            // カンマ区切りの最後の候補（最も高解像度な画像）を取得
            if let Some(last_entry) = srcset.split(',').last() {
                let candidate = last_entry.trim().split_whitespace().next().unwrap_or("");
                if !candidate.is_empty() && !candidate.starts_with("data:image") {
                    if let Ok(absolute_url) = base_url.join(candidate) {
                        let url_str = absolute_url.to_string();
                        if !images.contains(&url_str) {
                            images.push(url_str);
                            continue;
                        }
                    }
                }
            }
        }

        // 3. style="background-image: url(...)" 等のインラインCSSからの画像URL抽出
        if let Some(style) = element.value().attr("style") {
            if let Some(url_part) = style.split("url(").nth(1) {
                let raw_url = url_part.split(')').next().unwrap_or("").trim().trim_matches(|c| c == '\'' || c == '"');
                if !raw_url.is_empty() && !raw_url.starts_with("data:image") {
                    if let Ok(absolute_url) = base_url.join(raw_url) {
                        let url_str = absolute_url.to_string();
                        if !images.contains(&url_str) {
                            images.push(url_str);
                        }
                    }
                }
            }
        }
    }

    // 動画取得 (<video>, <source>, <meta og:video>, <iframe>, 主要動画サイトリンク)
    let mut videos = Vec::new();

    // 広告・トラッキングURLを除外する判定クロージャ（正規動画URLの誤除外を防ぐためドメイン・確定パスベースで判定）
    let is_ad_or_junk = |u: &str| -> bool {
        let lower = u.to_lowercase();
        lower.contains("doubleclick.net")
            || lower.contains("googlesyndication.com")
            || lower.contains("trafficfactory.biz")
            || lower.contains("exoclick.com")
            || lower.contains("juicyads.com")
            || lower.contains("adsterra.com")
            || lower.contains("popcash.net")
            || lower.contains("tsyndicate.com")
            || lower.contains("ero-advertising.com")
            || lower.contains("/pagead/")
            || lower.contains("/adv/")
            || lower.contains("/ad-server/")
    };

    // 1. <video> / <source> タグ（広告URLは除外）
    for element in document.select(&VIDEO_SELECTOR) {
        if let Some(src) = element.value().attr("src") {
            if let Ok(absolute_url) = base_url.join(src) {
                let url_str = absolute_url.to_string();
                if !url_str.starts_with("blob:") && !url_str.starts_with("data:") && !is_ad_or_junk(&url_str) && !videos.contains(&url_str) {
                    videos.push(url_str);
                }
            }
        }
    }

    // 1.5. <script> タグ内の本編動画URL (m3u8 / mp4) を抽出（XVideos, 各種プレイヤー対応）
    if let Ok(script_selector) = Selector::parse("script") {
        for script in document.select(&script_selector) {
            let script_text = script.text().collect::<Vec<_>>().join("");
            if script_text.contains(".m3u8") || script_text.contains(".mp4") {
                // http(s) から始まる動画URLパターンを抽出
                for part in script_text.split(['"', '\'', '`', '\\']) {
                    if (part.starts_with("http://") || part.starts_with("https://")) 
                        && (part.contains(".m3u8") || part.contains(".mp4"))
                        && !is_ad_or_junk(part)
                    {
                        let clean_url = part.replace("\\/", "/");
                        if !videos.contains(&clean_url) {
                            videos.push(clean_url);
                        }
                    }
                }
            }
        }
    }

    // 2. <meta property="og:video"> 等のメタタグ
    for element in document.select(&META_VIDEO_SELECTOR) {
        if let Some(content) = element.value().attr("content") {
            // Googleログインや不要なリダイレクトURLは除外
            if !content.contains("accounts.google.com") && !content.contains("signin") {
                if let Ok(absolute_url) = base_url.join(content) {
                    let url_str = absolute_url.to_string();
                    if !videos.contains(&url_str) {
                        videos.push(url_str);
                    }
                }
            }
        }
    }

    // 3. <iframe> 埋め込み動画（本物の動画プレイヤーのみに限定）
    for element in document.select(&IFRAME_SELECTOR) {
        if let Some(src) = element.value().attr("src") {
            if let Ok(absolute_url) = base_url.join(src) {
                let url_str = absolute_url.to_string();
                let lower = url_str.to_lowercase();
                
                // 本物の動画プレイヤーのURLパターンのみ許可
                let is_real_video_iframe = lower.contains("youtube.com/embed/")
                    || lower.contains("player.vimeo.com")
                    || lower.contains("embed.nicovideo.jp")
                    || lower.contains("pornhub.com/embed/")
                    || lower.contains("xvideos.com/embedframe/")
                    || lower.contains("bilibili.com/player")
                    || lower.contains("spankbang.com/embed")
                    || lower.ends_with(".mp4")
                    || lower.ends_with(".webm")
                    || lower.ends_with(".m3u8");

                if is_real_video_iframe && !is_ad_or_junk(&url_str) && !videos.contains(&url_str) {
                    videos.push(url_str);
                }
            }
        }
    }

    // 音声取得 (<audio>, <source>)
    let mut audios = Vec::new();
    for element in document.select(&AUDIO_SELECTOR) {
        if let Some(src) = element.value().attr("src") {
            if let Ok(absolute_url) = base_url.join(src) {
                let url_str = absolute_url.to_string();
                if !audios.contains(&url_str) {
                    audios.push(url_str);
                }
            }
        }
    }

    // 4. <a> タグの中の画像・動画・音声ファイルおよび動画サイトへのリンク
    for element in document.select(&LINK_SELECTOR) {
        if let Some(href) = element.value().attr("href") {
            let lower = href.to_lowercase();
            let clean_path = lower.split(['?', '#']).next().unwrap_or("");

            // 画像直リンク
            if clean_path.ends_with(".jpg") || clean_path.ends_with(".jpeg") || clean_path.ends_with(".png") || clean_path.ends_with(".gif") || clean_path.ends_with(".webp") {
                if let Ok(absolute_url) = base_url.join(href) {
                    let url_str = absolute_url.to_string();
                    if !images.contains(&url_str) {
                        images.push(url_str);
                    }
                }
            }
            // 音声直リンク
            if clean_path.ends_with(".mp3") || clean_path.ends_with(".wav") || clean_path.ends_with(".ogg") || clean_path.ends_with(".m4a") || clean_path.ends_with(".flac") || clean_path.ends_with(".aac") {
                if let Ok(absolute_url) = base_url.join(href) {
                    let url_str = absolute_url.to_string();
                    if !audios.contains(&url_str) {
                        audios.push(url_str);
                    }
                }
            }
            // 動画直リンク または 主要動画サイト（YouTube, ニコニコ, TikTok, Threads, 各種動画サイト等）
            let is_video_site = lower.contains("youtube.com/watch")
                || lower.contains("youtu.be/")
                || lower.contains("nicovideo.jp/watch")
                || lower.contains("tiktok.com/")
                || lower.contains("threads.net/")
                || lower.contains("instagram.com/reel/")
                || lower.contains("instagram.com/p/")
                || lower.contains("pornhub.com/view_video.php")
                || lower.contains("xvideos.com/video")
                || lower.contains("missav.")
                || clean_path.ends_with(".mp4")
                || clean_path.ends_with(".webm")
                || clean_path.ends_with(".m3u8");

            if is_video_site {
                if let Ok(absolute_url) = base_url.join(href) {
                    let url_str = absolute_url.to_string();
                    // blob: や data: スキームを除外
                    if !url_str.starts_with("blob:") && !url_str.starts_with("data:") && !videos.contains(&url_str) {
                        videos.push(url_str);
                    }
                }
            }
        }
    }

    // YouTube / 主要動画サイトのURL正規化クロージャ
    let normalize_video_url = |raw: &str| -> Option<String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with("blob:") || trimmed.starts_with("data:") {
            return None;
        }
        if trimmed.contains("youtube.com") || trimmed.contains("youtu.be") {
            let vid: String = if let Some(p) = trimmed.split("youtu.be/").nth(1) {
                p.split(['?', '#', '/']).next().unwrap_or("").to_string()
            } else if let Some(p) = trimmed.split("/embed/").nth(1) {
                p.split(['?', '#', '/']).next().unwrap_or("").to_string()
            } else if let Some(p) = trimmed.split("/shorts/").nth(1) {
                p.split(['?', '#', '/']).next().unwrap_or("").to_string()
            } else if let Ok(u) = Url::parse(trimmed) {
                u.query_pairs().find(|(k, _)| k == "v").map(|(_, v)| v.to_string()).unwrap_or_default()
            } else {
                String::new()
            };
            if !vid.is_empty() && vid.len() >= 8 {
                return Some(format!("https://www.youtube.com/watch?v={}", vid));
            }
            return None;
        }
        Some(trimmed.to_string())
    };

    // 重複・無効URLのクリーンアップ
    let mut cleaned_videos = Vec::new();
    for v in videos {
        if let Some(norm) = normalize_video_url(&v) {
            if !cleaned_videos.contains(&norm) {
                cleaned_videos.push(norm);
            }
        }
    }
    videos = cleaned_videos;

    // 入力対象URL自体が動画の場合の追加
    let base_lower = base_url.as_str().to_lowercase();
    if base_lower.contains("missav.")
        || base_lower.contains("youtube.com")
        || base_lower.contains("youtu.be")
        || base_lower.contains("nicovideo.jp/watch")
        || base_lower.contains("tiktok.com/")
        || base_lower.contains("threads.net/")
        || base_lower.contains("instagram.com/reel/")
        || base_lower.contains("instagram.com/p/")
        || base_lower.contains("pornhub.com/view_video.php")
        || base_lower.contains("xvideos.com/video")
        || (base_lower.contains("twitter.com") && base_lower.contains("/status/"))
        || (base_lower.contains("x.com") && base_lower.contains("/status/"))
    {
        if let Some(norm) = normalize_video_url(base_url.as_str()) {
            if !videos.contains(&norm) {
                videos.insert(0, norm);
            }
        }
    }

    Ok(Json(ScrapeResult {
        title,
        paragraphs,
        images,
        videos,
        audios,
    }))
}
#[derive(Deserialize)]
struct DownloadParams {
    url: String,
    audio_only: Option<bool>,
    format: Option<String>,
}

// その動画で利用可能な実際の解像度一覧（例: [2160, 1080, 720, 480, 360]）を取得するハンドラ
async fn video_formats_handler(
    State(state): State<AppState>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<SocketAddr>,
    Query(params): Query<DownloadParams>,
) -> Result<Json<Vec<u32>>, (StatusCode, String)> {
    if !state.check_rate_limit(addr.ip()).await {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "短時間にリクエストが多すぎます。1分ほど待ってから再試行してください。".to_string(),
        ));
    }

    let mut cmd = Command::new("yt-dlp");
    cmd.kill_on_drop(true); // タイムアウト時にOS側の子プロセスを確実に強制終了
    cmd.arg("-J").arg("--no-playlist").arg("--no-warnings").arg("--no-update").arg("--").arg(&params.url);

    let output = tokio::time::timeout(Duration::from_secs(15), cmd.output())
        .await
        .map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "解析がタイムアウトしました".to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("解析エラー: {}", e)))?;

    if !output.status.success() {
        return Err((StatusCode::BAD_REQUEST, "動画情報を取得できませんでした".to_string()));
    }

    let json: Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut heights = std::collections::BTreeSet::new();
    if let Some(formats) = json["formats"].as_array() {
        for f in formats {
            if let Some(h) = f["height"].as_u64() {
                if h > 0 {
                    heights.insert(h as u32);
                }
            }
        }
    }

    let mut list: Vec<u32> = heights.into_iter().collect();
    list.sort_by(|a, b| b.cmp(a)); // 高解像度順 (4K -> 1080 -> 720...)
    Ok(Json(list))
}

// yt-dlp を使ってブラウザ経由でダウンロードさせる処理
async fn download_handler(
    State(state): State<AppState>,
    Query(params): Query<DownloadParams>,
) -> Result<Response, (StatusCode, String)> {
    // 同時ダウンロード数制限：空きがない場合は混雑エラー
    let _permit = match tokio::time::timeout(Duration::from_secs(5), state.download_semaphore.acquire()).await {
        Ok(Ok(permit)) => permit,
        _ => return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "ダウンロード処理が混み合っています。数分後に再度お試しください。".to_string(),
        )),
    };

    // リクエストごとにユニークな一時ディレクトリを作成（衝突回避とクリーンアップ用）
    let temp_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let temp_dir = format!("./temp_downloads_{}", temp_id);
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("一時フォルダ作成失敗: {}", e)))?;

    // スキーム検証（引数インジェクション対策）
    let parsed_url = Url::parse(&params.url)
        .map_err(|_| (StatusCode::BAD_REQUEST, "無効なURL形式です。".to_string()))?;
    if parsed_url.scheme() != "http" && parsed_url.scheme() != "https" {
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        return Err((StatusCode::BAD_REQUEST, "http または https のURLのみ指定可能です。".to_string()));
    }

    // embed形式のYouTube URL (/embed/xxx) を通常の watch?v=xxx に正規化
    let mut target_url = params.url.clone();
    if params.url.contains("/embed/") {
        if let Some(after_embed) = params.url.split("/embed/").nth(1) {
            let id = after_embed.split(['?', '#']).next().unwrap_or("");
            if !id.is_empty() {
                target_url = format!("https://www.youtube.com/watch?v={}", id);
            }
        }
    }

    // yt-dlp コマンドを構築
    let mut cmd = Command::new("yt-dlp");
    cmd.kill_on_drop(true); // タイムアウトやクライアント切断時に確実に子プロセスを強制終了
    cmd.arg("-P").arg(&temp_dir);
    cmd.arg("-o").arg("%(title)s.%(ext)s");
    cmd.arg("--no-playlist");
    cmd.arg("--no-update");
    cmd.arg("--no-warnings");

    // 一般的なブラウザのUser-AgentとRefererを設定してアクセス拒否を回避
    cmd.arg("--user-agent").arg("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
    cmd.arg("--referer").arg(&target_url);

    // YouTube 403 Forbidden (Bot判定) 回避オプション
    cmd.arg("--extractor-args").arg("youtube:player_client=android,web");

    // 保存フォーマット判定（指定がない場合、従来の audio_only=true なら mp3、それ以外は mp4 をデフォルトとする）
    let format_str = params
        .format
        .as_deref()
        .unwrap_or(if params.audio_only.unwrap_or(false) { "mp3" } else { "mp4" });

    // 数値（例: "1080", "720", "2160" 等）が来たらその解像度で取得
    if let Ok(h) = format_str.parse::<u32>() {
        cmd.arg("-f").arg(format!("bv*[height<={h}]+ba/b[height<={h}]/best"));
        cmd.arg("--merge-output-format").arg("mp4");
    } else {
        match format_str {
            "mp3" => {
                cmd.arg("-x").arg("--audio-format").arg("mp3");
            }
            "m4a" => {
                cmd.arg("-x").arg("--audio-format").arg("m4a");
            }
            "wav" => {
                cmd.arg("-x").arg("--audio-format").arg("wav");
            }
            "webm" => {
                cmd.arg("-f").arg("bv*[ext=webm]+ba*[ext=webm]/b[ext=webm]/bv*+ba/b");
                cmd.arg("--merge-output-format").arg("webm");
            }
            _ => {
                // デフォルト: 制限なし最高画質
                cmd.arg("-f").arg("bv*+ba/b");
                cmd.arg("--merge-output-format").arg("mp4");
            }
        }
    }

    cmd.arg("--");
    cmd.arg(&target_url);

    // yt-dlp 実行（最大5分のタイムアウトを設定）
    let output = match tokio::time::timeout(Duration::from_secs(300), cmd.output()).await {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("⚠️ yt-dlpの実行に失敗しました: {}", e),
            ));
        }
        Err(_) => {
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            return Err((
                StatusCode::GATEWAY_TIMEOUT,
                "⏳ ダウンロードがタイムアウトしました（動画サイズが大きすぎるか、ネットワークが混雑しています）。".to_string(),
            ));
        }
    };

    if !output.status.success() {
        let err_msg = String::from_utf8_lossy(&output.stderr);
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("yt-dlp実行エラー:\n{}", err_msg)));
    }

    // 生成されたメディアファイルを取得（画像サムネイルや中間ファイルを除外し、動画・音声拡張子を優先探索）
    let mut dir_entries = tokio::fs::read_dir(&temp_dir)
        .await
        .map_err(|e| {
            let _ = tokio::fs::remove_dir_all(&temp_dir);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("フォルダ読み取り失敗: {}", e))
        })?;

    let valid_extensions = ["mp4", "webm", "mkv", "mp3", "m4a", "wav", "flac", "ogg", "aac"];
    let mut found_file = None;

    while let Ok(Some(entry)) = dir_entries.next_entry().await {
        let path = entry.path();
        if path.is_file() {
            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_lowercase();
            if valid_extensions.contains(&ext.as_str()) {
                found_file = Some(path);
                break;
            }
        }
    }

    let downloaded_file = match found_file {
        Some(path) => path,
        None => {
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "ダウンロードファイルが見つかりません".to_string()));
        }
    };

    // ファイル名を取得
    let filename = downloaded_file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("downloaded_media")
        .to_string();

    // 拡張子に応じた Content-Type の設定
    let ext = downloaded_file
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    let content_type = match ext.as_str() {
        "mp3" => "audio/mpeg",
        "m4a" => "audio/mp4",
        "wav" => "audio/wav",
        "webm" => "video/webm",
        "ogg" => "audio/ogg",
        "flac" => "audio/flac",
        _ => "video/mp4",
    };

    // ファイルをストリーミング用に開く（低メモリ環境でのOOMクラッシュ防止）
    let file = match tokio::fs::File::open(&downloaded_file).await {
        Ok(f) => f,
        Err(e) => {
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("ファイルオープン失敗: {}", e)));
        }
    };

    // 送信用ヘッダー構築
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));

    // 日本語ファイル名の文字化け防止 (RFC 5987 / RFC 6266 標準準拠: スペースを%20でエンコード)
    let encoded_filename: String = url::form_urlencoded::byte_serialize(filename.as_bytes())
        .collect::<String>()
        .replace('+', "%20");
    let ascii_safe_filename = filename.replace(|c: char| !c.is_ascii() || c == '"' || c == '\\', "_");
    let disposition_val = format!("attachment; filename=\"{}\"; filename*=UTF-8''{}", ascii_safe_filename, encoded_filename);
    if let Ok(val) = HeaderValue::from_str(&disposition_val) {
        headers.insert(header::CONTENT_DISPOSITION, val);
    }

    // ストリームを生成してレスポンス（バックグラウンドクリーンアップタスク付き）
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = axum::body::Body::from_stream(stream);

    // 送信完了後にフォルダを削除するガードタスク
    // ※ エフェメラル環境でのディスク枯渇を防ぐため15分後に自動クリーンアップ
    let temp_dir_cleanup = temp_dir.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(900)).await;
        let _ = tokio::fs::remove_dir_all(&temp_dir_cleanup).await;
    });

    Ok((headers, body).into_response())
}

// 外部画像・メディアダウンロード時のCORS制約を回避するプロキシハンドラ
async fn proxy_handler(
    State(state): State<AppState>,
    Query(params): Query<ProxyParams>,
) -> Result<Response, (StatusCode, String)> {
    let parsed_url = Url::parse(&params.url)
        .map_err(|_| (StatusCode::BAD_REQUEST, "無効な URL です。".to_string()))?;

    // blob: や data: など http/https 以外の不正スキームを即座に拒否して 502エラーを防ぐ
    if parsed_url.scheme() != "http" && parsed_url.scheme() != "https" {
        return Err((StatusCode::BAD_REQUEST, "httpまたはhttpsのURLのみ指定可能です（blob:や一時URLはプロキシできません）。".to_string()));
    }

    // SSRF対策：localhost やプライベートIP（10.x, 172.16-31.x, 192.168.x）、リンクローカル、0.0.0.0 へのリクエストを厳格に遮断
    if let Some(host) = parsed_url.host_str() {
        let host_lower = host.trim_matches(|c| c == '[' || c == ']').to_lowercase();
        let is_private_172 = if host_lower.starts_with("172.") {
            host_lower.split('.').nth(1)
                .and_then(|octet| octet.parse::<u8>().ok())
                .map(|val| (16..=31).contains(&val))
                .unwrap_or(false)
        } else {
            false
        };

        if host_lower == "localhost"
            || host_lower == "0.0.0.0"
            || host_lower.starts_with("127.")
            || host_lower.starts_with("10.")
            || host_lower.starts_with("192.168.")
            || host_lower.starts_with("169.254.")
            || is_private_172
            || host_lower == "::1"
            || host_lower == "::"
            || host_lower.starts_with("fc00:")
            || host_lower.starts_with("fe80:")
        {
            return Err((StatusCode::FORBIDDEN, "ローカルアドレスへのプロキシは許可されていません。".to_string()));
        }
    }

    // 403 Forbidden 回避用：URLに応じた適切な Referer（参照元）ヘッダーとブラウザ標準ヘッダーを自動生成
    let mut req_builder = state.client.get(parsed_url.as_str())
        .header(reqwest::header::USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        .header(reqwest::header::ACCEPT, "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8")
        .header("sec-ch-ua", "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"")
        .header("sec-ch-ua-mobile", "?0")
        .header("sec-ch-ua-platform", "\"Windows\"")
        .header("sec-fetch-dest", "image")
        .header("sec-fetch-mode", "no-cors")
        .header("sec-fetch-site", "cross-site");

    let host = parsed_url.host_str().unwrap_or("");
    // pximg.net (Pixiv) は外部サイトのリファラを拒否するため最優先で固定
    if host.contains("pximg.net") || host.contains("pixiv") {
        req_builder = req_builder.header(reqwest::header::REFERER, "https://www.pixiv.net/");
    } else if host.contains("twimg.com") || host.contains("twitter") || host.contains("x.com") {
        req_builder = req_builder.header(reqwest::header::REFERER, "https://x.com/");
    } else if let Some(ref ref_url) = params.referer {
        // その他一般CDN向け：親ページURLを優先
        req_builder = req_builder.header(reqwest::header::REFERER, ref_url.as_str());
    } else {
        // 一般サイト向け：その画像のオリジンをリファラとして設定
        let origin = format!("{}://{}", parsed_url.scheme(), host);
        req_builder = req_builder.header(reqwest::header::REFERER, origin);
    }

    let response = req_builder
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("取得失敗: {}", e)))?;

    let mut content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    // サーバーが汎用バイナリ(octet-stream)等を返してきた場合、URLの拡張子から画像MIMEタイプを自動補正
    if content_type == "application/octet-stream" || content_type.is_empty() {
        let path_lower = parsed_url.path().to_lowercase();
        if path_lower.ends_with(".jpg") || path_lower.ends_with(".jpeg") || path_lower.contains("@jpeg") || path_lower.contains("@jpg") {
            content_type = "image/jpeg".to_string();
        } else if path_lower.ends_with(".png") || path_lower.contains("@png") {
            content_type = "image/png".to_string();
        } else if path_lower.ends_with(".webp") {
            content_type = "image/webp".to_string();
        } else if path_lower.ends_with(".gif") {
            content_type = "image/gif".to_string();
        } else if path_lower.ends_with(".mp4") {
            content_type = "video/mp4".to_string();
        } else if path_lower.ends_with(".mp3") {
            content_type = "audio/mpeg".to_string();
        }
    }

    let mut headers = HeaderMap::new();
    if let Ok(ct) = HeaderValue::from_str(&content_type) {
        headers.insert(header::CONTENT_TYPE, ct);
    }

    // メモリに一括展開せずストリーミング転送（数GBの動画中継でもRAM消費はわずか数KB）
    let stream = response.bytes_stream();
    let body = axum::body::Body::from_stream(stream);

    Ok((headers, body).into_response())
}

// ページ全体をフォント完全保持でPDF出力するハンドラ
async fn pdf_handler(
    State(state): State<AppState>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<SocketAddr>,
    Query(params): Query<ScrapeParams>,
) -> Result<Response, (StatusCode, String)> {
    if !state.check_rate_limit(addr.ip()).await {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "短時間にリクエストが多すぎます。1分ほど待ってから再試行してください。".to_string(),
        ));
    }

    // 同時実行制限：ブラウザの同時起動数を制限
    let _permit = match tokio::time::timeout(Duration::from_secs(5), state.browser_semaphore.acquire()).await {
        Ok(Ok(permit)) => permit,
        _ => return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "現在アクセスが集中しています。恐れ入りますが、数十秒ほど待ってから再度お試しください。".to_string(),
        )),
    };

    // 30秒のタイムアウトを設定してハングを防止
    let pdf_future = do_pdf(state.clone(), params);
    match tokio::time::timeout(Duration::from_secs(30), pdf_future).await {
        Ok(result) => result,
        Err(_) => Err((StatusCode::GATEWAY_TIMEOUT, "PDF生成がタイムアウトしました（30秒）。ページが重すぎるかアクセスが遮断された可能性があります。".to_string())),
    }
}

async fn do_pdf(
    state: AppState,
    params: ScrapeParams,
) -> Result<Response, (StatusCode, String)> {
    let base_url = Url::parse(&params.url)
        .map_err(|_| (StatusCode::BAD_REQUEST, "無効なURLです。".to_string()))?;

    if base_url.scheme() != "http" && base_url.scheme() != "https" {
        return Err((StatusCode::BAD_REQUEST, "http または https のURLを指定してください。".to_string()));
    }

    let page_host = base_url.host_str().unwrap_or("");
    let is_pixiv = page_host.contains("pixiv.net");
    let is_twitter = page_host.contains("twitter.com") || page_host.contains("x.com");

    let page = if is_pixiv || is_twitter {
        let page = state
            .browser
            .new_page("about:blank")
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("ページを開けませんでした: {}", e)))?;

        if is_pixiv {
            let effective_phpsessid = params.pixiv_cookie.as_ref().or(state.pixiv_phpsessid.as_ref());
            if let Some(phpsessid) = effective_phpsessid {
                let cookie_params = chromiumoxide::cdp::browser_protocol::network::SetCookieParams::builder()
                    .name("PHPSESSID")
                    .value(phpsessid.clone())
                    .domain(".pixiv.net")
                    .path("/")
                    .http_only(true)
                    .secure(true)
                    .build();
                if let Ok(p_cookie) = cookie_params {
                    let _ = page.execute(p_cookie).await;
                }
            }
        } else if is_twitter {
            let effective_cookie = params.twitter_cookie.as_ref().or(state.twitter_auth_token.as_ref());
            if let Some(token) = effective_cookie {
                for domain in [".x.com", ".twitter.com"] {
                    let cookie_params = chromiumoxide::cdp::browser_protocol::network::SetCookieParams::builder()
                        .name("auth_token")
                        .value(token.clone())
                        .domain(domain)
                        .path("/")
                        .http_only(true)
                        .secure(true)
                        .build();
                    if let Ok(p) = cookie_params {
                        let _ = page.execute(p).await;
                    }
                }
            }
        }

        let _ = page.goto(base_url.as_str()).await;
        page
    } else {
        state
            .browser
            .new_page(base_url.as_str())
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("ページを開けませんでした: {}", e)))?
    };

    // 動的コンテンツの初期レンダリング待機
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // PDF生成前にも自動展開＆スクロールを行い、LazyLoad画像や隠し要素を読み込ませる
    let expand_script = r#"
        (() => {
            const buttons = Array.from(document.querySelectorAll('button, [role="button"]'));
            for (const btn of buttons) {
                const text = btn.innerText || '';
                if (text.includes('すべて見る') || text.includes('もっと見る') || text.includes('See all') || text.includes('続きを読む')) {
                    try { btn.click(); } catch(e) {}
                }
            }
        })()
    "#;
    let _ = page.evaluate(expand_script).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let _ = page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.33);").await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let _ = page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.66);").await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let _ = page.evaluate("window.scrollTo(0, document.body.scrollHeight);").await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    // PDF生成オプション（背景グラフィック印刷を有効化）
    let pdf_options = chromiumoxide::cdp::browser_protocol::page::PrintToPdfParams::builder()
        .print_background(true)
        .build();

    let pdf_bytes_result = page.pdf(pdf_options).await;
    // PDF生成の成否にかかわらず確実にタブを閉じてメモリ解放
    let _ = page.close().await;

    let pdf_bytes = match pdf_bytes_result {
        Ok(bytes) => bytes,
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("PDF生成失敗: {}", e))),
    };

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/pdf"));
    let disposition_val = "attachment; filename=\"page.pdf\"; filename*=UTF-8''page.pdf";
    if let Ok(val) = HeaderValue::from_str(&disposition_val) {
        headers.insert(header::CONTENT_DISPOSITION, val);
    }

    Ok((headers, pdf_bytes).into_response())
}