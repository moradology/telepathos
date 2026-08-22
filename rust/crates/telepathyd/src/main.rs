//! telepathyd — the steering-plane daemon.
//! v0: lane HTTP API (same endpoints as the Node bridge's api.ts).
//! Next: WS endpoint speaking the telepathy protocol, then the Hermes relay.

use axum::{response::{IntoResponse, Response}, extract::State, http::StatusCode, routing::{get, post}, Json, Router};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use telepathy_lanes::LaneRegistry;

mod hermes_search;
mod relay;

use relay::RelayState;

struct AppState {
    reg: Mutex<LaneRegistry>,
    path: PathBuf,
    relay: Arc<RelayState>,
    msg_seq: std::sync::atomic::AtomicU64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let lanes_path = std::env::var("TELEPATHY_LANES").unwrap_or_else(|_| "lanes.json".into());
    let relay = Arc::new(RelayState::default());
    let secrets: Vec<String> = std::env::var("TELEPATHY_RELAY_SECRETS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let pending_path = std::env::var("TELEPATHY_PENDING").unwrap_or_else(|_| "pending.json".into());
    relay.set_persist_path(&PathBuf::from(&pending_path));

    let state = Arc::new(AppState {
        reg: Mutex::new(LaneRegistry::load(&PathBuf::from(&lanes_path))),
        path: PathBuf::from(lanes_path),
        relay: relay.clone(),
        msg_seq: std::sync::atomic::AtomicU64::new(0),
    });

    // search backend: read-only FTS over the Hermes session store
    if let Ok(db) = std::env::var("TELEPATHY_HERMES_STATE_DB") {
        telepathy_steering::set_search_backend(move |query| {
            hermes_search::search_sessions(&db, query, &[])
        });
    }

    let relay_router = relay::router(relay.clone(), secrets);

    let app = Router::new()
        .route("/api/state", get(get_state))
        .route("/api/pending", get(get_pending))
        .route("/api/message", post(post_message))
        .route("/api/delivery", get(get_delivery))
        .nest_service("/relay", relay_router)
        .route("/api/lanes", post(create_lane))
        .route("/api/lanes/active", post(set_active))
        .route("/api/lanes/touch", post(touch))
        .route("/api/meta", post(meta))
        .with_state(state);

    let port = std::env::var("TELEPATHY_API_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8790);
    let bind = std::env::var("TELEPATHY_BIND").unwrap_or_else(|_| "127.0.0.1".into());
    let addr: SocketAddr = format!("{bind}:{port}")
        .parse()
        .unwrap_or(SocketAddr::from(([127, 0, 0, 1], port)));
    println!("telepathyd lane API on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// Pending (undelivered) items for the ACTIVE lane — phone checks on mic-open.
async fn get_pending(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let reg = state.reg.lock().await;
    let lane = reg.active();
    Json(serde_json::json!({
        "lane_id": lane.id,
        "count": state.relay.pending_count(&lane.id),
    }))
}

/// Phone bridge → lane: wrap as MessageEvent and push to the gateway.
/// 400 missing text · 404 unknown lane · 503 no gateway dialed in.
async fn post_message(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let lane_id = body["lane_id"].as_str().unwrap_or("telepathy:direct").to_string();
    let text = body["text"].as_str().unwrap_or_default().to_string();
    if text.is_empty() {
        return (StatusCode::BAD_REQUEST, "text required").into_response();
    }
    let lane_name = {
        let reg = state.reg.lock().await;
        match reg.lanes.iter().find(|l| l.id == lane_id) {
            Some(l) => l.name.clone(),
            None => return (StatusCode::NOT_FOUND, format!("unknown lane {lane_id}")).into_response(),
        }
    };
    let seq = state.msg_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let event = relay::message_event(&lane_id, &lane_name, &text, seq);
    match state.relay.push_inbound(&event).await {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

/// Phone bridge polls for gateway replies (chat_id-filtered by caller).
#[derive(serde::Deserialize)]
struct DeliveryQuery {
    #[serde(default)]
    after: u64,
    /// true → remove returned entries (phone has taken responsibility)
    #[serde(default)]
    consume: bool,
}

async fn get_delivery(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<DeliveryQuery>,
) -> Json<serde_json::Value> {
    let (items, latest) = state.relay.deliveries_after(q.after, q.consume);
    Json(serde_json::json!({ "deliveries": items, "latest": latest }))
}

async fn get_state(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let mut reg = state.reg.lock().await;
    let active_id = reg.active_id.clone();
    reg.touch(&active_id);
    let active = reg.active().name.clone();
    let mut body = serde_json::to_value(&*reg).unwrap();

    // enrich lanes with session titles from the Hermes store, when available
    if let Ok(db) = std::env::var("TELEPATHY_HERMES_STATE_DB") {
        let titles = hermes_search::latest_titles(&db);
        if let Some(lanes) = body["lanes"].as_array_mut() {
            for lane in lanes.iter_mut() {
                if let Some(id) = lane["id"].as_str() {
                    if let Some((_, title)) = titles.iter().find(|(cid, _)| cid == id) {
                        lane["title"] = serde_json::json!(title);
                    }
                }
            }
        }
    }

    body["active"] = serde_json::json!(active);
    Json(body)
}

async fn create_lane(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let name = body["name"].as_str().unwrap_or_default().to_string();
    if name.is_empty() {
        return Json(serde_json::json!({"error": "name required"}));
    }
    let mut reg = state.reg.lock().await;
    let lane = reg.create(&name);
    reg.switch(&lane.id);
    reg.save(&state.path);
    Json(serde_json::json!({ "ok": true, "lane": lane }))
}

async fn set_active(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let id = body["id"].as_str().unwrap_or_default().to_string();
    let mut reg = state.reg.lock().await;
    match reg.switch(&id) {
        Some(lane) => {
            reg.save(&state.path);
            Json(serde_json::json!({ "ok": true, "lane": lane }))
        }
        None => Json(serde_json::json!({ "error": format!("unknown lane {id}") })),
    }
}

async fn touch(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let id = body["id"].as_str().unwrap_or_default().to_string();
    let mut reg = state.reg.lock().await;
    reg.touch(&id);
    reg.save(&state.path);
    Json(serde_json::json!({ "ok": true }))
}

/// POST /api/meta {"utterance": "..."} — deterministic grammar first,
/// then (when TELEPATHY_META_MODEL is set) the steering agent.
async fn meta(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let utterance = body["utterance"].as_str().unwrap_or_default().to_string();
    // parse needs the lock only briefly
    let action = {
        let reg = state.reg.lock().await;
        telepathy_lanes::parse_meta(&utterance, &reg)
    };
    let reply = match &action {
        // deterministic verbs run locally, instantly
        telepathy_lanes::MetaAction::Switch(_)
        | telepathy_lanes::MetaAction::List
        | telepathy_lanes::MetaAction::New(_)
        | telepathy_lanes::MetaAction::Brief(_) => {
            let mut reg = state.reg.lock().await;
            telepathy_lanes::execute(&mut reg, action.clone())
        }
        // grammar miss → steering agent when configured
        telepathy_lanes::MetaAction::Unknown => {
            let model = std::env::var("TELEPATHY_META_MODEL").unwrap_or_default();
            if model.is_empty() {
                let mut reg = state.reg.lock().await;
                telepathy_lanes::execute(&mut reg, action.clone())
            } else {
                let provider = telepathy_steering::OpenAiProvider {
                    base_url: std::env::var("TELEPATHY_META_BASE_URL")
                        .unwrap_or_else(|_| "https://api.openai.com/v1".into()),
                    api_key: std::env::var("OPENAI_API_KEY").unwrap_or_default(),
                    model,
                };
                let mut reg = state.reg.lock().await;
                telepathy_steering::run(&provider, &mut reg, &utterance)
                    .await
                    .unwrap_or_else(|e| format!("Steering agent error: {e}"))
            }
        }
    };
    Json(serde_json::json!({ "reply": reply }))
}
