//! telepathyd — the steering-plane daemon.
//! v0: lane HTTP API (same endpoints as the Node bridge's api.ts).
//! Next: WS endpoint speaking the telepathy protocol, then the Hermes relay.

use axum::{extract::State, routing::{get, post}, Json, Router};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use telepathy_lanes::LaneRegistry;

struct AppState {
    reg: Mutex<LaneRegistry>,
    path: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let lanes_path = std::env::var("TELEPATHY_LANES").unwrap_or_else(|_| "lanes.json".into());
    let state = Arc::new(AppState {
        reg: Mutex::new(LaneRegistry::load(&PathBuf::from(&lanes_path))),
        path: PathBuf::from(lanes_path),
    });

    let app = Router::new()
        .route("/api/state", get(get_state))
        .route("/api/lanes", post(create_lane))
        .route("/api/lanes/active", post(set_active))
        .route("/api/lanes/touch", post(touch))
        .with_state(state);

    let port = std::env::var("TELEPATHY_API_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8790);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!("telepathyd lane API on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_state(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let mut reg = state.reg.lock().unwrap();
    let active_id = reg.active_id.clone();
    reg.touch(&active_id);
    let active = reg.active().name.clone();
    let mut body = serde_json::to_value(&*reg).unwrap();
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
    let mut reg = state.reg.lock().unwrap();
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
    let mut reg = state.reg.lock().unwrap();
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
    let mut reg = state.reg.lock().unwrap();
    reg.touch(&id);
    reg.save(&state.path);
    Json(serde_json::json!({ "ok": true }))
}
