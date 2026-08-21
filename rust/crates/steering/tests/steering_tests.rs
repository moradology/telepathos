use telepathy_lanes::LaneRegistry;
use telepathy_steering::{execute_tool, run, NullProvider};

#[tokio::test]
async fn null_provider_loop_returns_text() {
    let mut reg = LaneRegistry::default_direct();
    let out = run(&NullProvider, &mut reg, "anything").await.unwrap();
    assert!(out.contains("online"));
}

#[test]
fn tools_stay_constrained() {
    // policy: the full surface is exactly these five, forever
    let names: Vec<&str> = telepathy_steering::tools().iter().map(|t| t.name).collect();
    assert_eq!(
        names,
        vec!["list_lanes", "active_lane", "switch_lane", "create_lane", "lane_stats"]
    );
}

#[test]
fn unknown_tool_degrades_to_feedback() {
    let mut reg = LaneRegistry::default_direct();
    let out = execute_tool(&mut reg, "read_file", &serde_json::json!({"path": "/etc/passwd"}));
    assert!(out.starts_with("unknown tool"));
}

#[test]
fn switch_and_stats() {
    let mut reg = LaneRegistry::default_direct();
    reg.create("kerchunk");
    let out = execute_tool(&mut reg, "switch_lane", &serde_json::json!({"name": "kirk chunk"}));
    assert!(out.contains("now kerchunk"), "{out}");
    let out = execute_tool(&mut reg, "lane_stats", &serde_json::json!({}));
    assert!(out.contains("direct: 0 interactions"));
}
