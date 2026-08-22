use telepathy_lanes::LaneRegistry;
use telepathy_steering::{execute_tool, run, NullProvider, SteeringTool};

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
        vec!["list_lanes", "active_lane", "switch_lane", "create_lane", "lane_stats", "search_conversations"]
    );
}

#[test]
fn unresolved_names_never_execute() {
    // the loop resolves strings to enums; an unknown name has no execution path
    assert!(SteeringTool::from_name("read_file").is_none());
    assert!(SteeringTool::from_name("bash").is_none());
}

#[test]
fn switch_and_stats() {
    let mut reg = LaneRegistry::default_direct();
    reg.create("kerchunk");
    let out = execute_tool(&mut reg, SteeringTool::SwitchLane, &serde_json::json!({"name": "kirk chunk"}));
    assert!(out.contains("now kerchunk"), "{out}");
    let out = execute_tool(&mut reg, SteeringTool::LaneStats, &serde_json::json!({}));
    assert!(out.contains("direct: 0 interactions"));
}
