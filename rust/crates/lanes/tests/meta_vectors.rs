// Meta-grammar parity: Rust parse_meta vs protocol/meta-vectors.json —
// the same vectors the Node parseMeta suite runs.
use telepathy_lanes::{parse_meta, Lane, LaneRegistry};

fn test_reg() -> LaneRegistry {
    let mut reg = LaneRegistry::default_direct();
    reg.lanes.push(Lane {
        id: "telepathy:repo:kerchunk".into(),
        name: "kerchunk".into(),
        created_at: String::new(),
        last_active: String::new(),
        interactions: None,
    });
    reg
}

#[test]
fn meta_vectors_match_node() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../protocol/meta-vectors.json");
    let v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let reg = test_reg();

    let mut failures = 0;
    for case in v["cases"].as_array().unwrap() {
        let transcript = case["transcript"].as_str().unwrap();
        let expected_op = case["op"].as_str().unwrap();
        let action = parse_meta(transcript, &reg);
        let got_op = match &action {
            telepathy_lanes::MetaAction::Switch(_) => "switch",
            telepathy_lanes::MetaAction::List => "list",
            telepathy_lanes::MetaAction::New(_) => "new",
            telepathy_lanes::MetaAction::Brief(_) => "brief",
            telepathy_lanes::MetaAction::Note(_) => "note",
            telepathy_lanes::MetaAction::Fork(_) => "fork",
            telepathy_lanes::MetaAction::Unknown => "unknown",
        };
        if got_op != expected_op {
            println!("FAIL \"{}\": op {} != {}", transcript, got_op, expected_op);
            failures += 1;
            continue;
        }
        match &action {
            telepathy_lanes::MetaAction::Switch(l) => {
                if let Some(want) = case["lane"].as_str() {
                    assert_eq!(l.name, want, "lane mismatch for {}", transcript);
                }
            }
            telepathy_lanes::MetaAction::New(name) => {
                if let Some(want) = case["name"].as_str() {
                    assert_eq!(name, want, "name mismatch for {}", transcript);
                }
            }
            telepathy_lanes::MetaAction::Note(text) => {
                if let Some(want) = case["text"].as_str() {
                    assert_eq!(text, want, "note text mismatch for {}", transcript);
                }
            }
            _ => {}
        }
    }
}
