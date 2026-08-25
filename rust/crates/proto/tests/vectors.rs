// Shared-vector conformance: telepathos-proto must classify
// protocol/vectors.json exactly as server/src/protocol.ts does.
use telepathos_proto::ControlMsg;

fn vectors() -> serde_json::Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../protocol/vectors.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn control_vectors_match_reference() {
    let v = vectors();
    let mut failures = 0;

    for case in v["control"]["valid"].as_array().unwrap() {
        let frame = case["frame"].as_str().unwrap();
        match ControlMsg::parse(frame) {
            Some(msg) => {
                let got = serde_json::to_value(&msg).unwrap();
                let got_tag = got["type"].as_str().unwrap_or("(none)");
                if got_tag != case["tag"].as_str().unwrap() {
                    println!("FAIL {}: tag {:?} != {}", frame, got_tag, case["tag"]);
                    failures += 1;
                }
            }
            None => {
                println!("FAIL {}: rejected, expected {}", frame, case["tag"]);
                failures += 1;
            }
        }
    }

    for frame in v["control"]["invalid"].as_array().unwrap() {
        if ControlMsg::parse(frame.as_str().unwrap()).is_some() {
            println!("FAIL: accepted invalid {}", frame);
            failures += 1;
        }
    }

    assert_eq!(failures, 0, "{} control vector failures", failures);
}
