//! Offline parsing tests for oracle feed proposals.

use alloy::primitives::address;
use rpbot::services::oracle::parse_proposed_pyth_feed_lines;

#[test]
fn parse_proposed_pyth_feed_lines_accepts_comments() {
    let text = r#"
# header
0x03b54A0eF8042C0f6A77B15e637c9f5d7c6790D0=6df640f3b8963d8f8358f791f352b8364513f6ab1cca5ed3f1f7b5448980e784 # wstETH
"#;
    let lines = parse_proposed_pyth_feed_lines(text).expect("parse");
    assert_eq!(lines.len(), 1);
    assert_eq!(
        lines[0].token,
        address!("0x03b54A0eF8042C0f6A77B15e637c9f5d7c6790D0")
    );
    assert_eq!(lines[0].comment.as_deref(), Some("wstETH"));
}
