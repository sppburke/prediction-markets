#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use pe_resolver_card::validate_install;
use tempfile::tempdir;
use time::OffsetDateTime;

const RESOLVER_CARD: &[u8] = br#"{
  "schema_version": 1,
  "card_id": "00000000-0000-0000-0000-000000000000",
  "condition_id": "0x01",
  "family": "event_feed",
  "resolver_source": {
    "kind": "official_api",
    "value": "https://example.invalid/feed"
  },
  "upstream_sources": [],
  "output_space": { "kind": "binary" },
  "timing": {
    "kind": "point_in_time",
    "value": "2098-01-01T00:00:00Z"
  },
  "rounding": { "kind": "as_published" },
  "tie_rule": "source_defined",
  "finality": { "kind": "as_published" },
  "revision_policy": "accept_only_official_corrections",
  "status": "tradable",
  "valid_from": "2020-01-01T00:00:00Z",
  "valid_until": "2099-01-01T00:00:00Z"
}"#;

#[test]
fn validate_install_accepts_rfc3339_card_and_writes_private_identical_output() {
    let directory = tempdir().expect("temporary directory should exist");
    let input = directory.path().join("input.json");
    let output = directory.path().join("installed.json");
    fs::write(&input, RESOLVER_CARD).expect("resolver fixture should be written");
    let expected = validate_install(RESOLVER_CARD, OffsetDateTime::now_utc())
        .expect("resolver fixture should validate")
        .canonical_hash
        .to_hex()
        .to_string();

    let command = Command::new(env!("CARGO_BIN_EXE_pe-service-live-canary"))
        .args([
            "resolver-card",
            "validate-install",
            input.to_str().expect("temporary path should be UTF-8"),
            output.to_str().expect("temporary path should be UTF-8"),
        ])
        .env_remove("CREDENTIALS_DIRECTORY")
        .env(
            "RUNTIME_DIRECTORY",
            directory.path().join("missing-runtime"),
        )
        .output()
        .expect("canary CLI should run");

    assert!(
        command.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&command.stderr)
    );
    assert_eq!(
        String::from_utf8(command.stdout).expect("hash output should be UTF-8"),
        format!("{expected}\n")
    );
    assert_eq!(expected.len(), 64);
    assert!(
        expected
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        fs::read(&output).expect("installed resolver should be readable"),
        RESOLVER_CARD
    );
    assert_eq!(
        fs::metadata(output)
            .expect("installed resolver metadata should be readable")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
