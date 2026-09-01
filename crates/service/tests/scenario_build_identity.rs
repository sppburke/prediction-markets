//! Binary identity and staging-preflight scenarios (#544).

#![allow(clippy::unwrap_used)]

use std::process::Command;

#[test]
fn version_exposes_revision_and_configuration_identity_slot() {
    let output = Command::new(env!("CARGO_BIN_EXE_pe-service"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("revision="));
    assert!(stdout.contains("config_identity=runtime-applied"));
}

#[test]
fn staging_preflight_rejects_a_mismatched_revision() {
    let embedded = pe_service::build_info::embedded().source_revision;
    let candidate = if embedded == "0000000000000000000000000000000000000000" {
        "1111111111111111111111111111111111111111"
    } else {
        "0000000000000000000000000000000000000000"
    };
    let output = Command::new(env!("CARGO_BIN_EXE_pe-service"))
        .args(["--verify-staged-revision", candidate])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("staged binary revision mismatch")
    );
}
