#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::fs;
use std::process::{Command, Output};

use pe_core_types::{
    CollateralAmount, OutcomeId, PolymarketConditionId, PolymarketTokenId, Price, ShareAmount,
};
use pe_execution_core::{ProbeAuthorization, organic_evidence_bundle_hash, probe_authority_hash};
use rust_decimal::Decimal;
use tempfile::tempdir;
use time::OffsetDateTime;

fn run(arguments: &[&str]) -> Output {
    let directory = tempdir().expect("temporary environment should exist");
    Command::new(env!("CARGO_BIN_EXE_pe-service-live-canary"))
        .args(arguments)
        .env_remove("CREDENTIALS_DIRECTORY")
        .env(
            "RUNTIME_DIRECTORY",
            directory.path().join("missing-runtime"),
        )
        .output()
        .expect("canary CLI should run")
}

fn assert_hash_line(output: &Output, expected: &str) {
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout.clone()).expect("hash output should be UTF-8");
    assert_eq!(stdout, format!("{expected}\n"));
    assert_eq!(expected.len(), 64);
    assert!(
        expected
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
}

#[test]
fn probe_hash_cli_matches_the_canonical_owner_without_runtime_access() {
    let directory = tempdir().expect("temporary input directory should exist");
    let path = directory.path().join("probe.json");
    let authorization = ProbeAuthorization {
        schema_version: 1,
        campaign_id: "campaign".to_owned(),
        campaign_authorization_hash: "campaign-authority".to_owned(),
        probe_ordinal: 1,
        condition_id: PolymarketConditionId("condition".to_owned()),
        outcome_id: OutcomeId(0),
        token_id: PolymarketTokenId("11".to_owned()),
        shares: ShareAmount::from_atomic(2_000_000),
        worst_price: Price(Decimal::new(50, 2)),
        maximum_collateral: CollateralAmount::from_atomic(1_000_000),
        resolver_card_hash: "resolver".to_owned(),
        authority_hash: "ignored-self-field".to_owned(),
        expires_at: OffsetDateTime::UNIX_EPOCH,
    };
    fs::write(
        &path,
        serde_json::to_vec(&authorization).expect("probe should serialize"),
    )
    .expect("probe fixture should be written");
    let expected = probe_authority_hash(&authorization).expect("probe hash should compute");

    assert_hash_line(
        &run(&[
            "authority-hash",
            "probe",
            path.to_str().expect("temporary path should be UTF-8"),
        ]),
        &expected,
    );
}

#[test]
fn reviewed_probe_hash_cli_matches_the_canonical_owner_without_runtime_access() {
    let directory = tempdir().expect("temporary input directory should exist");
    let path = directory.path().join("reviewed-probes.json");
    let reviewed = vec!["probe-one".to_owned(), "probe-two".to_owned()];
    fs::write(
        &path,
        serde_json::to_vec(&reviewed).expect("reviewed probes should serialize"),
    )
    .expect("reviewed-probe fixture should be written");
    let expected = organic_evidence_bundle_hash(&reviewed).expect("bundle hash should compute");

    assert_hash_line(
        &run(&[
            "authority-hash",
            "reviewed-probes",
            path.to_str().expect("temporary path should be UTF-8"),
        ]),
        &expected,
    );
}

#[test]
fn authority_hash_cli_rejects_malformed_json_and_unknown_forms() {
    let directory = tempdir().expect("temporary input directory should exist");
    let path = directory.path().join("malformed.json");
    fs::write(&path, b"not-json").expect("malformed fixture should be written");
    let path = path.to_str().expect("temporary path should be UTF-8");

    let malformed = run(&["authority-hash", "probe", path]);
    assert!(!malformed.status.success());
    assert!(String::from_utf8_lossy(&malformed.stderr).contains("parse"));

    let unknown = run(&["authority-hash", "unknown", path]);
    assert!(!unknown.status.success());
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("unknown authority-hash form"));
}
