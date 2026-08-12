//! Sealed live-account credential decryption (#508 Decision 9).
//!
//! The site seals each account's credential bundle to ONE service-scoped age/X25519
//! recipient (`age-encryption` npm, armored output) and stores only ciphertext in
//! Supabase (`account_credentials.sealed_bundle`). pe-service decrypts with the private
//! identity delivered through systemd `LoadCredential=`/`$CREDENTIALS_DIRECTORY`
//! (root-owned 0600 source; the canary custody precedent) — boot-frozen, never in
//! Supabase, never logged. `{account_id, bundle_version, key_id}` are embedded in the
//! plaintext and validated against the row on decrypt: a wrong-recipient, wrong-account,
//! or stale-binding bundle is rejected (fail closed).
//!
//! Secret-bearing types deliberately do NOT derive `Debug`/`Serialize` — they cannot be
//! formatted into logs or status by construction.

use std::io::Read as _;
use std::path::Path;

use age::x25519::Identity;
use serde::Deserialize;

/// File name of the age identity inside `$CREDENTIALS_DIRECTORY` (the systemd
/// `LoadCredential=` drop-in maps it; docs/35 deploy runbook).
pub const AGE_IDENTITY_CREDENTIAL: &str = "pe-age-identity";

/// Typed decrypt failures. Every variant fails closed (the account cannot arm).
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("age identity unavailable: {0}")]
    Identity(String),
    #[error("sealed bundle failed to decrypt (wrong recipient or corrupt): {0}")]
    Decrypt(String),
    #[error("decrypted bundle is not valid JSON: {0}")]
    Parse(String),
    #[error("bundle binding mismatch: {0}")]
    BindingMismatch(&'static str),
}

/// The expected binding for one decrypt: the `account_credentials` row identity the
/// dispatch target froze (Decision 10) — an exact match is required.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialBinding {
    pub account_id: String,
    pub bundle_version: i64,
    pub key_id: String,
}

/// Decrypted per-account live credentials. NO `Debug`: secrets cannot be formatted.
#[derive(Clone, Deserialize)]
pub struct LiveAccountCredentials {
    // Binding fields embedded by the site at seal time (validated, then retained).
    pub account_id: String,
    pub bundle_version: i64,
    pub key_id: String,
    // CLOB V2 order credentials (the canary credential shape).
    pub private_key: String,
    pub deposit_wallet: String,
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
    // Redemption transport credentials (Decision 12; Relayer transport in v1).
    #[serde(default)]
    pub relayer_api_key: Option<String>,
    #[serde(default)]
    pub relayer_secret: Option<String>,
    #[serde(default)]
    pub relayer_passphrase: Option<String>,
}

/// Load the age identity from `$CREDENTIALS_DIRECTORY/pe-age-identity` (systemd
/// `LoadCredential=`). Absent/invalid ⇒ typed error (accounts can never arm).
pub fn load_identity_from_credentials_dir() -> Result<Identity, CredentialError> {
    let dir = std::env::var("CREDENTIALS_DIRECTORY")
        .map_err(|_| CredentialError::Identity("CREDENTIALS_DIRECTORY unset".into()))?;
    load_identity_from_path(Path::new(&dir).join(AGE_IDENTITY_CREDENTIAL).as_path())
}

/// Load an age X25519 identity from a file containing the `AGE-SECRET-KEY-1…` line.
pub fn load_identity_from_path(path: &Path) -> Result<Identity, CredentialError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| CredentialError::Identity(format!("{}: {e}", path.display())))?;
    raw.lines()
        .map(str::trim)
        .find(|l| l.starts_with("AGE-SECRET-KEY-1"))
        .ok_or_else(|| CredentialError::Identity("no AGE-SECRET-KEY line".into()))?
        .parse::<Identity>()
        .map_err(|e| CredentialError::Identity(e.to_string()))
}

/// Decrypt one armored sealed bundle and validate its embedded binding against the
/// frozen `expected` identity. Fail closed on any mismatch.
pub fn decrypt_bundle(
    armored: &str,
    identity: &Identity,
    expected: &CredentialBinding,
) -> Result<LiveAccountCredentials, CredentialError> {
    let reader = age::armor::ArmoredReader::new(armored.as_bytes());
    let decryptor =
        age::Decryptor::new(reader).map_err(|e| CredentialError::Decrypt(e.to_string()))?;
    let mut plaintext = Vec::new();
    decryptor
        .decrypt(std::iter::once(identity as &dyn age::Identity))
        .map_err(|e| CredentialError::Decrypt(e.to_string()))?
        .read_to_end(&mut plaintext)
        .map_err(|e| CredentialError::Decrypt(e.to_string()))?;
    let creds: LiveAccountCredentials =
        serde_json::from_slice(&plaintext).map_err(|e| CredentialError::Parse(e.to_string()))?;
    if creds.account_id != expected.account_id {
        return Err(CredentialError::BindingMismatch("account_id"));
    }
    if creds.bundle_version != expected.bundle_version {
        return Err(CredentialError::BindingMismatch("bundle_version"));
    }
    if creds.key_id != expected.key_id {
        return Err(CredentialError::BindingMismatch("key_id"));
    }
    Ok(creds)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::io::Write as _;

    use age::secrecy::ExposeSecret as _;

    use super::*;

    fn seal(plaintext: &str, recipient: &age::x25519::Recipient) -> String {
        let encryptor =
            age::Encryptor::with_recipients(std::iter::once(recipient as &dyn age::Recipient))
                .unwrap();
        let mut armored =
            age::armor::ArmoredWriter::wrap_output(Vec::new(), age::armor::Format::AsciiArmor)
                .unwrap();
        let mut writer = encryptor.wrap_output(&mut armored).unwrap();
        writer.write_all(plaintext.as_bytes()).unwrap();
        writer.finish().unwrap();
        String::from_utf8(armored.finish().unwrap()).unwrap()
    }

    fn bundle_json(account: &str, version: i64, key_id: &str) -> String {
        serde_json::json!({
            "account_id": account,
            "bundle_version": version,
            "key_id": key_id,
            "private_key": "0xdeadbeef",
            "deposit_wallet": "0x1111111111111111111111111111111111111111",
            "api_key": "00000000-0000-0000-0000-000000000000",
            "api_secret": "s3cret",
            "api_passphrase": "p4ss",
        })
        .to_string()
    }

    #[test]
    fn round_trips_and_validates_binding_fail_closed() {
        let identity = Identity::generate();
        let recipient = identity.to_public();
        let armored = seal(&bundle_json("sppburke", 3, "key-2"), &recipient);
        let expected = CredentialBinding {
            account_id: "sppburke".into(),
            bundle_version: 3,
            key_id: "key-2".into(),
        };
        let creds = decrypt_bundle(&armored, &identity, &expected).unwrap();
        assert_eq!(creds.api_key, "00000000-0000-0000-0000-000000000000");
        assert_eq!(creds.relayer_api_key, None);

        // Wrong account / stale version / wrong key id each fail closed.
        for (acct, ver, kid, which) in [
            ("other", 3, "key-2", "account_id"),
            ("sppburke", 2, "key-2", "bundle_version"),
            ("sppburke", 3, "key-1", "key_id"),
        ] {
            let exp = CredentialBinding {
                account_id: acct.into(),
                bundle_version: ver,
                key_id: kid.into(),
            };
            match decrypt_bundle(&armored, &identity, &exp) {
                Err(CredentialError::BindingMismatch(field)) => assert_eq!(field, which),
                other => panic!("expected BindingMismatch, got {:?}", other.is_ok()),
            }
        }

        // A bundle sealed to a DIFFERENT recipient fails to decrypt (wrong recipient).
        let stranger = Identity::generate();
        let foreign = seal(&bundle_json("sppburke", 3, "key-2"), &stranger.to_public());
        assert!(matches!(
            decrypt_bundle(&foreign, &identity, &expected),
            Err(CredentialError::Decrypt(_))
        ));
    }

    #[test]
    fn identity_file_parses_and_missing_fails_closed() {
        let identity = Identity::generate();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pe-age-identity");
        std::fs::write(
            &path,
            format!(
                "# created for #508 tests\n{}\n",
                identity.to_string().expose_secret()
            ),
        )
        .unwrap();
        let loaded = load_identity_from_path(&path).unwrap();
        assert_eq!(
            loaded.to_public().to_string(),
            identity.to_public().to_string()
        );
        assert!(load_identity_from_path(&dir.path().join("absent")).is_err());
    }
}
