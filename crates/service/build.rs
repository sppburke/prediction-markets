use std::error::Error;
use std::process::Command;

fn main() -> Result<(), Box<dyn Error>> {
    let git_head = Command::new("git")
        .args(["rev-parse", "--git-path", "HEAD"])
        .output()?;
    if git_head.status.success() {
        println!(
            "cargo:rerun-if-changed={}",
            String::from_utf8(git_head.stdout)?.trim()
        );
    }
    let symbolic = Command::new("git")
        .args(["symbolic-ref", "-q", "HEAD"])
        .output()?;
    if symbolic.status.success() {
        let reference = String::from_utf8(symbolic.stdout)?;
        let reference_path = Command::new("git")
            .args(["rev-parse", "--git-path", reference.trim()])
            .output()?;
        if reference_path.status.success() {
            println!(
                "cargo:rerun-if-changed={}",
                String::from_utf8(reference_path.stdout)?.trim()
            );
        }
    }
    let output = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .output()?;
    if !output.status.success() {
        return Err("git rev-parse --verify HEAD failed; build identity is mandatory".into());
    }
    let commit = String::from_utf8(output.stdout)?.trim().to_owned();
    if commit.len() != 40 || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("git returned an invalid full commit identity: {commit:?}").into());
    }
    println!("cargo:rustc-env=PE_BUILD_COMMIT={commit}");
    Ok(())
}
