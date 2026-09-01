#[allow(dead_code)]
mod build_identity;

use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use build_identity::resolve_revision;

fn git(args: &[&str]) -> Result<Output, Box<dyn Error>> {
    Ok(Command::new("git").args(args).output()?)
}

fn git_in(repo_root: &Path, args: &[&str]) -> Result<Output, Box<dyn Error>> {
    Ok(Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()?)
}

fn output_text(output: Output, label: &str) -> Result<String, String> {
    if !output.status.success() {
        return Err(format!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout)
        .map(|text| text.trim().to_owned())
        .map_err(|error| format!("{label} returned non-UTF-8 output: {error}"))
}

fn emit_git_rerun_paths(repo_root: &Path) -> Result<(), Box<dyn Error>> {
    for args in [
        ["rev-parse", "--git-path", "HEAD"].as_slice(),
        ["rev-parse", "--git-path", "index"].as_slice(),
    ] {
        if let Ok(path) = output_text(git_in(repo_root, args)?, "git rev-parse --git-path") {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Ok(reference) = output_text(
        git_in(repo_root, &["symbolic-ref", "-q", "HEAD"])?,
        "git symbolic-ref HEAD",
    ) && let Ok(path) = output_text(
        git_in(repo_root, &["rev-parse", "--git-path", &reference])?,
        "git rev-parse symbolic ref",
    ) {
        println!("cargo:rerun-if-changed={path}");
    }

    // Unstaged edits do not update `.git/index`; explicitly watch every tracked path so a cached
    // build script cannot keep a formerly-clean release identity after a source edit.
    let tracked = git_in(repo_root, &["ls-files", "--full-name", "-z"])?;
    if tracked.status.success() {
        for path in tracked
            .stdout
            .split(|byte| *byte == 0)
            .filter(|p| !p.is_empty())
        {
            if let Ok(relative) = std::str::from_utf8(path) {
                println!(
                    "cargo:rerun-if-changed={}",
                    repo_root.join(relative).display()
                );
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let repo_root = git(&["rev-parse", "--show-toplevel"])
        .map_err(|error| error.to_string())
        .and_then(|output| output_text(output, "git repository root"))
        .map(PathBuf::from);
    if let Ok(root) = &repo_root {
        emit_git_rerun_paths(root)?;
    }

    let revision = match &repo_root {
        Ok(root) => git_in(root, &["rev-parse", "--verify", "HEAD^{commit}"])
            .map_err(|error| error.to_string())
            .and_then(|output| output_text(output, "git rev-parse HEAD")),
        Err(error) => Err(error.clone()),
    };
    let dirty = match &repo_root {
        Ok(root) => git_in(
            root,
            &[
                "status",
                "--porcelain=v1",
                "--untracked-files=normal",
                "--ignore-submodules=none",
            ],
        )
        .map_err(|error| error.to_string())
        .and_then(|output| output_text(output, "git status")),
        Err(error) => Err(error.clone()),
    }
    .map(|status| !status.is_empty());
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned());
    let resolved = resolve_revision(
        &profile,
        revision.as_deref().map_err(String::as_str),
        dirty.as_ref().copied().map_err(String::as_str),
    )?;
    println!("cargo:rustc-env=PE_BUILD_COMMIT={resolved}");
    println!(
        "cargo:rustc-env=PE_BUILD_CONFIG_IDENTITY_SLOT={}",
        build_identity::RUNTIME_CONFIG_IDENTITY_SLOT
    );
    Ok(())
}
