//! `cargo xtask stack <subcommand>` — stacked PR and GitButler (`but`) workflow helper.
//!
//! Implements the Estate stacked-PR conventions from
//! `~/.agents/sops/rust-repo-best-practices.md` and integrates cleanly with
//! GitButler (`but`) and `gh`:
//!   - `stack status`              Display current branch, parent ref, ahead/behind counts, and GitButler status.
//!   - `stack base-ref`            Print the parent branch in the stack (or trunk `unsigned`/`main` if at bottom).
//!   - `stack new <name>`          Create a new stacked branch on top of the current branch (or via `but branch new`).
//!   - `stack sync`                Fetch origin and rebase the current branch onto its parent ref (`git rebase --update-refs`).
//!   - `stack pr [extra gh args]`  Create a draft/ready PR via `gh pr create --base <base-ref>`.
//!   - `stack retarget [base]`     Retarget the open PR for the current branch after parent merges.
//!   - `stack land <pr-number>`    Merge a green bottom-of-stack PR (`--merge`), retarget children, and fetch origin.
//!   - `stack but [but-args...]`   Run GitButler (`but`) CLI commands directly against the workspace.
//!
//! Reference discovery (`.git/HEAD`, `refs/heads/*`, `packed-refs`) runs in-process
//! without shelling out to `git`. Working-tree mutations (`rebase`, `switch`, `pull`)
//! and GitHub API calls (`gh`) invoke their respective CLIs.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub const PRIMARY_TRUNK: &str = "unsigned";
pub const FALLBACK_TRUNK: &str = "main";

/// Resolve the `.git` directory for `root` (handles both standard repos and worktrees).
pub fn resolve_git_dir(root: &Path) -> Result<PathBuf, String> {
    let dot_git = root.join(".git");
    if dot_git.is_dir() {
        return Ok(dot_git);
    }
    if dot_git.is_file() {
        let raw = fs::read_to_string(&dot_git)
            .map_err(|e| format!("failed to read {}: {e}", dot_git.display()))?;
        if let Some(rest) = raw.trim().strip_prefix("gitdir:") {
            let gitdir = PathBuf::from(rest.trim());
            let resolved = if gitdir.is_relative() {
                root.join(gitdir)
            } else {
                gitdir
            };
            return Ok(resolved);
        }
    }
    Err(format!(
        "could not locate `.git` directory at {}",
        root.display()
    ))
}

/// Resolve the common `.git` directory (for worktrees where `commondir` points to the main repo).
pub fn resolve_common_git_dir(git_dir: &Path) -> PathBuf {
    let commondir_file = git_dir.join("commondir");
    if let Ok(raw) = fs::read_to_string(&commondir_file) {
        let rel = raw.trim();
        if !rel.is_empty() {
            let candidate = git_dir.join(rel);
            if let Ok(canon) = candidate.canonicalize() {
                return canon;
            }
            return candidate;
        }
    }
    git_dir.to_path_buf()
}

/// Detect the trunk branch for `root`: respects `GLAEDA_STACK_TRUNK`, prefers
/// `unsigned` if present in local branches, and falls back to `main` if `main`
/// is present without `unsigned` (defaulting to `unsigned` otherwise).
pub fn detect_trunk_branch(root: &Path) -> String {
    if let Ok(explicit) = env::var("GLAEDA_STACK_TRUNK") {
        let trimmed = explicit.trim();
        if !trimmed.is_empty() {
            return trimmed.to_owned();
        }
    }
    if let Ok(branches) = list_local_branches_in_process(root) {
        if branches.contains_key(PRIMARY_TRUNK) {
            return PRIMARY_TRUNK.to_owned();
        }
        if branches.contains_key(FALLBACK_TRUNK) {
            return FALLBACK_TRUNK.to_owned();
        }
    }
    PRIMARY_TRUNK.to_owned()
}

/// Read the current branch short name from `.git/HEAD` in-process.
pub fn current_branch_in_process(root: &Path) -> Result<String, String> {
    let git_dir = resolve_git_dir(root)?;
    let head_raw = fs::read_to_string(git_dir.join("HEAD"))
        .map_err(|e| format!("failed to read .git/HEAD: {e}"))?;
    let trimmed = head_raw.trim();
    if let Some(ref_path) = trimmed.strip_prefix("ref: ") {
        if let Some(branch) = ref_path.strip_prefix("refs/heads/") {
            return Ok(branch.to_string());
        }
        return Ok(ref_path.to_string());
    }
    Ok(trimmed.chars().take(12).collect())
}

/// Collect all local branch refs (`refs/heads/<name>` -> `<oid>`) in-process.
pub fn list_local_branches_in_process(root: &Path) -> Result<BTreeMap<String, String>, String> {
    let git_dir = resolve_git_dir(root)?;
    let common_dir = resolve_common_git_dir(&git_dir);
    let mut branches = BTreeMap::new();

    let packed = common_dir.join("packed-refs");
    if let Ok(raw) = fs::read_to_string(&packed) {
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('^') {
                continue;
            }
            let mut parts = line.split_whitespace();
            if let (Some(oid), Some(refname)) = (parts.next(), parts.next())
                && let Some(short) = refname.strip_prefix("refs/heads/")
            {
                branches.insert(short.to_string(), oid.to_string());
            }
        }
    }

    let heads_dir = common_dir.join("refs/heads");
    if heads_dir.is_dir() {
        walk_loose_refs(&heads_dir, &heads_dir, &mut branches)
            .map_err(|e| format!("failed to walk refs/heads: {e}"))?;
    }

    Ok(branches)
}

fn walk_loose_refs(
    base: &Path,
    dir: &Path,
    out: &mut BTreeMap<String, String>,
) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_loose_refs(base, &path, out)?;
        } else if path.is_file()
            && let Ok(rel) = path.strip_prefix(base)
        {
            let name = rel.to_string_lossy().replace('\\', "/");
            if let Ok(oid) = fs::read_to_string(&path) {
                out.insert(name, oid.trim().to_string());
            }
        }
    }
    Ok(())
}

/// Read the recorded parent branch for `branch` from `.git/stack/<branch>.base`,
/// defaulting to the repository trunk (`unsigned` or `main`).
pub fn resolve_base_ref(root: &Path, branch: &str) -> Result<String, String> {
    let trunk = detect_trunk_branch(root);
    if branch == trunk
        || branch == PRIMARY_TRUNK
        || branch == FALLBACK_TRUNK
        || branch == "gitbutler/workspace"
    {
        return Ok(trunk);
    }
    let git_dir = resolve_git_dir(root)?;
    let common_dir = resolve_common_git_dir(&git_dir);
    let stack_meta = common_dir
        .join("stack")
        .join(format!("{}.base", branch.replace('/', "__")));
    if let Ok(parent) = fs::read_to_string(&stack_meta) {
        let parent = parent.trim();
        if !parent.is_empty() {
            return Ok(parent.to_string());
        }
    }
    Ok(trunk)
}

pub fn record_base_ref(root: &Path, branch: &str, base: &str) -> Result<(), String> {
    let git_dir = resolve_git_dir(root)?;
    let common_dir = resolve_common_git_dir(&git_dir);
    let stack_dir = common_dir.join("stack");
    fs::create_dir_all(&stack_dir)
        .map_err(|e| format!("failed to create {}: {e}", stack_dir.display()))?;
    let stack_meta = stack_dir.join(format!("{}.base", branch.replace('/', "__")));
    fs::write(&stack_meta, format!("{base}\n"))
        .map_err(|e| format!("failed to write {}: {e}", stack_meta.display()))?;
    Ok(())
}

/// Return the installed GitButler (`but`) version string if `but` is on `PATH`.
pub fn but_cli_version() -> Option<String> {
    let out = Command::new("but")
        .arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// True when GitButler workspace metadata or `gitbutler/workspace` branch is active.
pub fn is_gitbutler_workspace(root: &Path) -> bool {
    if let Ok(branch) = current_branch_in_process(root)
        && branch == "gitbutler/workspace"
    {
        return true;
    }
    if let Ok(git_dir) = resolve_git_dir(root) {
        let common = resolve_common_git_dir(&git_dir);
        return common.join("gitbutler").is_dir();
    }
    false
}

fn run_git(root: &Path, args: &[&str]) -> Result<(), String> {
    let status = Command::new("git")
        .current_dir(root)
        .args(args)
        .status()
        .map_err(|e| format!("failed to spawn git {}: {e}", args.join(" ")))?;
    if !status.success() {
        return Err(format!("git {} exited with {}", args.join(" "), status));
    }
    Ok(())
}

fn run_gh(root: &Path, args: &[String]) -> Result<(), String> {
    let status = Command::new("gh")
        .current_dir(root)
        .args(args)
        .status()
        .map_err(|e| format!("failed to spawn gh {}: {e}", args.join(" ")))?;
    if !status.success() {
        return Err(format!("gh {} exited with {}", args.join(" "), status));
    }
    Ok(())
}

fn ahead_behind(root: &Path, base: &str, head: &str) -> Option<(u32, u32)> {
    let out = Command::new("git")
        .current_dir(root)
        .args([
            "rev-list",
            "--left-right",
            "--count",
            &format!("{base}...{head}"),
        ])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut parts = text.split_whitespace();
    let behind: u32 = parts.next()?.parse().ok()?;
    let ahead: u32 = parts.next()?.parse().ok()?;
    Some((ahead, behind))
}

/// Entrypoint for `cargo xtask stack <subcommand>`.
pub fn run(root: &Path, args: &[String]) -> Result<(), String> {
    let sub = args.first().map(String::as_str).unwrap_or("status");
    match sub {
        "status" => cmd_status(root),
        "base-ref" => {
            let branch = current_branch_in_process(root)?;
            let base = resolve_base_ref(root, &branch)?;
            println!("{base}");
            Ok(())
        }
        "new" => {
            let name = args
                .get(1)
                .ok_or_else(|| "usage: cargo xtask stack new <branch-name>".to_string())?;
            cmd_new(root, name)
        }
        "sync" => cmd_sync(root),
        "pr" => cmd_pr(root, &args[1..]),
        "retarget" => cmd_retarget(root, args.get(1).map(String::as_str)),
        "land" => {
            let pr_num = args
                .get(1)
                .ok_or_else(|| "usage: cargo xtask stack land <pr-number>".to_string())?;
            cmd_land(root, pr_num)
        }
        "but" => cmd_but(root, &args[1..]),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        other => {
            print_help();
            Err(format!("unknown `cargo xtask stack` subcommand: {other}"))
        }
    }
}

fn cmd_status(root: &Path) -> Result<(), String> {
    let trunk = detect_trunk_branch(root);
    let current = current_branch_in_process(root)?;
    let branches = list_local_branches_in_process(root)?;
    let base = resolve_base_ref(root, &current)?;
    let but_ver = but_cli_version();
    let gb_active = is_gitbutler_workspace(root);

    println!("Stack status ({}):", root.display());
    println!("  trunk branch   : {trunk}");
    println!("  current branch : {current}");
    println!("  parent base-ref: {base}");
    println!(
        "  gitbutler (but): {} (workspace active: {})",
        but_ver.as_deref().unwrap_or("not found on PATH"),
        if gb_active { "yes" } else { "no" }
    );
    if let Some((ahead, behind)) = ahead_behind(root, &base, &current) {
        println!("  divergence     : +{ahead} ahead / -{behind} behind `{base}`");
    }
    println!("  local branches ({}):", branches.len());
    for (name, oid) in &branches {
        let marker = if name == &current { "*" } else { " " };
        let parent = resolve_base_ref(root, name).unwrap_or_else(|_| trunk.clone());
        let short_oid: String = oid.chars().take(10).collect();
        if name == &trunk {
            println!("  {marker} {name:<42} {short_oid} (trunk)");
        } else {
            println!("  {marker} {name:<42} {short_oid} -> base: {parent}");
        }
    }
    Ok(())
}

fn cmd_new(root: &Path, new_branch: &str) -> Result<(), String> {
    let trunk = detect_trunk_branch(root);
    let parent = current_branch_in_process(root)?;
    if parent == "gitbutler/workspace" {
        let status = Command::new("but")
            .current_dir(root)
            .args(["branch", "new", new_branch])
            .status()
            .map_err(|e| format!("failed to run `but branch new`: {e}"))?;
        if !status.success() {
            return Err(format!("`but branch new {new_branch}` failed"));
        }
        record_base_ref(root, new_branch, &trunk)?;
        println!("stack: created GitButler virtual branch `{new_branch}` on `origin/{trunk}`");
        return Ok(());
    }
    run_git(root, &["switch", "-c", new_branch])?;
    record_base_ref(root, new_branch, &parent)?;
    println!("stack: created `{new_branch}` stacked on `{parent}`");
    Ok(())
}

fn cmd_sync(root: &Path) -> Result<(), String> {
    let trunk = detect_trunk_branch(root);
    let branch = current_branch_in_process(root)?;
    if branch == trunk {
        run_git(root, &["fetch", "origin", "--prune"])?;
        run_git(root, &["pull", "--ff-only", "origin", &trunk])?;
        println!("stack: `{trunk}` fast-forwarded to `origin/{trunk}`");
        return Ok(());
    }
    let base = resolve_base_ref(root, &branch)?;
    run_git(root, &["fetch", "origin", "--prune"])?;
    let target = if base == trunk {
        format!("origin/{trunk}")
    } else {
        base.clone()
    };
    println!("stack: rebasing `{branch}` onto `{target}` with `--update-refs`...");
    run_git(root, &["rebase", "--update-refs", &target])?;
    Ok(())
}

fn cmd_pr(root: &Path, extra_args: &[String]) -> Result<(), String> {
    let trunk = detect_trunk_branch(root);
    let branch = current_branch_in_process(root)?;
    if branch == trunk {
        return Err(format!("refusing to open a PR from `{trunk}`"));
    }
    let base = resolve_base_ref(root, &branch)?;
    run_git(root, &["push", "-u", "origin", "HEAD"])?;
    let mut gh_args = vec![
        "pr".to_string(),
        "create".to_string(),
        "--base".to_string(),
        base.clone(),
        "--head".to_string(),
        branch.clone(),
    ];
    gh_args.extend_from_slice(extra_args);
    println!("stack: creating PR for `{branch}` -> base `{base}`...");
    run_gh(root, &gh_args)
}

fn cmd_retarget(root: &Path, explicit_base: Option<&str>) -> Result<(), String> {
    let trunk = detect_trunk_branch(root);
    let branch = current_branch_in_process(root)?;
    let new_base = explicit_base.unwrap_or(&trunk);
    record_base_ref(root, &branch, new_base)?;
    let gh_args = vec![
        "pr".to_string(),
        "edit".to_string(),
        branch.clone(),
        "--base".to_string(),
        new_base.to_string(),
    ];
    println!("stack: retargeted `{branch}` PR base to `{new_base}`");
    run_gh(root, &gh_args)
}

fn cmd_land(root: &Path, pr_number: &str) -> Result<(), String> {
    println!("stack: landing PR #{pr_number} via merge commit (`--merge --delete-branch`)...");
    run_gh(
        root,
        &[
            "pr".to_string(),
            "merge".to_string(),
            pr_number.to_string(),
            "--merge".to_string(),
            "--delete-branch".to_string(),
        ],
    )?;
    run_git(root, &["fetch", "origin", "--prune"])?;
    Ok(())
}

fn cmd_but(root: &Path, args: &[String]) -> Result<(), String> {
    let effective_args: Vec<&str> = if args.is_empty() {
        vec!["status"]
    } else {
        args.iter().map(String::as_str).collect()
    };
    let status = Command::new("but")
        .current_dir(root)
        .args(&effective_args)
        .status()
        .map_err(|e| format!("failed to spawn `but {}`: {e}", effective_args.join(" ")))?;
    if !status.success() {
        return Err(format!(
            "`but {}` exited with {status}",
            effective_args.join(" ")
        ));
    }
    Ok(())
}

pub fn print_help() {
    eprintln!(
        "\
Usage: cargo xtask stack <subcommand>

Subcommands:
  status                 Show current branch, recorded parent base-ref, GitButler status, and local stack
  base-ref               Print parent branch for the current branch (defaults to `unsigned`)
  new <branch-name>      Create a new branch stacked on the current branch (or GitButler virtual branch)
  sync                   Fetch origin and `git rebase --update-refs` onto parent base-ref
  pr [gh-pr-create-args] Push current branch and create a PR against its parent base-ref
  retarget [new-base]    Update recorded parent base-ref (default `unsigned`) and retarget open PR
  land <pr-number>       Merge bottom-of-stack PR (`gh pr merge --merge --delete-branch`) and fetch
  but [but-args...]      Run GitButler CLI (`but`) directly (defaults to `but status`)
"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_temp_git_repo(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let root = env::temp_dir().join(format!(
            "glaeda-xtask-stack-test-{}-{tag}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(root.join(".git/refs/heads/feat")).unwrap();
        root
    }

    #[test]
    fn reads_current_branch_and_loose_plus_packed_refs_in_process() {
        let root = make_temp_git_repo("refs");
        fs::write(
            root.join(".git/HEAD"),
            "ref: refs/heads/feat/spec-0012-stack\n",
        )
        .unwrap();
        fs::write(
            root.join(".git/packed-refs"),
            "# pack-refs with: peeled fully-peeled sorted\n0492075abcdef1234567890abcdef1234567890 refs/heads/unsigned\n",
        )
        .unwrap();
        fs::write(
            root.join(".git/refs/heads/feat/spec-0011-config"),
            "1111111abcdef1234567890abcdef1234567890\n",
        )
        .unwrap();
        fs::write(
            root.join(".git/refs/heads/feat/spec-0012-stack"),
            "2222222abcdef1234567890abcdef1234567890\n",
        )
        .unwrap();

        assert_eq!(
            current_branch_in_process(&root).unwrap(),
            "feat/spec-0012-stack"
        );
        let branches = list_local_branches_in_process(&root).unwrap();
        assert_eq!(branches.len(), 3);
        assert!(branches.contains_key("unsigned"));
        assert!(branches.contains_key("feat/spec-0011-config"));
        assert!(branches.contains_key("feat/spec-0012-stack"));
        assert_eq!(detect_trunk_branch(&root), "unsigned");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn records_and_resolves_stacked_base_refs() {
        let root = make_temp_git_repo("stack-meta");
        fs::write(root.join(".git/HEAD"), "ref: refs/heads/unsigned\n").unwrap();
        fs::write(
            root.join(".git/refs/heads/unsigned"),
            "0492075abcdef1234567890abcdef1234567890\n",
        )
        .unwrap();

        // Default base-ref when no stack file exists is trunk (`unsigned`).
        assert_eq!(
            resolve_base_ref(&root, "feat/spec-0011-config").unwrap(),
            "unsigned"
        );

        // Recording a stacked parent persists in `.git/stack/<branch>.base`.
        record_base_ref(&root, "feat/spec-0011-config", "unsigned").unwrap();
        record_base_ref(&root, "feat/spec-0012-stack", "feat/spec-0011-config").unwrap();

        assert_eq!(
            resolve_base_ref(&root, "feat/spec-0011-config").unwrap(),
            "unsigned"
        );
        assert_eq!(
            resolve_base_ref(&root, "feat/spec-0012-stack").unwrap(),
            "feat/spec-0011-config"
        );
        // Trunk itself always resolves to trunk.
        assert_eq!(resolve_base_ref(&root, "unsigned").unwrap(), "unsigned");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn resolves_gitdir_pointer_file_for_linked_worktrees() {
        let root = make_temp_git_repo("worktree");
        let real_git = root.join("real-git-dir");
        fs::create_dir_all(real_git.join("refs/heads")).unwrap();
        fs::write(real_git.join("HEAD"), "ref: refs/heads/unsigned\n").unwrap();

        let wt = root.join("wt");
        fs::create_dir_all(&wt).unwrap();
        fs::write(wt.join(".git"), format!("gitdir: {}\n", real_git.display())).unwrap();

        assert_eq!(current_branch_in_process(&wt).unwrap(), "unsigned");

        // Negative control: directory without `.git` fails cleanly.
        let empty = root.join("not-a-repo");
        fs::create_dir_all(&empty).unwrap();
        assert!(resolve_git_dir(&empty).is_err());

        let _ = fs::remove_dir_all(&root);
    }
}
