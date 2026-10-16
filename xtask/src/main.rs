//! Glaeda estate workspace automation (`cargo xtask <command>`).
//!
//! Std-only implementation of the Estate `cargo xtask` contract v1, `xwork`
//! FNV-1a worktree-fingerprinted verification pipeline, `spec-check`,
//! `toolchain-check`, `branch-hygiene`, and `stack` (stacked PR + GitButler
//! workflow helper).

#![forbid(unsafe_code)]

mod contract;
mod stack;

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::Instant;

const DEFAULT_JOBS: &str = "2";

/// Ordered stages in the `cargo xtask xwork` deterministic verification pipeline.
pub const XWORK_STAGES: &[&str] = &[
    "spec-check",
    "toolchain-check",
    "branch-hygiene",
    "fmt-check",
    "deny",
    "machete",
    "xtask-clippy",
    "xtask-tests",
];

/// Required estate toolchain and linter configuration files at the repository root.
pub const REQUIRED_TOOLCHAIN_FILES: &[&str] = &[
    "rust-toolchain.toml",
    "rustfmt.toml",
    "clippy.toml",
    "deny.toml",
    ".cargo/config.toml",
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn jobs_setting() -> String {
    env::var("JOBS")
        .or_else(|_| env::var("CARGO_BUILD_JOBS"))
        .unwrap_or_else(|_| DEFAULT_JOBS.to_string())
}

fn run_cargo(root: &Path, args: &[&str]) -> ExitCode {
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    eprintln!("+ {cargo} {}", args.join(" "));
    let status = Command::new(cargo).args(args).current_dir(root).status();
    match status {
        Ok(s) if s.success() => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(err) => {
            eprintln!("xtask: failed to invoke cargo {}: {err}", args.join(" "));
            ExitCode::FAILURE
        }
    }
}

fn has_subcommand(subcmd: &str) -> bool {
    Command::new("cargo")
        .arg(subcmd)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn has_binary(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cmd {
    Xwork,
    Check,
    Hygiene,
    SpecCheck,
    ToolchainCheck,
    BranchHygiene,
    Stack,
    Fmt,
    FmtCheck,
    Clippy,
    Test,
    Nextest,
    DocTest,
    Deny,
    Typos,
    Machete,
    Tools,
    Ci,
    Install,
    Build,
    Smoke,
    ModuleInventory,
    ThreatModelCheck,
}

const USAGE: &str = "usage: cargo xtask <xwork|check|hygiene|spec-check|toolchain-check|branch-hygiene|stack|fmt|fmt-check|clippy|test|nextest|doc-test|deny|typos|machete|tools|ci|install|build|smoke|module-inventory|threat-model-check>";

fn parse_command(name: &str) -> Option<Cmd> {
    Some(match name {
        "xwork" => Cmd::Xwork,
        "check" => Cmd::Check,
        "hygiene" => Cmd::Hygiene,
        "spec-check" => Cmd::SpecCheck,
        "toolchain-check" => Cmd::ToolchainCheck,
        "branch-hygiene" => Cmd::BranchHygiene,
        "stack" => Cmd::Stack,
        "fmt" => Cmd::Fmt,
        "fmt-check" => Cmd::FmtCheck,
        "clippy" | "lint" => Cmd::Clippy,
        "test" => Cmd::Test,
        "nextest" => Cmd::Nextest,
        "doc-test" => Cmd::DocTest,
        "deny" => Cmd::Deny,
        "typos" => Cmd::Typos,
        "machete" => Cmd::Machete,
        "tools" => Cmd::Tools,
        "ci" => Cmd::Ci,
        "install" => Cmd::Install,
        "build" => Cmd::Build,
        "smoke" => Cmd::Smoke,
        "module-inventory" => Cmd::ModuleInventory,
        "threat-model-check" => Cmd::ThreatModelCheck,
        _ => return None,
    })
}

fn run_command(root: &Path, cmd: Cmd, rest: &[String]) -> ExitCode {
    match cmd {
        Cmd::Xwork => run_xwork(root, rest),
        Cmd::Check => run_check(root),
        Cmd::Hygiene => run_hygiene(root),
        Cmd::SpecCheck => run_spec_check(root),
        Cmd::ToolchainCheck => run_toolchain_check(root),
        Cmd::BranchHygiene => run_branch_hygiene(root, rest),
        Cmd::Stack => match stack::run(root, rest) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("stack: {err}");
                ExitCode::FAILURE
            }
        },
        Cmd::Fmt => {
            if rest.iter().any(|a| a == "--check") {
                run_fmt_check(root)
            } else {
                run_fmt(root)
            }
        }
        Cmd::FmtCheck => run_fmt_check(root),
        Cmd::Clippy => run_clippy(root),
        Cmd::Test => run_test(root),
        Cmd::Nextest => run_nextest(root),
        Cmd::DocTest => run_doc_test(root),
        Cmd::Deny => run_deny(root),
        Cmd::Typos => run_typos(root),
        Cmd::Machete => run_machete(root),
        Cmd::Tools => run_tools(root),
        Cmd::Ci => run_ci(root),
        Cmd::Install => run_install(root),
        Cmd::Build => run_build(root),
        Cmd::Smoke => run_smoke(root),
        Cmd::ModuleInventory => run_module_inventory(root),
        Cmd::ThreatModelCheck => run_threat_model_check(root),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let root = repo_root();
    let Some((name, rest)) = args.split_first() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    if matches!(name.as_str(), "help" | "--help" | "-h") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    match parse_command(name) {
        Some(cmd) => run_command(&root, cmd, rest),
        None => {
            eprintln!("xtask: unknown command {name:?}");
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn exit_code(ok: bool) -> ExitCode {
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

pub fn run_tools(root: &Path) -> ExitCode {
    exit_code(contract::run_tools(root, &jobs_setting()))
}

pub fn run_typos(root: &Path) -> ExitCode {
    exit_code(contract::run(root, "typos", &[]))
}

pub fn run_ci(root: &Path) -> ExitCode {
    exit_code(contract::run_ci(contract::CI_STEPS, |step| {
        parse_command(step).is_some_and(|cmd| run_command(root, cmd, &[]) == ExitCode::SUCCESS)
    }))
}

pub fn run_install(root: &Path) -> ExitCode {
    exit_code(contract::run_install(root, &jobs_setting(), || {
        run_ci(root) == ExitCode::SUCCESS
    }))
}

pub fn run_fmt(root: &Path) -> ExitCode {
    run_cargo(root, &["fmt", "--all"])
}

pub fn run_fmt_check(root: &Path) -> ExitCode {
    run_cargo(root, &["fmt", "--all", "--", "--check"])
}

pub fn run_clippy(root: &Path) -> ExitCode {
    let jobs = jobs_setting();
    run_cargo(
        root,
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "-j",
            &jobs,
            "--",
            "-D",
            "warnings",
        ],
    )
}

pub fn run_build(root: &Path) -> ExitCode {
    let jobs = jobs_setting();
    run_cargo(root, &["build", "--workspace", "--locked", "-j", &jobs])
}

/// The one `cargo` command `test` runs (xtask contract v1): nextest when it is
/// installed, else `cargo test`.
#[must_use]
pub fn test_command(nextest: bool, jobs: &str) -> Vec<&str> {
    if nextest {
        vec![
            "nextest",
            "run",
            "--workspace",
            "--locked",
            "--build-jobs",
            jobs,
        ]
    } else {
        vec!["test", "--workspace", "--locked", "-j", jobs]
    }
}

pub fn run_test(root: &Path) -> ExitCode {
    let jobs = jobs_setting();
    run_cargo(root, &test_command(has_subcommand("nextest"), &jobs))
}

pub fn run_nextest(root: &Path) -> ExitCode {
    let jobs = jobs_setting();
    run_cargo(
        root,
        &[
            "nextest",
            "run",
            "--workspace",
            "--locked",
            "--build-jobs",
            &jobs,
        ],
    )
}

pub fn run_doc_test(root: &Path) -> ExitCode {
    let jobs = jobs_setting();
    run_cargo(
        root,
        &["test", "--doc", "--workspace", "--locked", "-j", &jobs],
    )
}

pub fn run_deny(root: &Path) -> ExitCode {
    run_cargo(root, &["deny", "check"])
}

pub fn run_machete(root: &Path) -> ExitCode {
    let (program, args) = contract::MACHETE;
    let status = Command::new(program).args(args).current_dir(root).status();
    match status {
        Ok(s) if s.success() => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(err) => {
            eprintln!("xtask: failed to invoke cargo-machete: {err}");
            ExitCode::FAILURE
        }
    }
}

pub fn run_module_inventory(root: &Path) -> ExitCode {
    let jobs = jobs_setting();
    run_cargo(
        root,
        &[
            "test",
            "-p",
            "glaeda",
            "--locked",
            "-j",
            &jobs,
            "--test",
            "module_inventory",
        ],
    )
}

pub fn run_threat_model_check(root: &Path) -> ExitCode {
    let jobs = jobs_setting();
    run_cargo(
        root,
        &[
            "test",
            "-p",
            "glaeda",
            "--locked",
            "-j",
            &jobs,
            "--test",
            "threat_model_enforcement_bindings",
        ],
    )
}

pub fn run_hygiene(root: &Path) -> ExitCode {
    for code in [
        run_spec_check(root),
        run_toolchain_check(root),
        run_branch_hygiene(root, &[]),
    ] {
        if code != ExitCode::SUCCESS {
            return code;
        }
    }
    ExitCode::SUCCESS
}

pub fn run_check(root: &Path) -> ExitCode {
    for code in [
        run_hygiene(root),
        run_fmt_check(root),
        run_deny(root),
        run_clippy(root),
    ] {
        if code != ExitCode::SUCCESS {
            return code;
        }
    }
    ExitCode::SUCCESS
}

pub fn run_smoke(root: &Path) -> ExitCode {
    let jobs = jobs_setting();
    let code = run_hygiene(root);
    if code != ExitCode::SUCCESS {
        return code;
    }
    if run_cargo(root, &["test", "-p", "xtask", "--locked", "-j", &jobs]) != ExitCode::SUCCESS {
        return ExitCode::FAILURE;
    }
    if run_threat_model_check(root) != ExitCode::SUCCESS {
        return ExitCode::FAILURE;
    }
    run_module_inventory(root)
}

// ============================================================================
// 0. `xwork` FNV-1a Worktree-Fingerprinted Verification Pipeline
// ============================================================================

/// Deterministic 64-bit FNV-1a digest for worktree state fingerprinting.
#[must_use]
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

pub fn compute_tree_fingerprint(root: &Path) -> Result<String, String> {
    let head_out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()
        .map_err(|e| format!("cannot run `git rev-parse HEAD`: {e}"))?;
    let head = if head_out.status.success() {
        String::from_utf8_lossy(&head_out.stdout).trim().to_string()
    } else {
        "no-head".to_string()
    };

    let status_out = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=all"])
        .current_dir(root)
        .output()
        .map_err(|e| format!("cannot run `git status --porcelain`: {e}"))?;

    let diff_out = Command::new("git")
        .args(["diff", "HEAD"])
        .current_dir(root)
        .output()
        .map_err(|e| format!("cannot run `git diff HEAD`: {e}"))?;

    let untracked_out = Command::new("git")
        .args(["ls-files", "--others", "--exclude-standard"])
        .current_dir(root)
        .output()
        .map_err(|e| format!("cannot run `git ls-files --others`: {e}"))?;

    let mut untracked_hash: u64 = 0xcbf2_9ce4_8422_2325;
    for rel in String::from_utf8_lossy(&untracked_out.stdout).lines() {
        let p = root.join(rel.trim());
        if let Ok(bytes) = fs::read(&p) {
            untracked_hash ^= fnv1a_64(rel.as_bytes());
            untracked_hash = untracked_hash.wrapping_mul(0x0100_0000_01b3);
            untracked_hash ^= fnv1a_64(&bytes);
            untracked_hash = untracked_hash.wrapping_mul(0x0100_0000_01b3);
        }
    }

    let status_hash = fnv1a_64(&status_out.stdout);
    let diff_hash = fnv1a_64(&diff_out.stdout);
    Ok(format!(
        "{head}:{status_hash:016x}:{diff_hash:016x}:{untracked_hash:016x}"
    ))
}

pub fn run_xwork(root: &Path, args: &[String]) -> ExitCode {
    let fresh = args.iter().any(|a| a == "--fresh" || a == "--no-cache");
    let fast_only = args.iter().any(|a| a == "--fast");
    let jobs = jobs_setting();

    let fingerprint = match compute_tree_fingerprint(root) {
        Ok(fp) => fp,
        Err(err) => {
            eprintln!("xwork: failed to compute workspace fingerprint: {err}");
            return ExitCode::FAILURE;
        }
    };

    let state_path = root.join("target/.xwork-state");
    let cache_key = format!(
        "{fingerprint}:mode={}",
        if fast_only { "fast" } else { "full" }
    );
    if !fresh && fs::read_to_string(&state_path).is_ok_and(|saved| saved.trim() == cache_key) {
        println!(
            "xwork: worktree {} already verified (--fresh or --no-cache to force)",
            &fingerprint[..fingerprint.len().min(24)]
        );
        return ExitCode::SUCCESS;
    }

    println!(
        "=== xwork: deterministic pipeline (jobs={jobs}, tree={}) ===",
        &fingerprint[..fingerprint.len().min(24)]
    );

    for &stage in XWORK_STAGES {
        if fast_only && matches!(stage, "xtask-clippy" | "xtask-tests") {
            continue;
        }
        println!("xwork: [{stage}] running...");
        let started = Instant::now();
        let code = match stage {
            "spec-check" => run_spec_check(root),
            "toolchain-check" => run_toolchain_check(root),
            "branch-hygiene" => run_branch_hygiene(root, &[]),
            "fmt-check" => run_fmt_check(root),
            "deny" => {
                if has_subcommand("deny") {
                    run_deny(root)
                } else {
                    println!("xwork: [deny] skipped (cargo-deny not installed)");
                    ExitCode::SUCCESS
                }
            }
            "machete" => {
                if has_binary("cargo-machete") {
                    run_machete(root)
                } else {
                    println!("xwork: [machete] skipped (cargo-machete not installed)");
                    ExitCode::SUCCESS
                }
            }
            "xtask-clippy" => run_cargo(
                root,
                &[
                    "clippy",
                    "-p",
                    "xtask",
                    "--all-targets",
                    "--locked",
                    "-j",
                    &jobs,
                    "--",
                    "-D",
                    "warnings",
                ],
            ),
            "xtask-tests" => run_cargo(root, &["test", "-p", "xtask", "--locked", "-j", &jobs]),
            _ => ExitCode::FAILURE,
        };
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if code != ExitCode::SUCCESS {
            eprintln!("xwork: [{stage}] FAILED ({elapsed_ms}ms)");
            return code;
        }
        println!("xwork: [{stage}] ok ({elapsed_ms}ms)");
    }

    let _ = fs::create_dir_all(root.join("target"));
    let _ = fs::write(&state_path, format!("{cache_key}\n"));
    println!("=== xwork: all stages PASSED ===");
    ExitCode::SUCCESS
}

// ============================================================================
// 1. Spec Check (`specs/NNNN-*.md`, `specs/README.md`, `.agents/persona.md`)
// ============================================================================

#[derive(Debug, Default)]
pub struct SpecReport {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

pub fn run_spec_check(root: &Path) -> ExitCode {
    let report = check_specs(root);
    for w in &report.warnings {
        println!("warning: {w}");
    }
    for e in &report.errors {
        eprintln!("error: {e}");
    }
    if report.errors.is_empty() {
        println!("spec-check: ok ({} warning(s))", report.warnings.len());
        ExitCode::SUCCESS
    } else {
        eprintln!("spec-check: {} error(s)", report.errors.len());
        ExitCode::FAILURE
    }
}

pub fn check_specs(root: &Path) -> SpecReport {
    let mut report = SpecReport::default();
    if !root.join(".agents/persona.md").is_file() {
        report
            .errors
            .push(".agents/persona.md:1: missing lane persona file".into());
    }

    let dir = root.join("specs");
    let mut names: Vec<String> = fs::read_dir(&dir)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .filter(|e| e.path().is_file())
                .filter_map(|e| e.file_name().into_string().ok())
                .filter(|n| n.ends_with(".md"))
                .collect()
        })
        .unwrap_or_default();
    names.sort();

    let mut numbers: BTreeMap<String, String> = BTreeMap::new();
    let mut spec_names: Vec<String> = Vec::new();
    for name in names.iter().filter(|n| n.as_str() != "README.md") {
        let rel = format!("specs/{name}");
        let Some(number) = spec_number(name) else {
            report
                .errors
                .push(format!("{rel}:1: file name must be NNNN-kebab-slug.md"));
            continue;
        };
        if let Some(first) = numbers.get(&number) {
            report.errors.push(format!(
                "{rel}:1: number {number} is already used by {first}"
            ));
            continue;
        }
        numbers.insert(number, name.clone());
        spec_names.push(name.clone());
    }

    for name in &spec_names {
        let rel = format!("specs/{name}");
        match fs::read_to_string(dir.join(name)) {
            Ok(text) => {
                if !text.lines().any(|l| l.starts_with("# ")) {
                    report.errors.push(format!("{rel}:1: missing H1 heading"));
                }
                check_relative_links(&rel, &dir, &text, &mut report);
            }
            Err(err) => report
                .errors
                .push(format!("{rel}:1: cannot read file: {err}")),
        }
    }

    let readme = dir.join("README.md");
    match fs::read_to_string(&readme) {
        Ok(text) => {
            check_relative_links("specs/README.md", &dir, &text, &mut report);
            for name in &spec_names {
                if !text.contains(name.as_str()) {
                    report
                        .errors
                        .push(format!("specs/README.md:1: {name} is not listed in index"));
                }
            }
        }
        Err(_) => {
            if !spec_names.is_empty() {
                report
                    .errors
                    .push("specs/README.md:1: file is missing".into());
            }
        }
    }

    report
}

pub fn spec_number(name: &str) -> Option<String> {
    let stem = name.strip_suffix(".md")?;
    let (num, slug) = stem.split_once('-')?;
    let digits = num.len() == 4 && num.chars().all(|c| c.is_ascii_digit());
    let slug_ok = !slug.is_empty()
        && slug.split('-').all(|seg| {
            !seg.is_empty()
                && seg
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        });
    (digits && slug_ok).then(|| num.to_string())
}

fn strip_inline_code(line: &str) -> String {
    let mut stripped = String::with_capacity(line.len());
    let mut in_code = false;
    for c in line.chars() {
        if c == '`' {
            in_code = !in_code;
        } else if !in_code {
            stripped.push(c);
        }
    }
    stripped
}

fn line_links(line: &str) -> Vec<String> {
    let stripped = strip_inline_code(line);
    let mut out = Vec::new();
    let mut rest = stripped.as_str();
    while let Some(i) = rest.find("](") {
        rest = &rest[i + 2..];
        if let Some(j) = rest.find(')') {
            if let Some(target) = rest[..j].split_whitespace().next() {
                out.push(target.to_string());
            }
            rest = &rest[j..];
        }
    }
    out
}

fn check_relative_links(rel: &str, dir: &Path, text: &str, report: &mut SpecReport) {
    let mut in_fence = false;
    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }
        for target in line_links(line) {
            let skip = ["http://", "https://", "mailto:", "#", "file://"];
            if skip.iter().any(|p| target.starts_with(p)) {
                continue;
            }
            let path = target.split('#').next().unwrap_or("");
            if !path.is_empty() && !dir.join(path).exists() {
                report.errors.push(format!(
                    "{rel}:{}: broken relative link `{target}`",
                    idx + 1
                ));
            }
        }
    }
}

// ============================================================================
// 2. Toolchain Check (`rust-toolchain.toml`, `clippy.toml`, `deny.toml`, etc.)
// ============================================================================

pub fn run_toolchain_check(root: &Path) -> ExitCode {
    match check_toolchain(root) {
        Ok(msg) => {
            println!("toolchain-check: {msg}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("toolchain-check: {err}");
            ExitCode::FAILURE
        }
    }
}

pub fn check_toolchain(root: &Path) -> Result<String, String> {
    for &req in REQUIRED_TOOLCHAIN_FILES {
        if !root.join(req).is_file() {
            return Err(format!("missing required toolchain config `{req}`"));
        }
    }
    let toolchain = fs::read_to_string(root.join("rust-toolchain.toml"))
        .map_err(|e| format!("cannot read rust-toolchain.toml: {e}"))?;
    let channel = toml_string(&toolchain, Some("toolchain"), "channel")
        .ok_or_else(|| "no `channel` in rust-toolchain.toml".to_string())?;

    let out = Command::new("rustc")
        .arg("--version")
        .current_dir(root)
        .output()
        .map_err(|e| format!("cannot run `rustc --version`: {e}"))?;
    if !out.status.success() {
        return Err("`rustc --version` failed".to_string());
    }
    let rustc = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let active_ver = rustc
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("unexpected `rustc --version` output: {rustc}"))?;
    if active_ver != channel {
        return Err(format!(
            "active rustc is {active_ver}, but rust-toolchain.toml pins {channel}"
        ));
    }
    Ok(format!("ok ({rustc}, channel={channel})"))
}

fn toml_string(src: &str, section: Option<&str>, key: &str) -> Option<String> {
    let mut current_section: Option<&str> = None;
    for raw in src.lines() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            current_section = Some(&line[1..line.len() - 1]);
            continue;
        }
        if current_section == section
            && let Some((k, v)) = line.split_once('=')
            && k.trim() == key
        {
            let v = v.trim().trim_matches('"').trim_matches('\'');
            return Some(v.to_string());
        }
    }
    None
}

// ============================================================================
// 3. Branch Hygiene (`branch-hygiene`)
// ============================================================================

pub fn run_branch_hygiene(root: &Path, args: &[String]) -> ExitCode {
    let prune = args.iter().any(|a| a == "--prune");
    let trunk = stack::detect_trunk_branch(root);
    let current = stack::current_branch_in_process(root).unwrap_or_else(|_| trunk.clone());
    let out = Command::new("git")
        .current_dir(root)
        .args(["branch", "--merged", &format!("origin/{trunk}")])
        .output();
    let Ok(out) = out else {
        println!("branch-hygiene: ok");
        return ExitCode::SUCCESS;
    };
    if !out.status.success() {
        println!("branch-hygiene: ok");
        return ExitCode::SUCCESS;
    }
    let merged: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|l| l.trim().trim_start_matches("* ").trim().to_string())
        .filter(|b| !b.is_empty() && b != &trunk && b != "main" && b != "unsigned" && b != &current)
        .collect();

    if merged.is_empty() {
        println!("branch-hygiene: ok (0 stale merged local branches)");
        return ExitCode::SUCCESS;
    }

    if prune {
        for b in &merged {
            let _ = Command::new("git")
                .current_dir(root)
                .args(["branch", "-d", b])
                .status();
            println!("branch-hygiene: pruned merged local branch `{b}`");
        }
    } else {
        println!(
            "branch-hygiene: ok ({} merged branch(es) eligible for `--prune`: {})",
            merged.len(),
            merged.join(", ")
        );
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn every_contract_command_is_registered() {
        for &name in contract::CONTRACT_COMMANDS {
            assert!(
                parse_command(name).is_some(),
                "contract command {name:?} must be handled by parse_command"
            );
        }
    }

    #[test]
    fn fnv1a_digest_is_deterministic_and_sensitive() {
        let a = fnv1a_64(b"glaeda-estate-tooling");
        let b = fnv1a_64(b"glaeda-estate-tooling");
        let c = fnv1a_64(b"glaeda-estate-tooling-modified");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn spec_number_parses_valid_names_and_rejects_invalid_ones() {
        assert_eq!(
            spec_number("0011-estate-rust-tooling-and-config.md"),
            Some("0011".to_string())
        );
        assert_eq!(spec_number("11-short.md"), None);
        assert_eq!(spec_number("0011-UpperCase.md"), None);
        assert_eq!(spec_number("README.md"), None);
    }

    #[test]
    fn check_specs_validates_repo_and_catches_broken_spec_in_negative_control() {
        let root = repo_root();
        let real_report = check_specs(&root);
        assert!(
            real_report.errors.is_empty(),
            "expected 0 spec-check errors in workspace, got: {:?}",
            real_report.errors
        );

        // Negative control: synthetic workspace missing index entry and with broken link fails.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp = env::temp_dir().join(format!(
            "glaeda-xtask-spec-neg-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(tmp.join("specs")).unwrap();
        fs::write(
            tmp.join("specs/0001-broken-spec.md"),
            "# Spec 0001\n\nSee [missing](./0099-does-not-exist.md).\n",
        )
        .unwrap();
        fs::write(tmp.join("specs/README.md"), "# Specs\n").unwrap();

        let neg_report = check_specs(&tmp);
        assert!(
            neg_report
                .errors
                .iter()
                .any(|e| e.contains("missing lane persona")),
            "expected missing .agents/persona.md error, got {:?}",
            neg_report.errors
        );
        assert!(
            neg_report
                .errors
                .iter()
                .any(|e| e.contains("broken relative link")),
            "expected broken relative link error, got {:?}",
            neg_report.errors
        );
        assert!(
            neg_report
                .errors
                .iter()
                .any(|e| e.contains("is not listed in index")),
            "expected unlisted spec error, got {:?}",
            neg_report.errors
        );

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn check_toolchain_succeeds_on_workspace_and_fails_when_file_missing() {
        let root = repo_root();
        assert!(check_toolchain(&root).is_ok());

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp = env::temp_dir().join(format!(
            "glaeda-xtask-tc-neg-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&tmp).unwrap();
        let err = check_toolchain(&tmp).unwrap_err();
        assert!(err.contains("missing required toolchain config"));
        let _ = fs::remove_dir_all(&tmp);
    }
}
