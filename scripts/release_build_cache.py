#!/usr/bin/env python3
"""Build release.yml's Rust binaries once per Rust inputs, not once per push (Leo, 2026-11-21: "we should be
able to speed these thigns up and be able to granularly just hot swap apply things as we go").

Most pushes to main change only scripts and docs, yet every release rebuilt glaeda (about 275k lines),
glaeda-worktree-reclaim and fleet-cas from scratch: about 3.5 of the build job's 4-5 minutes. release.yml keys
an actions/cache entry of the built binaries on `key` below, so a push whose Rust inputs match an earlier
release's reuses those bytes and skips cargo; any change to them builds as before.

    key --set S --target T          the cache key: the set's tracked input blobs (git ls-tree HEAD), the set's
                                    pathspecs, the target and `rustc -Vv`
    stash --set S --from DIR --out DIR
                                    copy the set's binaries and their cargo dep-info (.d) into the cache folder,
                                    with the root they were built in
    covered --set S --dir DIR       exit 0 when every source file the cached binaries' dep-info names inside the
                                    checkout is one the key hashed (or generated under target/); else exit 1,
                                    naming the file, and release.yml builds. So an include_str! of a file outside
                                    the set can never ship stale bytes: it only costs a build until the set
                                    names it.

The cache is only read and written by release.yml on main; actions/cache scopes entries to the branch that
wrote them, so a pull request's run can never seed what main restores.
"""
from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "glaeda-release-build-cache/v1"
SETS = {
    # the root crate: both binaries come from one cargo build. examples/ and tests/fixtures/ hold files src/ embeds
    # with include_str!/include_bytes!.
    "glaeda": {
        "pathspecs": ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "build.rs", ".cargo", "src", "examples",
                      "tests/fixtures"],
        "binaries": ["glaeda", "glaeda-worktree-reclaim"],
    },
    # fleet-cas: its build.rs compiles proto/ without rerun-if-changed, so the whole package counts, but the
    # scripts, tests and README, which never reach the binary
    "fleet-cas": {
        "pathspecs": ["rust-toolchain.toml", "tools/fleet-cas-prototype"],
        "exclude": ["tools/fleet-cas-prototype/scripts/", "tools/fleet-cas-prototype/tests/",
                    "tools/fleet-cas-prototype/README.md"],
        "binaries": ["fleet-cas"],
    },
}
BUILT_IN = "built-in.txt"  # the checkout root the binaries were built in: dep-info paths are absolute


def git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True, text=True).stdout


def tracked(root: Path, name: str, names_only: bool = False) -> str:
    """`git ls-tree -r HEAD` of the set (mode, blob, path per line), less its excluded prefixes."""
    lines = git(root, "ls-tree", "-r", "--full-tree", "HEAD", "--", *SETS[name]["pathspecs"]).splitlines()
    keep = [line for line in lines if not line.split("\t", 1)[-1].startswith(tuple(SETS[name].get("exclude", ())))]
    return "".join((line.split("\t", 1)[-1] if names_only else line) + "\n" for line in keep)


def key(root: Path, name: str, target: str, rustc: str) -> str:
    h = hashlib.sha256()
    for part in (SCHEMA, name, target, rustc, "\n".join(SETS[name]["pathspecs"] + SETS[name].get("exclude", [])), tracked(root, name)):
        h.update(part.encode())
        h.update(b"\0")
    return h.hexdigest()


def stash(root: Path, name: str, source: Path, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for binary in SETS[name]["binaries"]:
        for file in (binary, f"{binary}.d"):
            shutil.copy2(source / file, out / file)
    (out / BUILT_IN).write_text(str(root.resolve()) + "\n")


def depinfo_paths(text: str) -> list[str]:
    """The prerequisites of a make-style dep-info file (cargo's <bin>.d): `out: dep dep\\ with\\ space ...`."""
    paths: list[str] = []
    for line in text.replace("\\\n", " ").splitlines():
        if ":" not in line:
            continue
        _, _, rest = line.partition(": ")
        word, escaped = "", False
        for ch in rest:
            if escaped:
                word, escaped = word + ch, False
            elif ch == "\\":
                escaped = True
            elif ch.isspace():
                if word:
                    paths.append(word)
                word = ""
            else:
                word += ch
        if word:
            paths.append(word)
    return paths


def uncovered(root: Path, name: str, folder: Path) -> list[str]:
    """Checkout files the cached binaries were built from that the key did not hash; [] when all were."""
    built_in = (folder / BUILT_IN).read_text().strip()
    hashed = set(tracked(root, name, names_only=True).splitlines())
    missing: list[str] = []
    for binary in SETS[name]["binaries"]:
        if not (folder / binary).is_file():
            return [f"{binary} (not in the cache)"]
        for path in depinfo_paths((folder / f"{binary}.d").read_text()):
            if not path.startswith(built_in.rstrip("/") + "/"):
                continue  # registry crates and the toolchain: pinned by Cargo.lock and rustc -Vv in the key
            relative = path[len(built_in.rstrip("/")) + 1:]
            if relative.startswith("target/"):
                continue  # generated by a build script from inputs the key hashed
            if relative not in hashed:
                missing.append(relative)
    return sorted(set(missing))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)
    k = sub.add_parser("key")
    k.add_argument("--target", required=True)
    k.add_argument("--rustc", help="`rustc -Vv` output (default: run it)")
    s = sub.add_parser("stash")
    s.add_argument("--from", dest="source", type=Path, required=True, help="cargo's target/<triple>/release")
    s.add_argument("--out", type=Path, required=True)
    c = sub.add_parser("covered")
    c.add_argument("--dir", type=Path, required=True)
    for p in (k, s, c):
        p.add_argument("--set", required=True, choices=sorted(SETS))
        p.add_argument("--root", type=Path, default=ROOT)
    a = ap.parse_args(argv)
    if a.command == "key":
        rustc = a.rustc if a.rustc is not None else subprocess.run(
            ["rustc", "-Vv"], check=True, capture_output=True, text=True, cwd=a.root).stdout
        print(key(a.root, a.set, a.target, rustc.strip()))
        return 0
    if a.command == "stash":
        stash(a.root, a.set, a.source, a.out)
        return 0
    try:
        missing = uncovered(a.root, a.set, a.dir)
    except OSError as e:
        print(f"release build cache: {a.set}: unreadable ({e}); building", file=sys.stderr)
        return 1
    if missing:
        print(f"release build cache: {a.set}: built from files the key does not hash, so building: "
              f"{', '.join(missing[:20])} (add them to SETS in scripts/release_build_cache.py)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
