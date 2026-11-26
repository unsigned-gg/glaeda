#!/usr/bin/env python3
"""scripts/release_build_cache.py: release.yml reuses Rust binaries only for identical, fully hashed inputs."""
from __future__ import annotations

import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("release_build_cache", Path(__file__).with_name("release_build_cache.py"))
rbc = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rbc)


def git(root: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


class ReleaseBuildCache(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "repo"
        (self.root / "src").mkdir(parents=True)
        (self.root / "scripts").mkdir()
        (self.root / "tools/fleet-cas-prototype/src").mkdir(parents=True)
        (self.root / "tools/fleet-cas-prototype/scripts").mkdir()
        files = {"Cargo.toml": "[package]\n", "Cargo.lock": "", "src/main.rs": "fn main() {}\n",
                 "scripts/x.py": "print(1)\n", "tools/fleet-cas-prototype/src/main.rs": "fn main() {}\n",
                 "tools/fleet-cas-prototype/scripts/run.sh": "true\n", "docs.md": "hi\n"}
        for name, text in files.items():
            (self.root / name).write_text(text)
        git(self.root, "init", "-q")
        git(self.root, "-c", "user.email=t@t", "-c", "user.name=t", "add", "-A")
        git(self.root, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "one")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def commit(self, name: str, text: str) -> None:
        (self.root / name).write_text(text)
        git(self.root, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qam", name)

    def test_key_ignores_scripts_and_docs_but_not_rust_inputs(self) -> None:
        first = rbc.key(self.root, "glaeda", "t", "rustc 1")
        cas = rbc.key(self.root, "fleet-cas", "t", "rustc 1")
        self.commit("scripts/x.py", "print(2)\n")
        self.commit("docs.md", "bye\n")
        self.commit("tools/fleet-cas-prototype/scripts/run.sh", "false\n")
        self.assertEqual(rbc.key(self.root, "glaeda", "t", "rustc 1"), first)
        self.assertEqual(rbc.key(self.root, "fleet-cas", "t", "rustc 1"), cas)
        self.commit("src/main.rs", "fn main() { }\n")
        self.assertNotEqual(rbc.key(self.root, "glaeda", "t", "rustc 1"), first)
        self.assertEqual(rbc.key(self.root, "fleet-cas", "t", "rustc 1"), cas)
        self.commit("tools/fleet-cas-prototype/src/main.rs", "fn main() { }\n")
        self.assertNotEqual(rbc.key(self.root, "fleet-cas", "t", "rustc 1"), cas)

    def test_key_names_the_target_and_toolchain(self) -> None:
        base = rbc.key(self.root, "glaeda", "t", "rustc 1")
        self.assertNotEqual(rbc.key(self.root, "glaeda", "u", "rustc 1"), base)
        self.assertNotEqual(rbc.key(self.root, "glaeda", "t", "rustc 2"), base)

    def test_depinfo_parses_escaped_spaces_and_continuations(self) -> None:
        text = "/b/target/release/glaeda: /b/src/main.rs /b/src/a\\ b.rs \\\n /r/.cargo/x.rs\n\n/b/src/main.rs:\n"
        self.assertEqual(rbc.depinfo_paths(text), ["/b/src/main.rs", "/b/src/a b.rs", "/r/.cargo/x.rs"])

    def stash(self, deps: list[str], built_in: str = "/build/root") -> Path:
        folder = Path(self.tmp.name) / "bins"
        folder.mkdir(exist_ok=True)
        for binary in rbc.SETS["glaeda"]["binaries"]:
            (folder / binary).write_bytes(b"\x7fELF")
            (folder / f"{binary}.d").write_text(f"{built_in}/target/release/{binary}: {' '.join(deps)}\n")
        (folder / rbc.BUILT_IN).write_text(built_in + "\n")
        return folder

    def test_covered_accepts_hashed_registry_and_generated_files(self) -> None:
        folder = self.stash(["/build/root/src/main.rs", "/home/r/.cargo/registry/src/x/lib.rs",
                             "/build/root/target/release/build/out/gen.rs"])
        self.assertEqual(rbc.uncovered(self.root, "glaeda", folder), [])

    def test_covered_refuses_a_checkout_file_the_key_missed(self) -> None:
        folder = self.stash(["/build/root/src/main.rs", "/build/root/scripts/x.py"])
        self.assertEqual(rbc.uncovered(self.root, "glaeda", folder), ["scripts/x.py"])
        self.assertEqual(rbc.main(["covered", "--set", "glaeda", "--dir", str(folder), "--root", str(self.root)]), 1)

    def test_covered_refuses_a_missing_binary(self) -> None:
        folder = self.stash(["/build/root/src/main.rs"])
        (folder / "glaeda-worktree-reclaim").unlink()
        self.assertTrue(rbc.uncovered(self.root, "glaeda", folder))

    def test_stash_copies_binaries_depinfo_and_root(self) -> None:
        release = Path(self.tmp.name) / "release"
        release.mkdir()
        for binary in rbc.SETS["glaeda"]["binaries"]:
            (release / binary).write_bytes(b"bin")
            (release / f"{binary}.d").write_text(f"{self.root}/target/x: {self.root}/src/main.rs\n")
        out = Path(self.tmp.name) / "out"
        self.assertEqual(rbc.main(["stash", "--set", "glaeda", "--from", str(release), "--out", str(out),
                                   "--root", str(self.root)]), 0)
        self.assertEqual(rbc.main(["covered", "--set", "glaeda", "--dir", str(out), "--root", str(self.root)]), 0)


if __name__ == "__main__":
    unittest.main()
