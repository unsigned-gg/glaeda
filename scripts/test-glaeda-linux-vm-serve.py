#!/usr/bin/env python3
"""Bounded contract tests for glaeda-linux-vm-serve with fake go, cargo, git and glaeda; no network, no root."""
from __future__ import annotations

import hashlib
import importlib.machinery
import importlib.util
import io
import json
import os
import pathlib
import stat
import sys
import tarfile
import tempfile
import unittest
from types import SimpleNamespace

ROOT = pathlib.Path(__file__).resolve().parents[1]
LOADER = importlib.machinery.SourceFileLoader("glaeda_linux_vm_serve", str(ROOT / "scripts" / "glaeda-linux-vm-serve"))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
assert SPEC
serve = importlib.util.module_from_spec(SPEC)
sys.modules[LOADER.name] = serve
LOADER.exec_module(serve)

COMMIT = "c" * 40
HOST = {"system": "Darwin", "machine": "arm64", "euid": os.geteuid()}


def ok(stdout="", code=0, stderr=""):
    return SimpleNamespace(returncode=code, stdout=stdout, stderr=stderr)


def go_archive() -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name in ("go", "go/bin"):
            info = tarfile.TarInfo(name)
            info.type, info.mode = tarfile.DIRTYPE, 0o755
            tar.addfile(info)
        data = b"#!/bin/sh\n"
        info = tarfile.TarInfo("go/bin/go")
        info.size, info.mode = len(data), 0o755
        tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class Fake:
    """Answers the subprocesses the command runs; builds write deterministic bytes."""

    def __init__(self, *, toolchains="stable-aarch64-apple-darwin\n", dirty=False, plan_identity="sha256:" + "p" * 64,
                 status_state="loaded_exact"):
        self.toolchains = toolchains
        self.dirty = dirty
        self.plan_identity = plan_identity
        self.status_state = status_state
        self.calls: list[list[str]] = []
        self.envs: list[dict[str, str]] = []

    def __call__(self, argv, *, env, timeout, cwd=None):
        self.calls.append(list(argv))
        self.envs.append(dict(env))
        if argv[0] == "/usr/bin/git":
            if "rev-parse" in argv:
                return ok(COMMIT + "\n")
            return ok(" M src/x.rs\n" if self.dirty else "")
        if argv[0].endswith("/bin/go"):
            if argv[1] == "version":
                return ok(f"go version go{serve.GO_VERSION} darwin/arm64\n")
            out = pathlib.Path(argv[argv.index("-o") + 1])
            out.write_bytes(f"go:{argv[-1]}".encode())
            return ok()
        if argv[0].endswith("rustup"):
            if argv[1:3] == ["toolchain", "list"]:
                return ok(self.toolchains)
            if argv[1:3] == ["toolchain", "install"]:
                self.toolchains += f"{serve.RUST_TOOLCHAIN}-aarch64-apple-darwin\n"
                return ok()
            if argv[1] == "which":
                return ok(f"/rustup/toolchains/{argv[3]}-aarch64-apple-darwin/bin/{argv[4]}\n")
            raise AssertionError(f"unexpected rustup argv {argv}")
        if argv[0].endswith("/bin/cargo"):
            target = pathlib.Path(env["CARGO_TARGET_DIR"]) / "release"
            target.mkdir(parents=True, exist_ok=True)
            (target / "glaeda").write_bytes(b"glaeda-binary")
            return ok()
        if argv[0].endswith("/bin/rustc"):
            return ok("rustc 1.97.1 (x 2026-01-01)\n")
        if argv[0].endswith("/glaeda"):
            verb = argv[4]
            if verb == "plan":
                return ok(json.dumps({"plan_identity": self.plan_identity, "desired_state": "installed",
                                      "service_label": "io.smolrunner.disposable-worker", "launchd_domain": "gui/501",
                                      "requires_operator_approval": True}))
            if verb == "apply":
                return ok(json.dumps({"disposition": "applied", "plan_identity": self.plan_identity}))
            return ok(json.dumps({"state": self.status_state, "service_loaded": True, "plan_identity": self.plan_identity,
                                  "installation_remediation": "none", "runtime_health": "unknown"}))
        raise AssertionError(f"unexpected argv {argv}")


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = pathlib.Path(self.tmp.name)
        self.home = base / "home"
        (self.home / "Library" / "LaunchAgents").mkdir(parents=True, mode=0o755)
        os.chmod(self.home, 0o750)
        self.root = self.home / ".glaeda" / "linux-vm"
        self.root.mkdir(parents=True, mode=0o700)
        os.chmod(self.home / ".glaeda", 0o755)
        (self.root / "base.json").write_text(json.dumps({"state": "ready_stopped"}))
        self.rustup = base / "rustup"
        self.rustup.write_text("#!/bin/sh\n")
        self.rustup.chmod(0o755)
        self.opt = base / "opt"
        self.dirs = [str(self.opt), str(self.opt / "smolrunner"), str(self.opt / "smolrunner" / "bin")]
        archive = go_archive()
        self.patch("GO_BYTES", len(archive))
        self.patch("GO_SHA256", hashlib.sha256(archive).hexdigest())
        self.archive = archive
        self.downloads = 0

    def patch(self, name, value):
        old = getattr(serve, name)
        setattr(serve, name, value)
        self.addCleanup(setattr, serve, name, old)

    def download(self, url, dest, timeout):
        self.downloads += 1
        dest.write_bytes(self.archive)

    def make(self, fake, **kwargs):
        return serve.Serve(root=self.root, home=self.home, repo=ROOT, runner=fake, download=self.download,
                           install_bin=self.dirs[-1], install_dirs=self.dirs,
                           root_receipt=str(self.opt / "smolrunner" / "receipt.json"),
                           root_ids=(os.getuid(), os.getgid()), chown=lambda fd, uid, gid: None,
                           rustup_candidates=[self.rustup], host=dict(HOST, **kwargs))

    def stage_and_install(self, fake):
        report = self.make(fake).execute(apply=True)
        art = report["stage"]
        self.make(fake).install_root(apply=True, want={"glaeda": art["glaeda"], "scaleset-bridge": art["scaleset-bridge"]})
        return report


class EnrollmentEncoding(Case):
    def test_canonical_bytes_match_the_rust_pretty_encoding(self):
        document = serve.enrollment_document(
            state_root="/Users/u/.glaeda/linux-vm/worker-state", limactl="/l/limactl", lima_home="/l/home",
            bridge_sha256="a" * 64, token_file="/Users/u/.config/glaeda/scaleset-token.json",
            scale_set={"id": 7, "name": "glaeda-linux-arm64", "runner_group_id": 3, "owner": "manaflow-ai",
                       "repository": "cmux", "labels": ["glaeda-linux-arm64"]})
        want = """{
  "schema_version": 2,
  "state_root": "/Users/u/.glaeda/linux-vm/worker-state",
  "lima": {
    "program": "/l/limactl",
    "home": "/l/home",
    "source_instance": "glaeda-linux-arm64-base"
  },
  "bridge": {
    "program_digest": "sha256:%s"
  },
  "github": {
    "config_url": "https://github.com/manaflow-ai",
    "credential": {
      "kind": "installation_token_file",
      "app_id": 5067688,
      "path": "/Users/u/.config/glaeda/scaleset-token.json"
    }
  },
  "scale_set": {
    "id": 7,
    "name": "glaeda-linux-arm64",
    "runner_group_id": 3,
    "owner": "manaflow-ai",
    "repository": "cmux",
    "labels": [
      "glaeda-linux-arm64"
    ]
  },
  "resources": {
    "cpu_millis": 4000,
    "memory_bytes": 8589934592,
    "disk_bytes": 21474836480
  }
}
""" % ("a" * 64)
        self.assertEqual(serve.canonical_json(document).decode(), want)

    def test_lima_home_fits_the_socket_budget(self):
        # src/disposable_lima_worker.rs: home + 1 + (2 + 24) + len("/ssh.sock.1234567890123456") < 104
        home = "/Users/cmux/.glaeda/linux-vm/lima-home"
        self.assertLess(len(home) + 1 + 26 + len("/ssh.sock.1234567890123456"), 104)

    def test_scale_set_output_is_checked_like_the_decoder(self):
        good = {"mode": "apply", "enrollment": {"scale_set_id": 12, "scale_set_name": "glaeda-linux-arm64",
                                                "runner_group_id": 4, "labels": ["glaeda-linux-arm64"],
                                                "owner": "manaflow-ai", "repository": "cmux"}}
        self.assertEqual(serve.scale_set_from_enroll_output(json.dumps(good).encode())["id"], 12)
        bad = [
            {"mode": "plan", "enrollment": None},
            {"error": "token_expired"},
            dict(good, enrollment=dict(good["enrollment"], scale_set_id=0)),
            dict(good, enrollment=dict(good["enrollment"], runner_group_id=True)),
            dict(good, enrollment=dict(good["enrollment"], labels=["b", "a"])),
            dict(good, enrollment=dict(good["enrollment"], labels=["a", "a"])),
        ]
        for document in bad:
            with self.assertRaises(serve.Refused):
                serve.scale_set_from_enroll_output(json.dumps(document).encode())
        with self.assertRaises(serve.Refused):
            serve.scale_set_from_enroll_output(b"not json")


class OperatorStage(Case):
    def test_plan_has_no_side_effects(self):
        fake = Fake()
        report = self.make(fake).execute(apply=False)
        self.assertEqual(report["plan"], ["install_go", "install_rust_toolchain", "build_stage", "create_state_root"])
        self.assertEqual(report["disposition"], "plan")
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ["base.json"])
        self.assertEqual(self.downloads, 0)
        self.assertFalse(any("build" in call for call in fake.calls))

    def test_apply_builds_stages_and_is_idempotent(self):
        fake = Fake()
        report = self.make(fake).execute(apply=True)
        self.assertEqual(report["disposition"], "changed")
        self.assertEqual([a["action"] for a in report["actions"]],
                         ["install_go", "install_rust_toolchain", "build_stage", "create_state_root",
                          "write_enrollment_template"])
        stage = self.root / "serve" / "stage"
        for name in serve.ARTIFACTS:
            self.assertEqual(report["stage"][name], hashlib.sha256((stage / name).read_bytes()).hexdigest())
        self.assertEqual(stat.S_IMODE((self.root / "worker-state").stat().st_mode), 0o750)
        template = self.root / "serve" / "enrollment.template.json"
        self.assertEqual(stat.S_IMODE(template.stat().st_mode), 0o600)
        self.assertIn(f"sha256:{report['stage']['scaleset-bridge']}", template.read_text())
        # cargo through the pinned toolchain, go with a local toolchain and no cgo
        cargo, cargo_env = next((c, e) for c, e in zip(fake.calls, fake.envs) if c[0].endswith("/bin/cargo"))
        self.assertIn(f"/{serve.RUST_TOOLCHAIN}-", cargo[0])
        self.assertEqual(cargo[1:3], ["build", "--locked"])
        # cargo finds the pinned rustc first on PATH
        self.assertEqual(cargo_env["PATH"].split(":")[0], str(pathlib.Path(cargo[0]).parent))
        self.assertEqual(cargo_env["RUSTUP_TOOLCHAIN"], serve.RUST_TOOLCHAIN)
        go_env = next(e for c, e in zip(fake.calls, fake.envs) if c[0].endswith("/bin/go") and c[1] == "build")
        self.assertEqual((go_env["GOTOOLCHAIN"], go_env["CGO_ENABLED"], go_env["GOFLAGS"]), ("local", "0", "-mod=readonly"))
        self.assertFalse(report["installed"]["program"].get("contract_ok"))
        self.assertTrue(report["next"][0].startswith("fleet-sudo <host> "))

        again = self.make(fake).execute(apply=True)
        self.assertEqual(again["disposition"], "unchanged")
        self.assertEqual(self.downloads, 1)

    def test_apply_refuses_dirty_tree_root_and_missing_base(self):
        self.assertIn("uncommitted", self.run_refused(self.make(Fake(dirty=True)).execute, apply=True))
        self.assertIn("operator", self.run_refused(self.make(Fake(), euid=0).execute, apply=True))
        (self.root / "base.json").unlink()
        self.assertIn("base VM", self.run_refused(self.make(Fake()).execute, apply=True))

    def test_wrong_go_archive_is_refused_before_install(self):
        self.patch("GO_SHA256", "0" * 64)
        with self.assertRaises(serve.Failed):
            self.make(Fake()).execute(apply=True)
        self.assertFalse((self.root / f"go-{serve.GO_VERSION}").exists())

    def test_created_directories_ignore_a_permissive_umask(self):
        old = os.umask(0o002)
        try:
            self.make(Fake()).execute(apply=True)
        finally:
            os.umask(old)
        for path in (self.root / "serve", self.root / "serve" / "stage"):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700, path)
        self.assertEqual(stat.S_IMODE((self.root / "worker-state").stat().st_mode), 0o750)

    def test_foreign_state_root_is_left_alone(self):
        (self.root / "worker-state").mkdir(mode=0o700)
        os.chmod(self.root / "worker-state", 0o700)
        self.assertIn("not touching", self.run_refused(self.make(Fake()).execute, apply=True))

    def run_refused(self, method, **kwargs):
        with self.assertRaises(serve.Refused) as raised:
            method(**kwargs)
        return str(raised.exception)


class RootInstall(Case):
    def test_installs_exact_digests_0555_and_second_run_is_unchanged(self):
        fake = Fake()
        art = self.make(fake).execute(apply=True)["stage"]
        want = {"glaeda": art["glaeda"], "scaleset-bridge": art["scaleset-bridge"]}
        plan = self.make(fake).install_root(apply=False, want=want)
        self.assertEqual(plan["plan"], ["glaeda", "scaleset-bridge"])
        self.assertFalse(self.opt.exists())
        report = self.make(fake).install_root(apply=True, want=want)
        self.assertEqual(report["disposition"], "changed")
        for name in ("glaeda", "scaleset-bridge"):
            path = pathlib.Path(self.dirs[-1]) / name
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o555)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), want[name])
        self.assertTrue(report["installed"]["bridge"]["contract_ok"])
        self.assertTrue(report["installed"]["program"]["matches_stage"])
        self.assertEqual(self.make(fake).install_root(apply=True, want=want)["disposition"], "unchanged")

    def test_refuses_unreviewed_digest_foreign_file_and_non_root(self):
        fake = Fake()
        art = self.make(fake).execute(apply=True)["stage"]
        want = {"glaeda": art["glaeda"], "scaleset-bridge": art["scaleset-bridge"]}
        with self.assertRaises(serve.Refused):
            self.make(fake).install_root(apply=True, want=dict(want, glaeda="f" * 64))
        with self.assertRaises(serve.Refused):
            self.make(fake, euid=os.geteuid() + 1).install_root(apply=True, want=want)
        pathlib.Path(self.dirs[-1]).mkdir(parents=True)
        foreign = pathlib.Path(self.dirs[-1]) / "scaleset-bridge"
        foreign.write_bytes(b"someone else's")
        with self.assertRaises(serve.Refused):
            self.make(fake).install_root(apply=True, want=want)
        self.assertEqual(foreign.read_bytes(), b"someone else's")

    def test_a_staged_file_changed_after_review_is_refused(self):
        fake = Fake()
        art = self.make(fake).execute(apply=True)["stage"]
        (self.root / "serve" / "stage" / "glaeda").write_bytes(b"swapped")
        with self.assertRaises(serve.Refused):
            self.make(fake).install_root(apply=True, want={"glaeda": art["glaeda"],
                                                           "scaleset-bridge": art["scaleset-bridge"]})
        self.assertFalse((pathlib.Path(self.dirs[-1]) / "glaeda").exists())


class Finish(Case):
    def enroll_output(self):
        path = self.root / "serve" / "scaleset-enroll.json"
        path.write_text(json.dumps({"mode": "apply", "enrollment": {
            "scale_set_id": 12, "scale_set_name": "glaeda-linux-arm64", "runner_group_id": 4,
            "labels": ["glaeda-linux-arm64"], "owner": "manaflow-ai", "repository": "cmux"}}))
        return path

    def test_plan_writes_nothing_and_names_the_identity(self):
        fake = Fake()
        self.stage_and_install(fake)
        report = self.make(fake).finish(scale_set_file=self.enroll_output(), apply=False, approve=None)
        self.assertEqual(report["plan_identity"], fake.plan_identity)
        self.assertEqual(report["enrollment"], "absent")
        self.assertFalse((self.root / "serve" / "enrollment.json").exists())
        plan_call = [c for c in fake.calls if c[0].endswith("/glaeda")][-1]
        self.assertEqual(plan_call[plan_call.index("--enrollment-digest") + 1], report["enrollment_digest"])
        self.assertEqual(fake.envs[-1], {}, "glaeda runs with an empty environment")

    def test_apply_needs_the_exact_approval_and_the_token(self):
        fake = Fake()
        self.stage_and_install(fake)
        source = self.enroll_output()
        with self.assertRaises(serve.Refused):
            self.make(fake).finish(scale_set_file=source, apply=True, approve=None)
        with self.assertRaises(serve.Refused):
            self.make(fake).finish(scale_set_file=source, apply=True, approve="sha256:" + "q" * 64)
        with self.assertRaises(serve.Refused) as raised:
            self.make(fake).finish(scale_set_file=source, apply=True, approve=fake.plan_identity)
        self.assertIn("absent", str(raised.exception))
        self.assertFalse((self.root / "serve" / "enrollment.json").exists())

    def test_apply_writes_0600_enrollment_applies_and_checks_status(self):
        fake = Fake()
        self.stage_and_install(fake)
        token = self.home / ".config" / "glaeda" / "scaleset-token.json"
        token.parent.mkdir(parents=True)
        token.write_text("{}")
        report = self.make(fake).finish(scale_set_file=self.enroll_output(), apply=True, approve=fake.plan_identity)
        enrollment = self.root / "serve" / "enrollment.json"
        self.assertEqual(stat.S_IMODE(enrollment.stat().st_mode), 0o600)
        self.assertEqual(report["enrollment_digest"], "sha256:" + hashlib.sha256(enrollment.read_bytes()).hexdigest())
        self.assertEqual(json.loads(enrollment.read_text())["scale_set"]["id"], 12)
        self.assertEqual(report["service_status"]["state"], "loaded_exact")
        verbs = [c[4] for c in fake.calls if c[0].endswith("/glaeda")]
        self.assertEqual(verbs, ["plan", "plan", "apply", "status"])
        bad = Fake(status_state="configuration_only")
        with self.assertRaises(serve.Failed):
            self.make(bad).finish(scale_set_file=self.enroll_output(), apply=True, approve=bad.plan_identity)

    def test_refuses_before_install(self):
        fake = Fake()
        self.make(fake).execute(apply=True)
        with self.assertRaises(serve.Refused):
            self.make(fake).finish(scale_set_file=self.enroll_output(), apply=False, approve=None)


if __name__ == "__main__":
    unittest.main()
