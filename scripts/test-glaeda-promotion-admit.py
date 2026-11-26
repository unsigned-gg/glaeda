#!/usr/bin/env python3
"""Contract tests for scripts/glaeda-promotion-admit and the hook's PROMOTION_LEASE path.

Runs on Linux CI. The runner tree is a fake: its run.sh logs "Listening for Jobs" (unless told not to), records its
argv and environment facts, and on an assign file starts a fake bin/Runner.Worker that runs the lease runner's real
job-started and job-completed hooks. The controller is a local http.server thread, the Tailscale CLI a fake that
lists 127.0.0.1 as an Online peer, and gh a fake that fails the test if anything runs it.
"""

from __future__ import annotations

import contextlib
import datetime
import fcntl
import hashlib
import http.server
import importlib.machinery
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts" / "glaeda-promotion-admit"
HOOK = ROOT / "scripts" / "glaeda-cmux-runner-hook"
UNITS = 4
JIT = "eyJmYWtlIjoiaml0In0" + "A" * 40  # stands in for HQ's encoded JIT config; must never reach argv, disk or logs
SHA = "a" * 40


def load(name: str, path: Path):
    loader = importlib.machinery.SourceFileLoader(name, os.fspath(path))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module


admit = load("glaeda_promotion_admit", HELPER)

FAKE_RUN = """#!{python}
import hashlib, json, os, subprocess, sys, time, fcntl
from pathlib import Path
control = Path({control!r})
runner = Path(os.path.dirname(os.path.abspath(__file__)))
capacity = Path({capacity!r})
held = []
for unit in sorted(capacity.glob("unit-*")):
    fd = os.open(unit, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        held.append(False)
    except OSError:
        held.append(True)
    os.close(fd)
jit = os.environ.get("ACTIONS_RUNNER_INPUT_JITCONFIG", "")
(control / "listener-start.json").write_text(json.dumps({{
    "argv": sys.argv, "jit_sha": hashlib.sha256(jit.encode()).hexdigest(), "units_held": held,
    "hook_started": os.environ.get("ACTIONS_RUNNER_HOOK_JOB_STARTED")}}))
trace = runner / "_diag" / "Runner_20261101-000000-utc.log"
if not (control / "never-online").exists():
    if not (control / "no-diag").exists():  # the real listener writes its _diag trace from its first line
        trace.parent.mkdir(exist_ok=True)
        trace.write_text("[2026-11-01 00:00:00Z INFO Terminal] WRITE LINE: Listening for Jobs\\n")
    print("Connected to GitHub", flush=True)
    print("Current runner version: '2.338.0'", flush=True)
    print("Listening for Jobs", flush=True)
while True:
    for name in ("ack", "skip"):  # broker trace lines with no Runner.Worker and no job-start log yet
        if (control / name).exists():
            line = ("[2026-11-01 00:00:01Z INFO BrokerMessageListener] Acknowledging runner request 'req-1'."
                    if name == "ack" else "[2026-11-01 00:00:02Z INFO Runner] Skipping message Job.")
            with open(trace, "a") as out:
                out.write(line + "\\n")
            (control / name).rename(control / (name + ".taken"))
    if (control / "running").exists():
        print("2026-11-01 00:00:03Z: Running job: promote-nightly-next", flush=True)
        (control / "running").rename(control / "running.taken")
    job = control / "assign.json"
    if job.exists():
        spec = json.loads(job.read_text())
        job.rename(control / "assign.taken")
        env = {{**os.environ, **spec["env"]}}
        env.pop("ACTIONS_RUNNER_INPUT_JITCONFIG", None)
        worker = subprocess.Popen([os.fspath(runner / "bin" / "Runner.Worker")], env=env, cwd=os.fspath(runner))
        worker.wait()
        sys.exit(0)  # ephemeral: one job
    time.sleep(0.05)
"""

FAKE_WORKER = """#!{python}
import json, os, subprocess, sys, time
from pathlib import Path
control = Path({control!r})
spec = json.loads((control / "assign.taken").read_text())
(control / "worker.pid").write_text(str(os.getpid()))
time.sleep(spec.get("delay_hook", 0))
start = time.monotonic()
started = subprocess.run([os.environ["ACTIONS_RUNNER_HOOK_JOB_STARTED"]], capture_output=True, text=True)
(control / "started.json").write_text(json.dumps({{"rc": started.returncode, "out": started.stdout + started.stderr,
                                                   "seconds": time.monotonic() - start}}))
if started.returncode == 0:
    while not (control / "finish").exists():
        time.sleep(0.05)
completed = subprocess.run([os.environ["ACTIONS_RUNNER_HOOK_JOB_COMPLETED"]], capture_output=True, text=True)
(control / "completed.json").write_text(json.dumps({{"rc": completed.returncode, "out": completed.stdout}}))
while not (control / "worker-exit").exists():
    time.sleep(0.05)
"""

FAKE_GH = """#!/bin/sh
echo "$@" >> {log!r}
exit 1
"""

FAKE_TAILSCALE = """#!{python}
import json, os
from pathlib import Path
doc = json.loads(Path({doc!r}).read_text())
print(json.dumps(doc))
"""


def rfc(at: float) -> str:
    return datetime.datetime.fromtimestamp(at, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Controller(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        self.answer: tuple[int, dict | None, dict] = (200, {"status": "fleet"}, {})
        self.hits: list[str] = []
        super().__init__(("127.0.0.1", 0), Handler)


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        server: Controller = self.server  # type: ignore[assignment]
        server.hits.append(self.path)
        code, body, headers = server.answer
        data = json.dumps(body).encode() if body is not None else b"not json"
        self.send_response(code)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args) -> None:  # noqa: ANN002
        pass


class PromotionTest(unittest.TestCase):
    def setUp(self) -> None:
        base = os.environ.get("TMPDIR") or None
        self.tmp = tempfile.TemporaryDirectory(dir=base)
        self.dir = Path(os.path.realpath(self.tmp.name))
        self.home = self.dir / "home"
        self.fleet = self.dir / "fleet"
        self.capacity = self.fleet / "capacity"
        self.control = self.dir / "control"
        for path in (self.home, self.capacity, self.control):
            path.mkdir(parents=True)
        (self.fleet / "host.lock").touch()
        self.source = self.home / "actions-runner-glaeda"
        hooks = self.source / "glaeda-hooks"
        (self.source / "bin").mkdir(parents=True)
        hooks.mkdir()
        shutil.copy2(HOOK, hooks / "glaeda-cmux-runner-hook")
        shutil.copy2(ROOT / "scripts" / "glaeda_reservation.py", hooks / "glaeda_reservation.py")
        (hooks / "job-started.sh").write_text(
            f"#!/bin/bash\nexec {sys.executable} {hooks / 'glaeda-cmux-runner-hook'} job-started "
            f"--allowed-repo manaflow-ai/cmux --capacity-units {UNITS} --instance 0\n")
        for name, body in (("run.sh", FAKE_RUN), ("bin/Runner.Worker", FAKE_WORKER)):
            path = self.source / name
            path.write_text(body.format(python=sys.executable, control=os.fspath(self.control),
                                        capacity=os.fspath(self.capacity)))
            path.chmod(0o755)
        (self.source / ".runner").write_text("{}")  # the source runner's own registration: never copied
        (self.source / ".credentials").write_text("{}")
        bin_dir = self.dir / "bin"
        bin_dir.mkdir()
        self.gh_log = self.dir / "gh.log"
        (bin_dir / "gh").write_text(FAKE_GH.format(log=os.fspath(self.gh_log)))
        (bin_dir / "gh").chmod(0o755)
        self.tailnet = self.dir / "tailscale.json"
        self.set_tailnet("Running", True)
        (bin_dir / "tailscale").write_text(FAKE_TAILSCALE.format(python=sys.executable, doc=os.fspath(self.tailnet)))
        (bin_dir / "tailscale").chmod(0o755)
        hook_sha = hashlib.sha256(HOOK.read_bytes()).hexdigest()
        listed = self.dir / "raw" / "manaflow-ai/cmux" / SHA / "scripts/ci/glaeda-signing-hooks.txt"
        listed.parent.mkdir(parents=True)
        listed.write_text(f"{hook_sha} this hook\n")
        self.controller = Controller()
        threading.Thread(target=self.controller.serve_forever, daemon=True).start()
        self.env = {"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": os.fspath(self.home), "TMPDIR": os.fspath(self.dir),
                    "GLAEDA_FLEET_DIR": os.fspath(self.fleet), "GLAEDA_RUNNER_TELEMETRY": "0",
                    "GLAEDA_CANONICAL_ROOT_PARENT": "/nonexistent/glaeda-test-canonical-roots",
                    "GLAEDA_RUNNER_TOOLCHAIN_GATE": "0", "GLAEDA_PROMOTION_POLL_S": "0.3",
                    "GLAEDA_PROMOTION_TAILSCALE": os.fspath(bin_dir / "tailscale"),
                    "GLAEDA_SIGNING_HOOKS_URL": (self.dir / "raw").as_uri() + "/{repo}/{sha}/scripts/ci/glaeda-signing-hooks.txt"}
        self.state_root = self.home / ".local/state/glaeda/promotion"
        self.lease_id = "0123456789abcdef01234567"

    def tearDown(self) -> None:
        # end every supervisor and listener this test started (recorded pids under its own lease dirs only)
        for record in self.state_root.glob("*/supervisor.json") if self.state_root.is_dir() else ():
            with contextlib.suppress(Exception):
                os.kill(json.loads(record.read_text())["pid"], 9)
        for state in self.state_root.glob("*/state.json") if self.state_root.is_dir() else ():
            with contextlib.suppress(Exception):
                os.killpg(json.loads(state.read_text())["listener_pid"], 9)
        (self.control / "finish").touch()
        (self.control / "worker-exit").touch()
        for holder in (self.home / ".local/state/glaeda/cmux-runner").glob("host-lock-holder*.pid"):
            with contextlib.suppress(Exception):
                os.kill(int(holder.read_text()), 9)
        self.controller.shutdown()
        self.controller.server_close()
        self.assertFalse(self.gh_log.exists(), "gh was run: nothing here may call GitHub, let alone cancel a run")
        self.tmp.cleanup()

    # ---------------------------------------------------------------- helpers

    def set_tailnet(self, backend: str, online: bool, ip: str = "127.0.0.1") -> None:
        self.tailnet.write_text(json.dumps({"BackendState": backend, "Peer": {
            "nodekey:1": {"TailscaleIPs": [ip], "Online": online}}}))

    def request(self, **overrides) -> dict:
        now = time.time()
        lease = overrides.pop("lease_id", self.lease_id)
        doc = {"version": 1, "lease_id": lease, "repository": "manaflow-ai/cmux", "run_id": 4242, "run_attempt": 1,
               "source_sha": SHA, "job": "promote-nightly-next", "created_at": rfc(now - 1),
               "expires_at": rfc(now + 120), "runner_label": f"cmux-promotion-{lease}-promote-nightly-next",
               "runner_name": f"cmux-promotion-{lease}", "encoded_jit_config": JIT,
               "controller_url": f"http://127.0.0.1:{self.controller.server_address[1]}",
               "status_path": f"/v1/nightly-promotion-admission/{lease}"}
        doc.update(overrides)
        return doc

    def run_admit(self, request: dict, timeout: float = 90) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, os.fspath(HELPER), "--request-stdin"], input=json.dumps(request),
                              env=self.env, capture_output=True, text=True, timeout=timeout, check=False)

    def lease_dir(self) -> Path:
        return self.state_root / self.lease_id

    def state(self) -> dict:
        try:
            return json.loads((self.lease_dir() / "state.json").read_text())
        except (OSError, ValueError):
            return {}

    def units_held(self) -> list[bool]:
        held = []
        for unit in range(UNITS):
            fd = os.open(self.capacity / f"unit-{unit}", os.O_RDONLY | os.O_CREAT, 0o644)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                held.append(False)
            except OSError:
                held.append(True)
            finally:
                os.close(fd)
        return held

    def wait_for(self, what, timeout: float = 30.0, why: str = "") -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if what():
                return
            time.sleep(0.05)
        self.fail(f"timed out waiting: {why}; state {self.state()}; log "
                  f"{(self.lease_dir() / 'supervisor.log').read_text()[-1500:] if (self.lease_dir() / 'supervisor.log').exists() else ''}")

    def ready(self, **overrides) -> dict:
        result = self.run_admit(self.request(**overrides))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        return receipt

    def assign(self, **env) -> None:
        github_env = self.dir / "github-env"
        job_env = {"GITHUB_REPOSITORY": "manaflow-ai/cmux", "GITHUB_RUN_ID": "4242", "GITHUB_RUN_ATTEMPT": "1",
                   "GITHUB_SHA": SHA, "GITHUB_JOB": "promote-nightly-next",
                   "RUNNER_NAME": f"cmux-promotion-{self.lease_id}", "GITHUB_EVENT_NAME": "push",
                   "GITHUB_REF": "refs/heads/main", "GITHUB_ENV": os.fspath(github_env),
                   "GITHUB_WORKFLOW_REF": "manaflow-ai/cmux/.github/workflows/nightly.yml@refs/heads/main",
                   "GITHUB_EVENT_PATH": os.fspath(self.event())}
        delay = float(env.pop("DELAY_HOOK", 0))
        job_env.update(env)
        spec = {"env": job_env, "delay_hook": delay}
        (self.control / "assign.json").write_text(json.dumps(spec))

    def event(self) -> Path:
        path = self.dir / "push.json"
        path.write_text(json.dumps({"repository": {"full_name": "manaflow-ai/cmux", "fork": False},
                                    "ref": "refs/heads/main"}))
        return path

    def started(self) -> dict:
        self.wait_for(lambda: (self.control / "started.json").exists(), why="the job-started hook")
        return json.loads((self.control / "started.json").read_text())

    def listener_alive(self) -> bool:
        pid = self.state().get("listener_pid")
        if not pid:
            return False
        return any(pgid == pid for _, pgid, _ in admit.ps_table() or [])

    # ---------------------------------------------------------------- (1) the request

    def test_mismatched_identity_is_refused_and_nothing_is_claimed(self) -> None:
        lease = self.lease_id
        for overrides in ({"repository": "other/cmux"}, {"job": "build-sign-notarize-nightly"},
                          {"runner_label": "cmux-promotion-x-promote-nightly-next"},
                          {"runner_name": f"cmux-promotion-{lease}x"}, {"status_path": "/v1/other"},
                          {"lease_id": "XYZ"}, {"source_sha": "abc"}, {"run_id": 0}, {"run_attempt": True},
                          {"version": 2}, {"expires_at": rfc(time.time() - 5)}, {"created_at": "yesterday"},
                          {"created_at": rfc(time.time() + 3600)}, {"release_p12": "x"},
                          {"encoded_jit_config": "short"}):
            with self.subTest(overrides=list(overrides)):
                result = self.run_admit(self.request(**overrides))
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertNotIn(JIT, result.stderr)
        self.assertFalse(any(self.units_held()))
        self.assertFalse(self.lease_dir().exists())

    def test_validate_binds_labels_and_status_path_to_the_lease(self) -> None:
        request = self.request()
        self.assertIs(admit.validate(request, time.time()), request)
        self.assertNotIn("encoded_jit_config", admit.identity(request))
        self.assertEqual(admit.fingerprint(request), admit.fingerprint({**request, "encoded_jit_config": "B" * 40}))

    # ---------------------------------------------------------------- (2) and (3): claim first, receipt when online

    def test_claim_is_taken_before_the_listener_and_the_receipt_after_it_is_online(self) -> None:
        receipt = self.ready()
        lease = self.lease_id
        self.assertEqual(receipt, {
            "lease_id": lease, "repository": "manaflow-ai/cmux", "run_id": 4242, "run_attempt": 1, "source_sha": SHA,
            "ready_at": receipt["ready_at"], "runners": {"promote-nightly-next": f"cmux-promotion-{lease}-promote-nightly-next"},
            "exclusive_claim": lease, "runner_name": f"cmux-promotion-{lease}"})
        self.assertIsNotNone(admit.parse_time(receipt["ready_at"]))
        start = json.loads((self.control / "listener-start.json").read_text())
        self.assertEqual(start["units_held"], [True] * UNITS, "every unit was held before the listener started")
        for token in ("fast-0", "fast-1", "gui", "gateway-0", "gateway-1"):
            fd = os.open(self.capacity / f"{token}.token", os.O_RDONLY)
            with self.assertRaises(OSError, msg=token):
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.close(fd)
        # the helper exited (the ssh session ended); the detached supervisor and the holder keep everything
        self.assertEqual(self.units_held(), [True] * UNITS)
        self.assertEqual(self.state()["phase"], "ready")
        self.assertTrue(self.listener_alive())
        runner = self.lease_dir() / "runner"
        self.assertFalse((runner / ".runner").exists(), "the source runner's registration is never copied")
        self.assertFalse((runner / ".credentials").exists())

    def test_the_jit_config_never_reaches_argv_disk_or_logs(self) -> None:
        result = self.run_admit(self.request())
        self.assertEqual(result.returncode, 0, result.stderr)
        start = json.loads((self.control / "listener-start.json").read_text())
        self.assertEqual(start["jit_sha"], hashlib.sha256(JIT.encode()).hexdigest(), "the listener got it by env")
        self.assertNotIn(JIT, " ".join(start["argv"]))
        self.assertNotIn(JIT, result.stdout + result.stderr)
        for path in self.state_root.rglob("*"):
            if path.is_file():
                self.assertNotIn(JIT.encode(), path.read_bytes(), path)
        rows = admit.ps_table() or []
        self.assertFalse(any(JIT in command for _, _, command in rows))

    def test_a_repeat_request_returns_the_same_receipt_and_a_conflicting_one_is_refused(self) -> None:
        request = self.request()
        first = self.run_admit(request)
        self.assertEqual(first.returncode, 0, first.stderr)
        again = self.run_admit({**request, "encoded_jit_config": "B" * 40})  # a re-minted config: same lease
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(json.loads(again.stdout), json.loads(first.stdout))
        conflict = self.run_admit({**request, "run_attempt": 2})
        self.assertEqual(conflict.returncode, 3, conflict.stderr)
        self.assertEqual(conflict.stdout, "")
        self.assertIn("another request", conflict.stderr)
        self.assertEqual(self.units_held(), [True] * UNITS)

    def test_a_hook_the_signing_list_does_not_name_is_refused_before_draining(self) -> None:
        listed = self.dir / "raw" / "manaflow-ai/cmux" / SHA / "scripts/ci/glaeda-signing-hooks.txt"
        listed.write_text(f"{'0' * 64} another hook\n")
        result = self.run_admit(self.request())
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("does not list this hook", result.stderr)
        self.assertFalse(any(self.units_held()))
        self.assertFalse((self.control / "listener-start.json").exists())

    # ---------------------------------------------------------------- (4) handoff

    def test_the_matching_job_adopts_the_claim_at_once_with_no_gap(self) -> None:
        self.ready()
        holder = self.state()["holder_pid"]
        gaps: list[float] = []
        stop = threading.Event()

        def watch() -> None:  # the whole mini must read taken at every look through the handoff
            while not stop.is_set():
                fd = os.open(self.capacity / "unit-0", os.O_RDONLY)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    gaps.append(time.monotonic())
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    pass
                finally:
                    os.close(fd)
                time.sleep(0.005)

        watcher = threading.Thread(target=watch)
        watcher.start()
        try:
            self.assign()
            started = self.started()
        finally:
            stop.set()
            watcher.join()
        self.assertEqual(started["rc"], 0, started["out"])
        self.assertLess(started["seconds"], 15, "admission is immediate: no capacity wait, no drain")
        self.assertIn("adopted promotion lease", started["out"])
        self.assertIn("nothing taken or waited for", started["out"])
        self.assertNotIn("waiting", started["out"])
        self.assertNotIn("holding 4/4", started["out"], "no second take_capacity")
        self.assertEqual(gaps, [], "the claim was free for a moment during the handoff")
        env_lines = (self.dir / "github-env").read_text()
        self.assertIn("GLAEDA_JOB_CLAIM=exclusive\n", env_lines)
        self.assertIn(f"GLAEDA_HOOK_SHA256={hashlib.sha256(HOOK.read_bytes()).hexdigest()}\n", env_lines)
        self.assertEqual(self.state()["holder_pid"], holder, "the same holder keeps the locks")
        self.assertIsInstance(self.state()["adopted"], dict)
        self.assertFalse(list(self.capacity.glob("admit.want-*")), "the job never queued for capacity")

    def test_promotion_adopt_never_takes_capacity(self) -> None:
        """In process: adoption reads the lease and the holder, and never calls take_capacity or take_host_lock."""
        hook = load("glaeda_cmux_runner_hook_adopt", HOOK)
        lease_dir = self.dir / "lease"
        lease_dir.mkdir()
        state_dir = self.dir / "hook-state"
        state_dir.mkdir()
        request = self.request()
        (lease_dir / "lease.json").write_text(json.dumps(admit.identity(request)))
        holder = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            hook.holder_file(state_dir, request["runner_name"]).write_text(f"{holder.pid}\n")
            hook.promotion_write_state(lease_dir, {"phase": "ready", "holder_pid": holder.pid, "units": 4,
                                                   "units_total": 4})
            environ = {"GITHUB_REPOSITORY": "manaflow-ai/cmux", "GITHUB_RUN_ID": "4242", "GITHUB_RUN_ATTEMPT": "1",
                       "GITHUB_SHA": SHA, "GITHUB_JOB": "promote-nightly-next", "RUNNER_NAME": request["runner_name"],
                       "GITHUB_WORKFLOW_REF": "manaflow-ai/cmux/.github/workflows/nightly.yml@refs/heads/main"}
            owner = hook.JobOwner.of(os.getpid())
            with mock.patch.object(hook, "take_capacity", side_effect=AssertionError("a second claim")), \
                    mock.patch.object(hook, "take_host_lock", side_effect=AssertionError("a second claim")):
                start = time.monotonic()
                ok, note, taken = hook.promotion_adopt(lease_dir, owner, state_dir, environ)
            self.assertTrue(ok, note)
            self.assertLess(time.monotonic() - start, 2)
            self.assertEqual(taken, {"class": "exclusive", "units": 4, "units_total": 4})
            for name, value in (("GITHUB_RUN_ATTEMPT", "2"), ("GITHUB_SHA", "b" * 40), ("GITHUB_JOB", "other"),
                                ("GITHUB_REPOSITORY", "x/y"), ("RUNNER_NAME", "else"),
                                ("GITHUB_WORKFLOW_REF", "manaflow-ai/cmux/.github/workflows/ci.yml@refs/heads/main")):
                with self.subTest(name=name):
                    self.assertIsNotNone(hook.promotion_identity_refusal(admit.identity(request),
                                                                         {**environ, name: value}))
            hook.promotion_write_state(lease_dir, {"phase": "withdrawn", "holder_pid": holder.pid, "units": 4,
                                                   "units_total": 4})
            self.assertFalse(hook.promotion_adopt(lease_dir, owner, state_dir, environ)[0], "no claim to adopt")
        finally:
            holder.kill()
            holder.wait()

    def test_a_job_that_does_not_match_the_lease_is_refused_before_any_step(self) -> None:
        self.ready()
        self.assign(GITHUB_RUN_ATTEMPT="2")
        started = self.started()
        self.assertNotEqual(started["rc"], 0)
        self.assertIn("GITHUB_RUN_ATTEMPT", started["out"])
        self.assertNotIn("GLAEDA_JOB_CLAIM", (self.dir / "github-env").read_text()
                         if (self.dir / "github-env").exists() else "")
        self.assertNotIn("adopted", self.state())
        self.assertEqual(self.units_held(), [True] * UNITS, "the claim stays with the lease")

    # ---------------------------------------------------------------- release after the Worker ends

    def test_the_claim_is_released_only_after_the_adopted_worker_ends(self) -> None:
        self.ready()
        self.assign()
        self.assertEqual(self.started()["rc"], 0)
        (self.control / "finish").touch()
        self.wait_for(lambda: (self.control / "completed.json").exists(), why="job-completed")
        completed = json.loads((self.control / "completed.json").read_text())
        self.assertIn("keeps its exclusive claim until this job's Runner.Worker ends", completed["out"])
        time.sleep(1.5)
        self.assertEqual(self.units_held(), [True] * UNITS, "job-completed alone never releases")
        (self.control / "worker-exit").touch()
        self.wait_for(lambda: self.state().get("phase") == "released", why="release after the Worker")
        self.wait_for(lambda: not any(self.units_held()), why="units free")
        # cleanup() removes the runner dir after the units free and the state is written: wait for it, never race it
        self.wait_for(lambda: not (self.lease_dir() / "runner").exists(), why="runner dir removed")

    # ---------------------------------------------------------------- (5) status after the receipt

    def test_overflow_withdraws_an_unassigned_listener(self) -> None:
        """Proven unassigned: its _diag trace is read and shows no acknowledged request, no Worker, no Running job."""
        self.ready()
        self.controller.answer = (200, {"status": "overflow"}, {})
        self.wait_for(lambda: self.state().get("phase") == "withdrawn", why="overflow withdrawal")
        self.wait_for(lambda: not any(self.units_held()), why="units free")
        self.assertFalse(self.listener_alive())
        self.assertNotIn("assigned", self.state())

    def keeps_listener_and_claim(self, answer: tuple) -> None:
        self.controller.answer = answer
        hits = len(self.controller.hits)
        self.wait_for(lambda: len(self.controller.hits) >= hits + 3, why="status reads")
        self.assertEqual(self.state()["phase"], "ready", answer)
        self.assertEqual(self.units_held(), [True] * UNITS, answer)
        self.assertTrue(self.listener_alive(), answer)

    def test_an_assigned_listener_with_no_job_start_log_keeps_its_claim(self) -> None:
        """glaeda#1782: the listener acknowledged the job but no Runner.Worker, Running job or job-started hook has
        followed yet. overflow and parent_completed both keep it, and the persisted marker keeps it even once the
        trace no longer shows an open request."""
        self.ready()
        (self.control / "ack").touch()
        self.wait_for(lambda: (self.control / "ack.taken").exists(), why="the acknowledge")
        self.keeps_listener_and_claim((200, {"status": "overflow"}, {}))
        assigned = self.state().get("assigned")
        self.assertIsInstance(assigned, dict, "the assignment is persisted before the decision")
        self.assertIn("req-1", assigned["evidence"])
        (self.control / "skip").touch()  # the trace closes the request: the marker still holds
        self.wait_for(lambda: (self.control / "skip.taken").exists(), why="the skip line")
        self.keeps_listener_and_claim((200, {"status": "fleet", "parent_completed": True}, {}))
        self.assertEqual(self.state()["assigned"], assigned)

    def test_a_running_job_line_alone_keeps_the_claim(self) -> None:
        self.ready()
        (self.control / "running").touch()
        self.wait_for(lambda: (self.control / "running.taken").exists(), why="the Running job line")
        self.keeps_listener_and_claim((200, {"status": "overflow"}, {}))
        self.assertIn("Running job", self.state()["assigned"]["evidence"])

    def test_an_unreadable_assignment_state_keeps_the_listener(self) -> None:
        """Fail closed: a listener with a session but no readable _diag trace is never withdrawn, and nothing is
        marked assigned (an unknown is not a job)."""
        (self.control / "no-diag").touch()
        self.ready()
        self.keeps_listener_and_claim((200, {"status": "overflow"}, {}))
        self.keeps_listener_and_claim((200, {"status": "fleet", "parent_completed": True}, {}))
        self.assertNotIn("assigned", self.state())
        log = (self.lease_dir() / "supervisor.log").read_text()
        self.assertIn("cannot prove the listener unassigned", log)

    def test_fleet_keeps_an_unassigned_listener_and_its_claim(self) -> None:
        self.ready()
        self.controller.answer = (200, {"status": "fleet", "parent_completed": False}, {})
        self.wait_for(lambda: len(self.controller.hits) >= 3, why="status reads")
        self.assertEqual(self.state()["phase"], "ready")
        self.assertEqual(self.units_held(), [True] * UNITS)
        self.assertTrue(self.listener_alive())
        self.assertTrue(all(hit == f"/v1/nightly-promotion-admission/{self.lease_id}" for hit in self.controller.hits))

    def test_fleet_with_the_parent_completed_withdraws_an_unassigned_listener(self) -> None:
        self.ready()
        self.controller.answer = (200, {"status": "fleet", "parent_completed": True}, {})
        self.wait_for(lambda: self.state().get("phase") == "withdrawn", why="parent_completed withdrawal")
        self.wait_for(lambda: not any(self.units_held()), why="units free")
        self.assertFalse(self.listener_alive())
        self.assertNotIn("assigned", self.state())

    def test_an_unreachable_controller_keeps_the_claim(self) -> None:
        self.ready()
        for answer in ((500, {"status": "overflow"}, {}), (200, None, {}), (200, {"status": "maybe"}, {})):
            self.controller.answer = answer
            hits = len(self.controller.hits)
            self.wait_for(lambda: len(self.controller.hits) >= hits + 2, why="status reads")
            self.assertEqual(self.state()["phase"], "ready", answer)
            self.assertEqual(self.units_held(), [True] * UNITS)
        self.controller.shutdown()  # nothing answers at all
        time.sleep(1.5)
        self.assertEqual(self.state()["phase"], "ready")
        self.assertEqual(self.units_held(), [True] * UNITS)

    def test_a_runner_worker_keeps_the_claim_whatever_the_status_until_it_ends(self) -> None:
        self.ready()
        self.assign(DELAY_HOOK="3")  # a Worker that has not adopted yet
        self.wait_for(lambda: (self.control / "worker.pid").exists(), why="the Worker")
        self.controller.answer = (200, {"status": "overflow"}, {})
        time.sleep(2)
        self.assertEqual(self.state()["phase"], "ready", "a Worker keeps the claim")
        self.assertEqual(self.units_held(), [True] * UNITS)
        self.assertEqual(self.started()["rc"], 0, "and its job adopts it")
        time.sleep(1)
        self.assertEqual(self.units_held(), [True] * UNITS)
        (self.control / "finish").touch()
        (self.control / "worker-exit").touch()
        self.wait_for(lambda: self.state().get("phase") == "released", why="release after the Worker")

    def test_the_deadline_never_releases_an_accepted_receipt(self) -> None:
        self.ready(expires_at=rfc(time.time() + 3))
        self.controller.answer = (200, {"status": "fleet"}, {})
        time.sleep(5)
        self.assertEqual(self.state()["phase"], "ready")
        self.assertEqual(self.units_held(), [True] * UNITS)
        self.assertTrue(self.listener_alive())

    def test_before_a_receipt_expiry_withdraws_only_its_own_listener(self) -> None:
        (self.control / "never-online").touch()
        bystander = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
        try:
            result = self.run_admit(self.request(expires_at=rfc(time.time() + 4)), timeout=120)
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertEqual(result.stdout, "", "no receipt without readiness")
            self.wait_for(lambda: self.state().get("phase") == "expired", why="pre-receipt expiry")
            self.assertFalse((self.lease_dir() / "receipt.json").exists())
            self.wait_for(lambda: not self.listener_alive(), why="its listener stopped")
            self.assertFalse(any(self.units_held()))
            self.assertIsNone(bystander.poll(), "nothing but its own listener is stopped")
            self.assertEqual(self.controller.hits, [], "no status read before a receipt")
        finally:
            bystander.kill()
            bystander.wait()

    # ---------------------------------------------------------------- a supervisor or listener that dies

    def supervisor_pid(self, lease_dir: Path | None = None) -> int:
        return json.loads(((lease_dir or self.lease_dir()) / "supervisor.json").read_text())["pid"]

    def test_an_adopted_job_keeps_the_claim_when_the_supervisor_dies(self) -> None:
        """The holder follows the adopted Runner.Worker too (keep_while), not only the supervisor it was taken for."""
        self.ready()
        self.assign()
        self.assertEqual(self.started()["rc"], 0)
        os.kill(self.supervisor_pid(), 9)
        time.sleep(5)  # past two holder looks (HOLDER_POLL_S)
        self.assertEqual(self.units_held(), [True] * UNITS, "the job still holds the whole mini")
        (self.control / "finish").touch()
        (self.control / "worker-exit").touch()
        self.wait_for(lambda: not any(self.units_held()), why="the claim follows the Worker out")

    def test_a_listener_that_exits_while_ready_with_no_job_lets_the_claim_go(self) -> None:
        self.ready()
        os.killpg(self.state()["listener_pid"], 9)
        self.wait_for(lambda: self.state().get("phase") == "failed", why="dead listener withdrawal")
        self.assertIn("listener exited", self.state()["reason"])
        self.wait_for(lambda: not any(self.units_held()), why="units free")
        # cleanup() removes the runner dir after the units free and the state is written: wait for it, never race it
        self.wait_for(lambda: not (self.lease_dir() / "runner").exists(), why="runner dir removed")

    def test_only_the_recorded_supervisor_writes_the_lease_state(self) -> None:
        request = self.request()
        self.ready()
        before = (self.lease_dir() / "state.json").read_bytes()
        second = subprocess.run([sys.executable, os.fspath(HELPER), "--supervise", os.fspath(self.lease_dir())],
                                input=json.dumps(request), env=self.env, capture_output=True, text=True, timeout=60,
                                check=False)
        self.assertEqual(second.returncode, 1, second.stdout + second.stderr)
        self.assertIn("not this lease's supervisor", second.stdout + second.stderr)
        self.assertEqual((self.lease_dir() / "state.json").read_bytes(), before)
        self.assertEqual(self.units_held(), [True] * UNITS)
        # a live pid whose start token was not readable when recorded counts as alive, never as a dead supervisor
        record = self.dir / "record"
        record.mkdir()
        (record / "supervisor.json").write_text(json.dumps({"pid": os.getpid(), "identity": None}))
        self.assertTrue(admit.supervisor_alive(record))
        gone = subprocess.Popen([sys.executable, "-c", "pass"])
        gone.wait()
        (record / "supervisor.json").write_text(json.dumps({"pid": gone.pid, "identity": None}))
        self.assertFalse(admit.supervisor_alive(record))

    def test_a_failed_withdrawal_at_start_never_crashes_the_supervisor(self) -> None:
        """run()'s start-failure path: a withdraw() that times out on the lease lock ends the supervisor cleanly (its
        holder then lets the claim go with it, and the next request recovers the listener)."""
        lease_dir = self.dir / "fake-lease"
        lease_dir.mkdir()
        (lease_dir / "supervisor.json").write_text(json.dumps({"pid": os.getpid(), "identity": None}))
        (lease_dir / "state.json").write_text(json.dumps({"phase": "claiming"}))
        hook = load("glaeda_cmux_runner_hook_start_failure", HOOK)
        supervisor = object.__new__(admit.Supervisor)
        supervisor.lease = admit.Lease(lease_dir.parent, lease_dir.name)
        supervisor.lease_id = self.lease_id
        supervisor.hook = hook
        supervisor.listener = None
        said: list[str] = []
        supervisor.say = said.append

        def start() -> None:
            supervisor.listener = mock.Mock(pid=1)
            raise OSError("run.sh would not start")

        def withdraw(*_args, **_kwargs) -> bool:
            raise TimeoutError("the lease lock")

        supervisor.claim = lambda: True
        supervisor.prepare_runner = lambda: None
        supervisor.start_listener = start
        supervisor.withdraw = withdraw
        self.assertEqual(supervisor.run(), 1)
        self.assertTrue(any("could not withdraw (TimeoutError)" in line for line in said), said)

    def test_a_dead_supervisors_listener_and_runner_tree_are_recovered(self) -> None:
        self.ready()
        first = self.lease_dir()
        listener = self.state()["listener_pid"]
        (first / "runner" / ".credentials").write_text("{}")  # what the JIT listener decodes there
        os.kill(self.supervisor_pid(), 9)
        time.sleep(5)  # past two holder looks: no unclaimed window before recovery's frozen proof
        self.assertEqual(self.units_held(), [True] * UNITS)
        self.assertTrue(self.listener_alive(), "its listener is left behind until a request recovers it")
        other = "fedcba9876543210fedcba98"
        result = self.run_admit(self.request(lease_id=other))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads((first / "state.json").read_text())["phase"], "failed")
        self.assertFalse(any(pgid == listener for _, pgid, _ in admit.ps_table() or []), "the listener is stopped")
        self.assertFalse((first / "runner").exists(), "the runner tree, .credentials included, is removed")
        self.assertEqual(self.units_held(), [True] * UNITS, "the next lease holds the mini")

    def test_recovery_preserves_acknowledged_listener_and_claim_before_adoption(self) -> None:
        """#1788: supervisor dies after an acknowledgement persisted, before a Worker or job-start hook exists."""
        self.ready()
        (self.control / "ack").touch()
        self.controller.answer = (200, {"status": "overflow"}, {})
        self.wait_for(lambda: isinstance(self.state().get("assigned"), dict), why="sticky assigned marker")
        assigned = self.state()["assigned"]
        (self.control / "skip").touch()
        self.wait_for(lambda: (self.control / "skip.taken").exists(), why="trace closed; sticky marker is authority")
        os.kill(self.supervisor_pid(), 9)
        time.sleep(5)
        self.assertEqual(self.units_held(), [True] * UNITS, "claim survives the pre-adoption supervisor crash")
        hook = load("glaeda_cmux_runner_hook_assigned_recovery", HOOK)
        lease = admit.Lease(self.state_root, self.lease_id)
        # A previous recovery could crash after persisting assigned but before its finally resumes the group.
        os.killpg(self.state()["listener_pid"], signal.SIGSTOP)
        with mock.patch.dict(os.environ, self.env):
            note = admit.recover(lease, hook)
        self.assertIn("assigned", note)
        self.assertTrue(self.listener_alive())
        self.assertEqual(self.state()["assigned"], assigned)
        self.assertEqual(self.units_held(), [True] * UNITS)
        self.assign()
        self.assertEqual(self.started()["rc"], 0, "the acknowledged job can still adopt its uninterrupted claim")
        (self.control / "finish").touch()
        (self.control / "worker-exit").touch()
        self.wait_for(lambda: not self.listener_alive(), why="the ephemeral listener exits after its job")
        self.wait_for(lambda: not any(self.units_held()), why="claim follows the job and listener out")
        with mock.patch.dict(os.environ, self.env):
            self.assertIn("recovered", admit.recover(lease, hook))
        self.assertFalse(lease.runner_dir.exists())

    def test_recovery_claiming_holder_releases_after_supervisor_crash(self) -> None:
        """The holder exists before claiming becomes starting; no listener can own it in that interval."""
        lease = admit.Lease(self.state_root, self.lease_id)
        lease.dir.mkdir(parents=True)
        (lease.dir / "state.json").write_text(json.dumps({"phase": "claiming"}))
        hook = load("glaeda_cmux_runner_hook_claiming_recovery", HOOK)
        supervisor = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                      stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL)
        try:
            owner = admit.promotion_claim_owner(hook, lease, hook.JobOwner.of(supervisor.pid))
            fds = []
            for unit in range(UNITS):
                fd = os.open(self.capacity / f"unit-{unit}", os.O_RDONLY | os.O_CREAT, 0o600)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fds.append(fd)
            state_dir = self.home / ".local/state/glaeda/cmux-runner"
            held, note = hook.hold(fds, owner, state_dir, keep_while=lease.dir)
            self.assertTrue(held, note)
            self.assertEqual(self.units_held(), [True] * UNITS)
            supervisor.kill()
            supervisor.wait(timeout=5)
            self.wait_for(lambda: not any(self.units_held()), timeout=8,
                          why="claiming holder follows its supervisor out before a listener starts")
        finally:
            if supervisor.poll() is None:
                supervisor.kill()
            supervisor.wait(timeout=5)

    def test_recovery_rechecks_assignment_when_supervisor_dies_before_marker(self) -> None:
        self.ready()
        os.kill(self.supervisor_pid(), 9)
        (self.control / "ack").touch()
        self.wait_for(lambda: (self.control / "ack.taken").exists(), why="acknowledgement after supervisor crash")
        self.assertNotIn("assigned", self.state())
        hook = load("glaeda_cmux_runner_hook_unmarked_recovery", HOOK)
        lease = admit.Lease(self.state_root, self.lease_id)
        with mock.patch.dict(os.environ, self.env):
            self.assertIn("assigned", admit.recover(lease, hook))
        self.assertIsInstance(self.state().get("assigned"), dict)
        self.assertTrue(self.listener_alive())
        time.sleep(5)
        self.assertEqual(self.units_held(), [True] * UNITS)
        # A retained frozen listener was resumed; it can still take its exact job.
        self.assign()
        self.assertEqual(self.started()["rc"], 0)

    def test_recovery_unknown_assignment_keeps_listener_and_claim(self) -> None:
        (self.control / "no-diag").touch()
        self.ready()
        os.kill(self.supervisor_pid(), 9)
        hook = load("glaeda_cmux_runner_hook_unknown_recovery", HOOK)
        lease = admit.Lease(self.state_root, self.lease_id)
        with mock.patch.dict(os.environ, self.env):
            self.assertIn("unknown", admit.recover(lease, hook))
        time.sleep(5)
        self.assertTrue(self.listener_alive())
        self.assertEqual(self.units_held(), [True] * UNITS)
        self.assertTrue(lease.runner_dir.exists())
        with mock.patch.object(admit, "ps_table", return_value=None):
            self.assertIn("unknown", admit.recover(lease, hook))
            owner = admit.promotion_claim_owner(hook, lease, hook.JobOwner(999999999, "gone"))
            self.assertFalse(owner.gone(), "an unreadable process table is not proof to free the mini")
        state_path = lease.dir / "state.json"
        saved_state = state_path.read_text()
        try:
            state_path.write_text("{")
            self.assertIn("unknown", admit.recover(lease, hook))
            self.assertFalse(owner.gone(), "unreadable lease state keeps the claim too")
        finally:
            state_path.write_text(saved_state)

    def test_recovery_process_identity_rejects_reused_pid_and_keeps_unknown(self) -> None:
        runner = self.lease_dir() / "runner"
        rows = [(1234, 1234, str(runner / "run.sh")), (1235, 1234, "./bin/Runner.Listener")]
        with mock.patch.object(admit, "ps_table", return_value=rows):
            with mock.patch.object(admit, "process_identity", return_value="proc:old"):
                self.assertEqual(admit.listener_group(runner, 1234, "proc:old"), [1234, 1235])
            with mock.patch.object(admit, "process_identity", return_value="proc:new"):
                self.assertEqual(admit.listener_group(runner, 1234, "proc:old"), [])
            with mock.patch.object(admit, "process_identity", return_value=None):
                self.assertIsNone(admit.listener_group(runner, 1234, "proc:old"))
        with mock.patch.object(admit, "process_identity", return_value=None):
            with mock.patch.object(admit, "ps_table", return_value=[(1235, 1234, "./bin/Runner.Listener")]):
                self.assertIsNone(admit.listener_group(runner, 1234, "proc:old"))
            with mock.patch.object(admit, "ps_table", return_value=[]):
                self.assertEqual(admit.listener_group(runner, 1234, "proc:old"), [])

    def test_recovery_never_releases_before_exit_is_observed(self) -> None:
        lease = admit.Lease(self.state_root, self.lease_id)
        lease.dir.mkdir(parents=True)
        lease.runner_dir.mkdir()
        (lease.dir / "state.json").write_text(json.dumps({"phase": "ready", "listener_pid": 1234,
                                                        "listener_identity": "proc:old"}))
        hook = mock.Mock()
        hook.promotion_locked.return_value = contextlib.nullcontext()
        hook.promotion_adopted_alive.return_value = False
        for observed in ([1234], None):
            with mock.patch.object(admit, "listener_group", side_effect=[[1234], observed]), \
                    mock.patch.object(admit.Supervisor, "assignment", return_value=("unassigned", "test")), \
                    mock.patch.object(admit.os, "kill"), mock.patch.object(admit.os, "killpg"), \
                    mock.patch.object(admit, "STOP_WAIT_S", 0):
                self.assertIn("not yet proven", admit.recover(lease, hook))
            hook.release_holder.assert_not_called()
            hook.promotion_write_state.assert_not_called()
            self.assertTrue(lease.runner_dir.exists())

    def test_a_half_made_lease_dir_is_made_again_and_stale_temp_dirs_pruned(self) -> None:
        self.state_root.mkdir(parents=True)
        self.lease_dir().mkdir()
        (self.lease_dir() / "state.json").write_text(json.dumps({"phase": "claiming"}))  # no lease.json: half made
        stale = self.state_root / f".{self.lease_id}.tmp-1"
        stale.mkdir()
        os.utime(stale, (time.time() - 3600, time.time() - 3600))
        self.ready()
        self.assertEqual(self.state()["phase"], "ready")
        self.assertTrue((self.lease_dir() / "lease.json").exists())
        self.assertFalse(stale.exists())
        self.assertEqual(sorted(path.name for path in self.state_root.iterdir() if path.name.startswith(".")), [])

    def test_a_lease_runners_work_tree_counts_for_the_stray_check(self) -> None:
        hook = load("glaeda_cmux_runner_hook_globs", HOOK)
        work = self.home / ".local/state/glaeda/promotion" / self.lease_id / "runner/_work"
        work.mkdir(parents=True)
        self.assertIn(work, [path for pattern in hook.RUNNER_WORK_GLOBS for path in self.home.glob(pattern)])

    # ---------------------------------------------------------------- controller authority

    def status(self, url: str | None = None, now: float | None = None) -> dict:
        return admit.controller_status(url or f"http://127.0.0.1:{self.controller.server_address[1]}",
                                       f"/v1/nightly-promotion-admission/{self.lease_id}", self.lease_id, now)

    def test_authority_allows_only_https_or_an_online_tailnet_address(self) -> None:
        with mock.patch.dict(os.environ, {"GLAEDA_PROMOTION_TAILSCALE": self.env["GLAEDA_PROMOTION_TAILSCALE"]}):
            self.controller.answer = (200, {"status": "overflow"}, {})
            self.assertEqual(self.status()["status"], "overflow")
            port = self.controller.server_address[1]
            for url, why in ((f"http://localhost:{port}", "numeric"), (f"ftp://127.0.0.1:{port}", "scheme"),
                             (f"http://user@127.0.0.1:{port}", "user info")):
                with self.subTest(url=url):
                    self.assertIn(why, admit.verify_authority(url) or "")
                    self.assertEqual(self.status(url)["status"], "unreachable")
            self.set_tailnet("Running", True, ip="100.64.0.9")  # 127.0.0.1 is not a tailnet peer now
            self.assertIn("not a tailnet peer", admit.verify_authority(f"http://127.0.0.1:{port}"))
            self.assertEqual(self.status()["status"], "unreachable")
            self.set_tailnet("Running", False)
            self.assertIn("not Online", admit.verify_authority(f"http://127.0.0.1:{port}"))
            self.set_tailnet("Stopped", True)
            self.assertIn("not Running", admit.verify_authority(f"http://127.0.0.1:{port}"))
            self.assertIsNone(admit.verify_authority("https://controller.example.test"))

    def test_a_request_with_a_non_tailnet_controller_is_refused_before_any_claim(self) -> None:
        self.set_tailnet("Running", True, ip="100.64.0.9")
        result = self.run_admit(self.request())
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("not a tailnet peer", result.stderr)
        self.assertFalse(any(self.units_held()))
        result = self.run_admit(self.request(controller_url=f"http://localhost:{self.controller.server_address[1]}"))
        self.assertEqual(result.returncode, 2, result.stderr)

    def test_a_redirect_is_never_followed(self) -> None:
        with mock.patch.dict(os.environ, {"GLAEDA_PROMOTION_TAILSCALE": self.env["GLAEDA_PROMOTION_TAILSCALE"]}):
            self.controller.answer = (302, {"status": "overflow"}, {"Location": "http://127.0.0.1:1/elsewhere"})
            answer = self.status()
            self.assertEqual(answer["status"], "unreachable")
            self.assertIn("302", answer["why"])
            self.assertEqual(len(self.controller.hits), 1)

    def test_proxy_environment_is_ignored(self) -> None:
        proxy = Controller()
        proxy.answer = (200, {"status": "overflow"}, {})
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        try:
            proxied = f"http://127.0.0.1:{proxy.server_address[1]}"
            with mock.patch.dict(os.environ, {"GLAEDA_PROMOTION_TAILSCALE": self.env["GLAEDA_PROMOTION_TAILSCALE"],
                                              "HTTP_PROXY": proxied, "http_proxy": proxied, "HTTPS_PROXY": proxied,
                                              "https_proxy": proxied, "ALL_PROXY": proxied, "NO_PROXY": ""}):
                self.controller.answer = (200, {"status": "fleet"}, {})
                self.assertEqual(self.status()["status"], "fleet")
            self.assertEqual(proxy.hits, [], "the proxy was used")
            self.assertEqual(len(self.controller.hits), 1)
        finally:
            proxy.shutdown()
            proxy.server_close()

    def test_a_future_dated_reply_is_unreachable(self) -> None:
        with mock.patch.dict(os.environ, {"GLAEDA_PROMOTION_TAILSCALE": self.env["GLAEDA_PROMOTION_TAILSCALE"]}):
            now = time.time()
            self.controller.answer = (200, {"status": "overflow", "decided_at": rfc(now + 3600)}, {})
            answer = self.status(now=now)
            self.assertEqual(answer["status"], "unreachable")
            self.assertIn("future", answer["why"])
            self.controller.answer = (200, {"status": "overflow", "decided_at": rfc(now - 5)}, {})
            self.assertEqual(self.status(now=now)["status"], "overflow")
            self.controller.answer = (200, {"status": "overflow", "lease_id": "f" * 24}, {})
            self.assertEqual(self.status(now=now)["status"], "unreachable", "another lease's answer")

    def test_a_future_dated_overflow_keeps_the_claim_end_to_end(self) -> None:
        self.ready()
        self.controller.answer = (200, {"status": "overflow", "decided_at": rfc(time.time() + 3600)}, {})
        self.wait_for(lambda: len(self.controller.hits) >= 3, why="status reads")
        self.assertEqual(self.state()["phase"], "ready")
        self.assertEqual(self.units_held(), [True] * UNITS)

    # ---------------------------------------------------------------- never cancels

    def test_the_helper_has_no_github_cancel_path(self) -> None:
        text = HELPER.read_text()
        for needle in ("/cancel", "force-cancel", "run cancel", "api.github.com", '"gh"', "'gh'"):
            self.assertNotIn(needle, text)

    def test_no_em_dashes(self) -> None:
        for path in (HELPER, Path(__file__)):
            self.assertNotIn(chr(0x2014), path.read_text(encoding="utf-8"), path)


if __name__ == "__main__":
    unittest.main()
