#!/usr/bin/env python3
"""Contract tests for glaeda-linux-vm-rollout with faked step commands; no network, no root, no VM."""
from __future__ import annotations

import importlib.machinery
import importlib.util
import json
import pathlib
import stat
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
LOADER = importlib.machinery.SourceFileLoader("glaeda_linux_vm_rollout", str(ROOT / "scripts" / "glaeda-linux-vm-rollout"))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
assert SPEC
rollout = importlib.util.module_from_spec(SPEC)
sys.modules[LOADER.name] = rollout
LOADER.exec_module(rollout)

WORKFLOW = "manaflow-ai/cmux/.github/workflows/iroh-v2-production-drift.yml@refs/heads/main"
HOST = "mini-a"
SET = "glaeda-linux-arm64-mini-a"
INSTALLED_OK = {"dirs_ok": True, "program": {"matches_stage": True, "contract_ok": True},
                "bridge": {"matches_stage": True, "contract_ok": True}}
INSTALLED_NO = {"dirs_ok": False, "program": {"matches_stage": False, "contract_ok": False},
                "bridge": {"matches_stage": False, "contract_ok": False}}
SUDO = ("fleet-sudo <host> /repo/scripts/glaeda-linux-vm-serve install-root --from /h/.glaeda/linux-vm "
        "--glaeda-sha256 aa --bridge-sha256 bb --apply")


class Fake:
    """Scripted step documents keyed by step name; records every argv."""

    def __init__(self, **docs):
        self.docs = {
            "base": (0, {"disposition": "unchanged", "plan": [], "receipt": {"state": "ready_stopped"}}),
            "stage": (0, {"disposition": "unchanged", "plan": [], "stage": {"glaeda": "aa"},
                          "installed": INSTALLED_OK, "next": []}),
            "enroll": (0, {"mode": "apply", "runner_group": {"action": "exists", "name": "cmux-fleet-linux",
                                                             "selected_workflows": [WORKFLOW]},
                           "scale_set": {"action": "create"},
                           "enrollment": {"scale_set_id": 7, "scale_set_name": SET, "runner_group_id": 3,
                                          "labels": ["glaeda-linux-arm64"], "owner": "manaflow-ai",
                                          "repository": "cmux"}}),
            "finish": (0, {"disposition": "plan", "plan_identity": "pid-1", "enrollment": "absent"}),
            "finish_apply": (0, {"disposition": "changed", "service_status": {"state": "running"}}),
        }
        self.docs.update(docs)
        self.calls: list[tuple[str, list[str]]] = []

    def __call__(self, step, argv):
        self.calls.append((step, list(argv)))
        code, doc = self.docs[step]
        return code, json.loads(json.dumps(doc))

    def steps(self):
        return [step for step, _ in self.calls]


class RolloutTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.tmp.name)
        self.root = self.home / ".glaeda" / "linux-vm"
        (self.root / "serve" / "stage").mkdir(parents=True)
        self.token = self.home / ".config" / "glaeda" / "scaleset-token.json"
        self.token.parent.mkdir(parents=True)
        self.token.write_text("{}")

    def tearDown(self):
        self.tmp.cleanup()

    def make(self, fake, *, apply=False, approve=None, host=HOST, workflows=(WORKFLOW,)):
        r = rollout.Rollout(host=host, workflows=list(workflows), root=self.root, home=self.home,
                            scripts=pathlib.Path("/repo/scripts"), apply=apply, approve=approve)
        r.run = fake
        return r

    def test_scale_set_is_named_per_host_with_the_shared_label(self):
        fake = Fake()
        out = self.make(fake, apply=True).execute()
        self.assertEqual(out["scale_set"], SET)
        self.assertEqual(out["max_capacity"], 1)
        enroll = dict(fake.calls)["enroll"]
        self.assertEqual(enroll[enroll.index("--scale-set") + 1], SET)
        self.assertEqual(enroll[enroll.index("--label") + 1], "glaeda-linux-arm64")
        self.assertEqual(enroll[enroll.index("--workflow") + 1], WORKFLOW)
        self.assertIn("--apply", enroll)

    def test_plan_stops_at_base_with_pending_work_and_runs_nothing_else(self):
        fake = Fake(base=(0, {"disposition": "plan", "plan": ["install_lima"], "receipt": {}}))
        out = self.make(fake).execute()
        self.assertEqual(out["stopped_at"], "base")
        self.assertEqual(fake.steps(), ["base"])
        self.assertNotIn("--apply", fake.calls[0][1])
        self.assertTrue(out["next"][0].endswith("--apply"))

    def test_plan_stops_at_stage_when_stage_has_work(self):
        fake = Fake(stage=(0, {"disposition": "plan", "plan": ["build_stage"], "installed": INSTALLED_NO}))
        out = self.make(fake).execute()
        self.assertEqual(out["stopped_at"], "stage")
        self.assertEqual(fake.steps(), ["base", "stage"])

    def test_apply_stops_at_install_root_with_the_host_named_fleet_sudo_command(self):
        fake = Fake(stage=(0, {"disposition": "changed", "plan": [], "stage": {"glaeda": "aa"},
                               "installed": INSTALLED_NO, "next": [SUDO, "x"]}))
        out = self.make(fake, apply=True).execute()
        self.assertEqual(out["stopped_at"], "install_root")
        self.assertEqual(out["needs"], "root")
        self.assertTrue(out["next"][0].startswith(f"fleet-sudo {HOST} /repo/scripts/glaeda-linux-vm-serve install-root"))
        self.assertNotIn("<host>", out["next"][0])
        self.assertEqual(fake.steps(), ["base", "stage"])

    def test_base_apply_without_ready_receipt_fails(self):
        fake = Fake(base=(0, {"disposition": "changed", "receipt": {"state": "created"}}))
        with self.assertRaises(rollout.Failed):
            self.make(fake, apply=True).execute()

    def test_a_refused_step_refuses_the_rollout(self):
        fake = Fake(stage=(2, {"disposition": "refused", "reason": "the checkout has uncommitted changes"}))
        with self.assertRaisesRegex(rollout.Refused, "uncommitted"):
            self.make(fake, apply=True).execute()

    def test_absent_token_stops_before_enroll(self):
        self.token.unlink()
        fake = Fake()
        out = self.make(fake, apply=True).execute()
        self.assertEqual(out["stopped_at"], "token")
        self.assertEqual(out["needs"], "token_push")
        self.assertNotIn("enroll", fake.steps())

    def test_enroll_plan_does_not_apply_or_write(self):
        fake = Fake(enroll=(0, {"mode": "plan", "runner_group": {"action": "exists"},
                                "scale_set": {"action": "create"}, "enrollment": None}))
        out = self.make(fake).execute()
        self.assertEqual(out["stopped_at"], "enroll")
        self.assertNotIn("--apply", dict(fake.calls)["enroll"])
        self.assertFalse((self.root / "serve" / "scaleset-enroll.json").exists())

    def test_enroll_apply_writes_private_output_then_stops_for_approval(self):
        fake = Fake()
        out = self.make(fake, apply=True).execute()
        path = self.root / "serve" / "scaleset-enroll.json"
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(json.loads(path.read_text())["enrollment"]["scale_set_name"], SET)
        self.assertEqual(out["stopped_at"], "approve")
        self.assertEqual(out["plan_identity"], "pid-1")
        self.assertTrue(out["next"][0].endswith("--apply --approve-plan pid-1"))
        self.assertNotIn("finish_apply", fake.steps())

    def test_rerun_reuses_this_hosts_enrollment(self):
        self.make(Fake(), apply=True).execute()
        fake = Fake()
        out = self.make(fake, apply=True, approve="pid-1").execute()
        self.assertNotIn("enroll", fake.steps())
        self.assertEqual(out["stopped_at"], "serving")
        self.assertEqual(fake.steps(), ["base", "stage", "finish", "finish_apply"])
        self.assertEqual(dict(fake.calls)["finish_apply"][-2:], ["--approve-plan", "pid-1"])

    def test_enroll_output_for_another_scale_set_is_refused_not_overwritten(self):
        path = self.root / "serve" / "scaleset-enroll.json"
        path.write_text(json.dumps({"enrollment": {"scale_set_name": "glaeda-linux-arm64", "labels": ["glaeda-linux-arm64"]}}))
        with self.assertRaisesRegex(rollout.Refused, "another scale set"):
            self.make(Fake(), apply=True).execute()
        self.assertIn('"glaeda-linux-arm64"', path.read_text())

    def test_stored_enrollment_for_another_workflow_list_is_not_reused(self):
        self.make(Fake(), apply=True).execute()
        other = "manaflow-ai/cmux/.github/workflows/other.yml@refs/heads/main"
        with self.assertRaisesRegex(rollout.Refused, "workflow list"):
            self.make(Fake(), apply=True, workflows=(other,)).execute()

    def test_stored_enrollment_for_another_runner_group_is_not_reused(self):
        path = self.root / "serve" / "scaleset-enroll.json"
        doc = Fake().docs["enroll"][1]
        doc["runner_group"]["name"] = "glaeda-linux-trusted"
        path.write_text(json.dumps(doc))
        with self.assertRaisesRegex(rollout.Refused, "runner group"):
            self.make(Fake(), apply=True).execute()

    def test_next_commands_run_from_any_directory(self):
        out = self.make(Fake(base=(0, {"disposition": "plan", "plan": ["x"], "receipt": {}}))).execute()
        self.assertTrue(out["next"][0].startswith("python3 /repo/scripts/glaeda-linux-vm-rollout --host mini-a "))

    def test_enroll_error_refuses(self):
        fake = Fake(enroll=(1, {"error": "runner_group_conflict", "next": "pass --runner-group x"}))
        with self.assertRaisesRegex(rollout.Refused, "runner_group_conflict"):
            self.make(fake, apply=True).execute()

    def test_wrong_approval_is_refused(self):
        with self.assertRaisesRegex(rollout.Refused, "approval names pid-0"):
            self.make(Fake(), apply=True, approve="pid-0").execute()

    def test_inputs_are_validated(self):
        with self.assertRaises(rollout.Refused):
            self.make(Fake(), host="../x")
        with self.assertRaises(rollout.Refused):
            self.make(Fake(), workflows=())
        with self.assertRaises(rollout.Refused):
            self.make(Fake(), workflows=("manaflow-ai/cmux/.github/workflows/x.yml@refs/heads/dev",))

    def test_main_refuses_approval_without_apply(self):
        self.assertEqual(rollout.main(["--host", HOST, "--workflow", WORKFLOW, "--approve-plan", "p"]), 2)


if __name__ == "__main__":
    unittest.main()
