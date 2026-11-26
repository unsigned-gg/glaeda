#!/usr/bin/env python3
"""Every glaeda workflow job has a timeout, and none runs after a cancel unless listed with its reason.

scripts/workflow_job_bounds.py is byte-identical to manaflow-ai/cmux scripts/ci/workflow_job_bounds.py and
manaflow-ai/cmuxterm-hq tests/workflow_job_bounds.py (cmuxterm-hq
docs/investigations/2026-11-18-boneheaded-designs.md §11).
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
sys.path.insert(0, str(Path(__file__).resolve().parent))
from workflow_job_bounds import findings  # noqa: E402

ALWAYS_ON_CANCEL: dict[str, str] = {
    "ci.yml:ci-status": (
        "the required-check aggregator: on a cancel it must run and go red, because a skipped required check "
        "reads as passing; it runs on a hosted runner for under a minute and holds no group of its own"),
}


class WorkflowJobBounds(unittest.TestCase):
    def test_every_job_is_bounded(self) -> None:
        self.assertTrue(list(WORKFLOWS.glob("*.yml")), "no workflow files found, so nothing is checked")
        self.assertEqual(findings(WORKFLOWS, ALWAYS_ON_CANCEL), [])

    def test_the_check_fails_on_a_missing_timeout_and_an_unlisted_always(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "w.yml").write_text(
                "on: push\n"
                "jobs:\n"
                "  build:\n"
                "    runs-on: ubuntu-24.04\n"
                "    steps:\n"
                "      - run: echo timeout-minutes: 5\n"
                "  report:\n"
                "    needs: build\n"
                "    if: >-\n"
                "      always() &&\n"
                "      needs.build.result == 'failure'\n"
                "    runs-on: ubuntu-24.04\n"
                "    timeout-minutes: 5\n"
                "    steps:\n"
                "      - run: echo\n"
                "  call:\n"
                "    uses: ./.github/workflows/other.yml\n", encoding="utf-8")
            found = findings(Path(tmp), {"w.yml:gone": "stale"})
        self.assertEqual(len(found), 3, found)
        self.assertTrue(found[0].startswith("w.yml:build: no timeout-minutes"), found)
        self.assertTrue(found[1].startswith("w.yml:report: if: >- always() &&"), found)
        self.assertTrue(found[2].startswith("w.yml:gone: listed"), found)


if __name__ == "__main__":
    unittest.main(verbosity=2)
