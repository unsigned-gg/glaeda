#!/usr/bin/env python3
"""What `glaeda-gh land` costs the host it was asked on, for a queue of N PRs: processes spawned and their CPU seconds,
old (this host runs the queue: land_judge and its classifiers per PR) vs new (land.json "executor": it forwards).

The same harness both ways: a real LandQueue from scripts/glaeda-gh over a temporary GLAEDA_GH_DIR, real subprocesses,
and fakes for what would reach GitHub. The fake land-judge starts two classifier children (as base_red_excuse.py and
pr_owner.py are started by the real one) that each burn CLASSIFIER_CPU seconds of CPU; the fake gh shim merges at once.
The fake executor is one program that takes the forwarded batch and answers it, as `reach HOST glaeda-gh land serve`
does from this host's side. Every fake program logs one line when it starts, and the CPU is RUSAGE_CHILDREN's.

    python3 scripts/benchmark-land-forward.py [--prs 7] [--json]
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.machinery
import importlib.util
import json
import os
import resource
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLASSIFIER_CPU = 0.15
HEAD = "a" * 40

LAND_JUDGE = r'''
import json, os, subprocess, sys, time
log = os.environ["BENCH_LOG"]
if sys.argv[1] == "classify":
    open(log, "a").write("classifier\n")
    end = time.process_time() + float(os.environ["BENCH_CPU"])
    while time.process_time() < end:
        pass
    raise SystemExit(0)
open(log, "a").write("land_judge\n")
for _ in ("base_red_excuse", "pr_owner"):
    subprocess.run([sys.executable, __file__, "classify"], check=True)
repo, number, head = sys.argv[2], sys.argv[3], sys.argv[sys.argv.index("--sha") + 1]
print(json.dumps({"ok": True, "code": "ok", "reason": "", "head": head,
                  "action": {"v": 1, "kind": "merge", "repo": repo, "number": int(number), "head": head, "sha": head}}))
'''

GH = r'''#!/bin/sh
echo gh >> "$BENCH_LOG"
case "$1 $2" in
  "pr view") echo '{"state": "MERGED", "mergeCommit": {"oid": "ffffffffffffffffffffffffffffffffffffffff"}}' ;;
esac
exit 0
'''

EXECUTOR = r'''
import json, os, sys
open(os.environ["BENCH_LOG"], "a").write("executor\n")
req = json.loads(sys.stdin.readline())
print(json.dumps({"ok": True, "accepted": [r["id"] for r in req.get("requests") or []]}))
'''


def load() -> object:
    loader = importlib.machinery.SourceFileLoader("glaeda_gh", os.fspath(ROOT / "scripts" / "glaeda-gh"))
    spec = importlib.util.spec_from_loader("glaeda_gh", loader)
    gg = importlib.util.module_from_spec(spec)
    sys.modules["glaeda_gh"] = gg
    loader.exec_module(gg)
    return gg


def run(gg: object, mode: str, prs: int, tmp: Path, cpu: float = CLASSIFIER_CPU) -> dict:
    gg.BASE = tmp / mode / "gh"
    log = tmp / mode / "spawned.log"
    bin_dir = tmp / mode / "bin"
    bin_dir.mkdir(parents=True)
    mh = tmp / mode / "land-judge"
    mh.write_text(LAND_JUDGE)
    gh = bin_dir / "gh"
    gh.write_text(GH)
    gh.chmod(0o755)
    exe = tmp / mode / "executor.py"
    exe.write_text(EXECUTOR)
    os.environ["BENCH_LOG"] = os.fspath(log)  # the forward runs with this process's environment
    config = {"gh": os.fspath(gh), "landJudge": os.fspath(mh), "python": sys.executable, "acpmux": "/bin/true",
              "env": {"BENCH_LOG": os.fspath(log), "BENCH_CPU": str(cpu)}}
    if mode == "new":
        config["executor"] = [sys.executable, os.fspath(exe)]
    now = time.time()
    cache = {f"pr:o/r#{n}": {"key": f"pr:o/r#{n}", "fetchedAt": now, "dataAt": now,
                             "data": {"state": "OPEN", "headRefOid": HEAD, "mergeable": "MERGEABLE",
                                      "mergeStateStatus": "CLEAN",
                                      "checks": [{"name": "ci", "workflow": "w", "status": "COMPLETED",
                                                  "conclusion": "SUCCESS"}]}} for n in range(1, prs + 1)}
    for n in range(1, prs + 1):
        gg.land_request("land", f"o/r#{n}", [], "requester-host/session")
    q = gg.LandQueue(cache_of=cache.get, watch=lambda key: None, inline=True, config=config,
                     reads_upstream=lambda: False, behind=lambda *a: None)
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    t0 = time.monotonic()
    for i in range(20):  # ticks: each entry is judged and merged (old) or forwarded (new) within the first few
        q.tick(now + 2 * (i + 1))
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    lines = log.read_text().split() if log.exists() else []
    states: dict[str, int] = {}
    for e in q.entries.values():
        states[e["state"]] = states.get(e["state"], 0) + 1
    left = len(list(q.requests_dir().glob("*.json"))) if q.requests_dir().is_dir() else 0
    return {"mode": mode, "prs": prs, "processes": len(lines),
            "byProgram": {k: lines.count(k) for k in sorted(set(lines))},
            "cpuSeconds": round((after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime), 3),
            "wallSeconds": round(time.monotonic() - t0, 3), "entries": states, "requestsLeftHere": left}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--prs", type=int, default=7)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--classifier-cpu", type=float, default=CLASSIFIER_CPU, metavar="S",
                    help="CPU seconds each fake classifier burns")
    args = ap.parse_args()
    gg = load()
    with tempfile.TemporaryDirectory(prefix="bench-land-forward-") as tmp:
        os.environ["GH_QUOTA_STATE"] = os.fspath(Path(tmp) / "gh-quota")
        gg.LAND_JUDGE_PATHS = ()
        # The queue logs JSON event lines to stdout: kept off it, so --json prints one JSON document alone.
        with contextlib.redirect_stdout(sys.stderr):
            results = [run(gg, mode, args.prs, Path(tmp), args.classifier_cpu) for mode in ("old", "new")]
    if args.json:
        print(json.dumps(results, sort_keys=True))
        return 0
    for r in results:
        print(f"{r['mode']}: {r['prs']} PRs -> {r['processes']} processes on this host ({r['byProgram']}), "
              f"{r['cpuSeconds']} CPU s, {r['wallSeconds']} s wall; entries {r['entries']}; "
              f"requests left here {r['requestsLeftHere']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
