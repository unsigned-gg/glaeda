#!/usr/bin/env python3
"""bench-ci-reads-replay: one synthetic day of a broker host's CI reads through `glaeda-gh gh`, before and after.

    python3 scripts/bench-ci-reads-replay.py [--seed N] [--json]

huge-blue's calls.log on 2026-11-17 (24 h): cmux runs/N/jobs 6,883, cmux jobs/N/logs 5,822, cmuxterm-hq runs/N/jobs
2,727, cmuxterm-hq jobs/N/logs 2,577. Most come from the lane-gh broker's ci_log (ci_wait, ws_check_post: a finished
run's job list, then every job's log, again per retry and per post) and land_judge. This replays a day shaped like
that: runs start through the day and finish ~20 minutes later; a quarter of the job-list reads come while the run is
still going, the rest after it, spread over the next hours (exponential gaps); every job log is read only after the
run finished, about twice per job; a tenth of the runs are rerun 30 minutes after they finished (a new attempt, new
job ids); 30% of job-list reads carry a --jq (land_judge's shape), the rest are raw (the broker's). glaeda's daemon is
modelled as watching every run (ci_wait waits on each), refreshing a finished one every TERMINAL_REFRESH.

Every call goes through cmd_gh in this process on a simulated clock; the real gh is a stand-in that answers from the
simulated state and is counted. "before" turns this change's paths off (the job-log store in front of the broker, the
kept finished job list); "after" runs them, without a build controller (worst case) and with one. The number reported
is the calls that still reached GitHub (gh runs; a controller answer costs no user REST).
"""
from __future__ import annotations

import argparse
import contextlib
import importlib.machinery
import importlib.util
import io
import json
import os
import random
import sys
import tempfile
import time
from pathlib import Path
from unittest import mock

DAY = 86400.0
DONE = "2026-11-17T08:00:01Z Cleaning up orphan processes\n"
# (repo, runs a day, job-list reads per run, jobs read per run, log reads per job): totals as huge-blue's day.
SHAPES = (("manaflow-ai/cmux", 1500, 6883 / 1500, 2, 5822 / 3000),
          ("manaflow-ai/cmuxterm-hq", 600, 2727 / 600, 2, 2577 / 1200))


def load(base: Path):
    os.environ["GLAEDA_GH_DIR"] = str(base)
    loader = importlib.machinery.SourceFileLoader("glaeda_gh_bench", str(Path(__file__).with_name("glaeda-gh")))
    spec = importlib.util.spec_from_loader("glaeda_gh_bench", loader)
    gg = importlib.util.module_from_spec(spec)
    sys.modules[loader.name] = gg
    loader.exec_module(gg)
    return gg


def workload(seed: int) -> tuple[list[tuple[float, list[str]]], dict]:
    """(calls in time order, the runs' simulated state)."""
    rng = random.Random(seed)
    calls: list[tuple[float, list[str]]] = []
    runs: dict = {}
    next_id = 40_000_000_000
    for repo, per_day, list_reads, jobs_read, log_reads in SHAPES:
        for _ in range(per_day):
            next_id += 1
            start = rng.uniform(0, DAY - 3600)
            end = start + rng.uniform(600, 1800)
            rerun = end + 1800 if rng.random() < 0.10 else None
            run = {"repo": repo, "id": next_id, "end": end, "rerun": rerun, "rerun_end": (rerun or 0) + 1200,
                   "jobs": 4}
            runs[(repo, next_id)] = run
            n_list = max(1, round(rng.expovariate(1 / list_reads)))
            for _ in range(n_list):
                at = rng.uniform(start, end) if rng.random() < 0.25 else end + rng.expovariate(1 / 900)
                jq = ["--jq", "[.jobs[] | {id, status, conclusion}]"] if rng.random() < 0.30 else []
                calls.append((at, ["api", f"repos/{repo}/actions/runs/{next_id}/jobs?per_page=100", *jq]))
            for j in range(jobs_read):
                for _ in range(max(1, round(rng.expovariate(1 / log_reads)))):
                    at = end + rng.expovariate(1 / 900)
                    attempt = 2 if rerun and at > run["rerun_end"] else 1
                    job = next_id * 10 + attempt * 4 + j
                    calls.append((at, ["api", "--allow-escape-sequences", f"repos/{repo}/actions/jobs/{job}/logs"]))
    calls.sort(key=lambda c: c[0])
    return calls, runs


def run_state(run: dict, now: float) -> tuple[str, int]:
    if run["rerun"] and now >= run["rerun"]:
        return ("completed" if now >= run["rerun_end"] else "in_progress"), 2
    return ("completed" if now >= run["end"] else "in_progress"), 1


def replay(after: bool, controller: bool, seed: int) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        gg = load(Path(tmp) / "glaeda")
        calls, runs = workload(seed)
        clock = [0.0]
        reached = {"jobs": 0, "logs": 0}
        served_by_controller = [0]
        t0 = time.time()

        def child(real, args, capture, deadline):
            words = [a for a in args if not a.startswith("-")]
            path = words[1]
            parts = path.split("/")
            if path.endswith("/logs"):
                reached["logs"] += 1
                return 0, f"2026-11-17T08:00:00Z job {parts[4]}\n{DONE}".encode(), b""
            reached["jobs"] += 1
            run = runs[(f"{parts[1]}/{parts[2]}", int(parts[5]))]
            status, attempt = run_state(run, clock[0])
            jobs = [{"id": run["id"] * 10 + attempt * 4 + j, "status": status, "run_attempt": attempt,
                     "conclusion": "success" if status == "completed" else None} for j in range(run["jobs"])]
            body = json.dumps({"total_count": len(jobs), "jobs": jobs})
            if "--jq" in args:
                body = json.dumps([{k: j[k] for k in ("id", "status", "conclusion")} for j in jobs])
            return 0, (body + "\n").encode(), b""

        def read_cache(key):
            if not key.startswith("run:"):
                return None
            owner, name, run_id = key[4:].split("/")
            run = runs.get((f"{owner}/{name}", int(run_id)))
            if run is None:
                return None
            status, attempt = run_state(run, clock[0])
            # the daemon re-reads a finished run every TERMINAL_REFRESH, a live one every cycle
            every = gg.TERMINAL_REFRESH if status == "completed" else gg.CYCLE
            fetched = t0 + clock[0] - (clock[0] % every)
            return {"key": key, "fetchedAt": fetched, "dataAt": fetched, "error": None,
                    "data": {"id": int(run_id), "status": status, "conclusion": "success", "run_attempt": attempt}}

        def controller_job_log(ctl, repo, job):
            served_by_controller[0] += 1
            return 200, f"2026-11-17T08:00:00Z job {job}\n{DONE}".encode()

        def prune(now):
            # the answer cache's prune, by each answer's own simulated time: its real one compares file mtimes, which
            # the simulated clock runs a day ahead of
            for path in gg.gh_cache_dir().glob("*.json"):
                with contextlib.suppress(OSError, ValueError):
                    if path.name != "writes.json" and now - json.loads(path.read_text()).get("at", now) > gg.GH_CACHE_KEEP:
                        path.unlink()

        last_prune = [-1e9]

        def prune_every(now):
            if now - last_prune[0] >= gg.GH_CACHE_PRUNE_EVERY:
                last_prune[0] = now
                prune(now)

        patches = [mock.patch.object(gg, "gh_cache_child", child), mock.patch.object(gg, "gh_cache_prune", prune_every), mock.patch.object(gg, "read_cache", read_cache),
                   mock.patch.object(gg, "daemon_running", lambda *a, **k: True),
                   mock.patch.object(gg, "register", lambda *a, **k: None),
                   mock.patch.object(gg, "broker_real_gh", lambda: "/bin/false"),
                   mock.patch.object(gg, "broker_hold", lambda s: None),
                   mock.patch.object(gg, "gh_sweep_refusal", lambda *a, **k: None),
                   mock.patch.object(time, "time", lambda: t0 + clock[0])]
        if controller:
            patches.append(mock.patch.object(gg, "controller_job_log", controller_job_log))
            patches.append(mock.patch.object(gg.Controller, "from_env", classmethod(lambda cls: cls("http://c", "t"))))
        else:
            patches.append(mock.patch.object(gg.Controller, "from_env", classmethod(lambda cls: None)))
        if not after:
            patches += [mock.patch.object(gg, "gh_job_log_read", lambda *a: None),
                        mock.patch.object(gg, "gh_run_jobs_final", lambda *a: None),
                        mock.patch.object(gg, "gh_cache_final_get", lambda *a: None)]
        env = {"GLAEDA_GH_CACHE_TTL": str(gg.GH_CACHE_TTL), "CMUX_GH_LOG": str(Path(tmp) / "calls.log"),
               "GH_QUOTA_CHECKED": "1", "CMUX_GH_MIN_REPEAT_SECONDS": "0"}
        with contextlib.ExitStack() as stack, mock.patch.dict(os.environ, env):
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(mock.patch.object(sys.stdout, "isatty", lambda: False))
            for at, args in calls:
                clock[0] = at
                os.environ.pop(gg.BROKER_MARK, None)
                os.environ.pop(gg.BROKER_DEPTH, None)
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()), \
                        mock.patch.object(os, "execv", lambda *a: (_ for _ in ()).throw(SystemExit(0))):
                    try:
                        gg.cmd_gh(args)
                    except SystemExit:
                        reached["jobs" if "/jobs" in args[1] else "logs"] += 1  # passed to gh with no cache
        logs = sum(1 for _, a in calls if a[-1].endswith("/logs"))
        return {"calls": len(calls), "jobsReads": len(calls) - logs, "logReads": logs,
                "jobsReachedGitHub": reached["jobs"], "logsReachedGitHub": reached["logs"],
                "reachedGitHub": reached["jobs"] + reached["logs"], "controllerAnswers": served_by_controller[0]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--seed", type=int, default=1117)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    out = {"before": replay(False, False, a.seed), "after": replay(True, False, a.seed),
           "afterWithController": replay(True, True, a.seed)}
    if a.json:
        print(json.dumps(out, indent=1, sort_keys=True))
        return 0
    b = out["before"]
    print(f"one synthetic day: {b['calls']} calls ({b['jobsReads']} runs/N/jobs, {b['logReads']} jobs/N/logs)")
    for name, r in out.items():
        print(f"  {name:20} reached GitHub {r['reachedGitHub']:>6}  (jobs lists {r['jobsReachedGitHub']}, "
              f"logs {r['logsReachedGitHub']}; controller answered {r['controllerAnswers']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
