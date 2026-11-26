#!/usr/bin/env python3
"""One harness for `glaeda-gh budget`'s answer about a quota bucket: how often it comes back unknown, and when it
answers, by which route and how old the reading is.

Point it at any copy of scripts/glaeda-gh (the before copy and the after copy, so the two are compared on the same
inputs) and it replays a fixed set of host answers through that copy's own fleet_budget. Replay rather than live
reads, because the condition under measurement is which hosts happened to answer and what their daemons happened to
hold -- live that is unrepeatable, and sweeping the fleet N times to find out is a load on every host for an answer
that changes between runs. The host answers replayed here are the measured ones (see CONDITIONS).

    python3 scripts/harness-budget-graphql.py --script scripts/glaeda-gh [--runs 20] [--json]
    python3 scripts/harness-budget-graphql.py --script A --against B      # before and after, one table

Each invocation is a full sweep: the kept answer (FLEET_BUDGET_FILE, FLEET_BUDGET_TTL) is cleared between runs, so
`--runs N` is N independent sweeps and not one sweep read N times. No GitHub call and no ssh: `fetch` is the replay
and the free GET /rate_limit is a stub whose answer each condition names, so the same numbers come out on any host.
"""

from __future__ import annotations

import argparse
import importlib.machinery
import importlib.util
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
RUNS = 20
BUCKET = "graphql"

# A host answer as FLEET_HOST_SCRIPT returns it: {"budget", "daemon", "usage"}. "rest"/"graphql" under budget are the
# daemon's own readings, and a missing one is the whole point -- a daemon refreshes its GraphQL reading only when it
# sends a GraphQL query, while every REST answer carries its own headers.
def reading(remaining: int, *, reset_in: float, age: float, now: float) -> dict:
    return {"remaining": remaining, "limit": 5000, "reset": now + reset_in, "seenAt": now - age}


def host(now: float, *, rest: dict | None = None, graphql: dict | None = None, rest_spent: int = 10) -> dict:
    budget = {k: v for k, v in (("rest", rest), ("graphql", graphql)) if v is not None}
    return {"budget": budget, "daemon": {"at": now - 20, "lastHour": {}},
            "usage": [{"caller": "session:lane", "rest": rest_spent, "graphql": 0}]}


UNREACHABLE = {"error": "unreachable (no answer in 25s)"}


def conditions(now: float) -> dict[str, dict]:
    """Each condition: the answer every asked host gives, the free GET /rate_limit's answer, and any gh-quota mark.

    `as-measured` is the reading this change was opened for: on one host, 25 minutes apart, `budget` printed
    "GraphQL unknown (no reachable host's daemon has a fresh reading)" twice while REST answered to the call
    (4458/5000, then 4822/5000), with two of five hosts "unreachable (no answer in 25s)". This host's daemon held a
    fresh REST reading and no GraphQL one; the two reachable peers the same.
    """
    fresh_rest = reading(4458, reset_in=1500, age=40, now=now)
    return {
        "as-measured": {
            "why": "5 hosts, 2 unreachable; every reachable daemon has a fresh REST reading and no GraphQL one",
            "hosts": {"here": host(now, rest=fresh_rest), "peer-a": host(now, rest=fresh_rest),
                      "peer-b": host(now, rest=fresh_rest), "peer-c": UNREACHABLE, "peer-d": UNREACHABLE},
            "free": {"readings": {"graphql": reading(2670, reset_in=1980, age=0, now=now),
                                  "rest": fresh_rest}},
        },
        "one-peer-unreachable": {
            "why": "a peer holds a live GraphQL reading and another host cannot be reached at all",
            "hosts": {"here": host(now, rest=fresh_rest), "peer-a": host(now, rest=fresh_rest,
                                                                         graphql=reading(2670, reset_in=1980, age=19,
                                                                                         now=now)),
                      "peer-b": UNREACHABLE},
            "free": {"error": "not reached: a peer answered"},
        },
        "graphql-spent": {
            "why": "the login's GraphQL bucket at 0 with REST rich -- the condition that cost 3 waits 1800s each",
            "hosts": {"here": host(now, rest=fresh_rest), "peer-a": host(now, rest=fresh_rest), "peer-b": UNREACHABLE},
            "free": {"readings": {"graphql": reading(0, reset_in=2000, age=0, now=now), "rest": fresh_rest}},
            "mark": {"until": now + 2000},
        },
        "nothing-answers": {
            "why": "every host unreachable and no login here either: the one case that stays unknown",
            "hosts": {"here": UNREACHABLE, "peer-a": UNREACHABLE, "peer-b": UNREACHABLE},
            "free": {"error": "this host reads GitHub through its upstream (upstream.json) and has no login to ask"},
        },
    }


def load(path: Path) -> Any:
    name = "glaeda_gh_harness_" + path.resolve().parent.name.replace("-", "_")
    loader = importlib.machinery.SourceFileLoader(name, os.fspath(path))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # normal import ordering, so the module's own globals resolve
    spec.loader.exec_module(module)
    return module


def one_run(gg: Any, spec: dict, state: Path, now: float) -> dict:
    """One sweep through gg.fleet_budget, with the kept answer cleared first. Returns what `budget` would say about
    BUCKET: its number and route when it answered, else the sentence it printed."""
    for stale in (state / "fleet-budget.json", state / "fleet-budget.lock"):
        stale.unlink(missing_ok=True)
    marks = {BUCKET: spec["mark"]} if spec.get("mark") else {}
    free = spec["free"]
    patches = [mock.patch.object(gg, "policy_host", return_value="here"),
               mock.patch.object(gg, "fleet_hosts", return_value=list(spec["hosts"])),
               mock.patch.object(gg, "exhausted_marks", return_value=marks)]
    if hasattr(gg, "free_account_reading"):  # absent in the before copy: it has no third route
        patches.append(mock.patch.object(gg, "free_account_reading", side_effect=lambda now, make=None: free))
    for p in patches:
        p.start()
    try:
        doc = gg.fleet_budget(now, fetch=lambda h, local: dict(spec["hosts"][h]),
                           view=lambda: {"ok": False, "error": "no lane-gh here"})
        text = "\n".join(gg.fleet_lines(doc))
    finally:
        for p in reversed(patches):
            p.stop()
    account = (doc.get("account") or {}).get(BUCKET)
    line = next((ln for ln in text.splitlines() if ln.startswith("account (")), "")
    said = line.partition("GraphQL ")[2]
    if not isinstance(account, dict):
        return {"answered": False, "says": said, "bare": said.startswith("unknown (no reachable host")}
    return {"answered": True, "remaining": account.get("remaining"), "route": account.get("route", "peer"),
            "ageS": round(now - float(account.get("seenAt") or now), 1), "says": said, "bare": False}


def measure(script: Path, runs: int) -> dict:
    gg = load(script)
    out: dict[str, Any] = {}
    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp)
        env = {"GLAEDA_GH_HOME": os.fspath(state), "GLAEDA_GH_BUDGET_LANE_VMS": "",
               "GH_QUOTA_STATE": os.fspath(state / "quota")}
        with mock.patch.dict(os.environ, env), mock.patch.object(gg, "BASE", state):
            for name, spec in conditions(time.time()).items():
                rows = [one_run(gg, spec, state, time.time()) for _ in range(runs)]
                answered = [r for r in rows if r["answered"]]
                out[name] = {
                    "why": spec["why"],
                    "runs": len(rows),
                    "unknown": len(rows) - len(answered),
                    "bareUnknown": sum(1 for r in rows if r.get("bare")),
                    "routes": sorted({r["route"] for r in answered}),
                    "ageS": max((r["ageS"] for r in answered), default=None),
                    "remaining": sorted({r["remaining"] for r in answered}),
                    "says": rows[0]["says"][:300],
                }
    return out


def table(name: str, got: dict) -> list[str]:
    lines = [f"== {name}"]
    for cond, r in got.items():
        lines.append(f"  {cond:<22} unknown {r['unknown']}/{r['runs']} (bare unknown {r['bareUnknown']}), "
                     + (f"route {','.join(r['routes'])}, {r['remaining']} left, reading {r['ageS']}s old"
                        if r["routes"] else "no reading"))
        lines.append(f"      says: GraphQL {r['says']}")
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--script", type=Path, default=ROOT / "scripts" / "glaeda-gh", help="the copy to measure")
    ap.add_argument("--against", type=Path, help="a second copy, measured on the same conditions (before/after)")
    ap.add_argument("--runs", type=int, default=RUNS)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    got = {os.fspath(args.script): measure(args.script, args.runs)}
    if args.against:
        got[os.fspath(args.against)] = measure(args.against, args.runs)
    if args.json:
        print(json.dumps(got, indent=2, sort_keys=True))
        return 0
    for name, rows in got.items():
        for line in table(name, rows):
            print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
