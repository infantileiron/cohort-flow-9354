"""Independent oracle for retention head-counts and transition counts.

Recomputes from the demo events without importing the project's statistics /
retention / markov modules, then compares to the artifact produced by
``accept_run_recovery.py`` (same acceptance directory).

Run ``python -m scripts.accept_run_recovery`` first, then
``python -m scripts.oracle_retention``.
"""
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
ACCEPT_DIR = Path(os.environ.get("ACCEPTANCE_DIR", "/tmp/cohort-flow-acceptance"))
DB = ACCEPT_DIR / "db.sqlite"
ART = ACCEPT_DIR / "artifacts"
sys.path.insert(0, str(BACKEND))
os.environ.setdefault("DATABASE_URL", f"sqlite:///{DB}")
os.environ.setdefault("ARTIFACT_DIR", str(ART))
from src.utils import repository as repo
from src.services.seed import demo_rows

rows = demo_rows()
END = 5

# --- oracle: state per (learner, week) = event with max sequence ---
best = {}
for r in rows:
    if r["week"] > END:
        continue
    key = (r["learner_id"], r["week"])
    if key not in best or r["sequence"] > best[key]["sequence"]:
        best[key] = r
snapshots = defaultdict(dict)
for (learner, week), r in best.items():
    snapshots[learner][week] = r["state"]

cohorts = defaultdict(list)
for learner, weeks in snapshots.items():
    cohorts[min(weeks)].append(learner)

retention = []
for start in sorted(cohorts):
    people = cohorts[start]
    cells = []
    for age in range(END + 1):
        week = start + age
        if week > END:
            cells.append({"age": age, "calendar_week": week, "eligible": 0, "observed": 0,
                          "active": 0, "missing": 0, "censored": len(people), "retention": None})
            continue
        observed = [snapshots[p][week] for p in people if week in snapshots[p]]
        active = sum(s == "active" for s in observed)
        cells.append({"age": age, "calendar_week": week, "eligible": len(people),
                      "observed": len(observed), "active": active,
                      "missing": len(people) - len(observed), "censored": 0,
                      "retention": active / len(people) if len(observed) == len(people) else None})
    retention.append({"cohort_week": start, "size": len(people), "cells": cells})

states = ["active", "dormant", "completed", "lost"]
counts = [[0] * 4 for _ in states]
pairs = 0
for learner, weeks in snapshots.items():
    for week, state in weeks.items():
        nxt = weeks.get(week + 1)
        if nxt is not None:
            counts[states.index(state)][states.index(nxt)] += 1
            pairs += 1

run = [r for r in repo.all_rows("runs")
       if r["id"] != "run-baseline" and r["status"] == "completed"][-1]
result = json.loads((ART / f"{run['id']}.json").read_text())
assert result["learners"] == len(snapshots) == 120
assert result["cohorts"] == retention, "retention matrix mismatch"
assert result["markov"]["counts"] == counts, "transition counts mismatch"
assert result["markov"]["adjacent_pairs"] == pairs
print("oracle matches persisted result")
print("cohort sizes:", [(c["cohort_week"], c["size"]) for c in retention])
print("active counts:", [[cell["active"] for cell in c["cells"]] for c in retention])
print("transition counts:", counts, "pairs:", pairs)
print("artifact checksum:", result["checksum"])
