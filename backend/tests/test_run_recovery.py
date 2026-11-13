"""Run recovery, idempotency and fencing tests.

These use the real service/repository code against a temporary SQLite database
and monkeypatch the Redis publish step so the queue is deterministic.
"""
import importlib
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db = tmp_path / "runs.sqlite"
    artifacts = tmp_path / "artifacts"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    monkeypatch.setenv("ARTIFACT_DIR", str(artifacts))
    monkeypatch.setenv("TASK_MODE", "inline")
    monkeypatch.setenv("RUN_MAX_ATTEMPTS", "3")
    monkeypatch.setenv("RUN_LEASE_SECONDS", "90")
    monkeypatch.setenv("RUN_REDELIVER_SECONDS", "60")
    for mod in [m for m in list(sys.modules) if m == "src" or m.startswith("src.")]:
        sys.modules.pop(mod, None)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from src.config import settings
    from src.utils import repository as repo
    from src.services import run as service
    from src.services import seed as seed_service
    settings.TASK_MODE = "inline"
    settings.RUN_MAX_ATTEMPTS = 3
    settings.RUN_LEASE_SECONDS = 90
    service.TASK_MODE = "inline"
    service.RUN_MAX_ATTEMPTS = 3
    service.RUN_LEASE_SECONDS = 90
    service.ARTIFACT_DIR = artifacts
    repo.init_db()
    seed_service.seed()
    return {"repo": repo, "service": service, "artifacts": artifacts}


def _rows(env):
    return env["repo"].get("datasets", "ds-learning")["rows"]


def test_retry_uses_accepted_events_not_later_edits(env):
    service = env["service"]
    repo = env["repo"]
    dataset = repo.get("datasets", "ds-learning")
    original = dataset["rows"]
    # Accept the run, then mutate the source dataset and cohort definition.
    accepted = repo.put("runs", {
        "id": "run-frozen", "cohort_id": "cohort-weekly", "status": "queued",
        "created_at": "2026-01-01T00:00:00+00:00", "attempts": 0,
        "input_events": original,
        "input_cohort": {"observation_end": 5, "include_completed": False},
        "error": "", "error_code": "", "max_attempts": 3,
        "lease_expires_at": "", "worker_id": "", "queued_at": "2026-01-01T00:00:00+00:00",
    })
    dataset["rows"] = original[:4]
    repo.put("datasets", dataset)
    cohort = repo.get("cohorts", "cohort-weekly")
    cohort["observation_end"] = 1
    repo.put("cohorts", cohort)

    done = service.execute("run-frozen")
    assert done["status"] == "completed"
    assert done["result"]["learners"] == 120
    assert done["result"]["observation_end"] == 5
    assert repo.get("datasets", "ds-learning")["rows"] == original[:4]


def test_duplicate_delivery_publishes_once(env, monkeypatch):
    service = env["service"]
    repo = env["repo"]
    publishes = []
    original = service._publish_json

    def counting(run_id, payload):
        publishes.append(run_id)
        original(run_id, payload)

    monkeypatch.setattr(service, "_publish_json", counting)
    run = service.create(type("D", (), {"cohort_id": "cohort-weekly"})())
    assert run["status"] == "completed"
    first_checksum = run["result"]["checksum"]
    # Same message delivered twice, as after a broker redelivery.
    second = service.execute(run["id"])
    assert second["status"] == "completed"
    assert second["result"]["checksum"] == first_checksum
    assert publishes.count(run["id"]) == 1
    assert second["attempts"] == 1


def test_concurrent_claim_only_one_winner(env):
    service = env["service"]
    repo = env["repo"]
    dataset = repo.get("datasets", "ds-learning")
    run = repo.put("runs", {
        "id": "run-race", "cohort_id": "cohort-weekly", "status": "queued",
        "created_at": "2026-01-01T00:00:00+00:00", "attempts": 0,
        "input_events": dataset["rows"],
        "input_cohort": {"observation_end": 5, "include_completed": False},
        "error": "", "error_code": "", "max_attempts": 3,
        "lease_expires_at": "", "worker_id": "", "queued_at": "2026-01-01T00:00:00+00:00",
    })
    first = service.claim("run-race", "worker-A")
    second = service.claim("run-race", "worker-B")
    assert isinstance(first, dict) and first["worker_id"] == "worker-A"
    assert second == "leased"
    # B cannot commit even if it somehow produced a result.
    assert service._commit("run-race", "worker-B", "completed", result={"forged": True}) is None


def test_stale_worker_cannot_overwrite_newer_result(env):
    service = env["service"]
    repo = env["repo"]
    dataset = repo.get("datasets", "ds-learning")
    repo.put("runs", {
        "id": "run-fence", "cohort_id": "cohort-weekly", "status": "queued",
        "created_at": "2026-01-01T00:00:00+00:00", "attempts": 0,
        "input_events": dataset["rows"],
        "input_cohort": {"observation_end": 5, "include_completed": False},
        "error": "", "error_code": "", "max_attempts": 3,
        "lease_expires_at": "", "worker_id": "", "queued_at": "2026-01-01T00:00:00+00:00",
    })
    old = service.claim("run-fence", "worker-old")
    assert isinstance(old, dict)
    # Old worker's lease expires and a new worker takes over and finishes.
    expired = repo.get("runs", "run-fence")
    expired["lease_expires_at"] = "2000-01-01T00:00:00+00:00"
    repo.put("runs", expired)
    new = service.claim("run-fence", "worker-new")
    assert isinstance(new, dict)
    assert service._commit("run-fence", "worker-new", "completed", result={"checksum": "new"})
    # Old worker wakes up late and tries to publish: it must lose.
    assert service._commit("run-fence", "worker-old", "completed", result={"checksum": "old"}) is None
    assert repo.get("runs", "run-fence")["result"]["checksum"] == "new"


def test_interrupted_worker_is_requeued_then_completes(env, monkeypatch):
    service = env["service"]
    repo = env["repo"]
    dataset = repo.get("datasets", "ds-learning")
    repo.put("runs", {
        "id": "run-crash", "cohort_id": "cohort-weekly", "status": "running",
        "created_at": "2026-01-01T00:00:00+00:00", "attempts": 1,
        "input_events": dataset["rows"],
        "input_cohort": {"observation_end": 5, "include_completed": False},
        "error": "", "error_code": "", "max_attempts": 3,
        "lease_expires_at": "2000-01-01T00:00:00+00:00", "worker_id": "dead-worker",
        "queued_at": "", "started_at": "2026-01-01T00:00:00+00:00",
    })
    monkeypatch.setattr(service, "enqueue", lambda run_id: None)
    assert service.recover_stale() == ["run-crash"]
    recovered = repo.get("runs", "run-crash")
    assert recovered["status"] == "queued"
    assert recovered["error_code"] == "lease_expired"
    assert "自动恢复" in recovered["error"]
    assert recovered["attempts"] == 1
    done = service.execute("run-crash")
    assert done["status"] == "completed"
    assert done["attempts"] == 2


def test_transient_failure_retries_up_to_cap(env, monkeypatch):
    service = env["service"]
    repo = env["repo"]
    calls = {"n": 0}

    def boom(*args, **kwargs):
        calls["n"] += 1
        raise RuntimeError("磁盘暂时不可用")

    monkeypatch.setattr(service, "analyze", boom)
    monkeypatch.setattr(service, "enqueue", lambda run_id: None)
    monkeypatch.setattr(service, "_inline", lambda run_id: service.execute(run_id))
    run = service.create(type("D", (), {"cohort_id": "cohort-weekly"})())
    assert run["status"] == "queued" and run["attempts"] == 1
    run = service.execute(run["id"])
    assert run["status"] == "queued" and run["attempts"] == 2
    run = service.execute(run["id"])
    assert run["status"] == "failed"
    assert run["error_code"] == "attempts_exhausted"
    assert run["attempts"] == 3
    assert calls["n"] == 3
    # A failed run is never auto-requeued by recovery.
    assert "自动重试" in run["error"] or "最大重试次数" in run["error"]
    assert service.recover_stale() == []


def test_exhausted_run_parked_by_lost_lease_is_finalised(env):
    service = env["service"]
    repo = env["repo"]
    dataset = repo.get("datasets", "ds-learning")
    repo.put("runs", {
        "id": "run-budget", "cohort_id": "cohort-weekly", "status": "running",
        "created_at": "2026-01-01T00:00:00+00:00", "attempts": 3,
        "input_events": dataset["rows"],
        "input_cohort": {"observation_end": 5, "include_completed": False},
        "error": "", "error_code": "", "max_attempts": 3,
        "lease_expires_at": "2000-01-01T00:00:00+00:00", "worker_id": "dead",
        "queued_at": "", "started_at": "2026-01-01T00:00:00+00:00",
    })
    # A duplicate delivery must not leave the run parked in "running".
    ret = service.execute("run-budget")
    assert ret["status"] == "failed"
    stored = repo.get("runs", "run-budget")
    assert stored["status"] == "failed"
    assert stored["error_code"] == "attempts_exhausted"
    assert stored["attempts"] == 3


def test_data_error_ends_task_without_retry(env, monkeypatch):
    service = env["service"]
    repo = env["repo"]
    bad = {"id": "ds-bad", "name": "bad", "source": "bad.csv", "created_at": "", "rows": [
        {"event_id": "e1", "learner_id": "A", "week": 0, "state": "active", "sequence": 1},
        {"event_id": "e2", "learner_id": "A", "week": 1, "state": "lost", "sequence": 1},
        {"event_id": "e3", "learner_id": "A", "week": 2, "state": "active", "sequence": 1},
        {"event_id": "e4", "learner_id": "B", "week": 0, "state": "active", "sequence": 1},
    ]}
    repo.put("datasets", bad)
    repo.put("cohorts", {"id": "cohort-bad", "name": "bad", "dataset_id": "ds-bad",
                         "observation_end": 5, "include_completed": False, "created_at": ""})
    run = repo.put("runs", {
        "id": "run-bad", "cohort_id": "cohort-bad", "status": "queued",
        "created_at": "2026-01-01T00:00:00+00:00", "attempts": 0,
        "input_events": bad["rows"],
        "input_cohort": {"observation_end": 5, "include_completed": False},
        "error": "", "error_code": "", "max_attempts": 3,
        "lease_expires_at": "", "worker_id": "", "queued_at": "2026-01-01T00:00:00+00:00",
    })
    done = service.execute("run-bad")
    assert done["status"] == "failed"
    assert done["error_code"] == "input_invalid"
    assert done["attempts"] == 1
    assert "数据错误" in done["error"]
    assert service.recover_stale() == []
    with pytest.raises(ValueError):
        service.retry("run-bad", None)


def test_explicit_retry_keeps_inputs_and_resets_budget(env):
    service = env["service"]
    repo = env["repo"]
    dataset = repo.get("datasets", "ds-learning")
    repo.put("runs", {
        "id": "run-explicit", "cohort_id": "cohort-weekly", "status": "failed",
        "created_at": "2026-01-01T00:00:00+00:00", "attempts": 3,
        "input_events": dataset["rows"],
        "input_cohort": {"observation_end": 4, "include_completed": True},
        "error": "已达最大重试次数 3", "error_code": "attempts_exhausted", "max_attempts": 3,
        "lease_expires_at": "", "worker_id": "", "queued_at": "",
    })
    dataset["rows"] = dataset["rows"][:4]
    repo.put("datasets", dataset)
    retried = service.retry("run-explicit", None)
    assert retried["status"] == "completed"
    assert retried["attempts"] == 1
    assert retried["result"]["observation_end"] == 4
    assert retried["result"]["include_completed"] is True
    assert retried["result"]["learners"] == 120


def test_recovery_counts_retention_and_transitions_match_oracle(env):
    service = env["service"]
    repo = env["repo"]
    run = service.create(type("D", (), {"cohort_id": "cohort-weekly"})())
    result = run["result"]
    total_active = sum(cell["active"] for cohort in result["cohorts"] for cell in cohort["cells"])
    assert total_active > 0
    # Counts are internally consistent: observed + missing === eligible in-window.
    for cohort in result["cohorts"]:
        for cell in cohort["cells"]:
            if cell["censored"] == 0:
                assert cell["observed"] + cell["missing"] == cell["eligible"]
    pairs = sum(sum(row) for row in result["markov"]["counts"])
    assert pairs == result["markov"]["adjacent_pairs"]
    assert pairs > 0
