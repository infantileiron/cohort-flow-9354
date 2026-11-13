"""End-to-end worker tests over a real queue and real subprocesses.

fakeredis' TCP server speaks the Redis protocol, so ``python -m src.worker``
runs unmodified in a child process.  We kill a worker mid-computation and
verify another worker recovers and completes the accepted run.
"""
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import fakeredis
import pytest

BACKEND = Path(__file__).resolve().parents[1]

CRASH_AFTER_CLAIM = """
import os, sys
sys.path.insert(0, sys.argv[1])
from src.services import run as service
def die(*a, **k):
    os._exit(137)  # simulate SIGKILL after the queue message was consumed
service.analyze = die
service.execute(sys.argv[2])
"""


@pytest.fixture()
def live_env(tmp_path, monkeypatch):
    import socket
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = fakeredis.TcpFakeServer(("127.0.0.1", port))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    db = tmp_path / "live.sqlite"
    env = {
        "DATABASE_URL": f"sqlite:///{db}",
        "ARTIFACT_DIR": str(tmp_path / "artifacts"),
        "REDIS_URL": f"redis://127.0.0.1:{port}/0",
        "TASK_MODE": "redis",
        "SEED_DEMO": "false",
        "RUN_LEASE_SECONDS": "2",
        "RUN_MAX_ATTEMPTS": "3",
        "WORKER_RECOVERY_SECONDS": "1",
        "RUN_REDELIVER_SECONDS": "60",
        "PYTHONPATH": str(BACKEND),
    }
    monkeypatch.setenv("DATABASE_URL", env["DATABASE_URL"])
    monkeypatch.setenv("ARTIFACT_DIR", env["ARTIFACT_DIR"])
    monkeypatch.setenv("REDIS_URL", env["REDIS_URL"])
    monkeypatch.setenv("TASK_MODE", "redis")
    for name in ("RUN_LEASE_SECONDS", "RUN_MAX_ATTEMPTS", "WORKER_RECOVERY_SECONDS",
                 "RUN_REDELIVER_SECONDS"):
        monkeypatch.setenv(name, env[name])
    for mod in [m for m in list(sys.modules) if m == "src" or m.startswith("src.")]:
        sys.modules.pop(mod, None)
    sys.path.insert(0, str(BACKEND))
    from src.config import settings
    settings.TASK_MODE = "redis"
    settings.REDIS_URL = env["REDIS_URL"]
    settings.RUN_LEASE_SECONDS = 2
    settings.RUN_MAX_ATTEMPTS = 3
    settings.WORKER_RECOVERY_SECONDS = 1
    settings.RUN_REDELIVER_SECONDS = 60
    from src.utils import repository as repo
    from src.services import run as service
    from src.services import seed as seed_service
    service.TASK_MODE = "redis"
    service.REDIS_URL = env["REDIS_URL"]
    service.RUN_LEASE_SECONDS = 2
    service.RUN_MAX_ATTEMPTS = 3
    service.ARTIFACT_DIR = Path(env["ARTIFACT_DIR"])
    repo.init_db()
    seed_service.seed()
    yield {"repo": repo, "service": service, "env": env, "tmp": tmp_path}
    server.shutdown()


def _wait(predicate, timeout=25, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition not met in time")


def _worker(env, extra=()):
    return subprocess.Popen([sys.executable, "-m", "src.worker", *extra],
                            cwd=BACKEND, env={**os.environ, **env},
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def test_killed_worker_is_recovered_by_another_worker(live_env):
    env, service, repo = live_env["env"], live_env["service"], live_env["repo"]
    # User changes the source data immediately after accepting the task.
    run = service.create(type("D", (), {"cohort_id": "cohort-weekly"})())
    dataset = repo.get("datasets", "ds-learning")
    dataset["rows"] = dataset["rows"][:4]
    repo.put("datasets", dataset)
    cohort = repo.get("cohorts", "cohort-weekly")
    cohort["observation_end"] = 1
    repo.put("cohorts", cohort)

    # A worker consumes the queue message, then dies before committing.
    crash = subprocess.run([sys.executable, "-c", CRASH_AFTER_CLAIM, str(BACKEND), run["id"]],
                           cwd=BACKEND, env={**os.environ, **env})
    assert crash.returncode == 137
    claimed = repo.get("runs", run["id"])
    assert claimed["status"] == "running" and claimed["attempts"] == 1

    # A surviving worker starts, notices the lease expired and finishes the job.
    worker = _worker(env)
    try:
        done = _wait(lambda: (lambda r: r if r["status"] in ("completed", "failed") else None)(
            repo.get("runs", run["id"])))
    finally:
        worker.terminate()
        worker.wait(timeout=10)
    assert done["status"] == "completed", done.get("error")
    assert done["attempts"] == 2
    # Result was computed from the frozen snapshot, not the edited dataset.
    assert done["result"]["learners"] == 120
    assert done["result"]["observation_end"] == 5
    assert (Path(env["ARTIFACT_DIR"]) / (run["id"] + ".json")).exists()


def test_duplicate_delivery_and_data_change(live_env):
    env, service, repo = live_env["env"], live_env["service"], live_env["repo"]
    run = service.create(type("D", (), {"cohort_id": "cohort-weekly"})())
    # Duplicate queue delivery of the same accepted task.
    import redis
    client = redis.Redis.from_url(env["REDIS_URL"])
    client.rpush(service.QUEUE_KEY, run["id"])
    client.rpush(service.QUEUE_KEY, run["id"])

    worker = _worker(env)
    try:
        done = _wait(lambda: (lambda r: r if r["status"] in ("completed", "failed") else None)(
            repo.get("runs", run["id"])))
    finally:
        worker.terminate()
        worker.wait(timeout=10)
    assert done["status"] == "completed", done.get("error")
    # Only the first delivery claimed the run; duplicates were no-ops.
    assert done["attempts"] == 1

    # Retention head-counts and transition counts match a direct oracle run.
    from src.services.statistics import analyze
    oracle = analyze(repo.get("datasets", "ds-learning")["rows"], 5, False)
    assert done["result"]["cohorts"] == oracle["cohorts"]
    assert done["result"]["markov"]["counts"] == oracle["markov"]["counts"]
    assert done["result"]["checksum"] == oracle["checksum"]


def test_worker_recovers_run_whose_message_was_lost(live_env):
    env, service, repo = live_env["env"], live_env["service"], live_env["repo"]
    run = service.create(type("D", (), {"cohort_id": "cohort-weekly"})())
    import redis
    client = redis.Redis.from_url(env["REDIS_URL"])
    client.delete(service.QUEUE_KEY)  # the queue message vanishes (Redis flush/restart)
    queued = repo.get("runs", run["id"])
    queued["queued_at"] = "2000-01-01T00:00:00+00:00"
    repo.put("runs", queued)

    worker = _worker(env)
    try:
        done = _wait(lambda: (lambda r: r if r["status"] in ("completed", "failed") else None)(
            repo.get("runs", run["id"])))
    finally:
        worker.terminate()
        worker.wait(timeout=10)
    assert done["status"] == "completed"
    assert done["result"]["learners"] == 120
