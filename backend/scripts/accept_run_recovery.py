"""HTTP-level acceptance check: submit via API, kill the worker mid-run, verify recovery.

Runs against the FastAPI app with a real SQLite DB and fakeredis-backed worker
subprocesses.  Prints retention head-counts and transition counts for review.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
TMP = Path(os.environ.get("ACCEPTANCE_DIR", "/tmp/cohort-flow-acceptance"))
TMP.mkdir(exist_ok=True)
DB = TMP / "db.sqlite"
ART = TMP / "artifacts"
for p in (DB,):
    p.unlink(missing_ok=True)
import shutil
shutil.rmtree(ART, ignore_errors=True)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


REDIS_PORT = free_port()
API_PORT = free_port()

import fakeredis
server = fakeredis.TcpFakeServer(("127.0.0.1", REDIS_PORT))
threading.Thread(target=server.serve_forever, daemon=True).start()

ENV = {
    **os.environ,
    "DATABASE_URL": f"sqlite:///{DB}",
    "ARTIFACT_DIR": str(ART),
    "REDIS_URL": f"redis://127.0.0.1:{REDIS_PORT}/0",
    "TASK_MODE": "redis",
    "SEED_DEMO": "true",
    "JWT_SECRET": "acceptance-secret",
    "RUN_LEASE_SECONDS": "2",
    "RUN_MAX_ATTEMPTS": "3",
    "WORKER_RECOVERY_SECONDS": "1",
    "RUN_REDELIVER_SECONDS": "2",
    "PYTHONPATH": str(BACKEND),
}

api = subprocess.Popen([sys.executable, "-m", "uvicorn", "src.main:app", "--port", str(API_PORT),
                        "--log-level", "warning"], cwd=BACKEND, env=ENV)
try:
    import urllib.request

    def call(path, method="GET", body=None, token=None):
        req = urllib.request.Request(
            f"http://127.0.0.1:{API_PORT}{path}", method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", **({"Authorization": f"Bearer {token}"} if token else {})})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def wait_health():
        for _ in range(100):
            try:
                if call("/health")[0] == 200:
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise SystemExit("API failed to start")

    wait_health()
    _, login = call("/auth/login", "POST", {"email": "analyst@zhixing.test", "password": "Study2026!"})
    token = login["access_token"]

    # Submit a run; no worker is running yet, so it stays queued.
    status, created = call("/runs", "POST", {"cohort_id": "cohort-weekly"}, token)
    run_id = created["id"]
    print("created:", status, run_id, created["status"], "attempts", created["attempts"])
    assert status == 201 and created["status"] == "queued"

    # Change the source dataset after acceptance via direct DB write (simulates an edit).
    sys.path.insert(0, str(BACKEND))
    os.environ.update({k: ENV[k] for k in ("DATABASE_URL", "ARTIFACT_DIR", "REDIS_URL", "TASK_MODE")})
    from src.utils import repository as repo
    from src.services import dataset as dataset_service
    ds = repo.get("datasets", "ds-learning")
    original_rows = ds["rows"]
    ds["rows"] = original_rows[:4]
    repo.put("datasets", ds)
    cohort = repo.get("cohorts", "cohort-weekly")
    cohort["observation_end"] = 1
    repo.put("cohorts", cohort)

    # Start a worker that dies after claiming (SIGKILL simulation).
    crash_script = TMP / "crash_worker.py"
    crash_script.write_text(
        "import os,sys\nsys.path.insert(0,%r)\n"
        "from src.services import run as service\n"
        "service.analyze=lambda *a,**k: os._exit(137)\n"
        "service.execute(sys.argv[1])\n" % str(BACKEND))
    proc = subprocess.run([sys.executable, str(crash_script), run_id], cwd=BACKEND, env=ENV)
    print("crash worker exit:", proc.returncode)
    assert proc.returncode == 137

    # Now start the real worker; it must recover the orphaned run automatically.
    worker = subprocess.Popen([sys.executable, "-m", "src.worker"], cwd=BACKEND, env=ENV,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.time() + 40
        while time.time() < deadline:
            _, run = call(f"/runs/{run_id}", token=token)
            if run["status"] in ("completed", "failed"):
                break
            time.sleep(0.3)
        else:
            raise SystemExit("run did not finish")
        print("final:", run["status"], "attempts", run["attempts"], "/", run["max_attempts"],
              "error_code", run["error_code"])
        result = run["result"]
        print("learners:", result["learners"], "observation_end:", result["observation_end"],
              "events:", result["events"])
        # Confirm the run used the accepted snapshot, not the mutated source.
        assert result["learners"] == 120 and result["observation_end"] == 5
        assert run["status"] == "completed" and run["attempts"] == 2

        print("retention cohort sizes:", [(c["cohort_week"], c["size"]) for c in result["cohorts"]])
        print("retention active by age:",
              [[cell["active"] for cell in c["cells"]] for c in result["cohorts"]])
        print("transition counts:", result["markov"]["counts"])
        print("adjacent pairs:", result["markov"]["adjacent_pairs"])
        print("checksum:", result["checksum"])
        print("artifact:", (ART / f"{run_id}.json").exists())

        # Re-deliver the same job twice: result must not change or republish twice.
        import redis
        client = redis.Redis.from_url(ENV["REDIS_URL"])
        client.rpush("cohort-flow:jobs", run_id)
        client.rpush("cohort-flow:jobs", run_id)
        time.sleep(2)
        _, again = call(f"/runs/{run_id}", token=token)
        assert again["attempts"] == run["attempts"]
        assert again["result"]["checksum"] == result["checksum"]
        print("duplicate delivery kept attempts:", again["attempts"])
    finally:
        worker.send_signal(signal.SIGTERM)
        worker.wait(timeout=10)
finally:
    api.terminate()
    api.wait(timeout=10)
    server.shutdown()
print("ACCEPTANCE OK")
