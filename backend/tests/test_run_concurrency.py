"""Two independent OS processes racing to execute the same accepted run.

Only one may compute and publish; the loser must not overwrite the winner.
"""
import json
import multiprocessing as mp
import os
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]


def _worker(db_url, artifact_dir, run_id, out_q, claim_log, publish_log):
    os.environ["DATABASE_URL"] = db_url
    os.environ["ARTIFACT_DIR"] = artifact_dir
    os.environ["TASK_MODE"] = "redis"  # no inline loop; one attempt per process
    sys.path.insert(0, str(BACKEND))
    from src.config import settings
    settings.TASK_MODE = "redis"
    from src.services import run as service
    service.TASK_MODE = "redis"
    publish_file = Path(publish_log)
    claimed_file = Path(claim_log)
    real_claim, real_publish = service.claim, service._publish_json

    def claim(run_id, owner):
        got = real_claim(run_id, owner)
        if isinstance(got, dict):
            with claimed_file.open("a") as fh:
                fh.write(owner + "\n")
        return got

    def publish(run_id, payload):
        real_publish(run_id, payload)
        with publish_file.open("a") as fh:
            fh.write(run_id + "\n")

    service.claim = claim
    service._publish_json = publish
    result = service.execute(run_id)
    out_q.put(None if result is None else (result["status"], result["attempts"],
                                           (result.get("result") or {}).get("checksum")))


def test_two_processes_publish_one_result(tmp_path):
    db = tmp_path / "race.sqlite"
    artifacts = tmp_path / "artifacts"
    db_url = f"sqlite:///{db}"
    os.environ["DATABASE_URL"] = db_url
    os.environ["ARTIFACT_DIR"] = str(artifacts)
    sys.path.insert(0, str(BACKEND))
    from src.config import settings
    from src.utils import repository as repo
    from src.services import seed as seed_service
    repo.init_db()
    seed_service.seed()
    rows = repo.get("datasets", "ds-learning")["rows"]
    repo.put("runs", {
        "id": "run-mp", "cohort_id": "cohort-weekly", "status": "queued",
        "created_at": "2026-01-01T00:00:00+00:00", "attempts": 0,
        "input_events": rows,
        "input_cohort": {"observation_end": 5, "include_completed": False},
        "error": "", "error_code": "", "max_attempts": 3,
        "lease_expires_at": "", "worker_id": "", "queued_at": "2026-01-01T00:00:00+00:00",
    })

    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    publish_log = tmp_path / "publishes.log"
    claim_log = tmp_path / "claims.log"
    procs = [ctx.Process(target=_worker,
                         args=(db_url, str(artifacts), "run-mp", queue,
                               str(claim_log), str(publish_log)))
             for _ in range(2)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0
    outcomes = [queue.get(timeout=5) for _ in procs]
    # The loser either observes the winner's active lease (None) or, if the
    # winner already finished, reads the stored result. Either way it must not
    # claim or publish anything itself.
    assert any(o and o[0] == "completed" for o in outcomes), outcomes
    winners = [line for line in claim_log.read_text().splitlines() if line]
    assert len(winners) == 1, winners
    published = [line for line in publish_log.read_text().splitlines() if line]
    assert published == ["run-mp"], published
    stored = repo.get("runs", "run-mp")
    assert stored["status"] == "completed"
    assert stored["attempts"] == 1
    payload = json.loads((artifacts / "run-mp.json").read_text())
    assert payload["checksum"] == stored["result"]["checksum"]
    assert payload["learners"] == 120
