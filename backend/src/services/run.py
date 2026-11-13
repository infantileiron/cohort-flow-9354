from datetime import datetime, timedelta, timezone
from dataclasses import asdict
import json, os, socket
from fastapi import HTTPException
from src.models.entities import TransitionRun
from src.utils import repository as repo
from src.utils.identity import new_id, now
from src.services.statistics import analyze
from src.config.settings import TASK_MODE, REDIS_URL, ARTIFACT_DIR, RUN_MAX_ATTEMPTS, RUN_LEASE_SECONDS, RUN_REDELIVER_SECONDS

QUEUE_KEY = "cohort-flow:jobs"
ACTIVE_STATUSES = ("queued", "running")
EDITABLE_STATUSES = ("queued", "running", "failed")


class NonRetryableError(ValueError):
    """Deterministic input/definition problem: retrying cannot fix it."""


def _iso_plus(seconds):
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def list_runs(): return [public(r) for r in repo.all_rows("runs")]


def public(run):
    """API view of a run: the frozen input snapshot stays server-side."""
    return {k: v for k, v in run.items() if k != "input_events"}

def _validate_rows(rows):
    from src.services.retention import weekly_snapshots
    try:
        snapshots = weekly_snapshots(rows, 52)
    except ValueError as exc:
        raise NonRetryableError(str(exc)) from exc
    if len(snapshots) < 2:
        raise NonRetryableError("至少需要2名学员")


def snapshot_inputs(run):
    """Freeze event rows and cohort definition at acceptance time.

    Runs are computed only from these frozen values, so later edits to the
    dataset or cohort definition cannot change a run's result, and a retry after
    a crash resumes against exactly the inputs that were accepted.
    """
    cohort = repo.get("cohorts", run["cohort_id"])
    try:
        dataset = repo.get("datasets", cohort["dataset_id"])
    except HTTPException as exc:
        raise NonRetryableError("队列引用的数据集不存在") from exc
    rows = dataset.get("rows")
    _validate_rows(rows)
    return rows, {"observation_end": cohort.get("observation_end", 5),
                  "include_completed": bool(cohort.get("include_completed", False)),
                  "dataset_id": cohort["dataset_id"],
                  "name": cohort.get("name", "")}


def create(data):
    repo.get("cohorts", data.cohort_id)
    if sum(r["status"] in ACTIVE_STATUSES for r in repo.all_rows("runs")) >= 10:
        raise ValueError("运行队列已满，请等待已有任务完成")
    rows, cohort = snapshot_inputs({"cohort_id": data.cohort_id})
    run = asdict(TransitionRun(new_id("run"), data.cohort_id, created_at=now()))
    run.update(input_events=rows, input_cohort=cohort, attempts=0, error_code="",
               max_attempts=RUN_MAX_ATTEMPTS, lease_expires_at="", worker_id="", queued_at=now())
    repo.put("runs", run)
    if TASK_MODE == "inline":
        return _inline(run["id"])
    enqueue(run["id"])
    return run


def _inline(run_id):
    """Drive the retry loop in-process when no worker is configured."""
    budget = repo.get("runs", run_id).get("max_attempts", RUN_MAX_ATTEMPTS)
    for _ in range(budget + 1):
        item = execute(run_id)
        if item is None:
            item = repo.get("runs", run_id)
        if item["status"] != "queued":
            return item
        if item.get("attempts", 0) >= budget:
            return item
    return repo.get("runs", run_id)


def enqueue(run_id):
    try:
        import redis
        redis.Redis.from_url(REDIS_URL).rpush(QUEUE_KEY, run_id)
    except Exception:
        def fail(item):
            if not item or item["status"] != "queued":
                return None, None
            item.update(status="failed", finished_at=now(),
                        error="队列不可用，请恢复 Redis 后重试", error_code="queue_unavailable")
            return item, item
        repo.mutate("runs", run_id, fail)
        raise ValueError("队列不可用，请恢复 Redis 后重试")


def claim(run_id, owner):
    """Atomically take ownership of a run, issuing a lease.

    Returns the claimed run dict on success, or a reason string otherwise.
    Only the winner of this compare-and-swap may compute and publish a result.
    """
    def attempt(item):
        if item is None:
            return None, "missing"
        if item["status"] == "completed":
            return None, "completed"
        if item["status"] == "failed":
            return None, "failed"
        if item.get("attempts", 0) >= item.get("max_attempts", RUN_MAX_ATTEMPTS):
            return None, "attempts_exhausted"
        if item["status"] == "running" and item.get("lease_expires_at", "") > now():
            return None, "leased"
        if item["status"] not in EDITABLE_STATUSES:
            return None, "not_editable"
        item.update(status="running", attempts=item.get("attempts", 0) + 1,
                    worker_id=owner, lease_expires_at=_iso_plus(RUN_LEASE_SECONDS),
                    started_at=now(), error="", error_code="", finished_at="")
        return item, item
    return repo.mutate("runs", run_id, attempt)


def _effective_lease(item):
    # A worker that vanished without recording a failure is retried; a worker
    # that recorded a deterministic data error must not be retried at all.
    if item.get("error_code") == "input_invalid":
        return "input_invalid"
    if item.get("attempts", 0) >= item.get("max_attempts", RUN_MAX_ATTEMPTS):
        return "attempts_exhausted"
    return "transient"


def recover_stale():
    """Re-deliver accepted runs whose worker died or whose queue message was lost.

    Safe to call from every worker: each transition is a row-level
    compare-and-swap, and a run can only be claimed once at a time.
    """
    requeued = set()

    def stale(run):
        return run["status"] == "running" and bool(run.get("lease_expires_at")) and run["lease_expires_at"] <= now()

    def unpicked(run):
        # A queued run older than the redelivery interval may have lost its
        # Redis message (worker was down when it was pushed, or the list was
        # flushed).  Re-push is idempotent: duplicate deliveries are swallowed
        # by claim(), so this can never publish twice.
        return (run["status"] == "queued"
                and run.get("queued_at", run.get("created_at", "")) < _iso_plus(-RUN_REDELIVER_SECONDS))

    for item in repo.all_rows("runs"):
        if stale(item) or unpicked(item):
            requeued.add(item["id"])

    for run_id in requeued:
        def requeue(run):
            if not run:
                return None, False
            if stale(run):
                reason = _effective_lease(run)
                if reason == "input_invalid":
                    run.update(status="failed", finished_at=now(), error_code="input_invalid",
                               error=run.get("error") or "数据错误，任务已结束",
                               lease_expires_at="", worker_id="")
                    return run, False
                if reason == "attempts_exhausted":
                    run.update(status="failed", finished_at=now(), error_code="attempts_exhausted",
                               error=f"已达最大重试次数 {run.get('max_attempts', RUN_MAX_ATTEMPTS)}，"
                                     f"最后一次尝试未完成（{run.get('error') or '进程中断'}）",
                               lease_expires_at="", worker_id="")
                    return run, False
                run.update(error=f"计算进程中断，已自动恢复（第 {run.get('attempts', 0)} 次尝试未完成）",
                           error_code="lease_expired", status="queued", lease_expires_at="",
                           worker_id="", started_at="", queued_at=now())
                return run, True
            if unpicked(run):
                run.update(status="queued", queued_at=now())
                return run, True
            return None, False
        if repo.mutate("runs", run_id, requeue):
            try:
                import redis
                redis.Redis.from_url(REDIS_URL).rpush(QUEUE_KEY, run_id)
            except Exception:
                pass
    return sorted(requeued)


def _publish_json(run_id, payload):
    """Atomically publish the artifact; only called after the fenced DB commit."""
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    final = ARTIFACT_DIR / (run_id + ".json")
    tmp = ARTIFACT_DIR / (run_id + ".json.tmp-" + new_id("art"))
    tmp.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False))
    os.replace(tmp, final)


def _commit(run_id, owner, outcome, failure=None, result=None, error=""):
    """Compare-and-swap state guarded by worker ownership (fencing).

    A worker whose lease was revoked, or whose run was already finished by a
    newer attempt, fails the ownership check and writes nothing. Returns True
    only if this call changed the stored row.
    """
    def commit(run):
        if not run or run["status"] != "running" or run.get("worker_id") != owner:
            return None, None
        attempt = run.get("attempts", 0)
        budget = run.get("max_attempts", RUN_MAX_ATTEMPTS)
        if outcome == "completed":
            run.update(status="completed", result=result, error="", error_code="",
                       finished_at=now(), lease_expires_at="", worker_id="", queued_at="")
            return run, "completed"
        if outcome == "failed" and failure["code"] == "transient":
            if attempt >= budget:
                run.update(status="failed", finished_at=now(), error_code="attempts_exhausted",
                           error=f"已达最大重试次数 {budget}，最后一次尝试失败（{error}）",
                           lease_expires_at="", worker_id="", queued_at="")
                return run, "failed"
            run.update(status="queued", error=error, error_code="transient", started_at="",
                       lease_expires_at="", worker_id="", queued_at=now())
            return run, "queued"
        run.update(status="failed", error=failure["message"], error_code=failure["code"],
                   finished_at=now(), lease_expires_at="", worker_id="", queued_at="")
        return run, "failed"
    outcome_written = repo.mutate("runs", run_id, commit)
    if outcome_written == "queued" and TASK_MODE != "inline":
        try:
            enqueue(run_id)
        except Exception:
            # The run stays queued; the next recovery sweep re-delivers it.
            import logging
            logging.getLogger(__name__).exception("retry enqueue failed for %s", run_id)
    return outcome_written


def execute(run_id):
    owner = f"{socket.gethostname()}:{os.getpid()}:{new_id('attempt')}"
    claimed = claim(run_id, owner)
    if not isinstance(claimed, dict):
        if claimed == "completed":
            return repo.get("runs", run_id)
        if claimed == "attempts_exhausted":
            # Never leave a run parked in "running" once its budget is gone.
            def exhaust(run):
                if not run or run["status"] not in ("running", "queued"):
                    return None, None
                run.update(status="failed", finished_at=now(), error_code="attempts_exhausted",
                           error=f"已达最大重试次数 {run.get('max_attempts', RUN_MAX_ATTEMPTS)}",
                           lease_expires_at="", worker_id="", queued_at="")
                return run, run
            repo.mutate("runs", run_id, exhaust)
            return repo.get("runs", run_id)
        return None
    try:
        rows, cohort = claimed.get("input_events"), claimed.get("input_cohort") or {}
        if rows is None:
            rows, cohort = snapshot_inputs(claimed)
            claimed.update(input_events=rows, input_cohort=cohort)
        result = analyze(rows, cohort.get("observation_end", 5),
                         cohort.get("include_completed", False))
    except (NonRetryableError, ValueError) as exc:
        _commit(run_id, owner, "failed",
                {"code": "input_invalid", "message": f"数据错误，任务已结束：{exc}"})
        return repo.get("runs", run_id)
    except Exception as exc:
        message = str(exc) or exc.__class__.__name__
        _commit(run_id, owner, "failed", {"code": "transient", "message": message},
                error=f"计算中断，等待自动重试（尝试 {claimed.get('attempts', 0)}/"
                      f"{claimed.get('max_attempts', RUN_MAX_ATTEMPTS)}）：{message}")
        return repo.get("runs", run_id)
    # Publish only after winning the fenced commit: a stale worker that lost
    # ownership never overwrites a newer, complete result.
    try:
        committed = _commit(run_id, owner, "completed", result=result)
    except Exception as exc:
        # e.g. the result could not be serialised: treat as a retryable failure
        # instead of leaving the row stuck in "running".
        message = str(exc) or exc.__class__.__name__
        try:
            _commit(run_id, owner, "failed", {"code": "transient", "message": message},
                    error=f"结果写入失败，等待自动重试：{message}")
        except Exception:
            import logging
            logging.getLogger(__name__).exception("failed to record run failure %s", run_id)
        return repo.get("runs", run_id)
    if committed == "completed":
        try:
            _publish_json(run_id, result)
        except Exception:
            import logging
            logging.getLogger(__name__).exception("artifact publish failed for %s", run_id)
    return repo.get("runs", run_id)


def retry(id, user):
    """Explicit retry from the result page. Inputs stay frozen; budget resets."""
    def reset(run):
        if not run or run["status"] != "failed":
            raise ValueError("只有失败的任务可以重试")
        if run.get("error_code") == "input_invalid":
            raise ValueError("数据错误不能重试：请修正数据或重新保存队列定义后再提交")
        run.update(status="queued", attempts=0, error="", error_code="", lease_expires_at="",
                   worker_id="", finished_at="", started_at="", queued_at=now())
        return run, run
    repo.mutate("runs", id, reset)
    if TASK_MODE == "inline":
        return _inline(id)
    enqueue(id)
    return repo.get("runs", id)
