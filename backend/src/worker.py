import logging,time
from pathlib import Path
import redis
from src.config.settings import REDIS_URL, WORKER_RECOVERY_SECONDS
from src.services.run import QUEUE_KEY, execute, recover_stale
from src.utils.repository import init_db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")

def heartbeat():
    Path("/tmp/worker-heartbeat").write_text(str(time.time()))

def main():
    init_db(); client=redis.Redis.from_url(REDIS_URL)
    last_recovery = 0.0
    while True:
        heartbeat()
        now = time.monotonic()
        if now - last_recovery >= WORKER_RECOVERY_SECONDS:
            last_recovery = now
            try:
                for run_id in recover_stale():
                    log.info("recovered interrupted run %s", run_id)
            except Exception:
                log.exception("stale-run recovery failed")
        try:
            # Short blocking timeout so recovery/heartbeat keep running.
            item=client.blpop(QUEUE_KEY, timeout=3)
            if item: execute(item[1].decode())
        except Exception:
            log.exception("worker failure");time.sleep(2)

if __name__=="__main__": main()
