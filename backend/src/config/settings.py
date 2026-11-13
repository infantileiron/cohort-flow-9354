import os
from pathlib import Path
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////tmp/cohort-flow.sqlite")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
JWT_SECRET = os.getenv("JWT_SECRET", "cohort-flow-development-secret-change-this")
SEED_DEMO = os.getenv("SEED_DEMO", "true").lower() == "true"
TASK_MODE = os.getenv("TASK_MODE", "redis")
ARTIFACT_DIR = Path(os.getenv("ARTIFACT_DIR", "/tmp/cohort-flow-artifacts"))
# A worker holds a lease for this long; if it dies, the run is reclaimed and retried.
RUN_LEASE_SECONDS = int(os.getenv("RUN_LEASE_SECONDS", "90"))
RUN_MAX_ATTEMPTS = int(os.getenv("RUN_MAX_ATTEMPTS", "3"))
WORKER_RECOVERY_SECONDS = int(os.getenv("WORKER_RECOVERY_SECONDS", "10"))
# A queued run older than this is re-pushed, in case its Redis message was lost.
RUN_REDELIVER_SECONDS = int(os.getenv("RUN_REDELIVER_SECONDS", "60"))
