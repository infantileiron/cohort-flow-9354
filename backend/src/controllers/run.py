from src.services import run as service
from src.utils import repository as repo


def list_items(): return service.list_runs()


def detail(id): return service.public(repo.get("runs", id))


def create(data):
    # Self-heal any run stranded by a previous worker exit before accepting new work.
    service.recover_stale()
    return service.public(service.create(data))


def retry(id, user): return service.public(service.retry(id, user))
