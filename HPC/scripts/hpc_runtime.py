"""HPC worker lifecycle supervision; no calculation or third-party imports here."""

import json
import logging
import math
import os
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)
_CONTROL_FD = "_HPC_CONTROL_FD"
_RECOVERY = "_HPC_RECOVERY"
_channel: socket.socket | None = None
_send_lock = threading.Lock()
_job: dict | None = None
_cleanup_prefix: str | None = None
_work_dirs: list[str] = []


@dataclass(frozen=True)
class Timeouts:
    """HPC_TIMEOUTS accepts a JSON object overriding these positive seconds."""

    transfer: float = 60
    request: float = 30
    manifest: float = 60
    callback: float = 30
    cleanup: float = 120
    local_cleanup: float = 60
    exit: float = 30
    terminate: float = 10

    @classmethod
    def from_environment(cls) -> "Timeouts":
        values = json.loads(os.environ.get("HPC_TIMEOUTS", "{}"))
        if not isinstance(values, dict) or values.keys() - asdict(cls()).keys():
            raise ValueError("HPC_TIMEOUTS must contain only supported timeout names")
        if any(isinstance(value, bool) or not isinstance(value, (int, float))
               or not math.isfinite(value) or value <= 0 for value in values.values()):
            raise ValueError("HPC_TIMEOUTS values must be finite positive seconds")
        return cls(**values)


timeouts = Timeouts()


# ---------------------------------------------------------------------------
# Worker control messages and bounded phases
# ---------------------------------------------------------------------------

def emit(event: str, *, progress: bool = False, **fields) -> None:
    """Progress may be coalesced; lifecycle messages have a bounded reliable send."""
    if _channel is None:
        return
    payload = json.dumps({"event": event, "at": time.monotonic(), **fields}).encode()
    if not _send_lock.acquire(blocking=not progress):
        return
    try:
        if progress:
            if not select.select([], [_channel], [], 0)[1]:
                return
            try:
                _channel.send(payload, socket.MSG_DONTWAIT)
            except BlockingIOError:
                return
        else:
            _channel.send(payload)
    finally:
        _send_lock.release()


@contextmanager
def phase(name: str):
    token = uuid.uuid4().hex
    emit("phase_start", token=token, name=name, limit=getattr(timeouts, name))
    try:
        yield
    finally:
        emit("phase_end", token=token)


def configure_job(kind: str, key: str, manifest: dict, callback_variable: str) -> None:
    global _job  # pylint: disable=global-statement
    _job = {"kind": kind, "key": key, "manifest": {
        **manifest, "attempt_id": os.environ["HPC_ATTEMPT_ID"],
    }, "callback_variable": callback_variable}
    emit("job", job=_job)
    logger.info("HPC attempt=%s task=%s", os.environ["HPC_ATTEMPT_ID"], kind)


@contextmanager
def calculation():
    """Record the call boundary without starting a computation deadline."""
    started = time.monotonic()
    emit("calculation_start")
    try:
        yield
    finally:
        emit("calculation_end", seconds=time.monotonic() - started)


def work_directory(parent: Path, prefix: str) -> Path:
    """Register a private temporary directory; remove only after result reporting."""
    root = Path(tempfile.mkdtemp(dir=parent, prefix=prefix))
    _work_dirs.append(str(root))
    emit("work_directory", path=str(root))
    return root


def allow_output_cleanup(prefix: str) -> None:
    """Call only after validating ownership boundaries and before starting PUTs."""
    global _cleanup_prefix  # pylint: disable=global-statement
    _cleanup_prefix = prefix
    emit("cleanup_prefix", prefix=prefix)


def _clean_outputs(success: bool, skip: set[str]) -> None:
    if not success and _cleanup_prefix and "cleanup" not in skip:
        from hpc_io import clear_prefix  # pylint: disable=import-outside-toplevel
        try:
            with phase("cleanup"):
                clear_prefix(_cleanup_prefix)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("OSS cleanup incomplete; backend must reconcile prefix=%s", _cleanup_prefix)
    if _work_dirs and "local_cleanup" not in skip:
        with phase("local_cleanup"):
            os.chdir(Path(__file__).resolve().parent)
            for directory in _work_dirs:
                try:
                    shutil.rmtree(directory)
                except OSError:
                    logger.exception("local cleanup incomplete: %s", directory)


def worker_main(main: Callable[[], int]) -> int:
    try:
        code = main()
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else int(exc.code is not None)
    _clean_outputs(code in (0, 3), set())
    emit("returned", code=code)
    return code


def _recover(spec: dict) -> int:
    """Report/clean without rerunning calculations or restarting expired budgets."""
    global _job, _cleanup_prefix, _work_dirs  # pylint: disable=global-statement
    from hpc_io import report_result  # pylint: disable=import-outside-toplevel
    _job = spec["job"]
    _cleanup_prefix = spec["cleanup_prefix"]
    _work_dirs = spec["work_dirs"]
    skip = set(spec["started"])
    manifest = _job["manifest"]
    if not spec["result_known"]:
        manifest.update(is_success=False, error_type=spec["error_type"],
                        error_message=spec["reason"])
    committed = report_result(_job["key"], manifest, _job["kind"],
                              _job["callback_variable"], skip=skip,
                              committed=spec["manifest_ok"])
    _clean_outputs(manifest["is_success"], skip)
    code = (0 if committed else 3) if manifest["is_success"] else spec["code"]
    emit("returned", code=code)
    return code


# ---------------------------------------------------------------------------
# Parent process supervision and process-group termination
# ---------------------------------------------------------------------------

@dataclass
class State:
    job: dict | None = None
    result_known: bool = False
    manifest_ok: bool = False
    returned: int | None = None
    exit_deadline: float | None = None
    cleanup_prefix: str | None = None
    requests: dict[str, dict] = field(default_factory=dict)
    phases: dict[str, dict] = field(default_factory=dict)
    started: set[str] = field(default_factory=set)
    work_dirs: list[str] = field(default_factory=list)

    def receive(self, message: dict) -> None:
        event = message["event"]
        if event == "job":
            self.job = message["job"]
        elif event == "calculation_start":
            logger.info("HPC calculation started; no outer computation deadline")
        elif event == "calculation_end":
            logger.info("HPC calculation returned seconds=%.3f", message["seconds"])
        elif event == "result":
            self.job = message["job"]
            self.result_known = True
        elif event == "manifest_committed":
            self.manifest_ok = True
        elif event == "work_directory":
            self.work_dirs.append(message["path"])
        elif event == "cleanup_prefix":
            self.cleanup_prefix = message["prefix"]
        elif event == "request_start":
            self.requests[message["token"]] = message
        elif event == "request_progress":
            request = self.requests.get(message["token"])
            if request is not None and message["bytes"] > request.get("bytes", 0):
                request.update(at=message["at"], bytes=message["bytes"])
        elif event == "request_end":
            self.requests.pop(message["token"], None)
        elif event == "phase_start":
            self.started.add(message["name"])
            self.phases[message["token"]] = message
            logger.info("HPC phase=%s started", message["name"])
        elif event == "phase_end":
            completed = self.phases.pop(message["token"], None)
            if completed:
                logger.info("HPC phase=%s seconds=%.3f", completed["name"],
                            message["at"] - completed["at"])
        elif event == "returned":
            self.returned = message["code"]
            self.exit_deadline = message["at"] + timeouts.exit

    def expired(self) -> str | None:
        now = time.monotonic()
        for operation in (*self.phases.values(), *self.requests.values()):
            if now - operation["at"] >= operation["limit"]:
                return operation["name"]
        if self.exit_deadline is not None and now >= self.exit_deadline:
            return "exit"
        return None

    def timeout_code(self) -> int:
        if self.result_known and self.job:
            manifest = self.job["manifest"]
            if manifest["is_success"]:
                return 0 if self.manifest_ok else 3
            return 124 if manifest["error_type"] == "HpcTimeout" else 2
        if self.returned is not None:
            return self.returned
        return 124


def _group_exists(pid: int) -> bool:
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _reap_orphans() -> None:
    if os.getpid() != 1:
        return
    while True:
        try:
            child, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if child == 0:
            return


def _stop_group(process: subprocess.Popen) -> bool:
    """Do not wait indefinitely, including after SIGKILL."""
    for signum, grace in ((signal.SIGTERM, timeouts.terminate), (signal.SIGKILL, 2)):
        if _group_exists(process.pid):
            logger.warning("HPC terminating process group=%s signal=%s", process.pid, signum)
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            if process.poll() is not None:
                _reap_orphans()
            if not _group_exists(process.pid):
                logger.info("HPC worker reaped exit=%s", process.returncode)
                return True
            time.sleep(0.05)
    logger.error("HPC process group not confirmed stopped; skipping all OSS cleanup")
    return False


def _monitor(script: str, state: State, cancelled: list[int], recovery: dict | None = None):
    if cancelled:
        return 128 + cancelled[0], "cancelled", True
    parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
    env = {**os.environ, _CONTROL_FD: str(child.fileno())}
    if recovery is not None:
        env[_RECOVERY] = json.dumps(recovery)
    recovery_deadline = None
    if recovery is not None:
        pending = {"manifest", "callback", "cleanup", "local_cleanup"} - set(recovery["started"])
        # The report-only worker cannot compute: also bound imports and setup.
        recovery_deadline = (time.monotonic() + timeouts.request + timeouts.exit
                             + sum(getattr(timeouts, name) for name in pending))
    with parent, child:
        process = subprocess.Popen([sys.executable, script, *sys.argv[1:]],
                                   pass_fds=(child.fileno(),), start_new_session=True, env=env)
        child.close()
        parent.settimeout(0.2)
        while True:
            # Drain a bounded batch before checking clocks, so queued completion wins.
            for _ in range(256):
                try:
                    raw = parent.recv(65536)
                except TimeoutError:
                    break
                state.receive(json.loads(raw))
                parent.settimeout(0.001)
            parent.settimeout(0.2)
            if cancelled:
                return 128 + cancelled[0], "cancelled", _stop_group(process)
            code = process.poll()
            if code is not None:
                # poll can race with the final datagram; receive pending messages first.
                while True:
                    try:
                        state.receive(json.loads(parent.recv(65536, socket.MSG_DONTWAIT)))
                    except (BlockingIOError, TimeoutError):
                        break
                stopped = _stop_group(process)
                return code if code >= 0 else 128 - code, "exited", stopped
            reason = state.expired()
            if recovery_deadline is not None and time.monotonic() >= recovery_deadline:
                reason = "recovery"
            if reason:
                logger.error("HPC timeout phase=%s attempt=%s business_success=%s manifest_ok=%s",
                             reason, os.environ["HPC_ATTEMPT_ID"],
                             state.result_known and state.job["manifest"]["is_success"],
                             state.manifest_ok)
                return state.timeout_code(), reason, _stop_group(process)


def bootstrap(script: str) -> None:
    """Run before business imports, preserving the existing four CLI commands."""
    global _channel, timeouts  # pylint: disable=global-statement
    try:
        timeouts = Timeouts.from_environment()
    except (ValueError, TypeError) as exc:
        logger.error("invalid HPC timeout configuration: %s", exc)
        raise SystemExit(64) from exc
    if _CONTROL_FD in os.environ:
        _channel = socket.socket(fileno=int(os.environ.pop(_CONTROL_FD)))
        _channel.settimeout(1)
        if _RECOVERY in os.environ:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
            raise SystemExit(_recover(json.loads(os.environ.pop(_RECOVERY))))
        return
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    os.environ.setdefault("HPC_ATTEMPT_ID", uuid.uuid4().hex)
    if not os.environ["HPC_ATTEMPT_ID"].strip() or len(os.environ["HPC_ATTEMPT_ID"]) > 128:
        logger.error("HPC_ATTEMPT_ID must contain between 1 and 128 characters")
        raise SystemExit(64)
    cancelled: list[int] = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda received, _frame: cancelled.append(received))
    state = State()
    code, reason, stopped = _monitor(str(Path(script).resolve()), state, cancelled)
    if stopped and not cancelled and state.job is not None and state.returned is None:
        spec = {"job": state.job, "result_known": state.result_known,
                "manifest_ok": state.manifest_ok, "cleanup_prefix": state.cleanup_prefix,
                "work_dirs": state.work_dirs, "started": sorted(state.started), "code": code,
                "error_type": "HpcTimeout" if reason != "exited" else "WorkerExited",
                "reason": f"worker ended: {reason}; exit code {code}"}
        recovered = State(job=state.job, result_known=state.result_known,
                          manifest_ok=state.manifest_ok)
        recovery_code, _, _ = _monitor(str(Path(script).resolve()), recovered, cancelled, spec)
        if cancelled:
            code = 128 + cancelled[0]
        elif state.result_known and state.job["manifest"]["is_success"]:
            code = 0 if recovered.manifest_ok else 3
        elif recovery_code in (0, 3) and not state.job["manifest"]["is_success"]:
            logger.error("HPC recovery returned inconsistent success; preserving failure")
        state.manifest_ok = recovered.manifest_ok
    logger.info("HPC supervisor exit=%s reason=%s attempt=%s manifest_ok=%s",
                code, reason, os.environ["HPC_ATTEMPT_ID"], state.manifest_ok)
    raise SystemExit(code)
