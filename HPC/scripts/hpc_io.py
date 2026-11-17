"""Bounded OSS requests, result reporting, and output-prefix validation.

Verified against oss2 2.19.1: object APIs have no internal retry; iterators do.
Transfer callbacks count SDK-consumed bytes, not server-side persistence.
"""

import json
import logging
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator

import hpc_runtime as runtime
import oss2
import requests
from urllib3.exceptions import TimeoutError as HttpTimeoutError

logger = logging.getLogger(__name__)
_thread_local = threading.local()


class HpcTimeout(TimeoutError):
    """An external IO operation exhausted its bounded attempts."""


def valid_path(path: str) -> bool:
    return (bool(path) and all(part not in {"", ".", ".."} for part in path.split("/"))
            and "\\" not in path and "\x00" not in path)


def validate_output(prefix: str, protected: tuple[str, ...]) -> None:
    """Validate the deletion boundary; the submitter must also guarantee ownership."""
    if not valid_path(prefix) or "/" not in prefix:
        raise ValueError("output must be a task-private directory, not a top-level prefix")
    if any(key == prefix or key.startswith(prefix + "/") for key in protected):
        raise ValueError("output prefix must not contain inputs or the manifest")


def bucket() -> oss2.Bucket:
    if not hasattr(_thread_local, "bucket"):
        auth = oss2.StsAuth(os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
                            os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
                            os.environ["ALIBABA_CLOUD_SECURITY_TOKEN"])
        # oss2 passes this public parameter straight to requests.Session.request.
        _thread_local.bucket = oss2.Bucket(
            auth, os.environ["OSS_ENDPOINT_URL"], os.environ["OSS_BUCKET_NAME"],
            connect_timeout=(min(5, runtime.timeouts.request), runtime.timeouts.transfer))
    return _thread_local.bucket


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, requests.Timeout, HttpTimeoutError)):
        return True
    if isinstance(exc, oss2.exceptions.RequestError):
        return _is_timeout(exc.exception)
    return any(_is_timeout(arg) for arg in exc.args if isinstance(arg, BaseException))


def oss_request(operation: Callable, *args, deadline: float | None = None, **kwargs):
    """Retry a single replayable request at most three times, with bounded progress."""
    name = operation.__name__
    transfer = name in {"get_object_to_file", "put_object_from_file"}
    limit = runtime.timeouts.transfer if transfer else runtime.timeouts.request
    for attempt in range(3):
        if deadline is not None and time.monotonic() >= deadline:
            raise HpcTimeout(f"{name} exhausted its reporting budget")
        token = uuid.uuid4().hex
        runtime.emit("request_start", token=token, name=name, limit=limit)
        last_count, last_sent = 0, 0.0

        def progress(consumed: int, _total: int | None, *, request_token: str = token) -> None:
            nonlocal last_count, last_sent
            now = time.monotonic()
            if consumed > last_count and now - last_sent >= 0.25:
                runtime.emit("request_progress", progress=True, token=request_token, bytes=consumed)
                last_count, last_sent = consumed, now

        try:
            if transfer:
                result = operation(*args, progress_callback=progress, **kwargs)
                if name == "get_object_to_file":
                    result.close()
                return result
            return operation(*args, **kwargs)
        except (oss2.exceptions.OssError, requests.RequestException) as exc:
            retryable = isinstance(exc, (oss2.exceptions.RequestError,
                                        oss2.exceptions.InconsistentError,
                                        requests.RequestException)) or exc.status in (408, 429) or exc.status >= 500
            if not retryable:
                raise
            if attempt == 2:
                if _is_timeout(exc):
                    raise HpcTimeout(f"{name} timed out after three attempts") from exc
                raise
            logger.warning("OSS retry operation=%s next_attempt=%s error=%s", name,
                           attempt + 2, type(exc).__name__)
        finally:
            runtime.emit("request_end", token=token)
        delay = 0.5 * 2**attempt
        if deadline is not None:
            delay = min(delay, max(0, deadline - time.monotonic()))
        time.sleep(delay)


def objects(prefix: str) -> Iterator:
    """One bounded request per page; no SDK iterator retry layer or full-data load."""
    continuation = ""
    while True:
        page = oss_request(bucket().list_objects_v2, prefix=prefix.rstrip("/") + "/",
                           continuation_token=continuation, max_keys=1000)
        yield from page.object_list
        if not page.is_truncated:
            return
        continuation = page.next_continuation_token


def clear_prefix(prefix: str) -> None:
    if not valid_path(prefix) or "/" not in prefix:
        raise ValueError("refusing to delete a top-level or invalid prefix")
    while True:
        page = oss_request(bucket().list_objects_v2, prefix=prefix + "/", max_keys=1000)
        keys = [obj.key for obj in page.object_list]
        if not keys:
            return
        result = oss_request(bucket().batch_delete_objects, keys)
        if set(result.deleted_keys) != set(keys):
            raise RuntimeError("OSS cleanup did not confirm all deleted objects")


# ---------------------------------------------------------------------------
# Terminal manifest and optional backend notification
# ---------------------------------------------------------------------------

def callback_body(manifest: dict, kind: str) -> dict:
    fields = ["is_success", "error_type", "error_message", "attempt_id"]
    if kind == "preview":
        fields += ["file_id", "mode", "image_path"]
    else:
        fields += ["run_id"]
        if kind != "clustering":
            fields += ["output_size"]
    body = {field: manifest.get(field) for field in fields}
    if kind in {"algorithm", "conversion"}:
        body["out_path"] = manifest["output_path"] + "/" if manifest["is_success"] else None
    return body


def notify_callback(variable: str, body: dict) -> bool:
    url = os.environ.get(variable)
    if not url:
        return False
    with runtime.phase("callback"):
        deadline = time.monotonic() + runtime.timeouts.callback
        for attempt in range(2):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                with requests.post(url, json=body,
                                   headers={"Content-Type": "application/json",
                                            "X-FC-Token": os.environ.get("CALLBACK_TOKEN", "")},
                                   timeout=(min(3, remaining), min(10, remaining)),
                                   stream=True, allow_redirects=False) as response:
                    if response.status_code == 200:
                        logger.info("HPC callback delivered attempt_id=%s", body.get("attempt_id"))
                        return True
                    logger.warning("HPC callback rejected attempt=%s status=%s", attempt + 1,
                                   response.status_code)
            except requests.RequestException as exc:
                logger.warning("HPC callback failed attempt=%s error=%s", attempt + 1,
                               type(exc).__name__)
    return False


def report_result(key: str, manifest: dict, kind: str, callback_variable: str,
                  *, skip: set[str] | None = None, committed: bool = False) -> bool:
    skip = skip or set()
    manifest = {**manifest, "attempt_id": os.environ["HPC_ATTEMPT_ID"],
                "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    job = {"kind": kind, "key": key, "manifest": manifest,
           "callback_variable": callback_variable}
    runtime.emit("result", job=job)
    if "manifest" not in skip:
        try:
            with runtime.phase("manifest"):
                oss_request(bucket().put_object, key, json.dumps(manifest, ensure_ascii=False),
                            deadline=time.monotonic() + runtime.timeouts.manifest)
            committed = True
            runtime.emit("manifest_committed")
        except (oss2.exceptions.OssError, requests.RequestException, HpcTimeout) as exc:
            logger.error("HPC manifest unconfirmed attempt_id=%s error=%s", manifest["attempt_id"],
                         type(exc).__name__)
    if "callback" not in skip:
        notify_callback(callback_variable, callback_body(manifest, kind))
    return committed
