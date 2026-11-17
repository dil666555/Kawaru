"""FC imzML 预览共用入口，与 HPC imzml_preview 的绘图和发布行为一致。

mode=feature（默认）：continuous 生成 UMAP、TIC、随机离子图，其余仅生成 TIC。
mode=tic：仅生成 TIC。小输入且本地盘空间足够时通过 SDK 缓存到临时目录，
大输入或空间不足时直接通过 OSSFS 读取；全部绘图完成后再通过 SDK 发布图片，
终态写入 manifest，并按模式回调主服务。

部署入口为 index.handler，Python 3.12，实例并发度设为 1。
"""

import base64
import errno
import hashlib
import json
import logging
import os
import random
import shutil
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import chdir, closing
from pathlib import Path

import oss2
import requests

logger = logging.getLogger(__name__)
PREVIEW_CMAPS = ("viridis", "inferno", "hot")
STORAGE_ROOT = Path(os.environ.get("STORAGE_ROOT", "/home/storage")).resolve()
WORK_ROOT = Path(os.environ.get("WORK_ROOT", "/tmp")).resolve()
LOCAL_CACHE_THRESHOLD_BYTES = int(os.environ.get("LOCAL_CACHE_THRESHOLD_BYTES", str(8 * 1024 ** 3)))
RESULTS_PREFIX = os.environ.get("OSS_RESULTS_PREFIX", "fc-preview")
PUBLISH_MAX_WORKERS = int(os.environ.get("PUBLISH_MAX_WORKERS", "8"))
DISK_RESERVE_MB = 1024
OSS_REQUEST_TIMEOUT_SECONDS = 15

if PUBLISH_MAX_WORKERS < 1:
    raise ValueError("PUBLISH_MAX_WORKERS must be positive")

_thread_local = threading.local()


def handler(event, context):  # pylint: disable=unused-argument
    """接收直接 JSON 或 HTTP 触发器包装，按 mode 生成并发布预览。"""
    try:
        body = _parse_event(event)
        file_id = body.get("file_id")
        mode = body.get("mode", "feature")
        imzml_path = body.get("imzml_path")
        image_dest = body.get("image_dest")
        if isinstance(file_id, bool) or not isinstance(file_id, int) or file_id <= 0:
            raise ValueError("file_id must be a positive integer")
        if mode not in ("feature", "tic"):
            raise ValueError("mode must be feature or tic")
        if not isinstance(imzml_path, str) or not imzml_path.endswith(".imzML"):
            raise ValueError("imzml_path must be a relative .imzML key")
        ibd_path = body.get("ibd_path") or imzml_path.removesuffix(".imzML") + ".ibd"
        if not isinstance(ibd_path, str) or not ibd_path.endswith(".ibd"):
            raise ValueError("ibd_path must be a relative .ibd key")
        if not isinstance(image_dest, str):
            raise TypeError("image_dest must be a relative directory key")
        image_dest = image_dest.rstrip("/")
        if not all(_valid_oss_rel_path(path) for path in (
            imzml_path, ibd_path, image_dest, RESULTS_PREFIX,
        )):
            raise ValueError("paths must be non-empty relative OSS keys without . or ..")
        manifest_key = f"{RESULTS_PREFIX}/{mode}/{file_id}/manifest.json"
        if any(key.startswith(image_dest + "/") for key in (imzml_path, ibd_path, manifest_key)):
            raise ValueError("image destination must not contain inputs or the manifest")
    except (ValueError, TypeError) as exc:
        return _fc_response(400, str(exc))

    logger.info("fc-preview started: file_id=%s mode=%s", file_id, mode)
    preview_info = {}
    output_size = None
    output_file_count = None
    try:
        imzml_meta = _stat_object(imzml_path)
        ibd_meta = _stat_object(ibd_path)
        WORK_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=f"fc-preview-{file_id}-", dir=WORK_ROOT) as directory:
            work_root = Path(directory)
            # MassFlow 导入会创建 logs/；随本次调用的工作目录清理。
            with chdir(work_root):
                local_imzml = _prepare_input(imzml_meta, ibd_meta, work_root)
                output = work_root / "images"
                output.mkdir()
                preview_info = _generate_preview(local_imzml, output, mode, file_id)
                output_file_count, output_size = _publish_images(output, image_dest, preview_info["images"])
        is_success, error_type, error_message = True, None, None
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("preview failed: file_id=%s mode=%s", file_id, mode)
        is_success, error_type, error_message = False, type(exc).__name__, str(exc)

    manifest = {
        "file_id": file_id,
        "mode": mode,
        "imzml_path": imzml_path,
        "ibd_path": ibd_path,
        "image_dest": image_dest + "/",
        "image_path": image_dest + "/" if is_success else None,
        "output_size": output_size if is_success else None,
        "output_file_count": output_file_count if is_success else None,
        "is_success": is_success,
        "error_type": error_type,
        "error_message": error_message,
        "image_git_sha": os.environ.get("IMAGE_GIT_SHA"),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **preview_info,
    }
    manifest_ok = _report_result(manifest_key, manifest)
    status_code = 500 if not is_success else (200 if manifest_ok else 502)
    return _fc_response(status_code, {
        "status": "error" if not is_success else ("ok" if manifest_ok else "manifest_failed"),
        "file_id": file_id,
        "mode": mode,
        "image_path": manifest["image_path"],
        "manifest_key": manifest_key,
        "error": error_message,
    })


# ---------------------------------------------------------------------------
# MassFlow 绘图：与 HPC/scripts/imzml_preview.py 保持一致
# ---------------------------------------------------------------------------


def _pick_cmap(file_id: int) -> str:
    """与 HPC 一致，按 file_id 确定性选取 feature 预览配色。"""
    digest = hashlib.md5(str(file_id).encode("utf-8")).digest()
    return PREVIEW_CMAPS[digest[0] % len(PREVIEW_CMAPS)]


def _generate_preview(local_imzml: Path, output: Path, mode: str, file_id: int) -> dict:
    """沿用 HPC 的图片命名、配色及 ±0.1 Da 随机离子窗口。"""
    from massflow.data_manager import MSDataManagerImzML  # pylint: disable=import-outside-toplevel
    from massflow.tools.plot_ion_image import plot_ion_image, plot_tic_image  # pylint: disable=import-outside-toplevel

    with closing(MSDataManagerImzML(filepath=str(local_imzml))) as dm:
        dm.load_head_data()
        continuous = bool(dm.ms.meta.continuous)
        cmap = _pick_cmap(file_id) if mode == "feature" else random.choice(PREVIEW_CMAPS)
        if mode == "feature" and continuous:
            from massflow.segmentation import plot_umap_image  # pylint: disable=import-outside-toplevel

            # 抽样 10% 拟合，再分批映射全部谱；不写回输入，也不保存分析矩阵。
            plot_umap_image(dm, output_path=str(output / "preview.jpg"), sample_ratio=0.1)
            plot_tic_image(dm, output_path=str(output / "preview_2.jpg"),
                           cmap=cmap)
            mz_axis = dm.ms.shared_mz_list
            if mz_axis is None or len(mz_axis) == 0:
                raise ValueError("shared m/z list is empty; cannot pick a random ion")
            mz = float(mz_axis[random.randrange(len(mz_axis))])
            plot_ion_image(dm, mz=mz, mz_tolerance=0.1,
                           output_path=str(output / "preview_3.jpg"),
                           structure_only=True, show=False, cmap=cmap)
            return {
                "continuous": continuous,
                "images": ["preview.jpg", "preview_2.jpg", "preview_3.jpg"],
                "random_ion_mz": mz,
            }
        plot_tic_image(dm, output_path=str(output / "preview.jpg"),
                       cmap=cmap)
        return {"continuous": continuous, "images": ["preview.jpg"], "random_ion_mz": None}


# ---------------------------------------------------------------------------
# 输入缓存、OSSFS 读取与图片发布
# ---------------------------------------------------------------------------


def _valid_oss_rel_path(path: str) -> bool:
    """OSS 相对路径安全校验：非空、无空段/./..、无反斜杠与 NUL。"""
    return (
        bool(path)
        and all(part not in {"", ".", ".."} for part in path.split("/"))
        and "\\" not in path and "\x00" not in path
    )


def _get_thread_oss_bucket() -> oss2.Bucket:
    """按线程创建并复用 OSS Bucket 实例；STS 凭证与 bucket/endpoint 均从环境变量读取。"""
    credentials = (
        os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
        os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
        os.environ["ALIBABA_CLOUD_SECURITY_TOKEN"],
    )
    bucket = getattr(_thread_local, "oss_bucket", None)
    if bucket is None or getattr(_thread_local, "oss_credentials", None) != credentials:
        auth = oss2.StsAuth(*credentials)
        bucket = oss2.Bucket(
            auth, os.environ["OSS_ENDPOINT_URL"], os.environ["OSS_BUCKET_NAME"],
            connect_timeout=OSS_REQUEST_TIMEOUT_SECONDS,
        )
        _thread_local.oss_bucket = bucket
        _thread_local.oss_credentials = credentials
    return bucket


def _stat_object(key: str) -> dict:
    """HEAD 单个输入对象，返回大小与 ETag；对象不存在时报 FileNotFoundError。"""
    try:
        result = _get_thread_oss_bucket().head_object(key)
    except oss2.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"Object not found: oss://{os.environ['OSS_BUCKET_NAME']}/{key}") from None
    return {"key": key, "size": result.content_length, "etag": result.etag}


def _prepare_input(imzml_meta: dict, ibd_meta: dict, work_root: Path) -> Path:
    """小输入优先缓存；大输入或本地空间不足时用同名链接直接读取 OSSFS。"""
    local_imzml = work_root / "input.imzML"
    local_ibd = local_imzml.with_suffix(".ibd")
    total = imzml_meta["size"] + ibd_meta["size"]
    free = shutil.disk_usage(work_root).free
    if total < LOCAL_CACHE_THRESHOLD_BYTES and total + DISK_RESERVE_MB * 1024 * 1024 <= free:
        logger.info("caching input pair locally: bytes=%d free=%d", total, free)
        try:
            _download_object(imzml_meta["key"], imzml_meta, str(local_imzml), work_root)
            _download_object(ibd_meta["key"], ibd_meta, str(local_ibd), work_root)
        except OSError as exc:
            if exc.errno != errno.ENOSPC:
                raise
            logger.warning("local cache ran out of disk space; reading input through OSSFS")
            local_imzml.unlink(missing_ok=True)
            local_ibd.unlink(missing_ok=True)
        else:
            return local_imzml

    logger.info("reading input pair through OSSFS: bytes=%d free=%d root=%s", total, free, STORAGE_ROOT)
    # 本地仅建链接，不复制输入；不同 basename 的远端输入也能按 MassFlow 的同名规则配对。
    local_imzml.symlink_to(STORAGE_ROOT / imzml_meta["key"])
    local_ibd.symlink_to(STORAGE_ROOT / ibd_meta["key"])
    return local_imzml


def _download_object(key: str, meta: dict, dst: str, work_dir: Path) -> None:
    """流式下载单个对象；经 If-Match/大小/ETag 校验后原子替换目标文件，不产生半截文件。"""
    fd, temporary = tempfile.mkstemp(dir=work_dir)
    os.close(fd)
    tmp = Path(temporary)
    try:
        result = _get_thread_oss_bucket().get_object_to_file(
            key, str(tmp),
            headers={"If-Match": f'"{meta["etag"]}"'},
        )
        st = tmp.stat()
        if st.st_size != meta["size"] or result.etag != meta["etag"]:
            raise OSError(f"Source changed during download: {key}")
        os.replace(tmp, dst)
    finally:
        tmp.unlink(missing_ok=True)


def _oss_request[**P, R](operation: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
    """沿用 FC 的 OSS 重试规则，只用于幂等的发布、列举和删除操作。"""
    for attempt in range(2):
        try:
            return operation(*args, **kwargs)
        except (oss2.exceptions.RequestError, oss2.exceptions.InconsistentError):
            pass
        except oss2.exceptions.ServerError as exc:
            if not (exc.status in (408, 429) or 500 <= exc.status < 600):
                raise
        logger.warning("OSS request retry: operation=%s next_attempt=%d", operation.__name__, attempt + 2)
        time.sleep(0.5 * 2**attempt)
    return operation(*args, **kwargs)


def _clear_oss_prefix(prefix: str) -> None:
    """每次最多列举并删除 1000 个对象，不保存完整对象清单。"""
    bucket = _get_thread_oss_bucket()
    while True:
        page = _oss_request(bucket.list_objects_v2, prefix=prefix + "/", max_keys=1000)
        keys = [obj.key for obj in page.object_list]
        if not keys:
            return
        result = _oss_request(bucket.batch_delete_objects, keys)
        if set(result.deleted_keys) != set(keys):
            raise RuntimeError("OSS prefix cleanup did not confirm all deleted objects")


def _publish_images(source: Path, prefix: str, images: list[str]) -> tuple[int, int]:
    """只发布绘图返回的 1 或 3 张图片；失败后清理已发布的部分图片。"""
    def upload_one(name: str) -> int:
        path = source / name
        _oss_request(_get_thread_oss_bucket().put_object_from_file, f"{prefix}/{name}", str(path))
        return path.stat().st_size

    _clear_oss_prefix(prefix)
    try:
        with ThreadPoolExecutor(max_workers=PUBLISH_MAX_WORKERS) as executor:
            total_size = sum(executor.map(upload_one, images))
    except Exception:  # pylint: disable=broad-except
        # executor 已等待在途上传结束，清理后不会再有线程写回图片。
        try:
            _clear_oss_prefix(prefix)
        except Exception:  # pylint: disable=broad-except
            logger.exception("failed to clean partial output: %s", prefix)
        raise
    return len(images), total_size


# ---------------------------------------------------------------------------
# 事件解析与终态上报
# ---------------------------------------------------------------------------


def _parse_event(event) -> dict:
    """解析控制台直接事件、HTTP body 及 base64 编码的 body。"""
    body = json.loads(event) if isinstance(event, (str, bytes)) else event
    if not isinstance(body, dict):
        raise TypeError("event must be a JSON object")
    if "body" in body:
        encoded = body.get("isBase64Encoded")
        body = body["body"]
        if encoded:
            body = base64.b64decode(body, validate=True).decode("utf-8")
        if isinstance(body, (str, bytes)):
            body = json.loads(body)
    if not isinstance(body, dict):
        raise TypeError("body must be a JSON object")
    return body


def _fc_response(status_code: int, body) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "isBase64Encoded": False,
        "body": json.dumps(body, ensure_ascii=False),
    }


def _report_result(manifest_key: str, manifest: dict) -> bool:
    """先发布 manifest，再沿用对应 FC 任务的回调变量和 file_id/image_path 字段。"""
    manifest_ok = True
    try:
        _oss_request(_get_thread_oss_bucket().put_object, manifest_key,
                     json.dumps(manifest, ensure_ascii=False))
    except (oss2.exceptions.OssError, requests.RequestException):
        logger.exception("manifest upload failed: %s", manifest_key)
        manifest_ok = False
    callback_variable = "FEATURE_FC_CALLBACK_URL" if manifest["mode"] == "feature" else "TIC_FC_CALLBACK_URL"
    callback_url = os.environ.get(callback_variable)
    if callback_url:
        body = {name: manifest[name] for name in (
            "file_id", "is_success", "image_path", "error_type", "error_message",
        )}
        for attempt in range(2):
            try:
                with requests.post(
                    callback_url, json=body,
                    headers={"Content-Type": "application/json",
                             "X-FC-Token": os.environ.get("CALLBACK_TOKEN", "")},
                    timeout=(3, 10),
                ) as response:
                    if response.status_code == 200:
                        break
                    logger.error("preview callback failed: attempt=%d status=%s", attempt + 1, response.status_code)
            except requests.RequestException:
                logger.exception("preview callback request failed: attempt=%d", attempt + 1)
    return manifest_ok
