"""HPC 原始 imzML/ibd 转 Zarr 作业，不启用预处理算法。

从 OSS 下载输入对，在本地转换后有界并发发布到 OSS；最后写 manifest，
可选回调 RAW_CONVERT_FC_CALLBACK_URL。输出格式、TIC、平均谱和元数据沿用 FC。
退出码：0 成功 / 2 转换或发布失败 / 3 manifest 上传失败 / 64 配置错误。
--warm-up 在镜像构建时预热 continuous 和 processed 两条转换路径。
"""

import argparse
import json
import logging
import os
import tempfile
import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack, closing
from pathlib import Path

import oss2
import requests
from algorithm_processing import (
    _check_disk_space,
    _download_object,
    _get_thread_oss_bucket,
    _reassemble_security_token,
    _stat_object,
    _valid_oss_rel_path,
)

logger = logging.getLogger(__name__)


def main() -> int:
    """运行一次原始格式转换，凭证和存储配置与其他 HPC 作业一致。"""
    parser = argparse.ArgumentParser(description="HPC imzML/ibd 转 Zarr 作业")
    parser.add_argument("--run-id", help="原始转换 run id，正整数")
    parser.add_argument("--imzml-path", help="源 .imzML 文件的 OSS 相对 key")
    parser.add_argument("--ibd-path", help="源 .ibd 文件的 OSS key；默认按 imzML 同目录同名推导")
    parser.add_argument("--output-path", help="本次作业独占的 OSS 相对 .zarr 路径")
    parser.add_argument("--metadata-json", default="{}", help="写入 Zarr 的附加元数据 JSON 对象")
    parser.add_argument("--warm-up", action="store_true", help="构建期预热两种输入格式")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.warm_up:
        warm_up()
        return 0

    imzml_path = args.imzml_path or ""
    ibd_path = args.ibd_path or imzml_path.removesuffix(".imzML") + ".ibd"
    output_path = (args.output_path or "").rstrip("/")
    results_prefix = os.environ.get("OSS_RESULTS_PREFIX", "hpc-imz-to-zarr")
    try:
        run_id = int(args.run_id or "")
        metadata = json.loads(args.metadata_json)
        workers = int(os.environ.get("PUBLISH_MAX_WORKERS", "8"))
        if (
            run_id <= 0 or workers <= 0 or not isinstance(metadata, dict)
            or not imzml_path.endswith(".imzML") or not ibd_path.endswith(".ibd")
            or not output_path.endswith(".zarr") or Path(output_path).name == ".zarr"
            or not all(_valid_oss_rel_path(path) for path in (
                imzml_path, ibd_path, output_path, results_prefix,
            ))
        ):
            raise ValueError("invalid run id, paths, metadata or publish worker count")
        manifest_key = f"{results_prefix}/{run_id}/manifest.json"
        if any(key.startswith(output_path + "/") for key in (imzml_path, ibd_path, manifest_key)):
            raise ValueError("output prefix must not contain inputs or the manifest")
    except ValueError as exc:
        logger.error("invalid configuration: %s", exc)
        return 64
    _reassemble_security_token()
    missing = [name for name in (
        "OSS_BUCKET_NAME", "OSS_ENDPOINT_URL", "ALIBABA_CLOUD_ACCESS_KEY_ID",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET", "ALIBABA_CLOUD_SECURITY_TOKEN",
    ) if not os.environ.get(name)]
    if missing:
        logger.error("missing required environment variables: %s", ", ".join(missing))
        return 64

    started = time.perf_counter()
    output_format = None
    output_size = None
    output_file_count = None
    try:
        imzml_meta = _stat_object(imzml_path)
        ibd_meta = _stat_object(ibd_path)
        work_parent = Path(os.environ.get("WORK_ROOT", "/tmp")).resolve()
        work_parent.mkdir(parents=True, exist_ok=True)
        _check_disk_space(work_parent, imzml_meta["size"] + ibd_meta["size"])
        with tempfile.TemporaryDirectory(prefix=f"hpc-imz-to-zarr-{run_id}-", dir=work_parent) as directory:
            work_root = Path(directory)
            previous = Path.cwd()
            # MassFlow 导入会在 cwd 创建 logs/，随作业工作目录一起清理。
            os.chdir(work_root)
            try:
                local_imzml = work_root / "input.imzML"
                _download_object(imzml_path, imzml_meta, str(local_imzml), work_root)
                # MassFlow 按本地同名推导 ibd，远端 key 可由 --ibd-path 单独指定。
                _download_object(ibd_path, ibd_meta, str(local_imzml.with_suffix(".ibd")), work_root)
                result_dir = work_root / "result.zarr"
                output_format = _convert(local_imzml, result_dir, work_root / "temp", metadata)
                output_file_count, output_size = _publish_directory(result_dir, output_path, workers)
            finally:
                os.chdir(previous)
        is_success, error_type, error_message = True, None, None
    except Exception as exc: # pylint: disable=broad-except
        logger.exception("raw conversion failed: run_id=%s", run_id)
        is_success, error_type, error_message = False, type(exc).__name__, str(exc)

    manifest = {
        "run_id": run_id,
        "imzml_path": imzml_path,
        "ibd_path": ibd_path,
        "output_path": output_path,
        "output_format": output_format,
        "output_size": output_size if is_success else None,
        "output_file_count": output_file_count if is_success else None,
        "is_success": is_success,
        "error_type": error_type,
        "error_message": error_message,
        "image_git_sha": os.environ.get("IMAGE_GIT_SHA"),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    manifest_ok = _report_result(manifest_key, manifest)
    logger.info("raw conversion finished: run_id=%s success=%s format=%s bytes=%s files=%s seconds=%.3f",
                run_id, is_success, output_format, output_size, output_file_count,
                time.perf_counter() - started)
    if not is_success:
        return 2
    return 0 if manifest_ok else 3


# ---------------------------------------------------------------------------
# MassFlow 原始转换
# ---------------------------------------------------------------------------

def _convert(local_imzml: Path, output: Path, temp_dir: Path, metadata: dict) -> str:
    """continuous 输出 ion_image_zarr，其余输出 spectra_zarr，与 FC 保持一致。"""
    from massflow.data_manager import MSDataManagerImzML  # pylint: disable=import-outside-toplevel
    from massflow.preprocess import Preprocessor  # pylint: disable=import-outside-toplevel

    with ExitStack() as stack:
        dm = stack.enter_context(closing(MSDataManagerImzML(filepath=str(local_imzml))))
        dm.load_head_data()
        output_format = "ion_image_zarr" if dm.ms.meta.continuous is True else "spectra_zarr"
        result = stack.enter_context(closing(Preprocessor(
            dm,
            output_format=output_format,
            output_path=str(output),
            temp_dir=str(temp_dir),
            compute_mean_spectrum=True,
            compute_tic=True,
        ).start()))
        if metadata:
            result.add_metadata(metadata) # pyright: ignore[reportAttributeAccessIssue]
    return output_format


# ---------------------------------------------------------------------------
# OSS 发布与终态上报
# ---------------------------------------------------------------------------

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
    """每次最多列举并删除 1000 个对象，不保存完整 Zarr 文件清单。"""
    bucket = _get_thread_oss_bucket()
    while True:
        page = _oss_request(bucket.list_objects_v2, prefix=prefix + "/", max_keys=1000)
        keys = [obj.key for obj in page.object_list]
        if not keys:
            return
        result = _oss_request(bucket.batch_delete_objects, keys)
        if set(result.deleted_keys) != set(keys):
            raise RuntimeError("OSS prefix cleanup did not confirm all deleted objects")


def _publish_directory(source: Path, prefix: str, workers: int) -> tuple[int, int]:
    """流式遍历目录，最多保留 2 倍 worker 数的任务；发布失败清理部分输出。"""
    def upload_one(path: Path) -> int:
        key = prefix + "/" + path.relative_to(source).as_posix()
        _oss_request(_get_thread_oss_bucket().put_object_from_file, key, str(path))
        return path.stat().st_size

    pending = set()
    file_count = total_size = 0
    try:
        _clear_oss_prefix(prefix)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            try:
                for directory, _, filenames in os.walk(source):
                    for filename in filenames:
                        pending.add(executor.submit(upload_one, Path(directory) / filename))
                        if len(pending) >= workers * 2:
                            completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                            for future in completed:
                                total_size += future.result()
                                file_count += 1
                while pending:
                    completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in completed:
                        total_size += future.result()
                        file_count += 1
            except Exception:
                for future in pending:
                    future.cancel()
                raise
    except Exception:
        # 退出 executor 后所有在途 PUT 已结束，清理后不会再有 worker 写回。
        try:
            _clear_oss_prefix(prefix)
        except Exception: # pylint: disable=broad-except
            logger.exception("failed to clean partial output: %s", prefix)
        raise
    return file_count, total_size


def _report_result(manifest_key: str, manifest: dict) -> bool:
    """先写 manifest，再按 FC 的字段回调；回调失败不改变计算终态。"""
    manifest_ok = True
    try:
        _oss_request(_get_thread_oss_bucket().put_object, manifest_key,
                     json.dumps(manifest, ensure_ascii=False))
    except Exception: # pylint: disable=broad-except
        logger.exception("manifest upload failed: %s", manifest_key)
        manifest_ok = False
    callback_url = os.environ.get("RAW_CONVERT_FC_CALLBACK_URL")
    if callback_url:
        body = {name: manifest[name] for name in (
            "run_id", "is_success", "output_size", "error_type", "error_message",
        )}
        body["out_path"] = manifest["output_path"] + "/" if manifest["is_success"] else None
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
                    logger.error("raw conversion callback failed: attempt=%d status=%s",
                                 attempt + 1, response.status_code)
            except requests.RequestException:
                logger.exception("raw conversion callback request failed: attempt=%d", attempt + 1)
    return manifest_ok


# ---------------------------------------------------------------------------
# 镜像构建期预热
# ---------------------------------------------------------------------------

def warm_up() -> None:
    """用小型 continuous/processed 输入预热两种 Zarr 输出，不访问 OSS。"""
    import numpy as np  # pylint: disable=import-outside-toplevel
    from pyimzml.ImzMLWriter import ImzMLWriter  # pylint: disable=import-outside-toplevel

    with tempfile.TemporaryDirectory() as directory:
        previous = Path.cwd()
        os.chdir(directory)
        try:
            for mode in ("continuous", "processed"):
                base = Path(directory) / mode
                with ImzMLWriter(str(base), mode=mode) as writer:
                    for x in range(1, 5):
                        mz = np.linspace(100, 500, 32 + (x if mode == "processed" else 0))
                        writer.addSpectrum(mz, np.ones(mz.size, dtype=np.float32) * x, (x, 1, 1))
                _convert(base.with_suffix(".imzML"), base.with_suffix(".zarr"),
                         Path(directory) / f"temp-{mode}", {"source": "hpc-warmup"})
        finally:
            os.chdir(previous)


if __name__ == "__main__":
    raise SystemExit(main())
