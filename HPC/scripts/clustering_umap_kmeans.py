# -*- coding: utf-8 -*-
"""
HPC UMAP/KMeans 聚类作业（E-HPC INSTANT 容器）。

main：容器命令行入口，输入 --run-id / --source-zarr-path：列举源 zarr、并发下载
（默认跳过 ion_image/intensity 数据块，元数据仍下载）、丢弃下载文件的 page cache
（cgroup 配额按 进程+缓存 总量计费，见 _drop_file_cache）、UMAP 结果写回源 zarr、
快照差异增量回传；终态写 manifest.json 到 OSS（peak_rss 之外附进程 RSS 构成的
采样高水位与主缺页数），配置了回调环境变量时同时回调主服务 clustering_callback
端点。
warm_up（--warm-up）：镜像构建期入口，用合成数据集真实跑一遍 UMAP，把 numba 编译
缓存和 matplotlib 字体缓存预写进镜像层。

下载与上传均固定使用 8 个 worker；下载以 64 个对象为一批，单个对象经条件 GET
（If-Match）、大小及 ETag 校验后原子替换目标文件，不产生半截文件；系统盘剩余空间
不足时直接报 ENOSPC。

退出码：0 成功 / 2 UMAP 流水线失败 / 3 manifest 上传失败 / 124 外层 IO 超时 / 64 配置或凭证错误。

环境变量：
    OSS_BUCKET_NAME / OSS_ENDPOINT_URL            OSS bucket / endpoint（必填）
    ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET /
    ALIBABA_CLOUD_SECURITY_TOKEN                  提交方经作业环境变量传入的 STS 临时凭证（必填）；
                                                  INSTANT 单个环境变量值上限 256 字符，token 可
                                                  分块为 OSS_TOKEN_0..n 传入，本变量未设时按序号拼回
    CLUSTERING_FC_CALLBACK_URL / CALLBACK_TOKEN   可选回调；不设则只写 manifest
    OSS_RESULTS_PREFIX                            manifest 归档前缀，默认 hpc-clustering
    WORK_ROOT                                     工作目录父目录，默认 /tmp（系统盘）
    OSS_DOWNLOAD_EXCLUDE_ARRAYS                   默认 ion_image/intensity；空串为完整下载
    UMAP_FULL_MATRIX_MB                           可选固定矩阵预算（MiB）：正值直接生效；
                                                  未设或非正值时按作业内存自动反推
    MEMORY_LIMIT_MB                               可选：显式指定可用内存（MiB）；默认读
                                                  cgroup 限制，取不到按 2048
"""

# Supervision must start before importing calculation dependencies.
if __name__ == "__main__":
    from hpc_runtime import bootstrap
    bootstrap(__file__)

import argparse
import errno
import logging
import math
import os
import resource
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from pathlib import Path

import hpc_runtime as runtime
import oss2
from hpc_io import bucket, oss_request, report_result, valid_path
from hpc_io import objects as oss_objects

logger = logging.getLogger()

# 峰值内存模型（实测标定的上包络，全量/抽样两条路径均适用）：
#     peak_mib ≈ UMAP_MEM_BASE_MB + UMAP_MEM_PER_MATRIX_MB × M
# M 为被拟合矩阵的 MiB（全量=完整矩阵，抽样=样本矩阵）。系数随 massflow/umap
# 版本锁定（当前 0.1.2 / 0.5.12）；升级依赖后用 manifest 里的 peak_rss_mib 复核。
UMAP_MEM_BASE_MB = 815
UMAP_MEM_PER_MATRIX_MB = 5.2
# 自动模式的安全系数：只按可用内存的 85% 反推矩阵预算，余量吸收标定平台差异、
# 运行噪声（实测 ±4%）与高维数据的固定读取瞬态。
UMAP_MEMORY_SAFETY = 0.85
# 下载时跳过数据 chunk 的数组（zarr 根内相对路径）。
# ion_image/intensity 是与 spectra/intensity 相同数据的 ion-major 副本：UMAP 流程只读
# spectra 视图与 axes/mz、axes/coordinates，但 reader 打开时的布局校验要求 ion_image
# 元数据完整并读 ion_image/offsets 首尾值，因此只跳过该数组的 chunk，元数据始终下载。
# 置空环境变量可恢复完整复制。
OSS_DOWNLOAD_EXCLUDE_ARRAYS = frozenset(
    p.strip().strip("/")
    for p in os.environ.get("OSS_DOWNLOAD_EXCLUDE_ARRAYS", "ion_image/intensity").split(",")
    if p.strip()
)
_UNUSED_ARRAYS = frozenset({"ion_image/intensity"})
_ZARR_METADATA_FILES = frozenset({"zarr.json", ".zarray", ".zattrs", ".zgroup", ".zmetadata"})
# 下载与回传都固定使用 8 个 worker，分批提交避免一次性排队过多任务。
OSS_DOWNLOAD_MAX_WORKERS = 8
OSS_DOWNLOAD_SUBMIT_BATCH = 64
OSS_UPLOAD_MAX_WORKERS = 8
OSS_UPLOAD_SUBMIT_BATCH = 500
# 系统盘除下载量外预留的空间（UMAP 输出、临时文件与 MassFlow 日志）
DISK_RESERVE_MB = 1024



def _reassemble_security_token() -> None:
    """INSTANT 作业环境变量单值上限 256 字符：STS token 由提交方分块为 OSS_TOKEN_0..n
    传入时，在 ALIBABA_CLOUD_SECURITY_TOKEN 未设的情况下按序号拼回整段。"""
    if os.environ.get("ALIBABA_CLOUD_SECURITY_TOKEN"):
        return
    parts = []
    while f"OSS_TOKEN_{len(parts)}" in os.environ:
        parts.append(os.environ[f"OSS_TOKEN_{len(parts)}"])
    if parts:
        os.environ["ALIBABA_CLOUD_SECURITY_TOKEN"] = "".join(parts)


def main() -> int:
    """容器命令行入口；返回进程退出码（见模块 docstring）。"""

    # --- 命令行参数解析与日志配置 ---------------------------------------------

    parser = argparse.ArgumentParser(description="HPC UMAP/KMeans 聚类作业")
    parser.add_argument("--run-id", help="算法 run id，终态上报用")
    parser.add_argument("--source-zarr-path", help="源 zarr 的 OSS 相对路径（也是 OSS key 前缀，算法 run 的 out_path）")
    parser.add_argument("--warm-up", action="store_true", help="镜像构建期预热：用合成数据跑一遍 UMAP，预编译 numba 缓存")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # --- 构建期预热分支（--warm-up） --------------------------------------------

    if args.warm_up:
        warm_up()
        return 0

    # --- 入参与环境变量校验（配置类错误统一退出码 64） --------
    try:
        run_id = int(args.run_id or "")
    except ValueError:
        run_id = 0
    oss_prefix = (args.source_zarr_path or "").rstrip("/")
    if run_id <= 0 or (
        not oss_prefix.endswith(".zarr")
        or any(part in {"", ".", ".."} for part in oss_prefix.split("/"))
        or "\\" in oss_prefix or "\x00" in oss_prefix
    ):
        logger.error("invalid arguments: run_id=%r source_zarr_path=%r",
                     args.run_id, args.source_zarr_path)
        return 64
    results_prefix = os.environ.get("OSS_RESULTS_PREFIX", "hpc-clustering")
    manifest_key = f"{results_prefix}/{run_id}/manifest.json"
    if not valid_path(results_prefix) or manifest_key.startswith(oss_prefix + "/"):
        logger.error("manifest must use a valid prefix outside the source Zarr")
        return 64
    _reassemble_security_token()
    missing = [
        name for name in (
            "OSS_BUCKET_NAME", "OSS_ENDPOINT_URL",
            "ALIBABA_CLOUD_ACCESS_KEY_ID", "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
            "ALIBABA_CLOUD_SECURITY_TOKEN",
        ) if not os.environ.get(name)
    ]
    if missing:
        logger.error("missing required environment variables: %s", ", ".join(missing))
        return 64
    logger.info("hpc-clustering: run_id=%s, source=%s", run_id, oss_prefix)
    runtime.configure_job("clustering", manifest_key, {
        "run_id": run_id, "source_zarr_path": oss_prefix, "is_success": False,
        "error_type": None, "error_message": None,
        "image_git_sha": os.environ.get("IMAGE_GIT_SHA"),
    }, "CLUSTERING_FC_CALLBACK_URL")
    # RSS 构成高水位采样尽早启动：覆盖下载重读与 UMAP 分配全程
    _start_memory_sampler()

    # --- 作业流水线：列举源 zarr → 并发下载 → UMAP → 增量回传 -------------------

    sampling = None
    try:
        objects = _list_source_objects(oss_prefix)
        if not objects:
            raise FileNotFoundError(
                f"Source zarr not found: no objects under "
                f"oss://{os.environ['OSS_BUCKET_NAME']}/{oss_prefix}")
        source_size = sum(o["size"] for o in objects)
        logger.info("source zarr: %d objects, %.2fMB", len(objects), source_size / 1024 / 1024)

        # 数组 chunk 级下载排除：只跳排除数组的数据块，元数据仍下载
        download_objects, skipped_files, skipped_bytes = _split_download_objects(objects, oss_prefix)
        download_size = sum(o["size"] for o in download_objects)
        if skipped_files:
            logger.info(
                "download exclusion: skip %d chunk files %.2fMB of arrays [%s], "
                "%d objects %.2fMB to download",
                skipped_files, skipped_bytes / 1024 / 1024,
                ",".join(sorted(OSS_DOWNLOAD_EXCLUDE_ARRAYS)),
                len(download_objects), download_size / 1024 / 1024,
            )

        # 作业只有一块按规格分配的系统盘：目录按 run_id 隔离，空间不足直接失败
        work_parent = Path(os.environ.get("WORK_ROOT", "/tmp")).resolve()
        work_parent.mkdir(parents=True, exist_ok=True)
        _check_disk_space(work_parent, download_size)
        work_root = runtime.work_directory(work_parent, f"hpc-clustering-{run_id}-")
        # MassFlow 导入时会在当前目录建 logs/，先切到工作目录再触发导入
        os.chdir(work_root)
        local_source = str(work_root / Path(oss_prefix).name)

        _download_objects(download_objects, local_source, oss_prefix)
        # 下载的 page cache（含未回写脏页）计入 cgroup 配额；趁 UMAP 尚未分配
        # 大块内存前丢弃，避免与后续内存冲击叠加触发 OOM kill
        _drop_file_cache(local_source)

        # 下载后对本地副本做快照，作为增量回传的基线
        before_snapshot = _snapshot_dir(local_source)

        with runtime.calculation():
            sampling = _run_umap(local_source, work_root)

        # 增量回传：只上传新增/内容变化的文件（UMAP 为追加写，通常只有新增的 group）
        after_snapshot = _snapshot_dir(local_source)
        changed_files = _diff_snapshot(before_snapshot, after_snapshot)
        logger.info("zarr changed files: %d (total %d)", len(changed_files), len(after_snapshot))
        if changed_files:
            uploaded_files, uploaded_bytes = _upload_files_to_oss(
                local_source, changed_files, oss_prefix)
            logger.info("incremental upload done: %d files %.2fMB",
                        uploaded_files, uploaded_bytes / 1024 / 1024)
        else:
            logger.info("no changed files, skip upload")

        is_success, error_type, error_message = True, None, None
    except Exception as exc: # pylint: disable=broad-except
        logger.exception("umap analysis failed: run_id=%s", run_id)
        is_success, error_type, error_message = False, type(exc).__name__, str(exc)

    # --- 终态上报与退出码 -------------------------------------------------------

    # manifest 总是上传（未配置回调时的完成标记）；回调可选、尽力而为。
    # 数据已回传成功时回调失败不能翻转终态。
    manifest_ok = _write_manifest(run_id, oss_prefix, is_success, error_type, error_message, sampling)
    _notify_callback(run_id=run_id, is_success=is_success,
                     error_type=error_type, error_message=error_message)
    if not is_success:
        return 124 if error_type == "HpcTimeout" else 2
    logger.info("umap analysis completed: run_id=%s", run_id)
    return 0 if manifest_ok else 3


# ---------------------------------------------------------------------------
# 源数据列举、磁盘检查与并发下载
# ---------------------------------------------------------------------------

def _list_source_objects(oss_prefix: str) -> list[dict]:
    """按页列举对象并校验本地映射；元数据内存随对象数增长，不读取对象内容。"""
    objects = []
    for obj in oss_objects(oss_prefix):
        if obj.key.endswith("/"):
            continue
        rel = obj.key[len(oss_prefix) + 1:]
        if (
            not obj.key.startswith(f"{oss_prefix}/")
            or any(part in {"", ".", ".."} for part in rel.split("/"))
            or "\\" in rel or "\x00" in rel
        ):
            raise ValueError(f"Object key cannot be mapped to a local file: {obj.key!r}")
        objects.append({"key": obj.key, "size": obj.size, "etag": obj.etag.strip('"')})
    return objects


def _split_download_objects(objects: list[dict], oss_prefix: str) -> tuple[list[dict], int, int]:
    """只排除已核实不用读取的数组数据，保留 Zarr v2/v3 元数据。"""
    unsupported = OSS_DOWNLOAD_EXCLUDE_ARRAYS - _UNUSED_ARRAYS
    if unsupported:
        raise ValueError(f"Arrays are not verified safe to exclude: {sorted(unsupported)}")
    keys = {obj["key"] for obj in objects}
    has_spectra = any(
        f"{oss_prefix}/spectra/intensity/{name}" in keys
        for name in ("zarr.json", ".zarray")
    )
    if not OSS_DOWNLOAD_EXCLUDE_ARRAYS or not has_spectra:
        return objects, 0, 0
    kept = []
    skipped_files = skipped_bytes = 0
    for obj in objects:
        rel = obj["key"][len(oss_prefix) + 1:]
        excluded = (
            Path(rel).name not in _ZARR_METADATA_FILES
            and any(rel.startswith(f"{array}/") for array in OSS_DOWNLOAD_EXCLUDE_ARRAYS)
        )
        if excluded:
            skipped_files += 1
            skipped_bytes += obj["size"]
        else:
            kept.append(obj)
    return kept, skipped_files, skipped_bytes


def _check_disk_space(work_root: Path, download_bytes: int) -> None:
    """系统盘剩余空间需覆盖下载量加预留；不足时提示调大作业磁盘规格。"""
    free = shutil.disk_usage(work_root).free
    needed = download_bytes + DISK_RESERVE_MB * 1024 * 1024
    if free < needed:
        raise OSError(errno.ENOSPC, f"{work_root} has {free / 2**20:.0f}MiB free but needs "
                                    f"{needed / 2**20:.0f}MiB; increase the job system disk size")


def _download_objects(objects: list[dict], local_source: str, oss_prefix: str) -> None:
    """8 worker 并发下载；单个对象经 If-Match/大小/ETag 校验后原子替换目标文件。"""
    source_root = Path(local_source)
    source_root.mkdir(parents=True, exist_ok=True)
    # 临时文件位于 Zarr 之外，避免与合法对象名冲突
    partial_root = source_root.parent / ".download_parts"
    partial_root.mkdir()

    stop_batch = threading.Event()

    def download_one(obj: dict) -> int:
        if stop_batch.is_set():
            raise RuntimeError("Download batch cancelled after an object failed")
        dst = source_root / obj["key"][len(oss_prefix) + 1:]
        dst.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(dir=partial_root)
        os.close(fd)
        tmp = Path(temporary)
        try:
            result = oss_request(
                _get_thread_oss_bucket().get_object_to_file, obj["key"], str(tmp),
                headers={"If-Match": f'"{obj["etag"]}"'},
            )
            st = tmp.stat()
            if st.st_size != obj["size"] or result.etag != obj["etag"]:
                raise IOError(f"Source changed during download: {obj['key']}")
            os.replace(tmp, dst)
            return st.st_size
        finally:
            tmp.unlink(missing_ok=True)

    total_bytes = sum(obj["size"] for obj in objects)
    done_bytes = done_count = 0
    file_iterator = iter(objects)
    with ThreadPoolExecutor(max_workers=OSS_DOWNLOAD_MAX_WORKERS) as executor:
        while True:
            batch = list(islice(file_iterator, OSS_DOWNLOAD_SUBMIT_BATCH))
            if not batch:
                break
            futures = [executor.submit(download_one, obj) for obj in batch]
            first_error = None
            for future in as_completed(futures):
                try:
                    done_bytes += future.result()
                    done_count += 1
                except Exception as exc: #pylint: disable=broad-except
                    if first_error is None:
                        first_error = exc
                        stop_batch.set()
            if first_error is not None:
                raise first_error
            logger.info("download progress: %d/%d objects, %.2f/%.2fMiB",
                        done_count, len(objects), done_bytes / 2**20, total_bytes / 2**20)



def _drop_file_cache(root: str) -> None:
    """对本地目录全部文件 posix_fadvise(DONTNEED)，丢弃下载占用的 page cache。
    cgroup 的 memory.max 按 进程 + page cache 总量计费而 peak_rss 只含进程：
    下载 ~4.4GiB 后紧接 UMAP 的大块分配会叠加越限被 OOM kill（2026-10-20 生产
    标定首档的失败根因）。丢弃促使脏页尽早回写、缓存转可回收，后续分配冲击由
    reclaim 兜底；massflow 重读文件改为走磁盘，略慢但安全。"""
    if not hasattr(os, "posix_fadvise"):
        # 部分 Python 构建（如 python.org 的 macOS 版）未暴露 posix_fadvise；
        # 本地调试环境无 cgroup 配额压力，直接跳过
        return
    files = 0
    for path in Path(root).rglob("*"):
        if not path.is_file():
            continue
        fd = os.open(path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED) # type: ignore # pylint: disable=E1101
        finally:
            os.close(fd)
        files += 1
    logger.info("file cache dropped: %d files under %s", files, root)


# ---------------------------------------------------------------------------
# 抽样预算与 UMAP 计算
# ---------------------------------------------------------------------------

def _available_memory_mb() -> int:
    """作业可用内存（MiB）：MEMORY_LIMIT_MB 环境变量 > cgroup 限制 > 2048 兜底。"""
    override = os.environ.get("MEMORY_LIMIT_MB")
    if override:
        value = int(override)
        if value > 0:
            logger.info("memory limit: MEMORY_LIMIT_MB=%dMiB", value)
            return value
        logger.warning("ignoring non-positive MEMORY_LIMIT_MB=%r", override)
    # cgroup v2: memory.max（未设限制时为 "max"）；v1: memory.limit_in_bytes（未设时为天文数字）
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            text = Path(path).read_text().strip() # pylint: disable=unspecified-encoding
        except OSError:
            continue
        if text and text != "max":
            value = int(text)
            if value < 1 << 50:
                logger.info("memory limit: %s=%dMiB", path, value // 2**20)
                return value // 2**20
    logger.warning("memory limit unknown; assuming 2048MiB")
    return 2048


def _matrix_budget_mb() -> int:
    """全量矩阵预算（MiB）：UMAP_FULL_MATRIX_MB 为正值时直接生效（固定预算）；
    未设或非正值时按可用内存反推：预算 = (内存 × 0.85 − 底座) / 放大系数。
    """
    raw = os.environ.get("UMAP_FULL_MATRIX_MB", "")
    if raw:
        budget = int(raw)
        if budget > 0:
            logger.info("matrix budget: UMAP_FULL_MATRIX_MB=%dMiB (fixed)", budget)
            return budget
    available = _available_memory_mb()
    budget = max(int((available * UMAP_MEMORY_SAFETY - UMAP_MEM_BASE_MB)
                     / UMAP_MEM_PER_MATRIX_MB), 0)
    logger.info("matrix budget: %dMiB (auto from %dMiB memory, safety=%.2f)",
                budget, available, UMAP_MEMORY_SAFETY)
    return budget


def _select_sample_ratio(dm) -> dict:
    """使用 load_head_data 已加载的共享 m/z 轴，不加载完整强度矩阵。
    返回本次采样决策（写入 manifest，与 peak_rss_mib 并排供线上复核预算）。"""
    if dm.storage_mode != "continuous":
        raise ValueError("UMAP requires continuous spectra with a shared m/z axis")
    n_pixels = len(dm.ms)
    n_features = len(dm.ms.shared_mz_list)
    if n_pixels < 5 or n_features == 0:
        raise ValueError("3-D UMAP requires at least 5 spectra and a non-empty m/z axis")
    row_bytes = 4 * n_features
    budget_mb = _matrix_budget_mb()
    budget_rows = budget_mb * 2**20 // row_bytes
    if n_pixels <= budget_rows:
        sample_count = n_pixels
    else:
        if budget_rows < 5:
            raise ValueError("UMAP sample memory budget cannot fit 5 spectra")
        sample_count = budget_rows
    ratio = sample_count / n_pixels
    # MassFlow 用 ceil(N * ratio) 计算样本数；避免浮点舍入使预算多出一行。
    if sample_count < n_pixels and math.ceil(n_pixels * ratio) > sample_count:
        ratio = math.nextafter(ratio, 0.0)
    logger.info(
        "UMAP pixels=%d features=%d full=%.2fMiB budget=%dMiB samples=%d ratio=%.6f sample=%.2fMiB",
        n_pixels, n_features, n_pixels * row_bytes / 2**20, budget_mb,
        sample_count, ratio, sample_count * row_bytes / 2**20,
    )
    return {
        "pixels": n_pixels,
        "features": n_features,
        "full_matrix_mib": round(n_pixels * row_bytes / 2**20, 2),
        "budget_mib": budget_mb,
        "samples": sample_count,
        "ratio": ratio,
        "sample_matrix_mib": round(sample_count * row_bytes / 2**20, 2),
    }


def _run_umap(local_source: str, work_dir: Path) -> dict:
    """打开本地 zarr 副本执行 UMAP：结果写回副本内部（analysis/umap），并输出 RGB 空间图。
    返回本次采样决策；UMAP 失败时抛出，不返回。"""
    # MassFlow 导入会创建 logs/，须等调用方切换到工作目录后再导入。
    from massflow.data_manager import MSDataManagerZarr # pylint: disable=import-outside-toplevel
    from massflow.segmentation import plot_umap_image # pylint: disable=import-outside-toplevel

    dm = MSDataManagerZarr(filepath=local_source)
    try:
        dm.load_head_data()
        sampling = _select_sample_ratio(dm)
        plot_umap_image(
            dm,
            save_matrix="zarr",
            sample_ratio=sampling["ratio"],
            transform_batch_size=1024,
            output_path=str(work_dir / "umap_image.jpg"),
        )
    finally:
        dm.close()
    return sampling


# ---------------------------------------------------------------------------
# 快照差异、增量回传与终态上报
# ---------------------------------------------------------------------------

def _snapshot_dir(root: str) -> dict:
    """记录目录下所有文件的 {相对路径: (大小, mtime_ns)}，作为增量回传基线。"""
    snapshot = {}
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            fp = os.path.join(dirpath, name)
            st = os.stat(fp)
            snapshot[os.path.relpath(fp, root)] = (st.st_size, st.st_mtime_ns)
    return snapshot


def _diff_snapshot(before: dict, after: dict) -> list[str]:
    """求出新增或 (大小, mtime) 变化的相对路径。

    同路径被改写的元数据文件（如 .zattrs）也会被捕获；UMAP 为追加写，无需处理删除。
    """
    return sorted(rel for rel, meta in after.items() if before.get(rel) != meta)


def _get_thread_oss_bucket() -> oss2.Bucket:
    """复用共享 OSS 客户端；网络超时与重试统一管理。"""
    return bucket()


def _upload_files_to_oss(local_root: str, rel_paths: list[str], oss_prefix: str) -> tuple[int, int]:
    """将本地目录中指定的文件按相对路径并行上传回源 zarr 前缀（结果写回源 zarr）。"""
    source_root = Path(local_root)
    normalized_prefix = oss_prefix.strip("/")

    def upload_one(rel_path: str) -> int:
        local_path = source_root / rel_path
        object_key = f"{normalized_prefix}/{Path(rel_path).as_posix()}"
        oss_request(_get_thread_oss_bucket().put_object_from_file, object_key, str(local_path))
        return local_path.stat().st_size

    uploaded_files = 0
    uploaded_bytes = 0
    file_iterator = iter(rel_paths)
    with ThreadPoolExecutor(max_workers=OSS_UPLOAD_MAX_WORKERS) as executor:
        while True:
            batch = list(islice(file_iterator, OSS_UPLOAD_SUBMIT_BATCH))
            if not batch:
                break
            for file_size in executor.map(upload_one, batch):
                uploaded_files += 1
                uploaded_bytes += file_size
    return uploaded_files, uploaded_bytes


def _notify_callback(
    run_id: int,
    is_success: bool,
    error_type: str | None = None,
    error_message: str | None = None,
) -> bool:
    """回调主服务 clustering_callback 端点（只携带状态，不带路径/大小）；未配置则跳过。"""
    callback_url = os.environ.get("CLUSTERING_FC_CALLBACK_URL")
    if not callback_url:
        return False
    body = {
        "run_id": run_id,
        "is_success": is_success,
        "error_type": error_type,
        "error_message": error_message,
    }
    from hpc_io import notify_callback  # pylint: disable=import-outside-toplevel
    body["attempt_id"] = os.environ["HPC_ATTEMPT_ID"]
    return notify_callback("CLUSTERING_FC_CALLBACK_URL", body)


def _peak_rss_mib() -> float:
    """进程峰值 RSS（MiB）；macOS 的 ru_maxrss 单位是字节，Linux 是 KiB。"""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(rss / 2**20 if sys.platform == "darwin" else rss / 1024, 1)


# ---------------------------------------------------------------------------
# 进程内存口径：INSTANT 容器不暴露 cgroupfs（c3a1589 的 cgroup 采集生产实测为
# null；_available_memory_mb 同样读不到，才有 MEMORY_LIMIT_MB 透传），真实配额
# 口径拿不到。改采样 /proc/self/status 的 RSS 构成高水位：anon 峰值与
# peak_rss_mib（ru_maxrss 含文件页）之差即文件映射页
# ---------------------------------------------------------------------------

# 采样线程维护的 /proc/self/status 高水位（kB）：VmRSS / RssAnon / RssFile。
# 采样是下界：瞬时尖峰可能漏采
_proc_peak = {"VmRSS": 0, "RssAnon": 0, "RssFile": 0}


def _read_status_kb() -> dict[str, int]:
    """/proc/self/status 的 VmRSS/RssAnon/RssFile（kB）；不可读（非 Linux）返回空。"""
    try:
        text = Path("/proc/self/status").read_text() # pylint: disable=unspecified-encoding
    except OSError:
        return {}
    status = {}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        if key in ("VmRSS", "RssAnon", "RssFile"):
            status[key] = int(rest.split()[0])
    return status


def _start_memory_sampler(interval_seconds: float = 0.5) -> None:
    """守护线程采样进程 RSS 构成高水位；/proc/self/status 不可读则不启动。"""

    def sample() -> None:
        while True:
            for key, value in _read_status_kb().items():
                _proc_peak[key] = max(_proc_peak[key], value)
            time.sleep(interval_seconds)

    if not _read_status_kb():
        return
    threading.Thread(target=sample, daemon=True).start()


def _proc_memory_mib() -> dict | None:
    """manifest 的进程 RSS 构成（MiB）：采样高水位的总量与 anon/file 分项。
    不可读环境返回 None。"""
    if not _proc_peak["VmRSS"]:
        return None
    mib = 1024.0
    return {
        "rss_peak_mib": round(_proc_peak["VmRSS"] / mib, 1),
        "anon_peak_mib": round(_proc_peak["RssAnon"] / mib, 1),
        "file_peak_mib": round(_proc_peak["RssFile"] / mib, 1),
    }


def _write_manifest(
    run_id: int,
    oss_prefix: str,
    is_success: bool,
    error_type: str | None,
    error_message: str | None,
    sampling: dict | None = None,
) -> bool:
    """上传终态 manifest.json；未配置回调时它是后端判断作业完成的标记（最后上传）。
    sampling 为本次 UMAP 采样决策；作业在采样前失败时为 None。"""
    results_prefix = os.environ.get("OSS_RESULTS_PREFIX", "hpc-clustering").strip("/")
    manifest = {
        "run_id": run_id,
        "source_zarr_path": oss_prefix,
        "is_success": is_success,
        "error_type": error_type,
        "error_message": error_message,
        # 实测峰值内存：上线后用它复核峰值模型的 815 / 5.2 两个系数
        "peak_rss_mib": _peak_rss_mib(),
        # 采样决策：与 peak_rss_mib 并排，供线上复核矩阵预算是否合理
        "sampling": sampling,
        # 进程 RSS 构成高水位（INSTANT 容器无 cgroupfs，采不到配额口径）：
        # anon 峰值与 peak_rss_mib 之差即文件页；复核预算模型先看这组
        "proc_memory": _proc_memory_mib(),
        # 主缺页数：fadvise 后重读走磁盘的审计证据（~4.4GiB/4KiB ≈ 百万量级）
        "major_faults": resource.getrusage(resource.RUSAGE_SELF).ru_majflt,
        "image_git_sha": os.getenv("IMAGE_GIT_SHA"),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    key = f"{results_prefix}/{run_id}/manifest.json"
    return report_result(key, manifest, "clustering", "CLUSTERING_FC_CALLBACK_URL",
                         skip={"callback"})


# ---------------------------------------------------------------------------
# 镜像构建期预热：合成数据 + 跑一遍 UMAP 预编译 numba
# ---------------------------------------------------------------------------

def _write_synthetic_zarr(path: Path) -> Path:
    """生成一个迷你合成 MSI Zarr，供镜像构建期预热使用."""
    # 合成数据依赖只在镜像预热时加载，并在切换工作目录后导入 MassFlow。
    import numpy as np # pylint: disable=import-outside-toplevel
    from massflow.msi_zarr.writer import MSIZarrWriter # pylint: disable=import-outside-toplevel

    rng = np.random.default_rng(0)
    width, height, mz = 12, 10, np.linspace(100, 500, 40)
    writer = MSIZarrWriter(str(path), row_axis="ion", encoding="continuous", metadata=None,
                           width=width, height=height, include_spectra=True)
    for y in range(1, height + 1):
        for x in range(1, width + 1):
            writer.add_spectrum(rng.random(mz.size, dtype=np.float32) + (x > width // 2),
                                (x, y, 1), mz)
    writer.finalize()
    writer.close()
    return path


def warm_up() -> None:
    """镜像构建期跑一遍合成数据 UMAP，把 numba 编译缓存与 matplotlib 字体缓存写进镜像层."""
    with tempfile.TemporaryDirectory() as directory:
        previous = os.getcwd()
        os.chdir(directory)  # MassFlow 导入时会在当前目录建 logs/
        try:
            _run_umap(str(_write_synthetic_zarr(Path(directory) / "warmup.zarr")), Path(directory))
        finally:
            os.chdir(previous)


if __name__ == "__main__":
    raise SystemExit(runtime.worker_main(main))
