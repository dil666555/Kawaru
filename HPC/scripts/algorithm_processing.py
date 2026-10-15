# -*- coding: utf-8 -*-
"""
HPC 算法预处理作业（E-HPC INSTANT 容器）。

main：容器命令行入口，输入 --run-id / --imzml-path / --config-json / --output-path
（可选 --metadata-json）：从 OSS 下载 imzML/ibd 输入对（顺序流式下载，经条件 GET
If-Match、大小及 ETag 校验后原子替换目标文件）、执行 MassFlow 预处理管线
（基线校正/降噪/归一化/寻峰/峰对齐按配置启用）、输出 zarr 以 8 worker 并行上传
到 OSS 输出前缀；终态写 manifest.json 到 OSS，配置了回调环境变量时同时回调主服务
algorithm 端点（成功时带 out_path/output_size）。失败时尽力
清理已上传的部分输出对象；系统盘剩余空间不足时直接报 ENOSPC。
warm_up（--warm-up）：镜像构建期入口，用合成 imzML 跑一遍全算法管线，把 numba
编译缓存预写进镜像层。

退出码：0 成功 / 2 管线失败 / 3 manifest 上传失败 / 64 配置或凭证错误。

环境变量：
    OSS_BUCKET_NAME / OSS_ENDPOINT_URL            OSS bucket / endpoint（必填）
    ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET /
    ALIBABA_CLOUD_SECURITY_TOKEN                  提交方经作业环境变量传入的 STS 临时凭证（必填）；
                                                  INSTANT 单个环境变量值上限 256 字符，token 可
                                                  分块为 OSS_TOKEN_0..n 传入，本变量未设时按序号拼回
    ALGORITHM_FC_CALLBACK_URL / CALLBACK_TOKEN    可选回调；不设则只写 manifest
    OSS_RESULTS_PREFIX                            manifest 归档前缀，默认 hpc-algorithm
    WORK_ROOT                                     工作目录父目录，默认 /tmp（系统盘）
"""

import argparse
import errno
import json
import logging
import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path

import oss2
import requests
# 容器依赖由 HPC/image/requirements.txt 安装；本地检查环境可能未安装 pydantic。
from pydantic import BaseModel, Field, ValidationError # pylint: disable=E0401 # pyright: ignore[reportMissingImports]

logger = logging.getLogger()

# 上传固定 8 个 worker，分批提交避免一次性排队过多任务。
OSS_UPLOAD_MAX_WORKERS = 8
OSS_UPLOAD_SUBMIT_BATCH = 500
# 系统盘除输入下载量外预留的空间（结果 zarr、管线临时文件与 MassFlow 日志）
DISK_RESERVE_MB = 1024
OSS_REQUEST_TIMEOUT_SECONDS = 15
# Preprocessor numba 并行内核的线程上限

_thread_local = threading.local()


# ---------------------------------------------------------------------------
# 算法参数模型（与主服务 models/processing_models.py 保持一致）
# ---------------------------------------------------------------------------

class PeakPickParams(BaseModel):
    """峰检测参数。"""

    method: str = Field(default="diff")
    snr: float = Field(default=2.0, gt=0)
    return_type: str = Field(default="height")
    width: int = Field(default=5, ge=1)
    backend: str = Field(default="python")


class NormalizationParams(BaseModel):
    """归一化参数。"""

    method: str = Field(default="tic_numba")
    scale: float | None = None
    ref: float | None = None
    ref_tolerance: float = Field(default=0.1, ge=0)


class PeakAlignParams(BaseModel):
    """峰对齐参数。"""

    reference: object | None = None  # np.ndarray | None，默认 None 时自动计算
    tolerance: float | None = None
    units: str = Field(default="ppm")
    binfun: str = Field(default="min")
    binratio: float = Field(default=2.0, gt=0)
    min_count: int | None = None
    min_frequency: float | None = Field(default=0.01)


class NoiseReductionParams(BaseModel):
    """平滑降噪参数。"""

    method: str = Field(default="gaussian_numba")
    window: int = Field(default=5, gt=0)
    sd: float | None = None
    polyorder: int = Field(default=3, ge=0)
    deriv: int = Field(default=0, ge=0)
    delta: float = Field(default=1.0, gt=0)


class BaselineCorrectionParams(BaseModel):
    """基线校正参数。"""

    method: str = Field(default="locmin_numba")


class ProcessingAlgorithmsConfig(BaseModel):
    """预处理算法配置；字段为 None 时不执行对应算法。"""

    peak_pick: PeakPickParams | None = None
    normalization: NormalizationParams | None = None
    peak_align: PeakAlignParams | None = None
    noise_reduction: NoiseReductionParams | None = None
    baseline_correction: BaselineCorrectionParams | None = None


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

    parser = argparse.ArgumentParser(description="HPC 算法预处理作业")
    parser.add_argument("--run-id", help="算法处理 run id，终态上报用")
    parser.add_argument("--imzml-path", help="源 imzML 文件的 OSS 相对路径（ibd 按同名推导）")
    parser.add_argument("--config-json", help="算法配置 JSON 字符串（ProcessingAlgorithmsConfig）")
    parser.add_argument("--output-path", help="处理结果的 OSS 输出路径（如 processed/run_xxx.zarr）")
    parser.add_argument("--metadata-json", default="", help="可选，写入输出 zarr 的附加元数据 JSON 对象")
    parser.add_argument("--warm-up", action="store_true", help="镜像构建期预热：用合成数据跑一遍全算法管线")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # --- 构建期预热分支（--warm-up） -------------------------------------------

    if args.warm_up:
        warm_up()
        return 0

    # --- 入参与环境变量校验（配置类错误统一退出码 64） ------------------------

    try:
        run_id = int(args.run_id or "")
    except ValueError:
        run_id = 0
    imzml_path = (args.imzml_path or "").strip("/")
    output_path = (args.output_path or "").strip("/")
    try:
        config = ProcessingAlgorithmsConfig.model_validate_json(args.config_json or "")
    except ValidationError as exc:
        logger.error("invalid config_json: %s", exc)
        config = None
    metadata: dict | None = {}
    if args.metadata_json:
        try:
            metadata = json.loads(args.metadata_json)
        except json.JSONDecodeError:
            logger.error("invalid metadata_json: %r", args.metadata_json)
            metadata = None
    if (
        run_id <= 0
        or not imzml_path.endswith(".imzML")
        or not _valid_oss_rel_path(imzml_path)
        or not _valid_oss_rel_path(output_path)
        or config is None
        or not isinstance(metadata, dict)
    ):
        logger.error("invalid arguments: run_id=%r imzml_path=%r output_path=%r",
                     args.run_id, args.imzml_path, args.output_path)
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
    ibd_path = imzml_path[: -len(".imzML")] + ".ibd"
    logger.info("hpc-algorithm: run_id=%s, imzml=%s, output=%s", run_id, imzml_path, output_path)

    # --- 作业流水线：下载输入 → 预处理管线 → 上传输出 --------------------------

    try:
        imzml_meta = _stat_object(imzml_path)
        ibd_meta = _stat_object(ibd_path)
        logger.info("source files: imzML %.2fMiB, ibd %.2fMiB",
                    imzml_meta["size"] / 2**20, ibd_meta["size"] / 2**20)

        # 作业只有一块按规格分配的系统盘：目录按 run_id 隔离，空间不足直接失败
        work_parent = Path(os.environ.get("WORK_ROOT", "/tmp"))
        work_parent.mkdir(parents=True, exist_ok=True)
        _check_disk_space(work_parent, imzml_meta["size"] + ibd_meta["size"])
        work_root = work_parent / f"hpc-algorithm-{run_id}"
        work_root.mkdir(parents=True, exist_ok=True)
        # MassFlow 导入时会在当前目录建 logs/，先切到工作目录再触发导入
        os.chdir(work_root)
        input_dir = work_root / "input"
        input_dir.mkdir()
        temp_dir = work_root / "temp"
        temp_dir.mkdir()

        local_imzml = str(input_dir / Path(imzml_path).name)
        result_dir = work_root / "result.zarr"
        _download_object(imzml_path, imzml_meta, local_imzml, work_root)
        _download_object(ibd_path, ibd_meta, str(input_dir / Path(ibd_path).name), work_root)

        _run_algorithm_pipeline(local_imzml, config, str(result_dir), str(temp_dir), metadata)

        rel_paths = sorted(
            path.relative_to(result_dir).as_posix()
            for path in result_dir.rglob("*") if path.is_file()
        )
        uploaded_files, output_size = _upload_files_to_oss(str(result_dir), rel_paths, output_path)
        logger.info("output upload done: %d files %.2fMiB -> %s",
                    uploaded_files, output_size / 2**20, output_path)

        is_success, error_type, error_message = True, None, None
    except Exception as exc: # pylint: disable=broad-except
        logger.exception("algorithm pipeline failed: run_id=%s", run_id)
        is_success, error_type, error_message = False, type(exc).__name__, str(exc)
        output_size = None
        # 清理并行上传过程中可能已经写入的部分输出对象，避免半截 zarr 留在输出前缀下
        try:
            deleted_objects = _delete_oss_prefix(output_path)
            logger.info("cleaned up partial output: %s, deleted_objects=%d",
                        output_path, deleted_objects)
        except Exception: # pylint: disable=broad-except
            logger.exception("failed to clean up partial output: %s", output_path)

    # --- 终态上报与退出码 ------------------------------------------------------

    # manifest 总是上传（未配置回调时的完成标记）；回调可选、尽力而为。
    # 输出已上传成功时回调失败不能翻转终态。失败回调不带路径与大小。
    manifest_ok = _write_manifest(run_id, imzml_path, output_path, is_success,
                                  error_type, error_message, output_size)
    _notify_callback(
        run_id=run_id,
        is_success=is_success,
        out_path=output_path if is_success else None,
        output_size=output_size,
        error_type=error_type,
        error_message=error_message,
    )
    if not is_success:
        return 2
    logger.info("algorithm processing completed: run_id=%s", run_id)
    return 0 if manifest_ok else 3


# ---------------------------------------------------------------------------
# OSS 客户端、输入下载与磁盘检查
# ---------------------------------------------------------------------------

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


def _valid_oss_rel_path(path: str) -> bool:
    """OSS 相对路径安全校验：非空、无空段/./..、无反斜杠与 NUL。"""
    return (
        bool(path)
        and all(part not in {"", ".", ".."} for part in path.split("/"))
        and "\\" not in path and "\x00" not in path
    )


def _stat_object(key: str) -> dict:
    """HEAD 单个输入对象，返回大小与 ETag；对象不存在时报 FileNotFoundError。"""
    try:
        result = _get_thread_oss_bucket().head_object(key)
    except oss2.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"Object not found: oss://{os.environ['OSS_BUCKET_NAME']}/{key}") from None
    return {"key": key, "size": result.content_length, "etag": result.etag}


def _check_disk_space(work_root: Path, download_bytes: int) -> None:
    """系统盘剩余空间需覆盖下载量加预留；不足时提示调大作业磁盘规格。"""
    free = shutil.disk_usage(work_root).free
    needed = download_bytes + DISK_RESERVE_MB * 1024 * 1024
    if free < needed:
        raise OSError(errno.ENOSPC, f"{work_root} has {free / 2**20:.0f}MiB free but needs "
                                    f"{needed / 2**20:.0f}MiB; increase the job system disk size")


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
            raise IOError(f"Source changed during download: {key}")
        os.replace(tmp, dst)
    finally:
        tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# MassFlow 预处理管线
# ---------------------------------------------------------------------------

def _run_algorithm_pipeline(
    local_imzml: str,
    config: ProcessingAlgorithmsConfig,
    result_dir: str,
    temp_dir: str,
    metadata: dict,
) -> None:
    """执行 MassFlow 预处理管线，输出 zarr 写入 result_dir（由调用方上传）。"""
    # MassFlow 导入会创建 logs/，须等调用方切换到工作目录后再导入。
    from massflow.data_manager import MSDataManagerImzML # pylint: disable=import-outside-toplevel
    from massflow.preprocess import Preprocessor # pylint: disable=import-outside-toplevel

    dm = MSDataManagerImzML(filepath=local_imzml)
    dm.load_head_data()

    # 根据输入数据类型和算法配置动态决定输出格式。
    is_processed = getattr(dm.ms.meta, "processed")
    is_continuous = getattr(dm.ms.meta, "continuous")
    task_list = [
        name for name, algorithm_config in (
            ("peak_pick", config.peak_pick),
            ("peak_align", config.peak_align),
        ) if algorithm_config is not None
    ]
    should_output_spectra = (
        is_processed
        and "peak_align" not in task_list
    ) or (
        is_continuous
        and "peak_pick" in task_list
        and "peak_align" not in task_list
    )
    output_format = "spectra_zarr" if should_output_spectra else "ion_image_zarr"
    enabled_algorithms = [
        name
        for name, algorithm_config in (
            ("baseline_correction", config.baseline_correction),
            ("noise_reduction", config.noise_reduction),
            ("normalization", config.normalization),
            ("peak_pick", config.peak_pick),
            ("peak_align", config.peak_align),
        )
        if algorithm_config is not None
    ]
    logger.info("pipeline configured: input_type=%s output_format=%s algorithms=%s "
                "spectrum_count=%d",
                "processed" if is_processed else "continuous",
                output_format, enabled_algorithms, len(dm.ms))

    preprocessor = Preprocessor(
        dm,
        batch_size=256,
        output_format=output_format,
        output_path=result_dir,
        temp_dir=temp_dir,
        compute_mean_spectrum=True,
        compute_tic=True,
    )

    if config.baseline_correction is not None:
        preprocessor = preprocessor.baseline_correction(
            **config.baseline_correction.model_dump()
        )
    if config.noise_reduction is not None:
        preprocessor = preprocessor.noise_reduction(
            **config.noise_reduction.model_dump()
        )
    if config.normalization is not None:
        preprocessor = preprocessor.normalization(
            **config.normalization.model_dump()
        )
    if config.peak_pick is not None:
        preprocessor = preprocessor.peak_pick(
            **config.peak_pick.model_dump()
        )
    if config.peak_align is not None:
        preprocessor = preprocessor.peak_align(
            **config.peak_align.model_dump()
        )

    result_dm = preprocessor.start()

    if metadata:
        # 此处只输出两种 Zarr，均支持 add_metadata；start() 的返回注解包含其他格式。
        result_dm.add_metadata(metadata) # pyright: ignore[reportAttributeAccessIssue]

    dm.close()
    result_dm.close()


# ---------------------------------------------------------------------------
# 输出上传、失败清理与终态上报
# ---------------------------------------------------------------------------

def _upload_files_to_oss(local_root: str, rel_paths: list[str], oss_prefix: str) -> tuple[int, int]:
    """将本地目录中指定的文件按相对路径并行上传到 OSS 输出前缀。"""
    source_root = Path(local_root)
    normalized_prefix = oss_prefix.strip("/")

    def upload_one(rel_path: str) -> int:
        local_path = source_root / rel_path
        object_key = f"{normalized_prefix}/{Path(rel_path).as_posix()}"
        _get_thread_oss_bucket().put_object_from_file(object_key, str(local_path))
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


def _delete_oss_prefix(oss_prefix: str) -> int:
    """删除指定 OSS 前缀下的对象，供上传失败后的清理使用。"""
    bucket = _get_thread_oss_bucket()
    prefix = oss_prefix.rstrip("/") + "/"
    deleted_objects = 0
    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        bucket.delete_object(obj.key)
        deleted_objects += 1
    return deleted_objects


def _notify_callback(
    run_id: int,
    is_success: bool,
    out_path: str | None = None,
    output_size: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> bool:
    """回调主服务 algorithm 端点（成功带路径与大小）；未配置则跳过。"""
    callback_url = os.environ.get("ALGORITHM_FC_CALLBACK_URL")
    if not callback_url:
        return False
    body = {
        "run_id": run_id,
        "is_success": is_success,
        "out_path": out_path,
        "output_size": output_size,
        "error_type": error_type,
        "error_message": error_message,
    }
    for attempt in range(2):
        try:
            res = requests.post(
                callback_url,
                json=body,
                headers={"Content-Type": "application/json",
                         "X-FC-Token": os.environ.get("CALLBACK_TOKEN", "")},
                timeout=(3, 10),
            )
            status = res.status_code
            res.close()
            if status == 200:
                return True
            logger.error("algorithm callback failed: attempt=%d status=%s", attempt + 1, status)
        except requests.RequestException:
            logger.exception("algorithm callback request failed: attempt=%d", attempt + 1)
    return False


def _write_manifest(
    run_id: int,
    imzml_path: str,
    output_path: str,
    is_success: bool,
    error_type: str | None,
    error_message: str | None,
    output_size: int | None,
) -> bool:
    """上传终态 manifest.json；未配置回调时它是后端判断作业完成的标记（最后上传）。"""
    results_prefix = os.environ.get("OSS_RESULTS_PREFIX", "hpc-algorithm").strip("/")
    manifest = {
        "run_id": run_id,
        "imzml_path": imzml_path,
        "output_path": output_path,
        "is_success": is_success,
        "error_type": error_type,
        "error_message": error_message,
        "output_size": output_size,
        "image_git_sha": os.getenv("IMAGE_GIT_SHA"),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    key = f"{results_prefix}/{run_id}/manifest.json"
    try:
        _get_thread_oss_bucket().put_object(key, json.dumps(manifest, ensure_ascii=False))
    except Exception: # pylint: disable=broad-except
        # 网络上传失败只降级退出码，不能掩盖流水线本身的结果
        logger.exception("manifest upload failed: run_id=%s", run_id)
        return False
    logger.info("manifest uploaded: %s", key)
    return True


# ---------------------------------------------------------------------------
# 镜像构建期预热：合成 imzML + 全算法管线预编译 numba
# ---------------------------------------------------------------------------

def _write_synthetic_imzml(base_path: Path) -> Path:
    """生成迷你合成 continuous imzML（base_path 不带扩展名，产出 .imzML/.ibd 对）."""
    # 合成数据依赖只在镜像预热时加载。
    import numpy as np # pylint: disable=import-outside-toplevel
    from pyimzml.ImzMLWriter import ImzMLWriter # pylint: disable=import-outside-toplevel

    rng = np.random.default_rng(0)
    width, height, mz_count = 6, 5, 200
    mz = np.linspace(100, 500, mz_count)
    with ImzMLWriter(str(base_path), mode="continuous") as writer:
        for y in range(1, height + 1):
            for x in range(1, width + 1):
                # 底噪上叠加两个高信噪比高斯峰，让寻峰/对齐有真实结构
                intensities = rng.random(mz_count).astype(np.float32) * 0.5
                for center, amplitude in ((150.0, 8.0), (350.0, 12.0)):
                    peak = np.exp(-((mz - center) ** 2) / (2 * 2.0**2))
                    intensities = intensities + (peak * amplitude).astype(np.float32)
                writer.addSpectrum(mz, intensities, (x, y, 1))
    return base_path.with_suffix(".imzML")


def warm_up() -> None:
    """镜像构建期用合成数据跑一遍全算法管线，把 numba 编译缓存写进镜像层."""
    config = ProcessingAlgorithmsConfig(
        baseline_correction=BaselineCorrectionParams(),
        noise_reduction=NoiseReductionParams(),
        normalization=NormalizationParams(),
        peak_pick=PeakPickParams(),
        peak_align=PeakAlignParams(),
    )
    with tempfile.TemporaryDirectory() as directory:
        previous = os.getcwd()
        os.chdir(directory)  # MassFlow 导入时会在当前目录建 logs/
        try:
            _run_algorithm_pipeline(
                str(_write_synthetic_imzml(Path(directory) / "warmup")), config,
                str(Path(directory) / "result.zarr"), str(Path(directory) / "temp"), {},
            )
        finally:
            os.chdir(previous)


if __name__ == "__main__":
    raise SystemExit(main())
