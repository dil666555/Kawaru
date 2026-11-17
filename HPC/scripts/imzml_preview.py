"""HPC 上传后 imzML 预览作业，迁移 FC 的 feature 和 TIC 两种任务。

feature 模式：continuous 数据生成 UMAP、TIC、随机离子图，否则仅生成 TIC。
tic 模式：仅生成 TIC。输入顺序流式下载到本地，图片通过 OSS SDK 并发发布。
终态写入 manifest，可选通过 FEATURE_FC_CALLBACK_URL / TIC_FC_CALLBACK_URL 回调。
退出码：0 成功 / 2 绘图或发布失败 / 3 manifest 上传失败 / 124 外层 IO 超时 / 64 配置错误。
"""

# Supervision must start before importing calculation dependencies.
if __name__ == "__main__":
    from hpc_runtime import bootstrap
    bootstrap(__file__)

import argparse
import hashlib
import logging
import os
import random
import tempfile
import time
from contextlib import closing
from pathlib import Path

import hpc_runtime as runtime
from algorithm_processing import (
    _check_disk_space,
    _download_object,
    _reassemble_security_token,
    _stat_object,
    _valid_oss_rel_path,
)
from hpc_io import report_result, validate_output
from imz_to_zarr import _publish_directory

logger = logging.getLogger(__name__)
PREVIEW_CMAPS = ("viridis", "inferno", "hot")


def main() -> int:
    """下载 imzML/ibd，生成预览并在图片全部发布后上报终态。"""
    parser = argparse.ArgumentParser(description="HPC imzML 上传后预览图作业")
    parser.add_argument("--file-id", help="源文件 ID，正整数")
    parser.add_argument("--imzml-path", help="源 .imzML 文件的 OSS 相对 key")
    parser.add_argument("--ibd-path", help="源 .ibd 的 OSS key；默认按 imzML 同目录同名推导")
    parser.add_argument("--image-dest", help="本次任务独占的 OSS 图片目录，例如 images/<public_id>/")
    parser.add_argument("--mode", choices=("feature", "tic"), default="feature", help="默认 feature，tic 仅绘制 TIC")
    parser.add_argument("--warm-up", action="store_true", help="构建期预热 UMAP、TIC 和随机离子绘图")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.warm_up:
        warm_up()
        return 0

    imzml_path = args.imzml_path or ""
    ibd_path = args.ibd_path or imzml_path.removesuffix(".imzML") + ".ibd"
    image_dest = (args.image_dest or "").rstrip("/")
    results_prefix = os.environ.get("OSS_RESULTS_PREFIX", "hpc-preview")
    try:
        file_id = int(args.file_id or "")
        workers = int(os.environ.get("PUBLISH_MAX_WORKERS", "8"))
        if (
            file_id <= 0 or workers <= 0
            or not imzml_path.endswith(".imzML") or not ibd_path.endswith(".ibd")
            or not all(_valid_oss_rel_path(path) for path in (
                imzml_path, ibd_path, image_dest, results_prefix,
            ))
        ):
            raise ValueError("invalid file id, paths or publish worker count")
        manifest_key = f"{results_prefix}/{args.mode}/{file_id}/manifest.json"
        validate_output(image_dest, (imzml_path, ibd_path, manifest_key))
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

    callback_variable = "FEATURE_FC_CALLBACK_URL" if args.mode == "feature" else "TIC_FC_CALLBACK_URL"
    runtime.configure_job("preview", manifest_key, {
        "file_id": file_id, "mode": args.mode, "imzml_path": imzml_path, "ibd_path": ibd_path,
        "image_dest": image_dest + "/", "image_path": None, "output_size": None,
        "output_file_count": None, "is_success": False, "error_type": None,
        "error_message": None, "image_git_sha": os.environ.get("IMAGE_GIT_SHA"),
    }, callback_variable)
    started = time.perf_counter()
    preview_info: dict = {}
    output_size = None
    output_file_count = None
    try:
        imzml_meta = _stat_object(imzml_path)
        ibd_meta = _stat_object(ibd_path)
        work_parent = Path(os.environ.get("WORK_ROOT", "/tmp")).resolve()
        work_parent.mkdir(parents=True, exist_ok=True)
        _check_disk_space(work_parent, imzml_meta["size"] + ibd_meta["size"])
        work_root = runtime.work_directory(work_parent, f"hpc-preview-{file_id}-")
        previous = Path.cwd()
        # MassFlow 导入会创建 logs/，将其和输入、绘图产物一起放在作业临时目录。
        os.chdir(work_root)
        try:
            local_imzml = work_root / "input.imzML"
            _download_object(imzml_path, imzml_meta, str(local_imzml), work_root)
            _download_object(ibd_path, ibd_meta, str(local_imzml.with_suffix(".ibd")), work_root)
            output = work_root / "images"
            output.mkdir()
            with runtime.calculation():
                preview_info = _generate_preview(local_imzml, output, args.mode, file_id)
            output_file_count, output_size = _publish_directory(output, image_dest, workers)
        finally:
            os.chdir(previous)
        is_success, error_type, error_message = True, None, None
    except Exception as exc: # pylint: disable=broad-except
        logger.exception("preview failed: file_id=%s mode=%s", file_id, args.mode)
        is_success, error_type, error_message = False, type(exc).__name__, str(exc)

    manifest = {
        "file_id": file_id,
        "mode": args.mode,
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
    logger.info("preview finished: file_id=%s mode=%s success=%s files=%s seconds=%.3f",
                file_id, args.mode, is_success, output_file_count, time.perf_counter() - started)
    if not is_success:
        return 124 if error_type == "HpcTimeout" else 2
    return 0 if manifest_ok else 3


# ---------------------------------------------------------------------------
# MassFlow 绘图
# ---------------------------------------------------------------------------

def _pick_cmap(file_id: int) -> str:
    """与新版 FC 一致，按 file_id 确定性选取 feature 预览配色。"""
    digest = hashlib.md5(str(file_id).encode("utf-8")).digest()
    return PREVIEW_CMAPS[digest[0] % len(PREVIEW_CMAPS)]


def _generate_preview(local_imzml: Path, output: Path, mode: str, file_id: int) -> dict:
    """沿用 FC 的图片命名、配色及 ±0.1 Da 随机离子窗口。"""
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
# 终态上报
# ---------------------------------------------------------------------------

def _report_result(manifest_key: str, manifest: dict) -> bool:
    """先发布 manifest，再沿用对应 FC 任务的回调变量和 file_id/image_path 字段。"""
    callback_variable = "FEATURE_FC_CALLBACK_URL" if manifest["mode"] == "feature" else "TIC_FC_CALLBACK_URL"
    return report_result(manifest_key, manifest, "preview", callback_variable)


# ---------------------------------------------------------------------------
# 镜像构建期预热
# ---------------------------------------------------------------------------

def warm_up() -> None:
    """用 continuous/processed 合成输入预热三张图、TIC 入口及降级分支，不访问 OSS。"""
    import numpy as np  # pylint: disable=import-outside-toplevel
    from pyimzml.ImzMLWriter import ImzMLWriter  # pylint: disable=import-outside-toplevel

    with tempfile.TemporaryDirectory() as directory:
        previous = Path.cwd()
        os.chdir(directory)
        try:
            rng = np.random.default_rng(0)
            for storage_mode in ("continuous", "processed"):
                base = Path(directory) / storage_mode
                with ImzMLWriter(str(base), mode=storage_mode) as writer:
                    # 60 条谱确保 10% 抽样后有 6 条训练谱。
                    for y in range(1, 11):
                        for x in range(1, 7):
                            mz = np.linspace(100, 500, 32 + (x if storage_mode == "processed" else 0))
                            intensity = rng.random(mz.size).astype(np.float32) + 0.1
                            writer.addSpectrum(mz, intensity, (x, y, 1))
                for mode in ("feature", "tic"):
                    output = Path(directory) / f"{storage_mode}-{mode}"
                    output.mkdir()
                    _generate_preview(base.with_suffix(".imzML"), output, mode, file_id=1)
        finally:
            os.chdir(previous)


if __name__ == "__main__":
    raise SystemExit(runtime.worker_main(main))
