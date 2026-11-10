"""imzml_preview 镜像 E2E：两种 imzML 存储模式 × feature/tic 两种预览模式。

每个用例合成独立的小型输入对，不依赖真实 imzml_dataset。校验三图生成、
processed 的 feature→TIC 降级、单独 TIC、JPEG 可解码、manifest、构建 SHA、
随机离子来自输入轴、源文件 ETag 未变。产物保留在本次 run 前缀下。

用法（四个真实计费作业；必须在 shell 显式启用，否则跳过）：
    KAWARU_INSTANT_E2E=1 python HPC/tests/imzml_preview_instant_tests.py
"""

import json
import os
import unittest
from contextlib import closing
from io import BytesIO

import instant_support as support


@unittest.skipUnless(os.environ.get("KAWARU_INSTANT_E2E") == "1",
                     "set KAWARU_INSTANT_E2E=1 to submit real (billed) INSTANT jobs")
class ImzmlPreviewEndToEndTests(unittest.TestCase):
    """验证预览分支及完整图片的 OSS 发布。"""

    def test_continuous_feature(self):
        """continuous feature 模式应生成 UMAP、TIC、随机离子三张图。"""
        self._run_preview("continuous", "feature")

    def test_processed_feature(self):
        """processed feature 模式应降级为单张 TIC。"""
        self._run_preview("processed", "feature")

    def test_continuous_tic(self):
        """continuous tic 模式应仅生成 TIC。"""
        self._run_preview("continuous", "tic")

    def test_processed_tic(self):
        """processed tic 模式应仅生成 TIC。"""
        self._run_preview("processed", "tic")

    def _run_preview(self, storage_mode: str, mode: str) -> None:
        from oss2.exceptions import OssError  # pylint: disable=import-outside-toplevel
        from PIL import Image  # pylint: disable=import-outside-toplevel
        from requests import RequestException  # pylint: disable=import-outside-toplevel
        from Tea.exceptions import TeaException  # pylint: disable=import-outside-toplevel

        support.load_env_file()
        config = support.load_config()
        image, expected_sha = support.resolve_image(config)
        file_id = support.new_run_id()
        run_prefix = f"{support.run_prefix(config, file_id)}/preview/{storage_mode}/{mode}"
        image_dest = f"{run_prefix}/images"
        manifest_key = f"{run_prefix}/{mode}/{file_id}/manifest.json"
        try:
            creds = support.assume_oss_role(
                support.make_sts_client(config["region"]), config, file_id,
                support.oss_session_policy(config["oss_bucket"], [run_prefix], run_prefix))
            bucket = support.make_bucket(creds, config)
            fixture = support.upload_imzml_fixture(bucket, f"{run_prefix}/input", storage_mode)
            env_vars = support.credential_env_vars(creds, {
                "OSS_BUCKET_NAME": config["oss_bucket"],
                "OSS_ENDPOINT_URL": config["oss_endpoint"],
                "OSS_RESULTS_PREFIX": run_prefix,
            })
            request = support.build_create_job_request(
                config, image,
                job_name=f"kw-preview-{storage_mode[0]}-{mode[0]}-{file_id}",
                description=f"Kawaru preview E2E {storage_mode}/{mode} run {file_id}",
                command=["python", "/app/imzml_preview.py"],
                args=["--file-id", str(file_id), "--imzml-path", fixture["imzml_path"],
                      "--ibd-path", fixture["ibd_path"], "--image-dest", image_dest + "/",
                      "--mode", mode],
                env_vars=env_vars)
            job_id = support.submit_job(support.make_client(config["region"]), config, request)
        # 仅脱敏 SDK/网络异常，编程错误保留原始 traceback。
        except (TeaException, OssError, RequestException) as exc:
            raise AssertionError(f"submission failed: {support.error_summary(exc)}") from None

        info = support.wait_for_job(support.make_client(config["region"]), job_id)
        self.assertIn(info.get("Status"), support.SUCCESS_STATES,
                      f"job {job_id}: {support.failure_reasons(info)}")
        manifest = support.get_object_json(bucket, manifest_key)
        self.assertTrue(manifest["is_success"],
                        f"{manifest.get('error_type')}: {manifest.get('error_message')}")
        self.assertEqual(manifest["file_id"], file_id)
        self.assertEqual(manifest["mode"], mode)
        self.assertEqual(manifest["image_git_sha"], expected_sha)
        self.assertEqual(manifest["imzml_path"], fixture["imzml_path"])
        self.assertEqual(manifest["ibd_path"], fixture["ibd_path"])
        self.assertEqual(manifest["image_dest"], image_dest + "/")
        self.assertEqual(manifest["image_path"], image_dest + "/")
        self.assertIs(manifest["continuous"], storage_mode == "continuous")
        images = ["preview.jpg"]
        if storage_mode == "continuous" and mode == "feature":
            images.extend(["preview_2.jpg", "preview_3.jpg"])
            self.assertIn(manifest["random_ion_mz"], fixture["mz_axis"])
        else:
            self.assertIsNone(manifest["random_ion_mz"])
        self.assertEqual(manifest["images"], images)
        self.assertEqual(manifest["output_file_count"], len(images))
        objects = support.list_prefix(bucket, image_dest)
        self.assertEqual(set(objects), {f"{image_dest}/{name}" for name in images})
        output_size = 0
        for key in objects:
            with closing(bucket.get_object(key)) as response:
                data = response.read()
            output_size += len(data)
            with Image.open(BytesIO(data)) as preview:
                self.assertEqual(preview.format, "JPEG")
                self.assertGreater(preview.width, 0)
                self.assertGreater(preview.height, 0)
                preview.load()
        self.assertGreater(output_size, 0)
        self.assertEqual(manifest["output_size"], output_size)
        for key, etag in fixture["input_etags"].items():
            self.assertEqual(bucket.head_object(key).etag, etag, f"source was modified: {key}")
        print(json.dumps({"file_id": file_id, "job_id": job_id, "mode": mode,
                          "storage_mode": storage_mode, "manifest": manifest_key,
                          "images": images, "output_size": output_size}, indent=2))


if __name__ == "__main__":
    unittest.main()
