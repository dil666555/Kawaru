"""imz_to_zarr 镜像 E2E：合成 continuous/processed 输入 → INSTANT → OSS Zarr。

使用现有 instant.local.json 和 .env，每个用例生成独立的小型输入对，不依赖真实
imzml_dataset。校验输出格式、Zarr 完成状态与布局、TIC、continuous 平均谱、
附加元数据、manifest、构建 SHA，以及源 imzML/ibd 的 ETag 未发生变化。
测试产物保留在本次 run 前缀下，便于排查，不写入已有基准数据。

用法（两个真实计费作业；必须在 shell 显式启用，否则跳过）：
    KAWARU_INSTANT_E2E=1 python HPC/tests/imz_to_zarr_instant_tests.py
"""

import json
import os
import unittest

import instant_support as support


@unittest.skipUnless(os.environ.get("KAWARU_INSTANT_E2E") == "1",
                     "set KAWARU_INSTANT_E2E=1 to submit real (billed) INSTANT jobs")
class ImzToZarrEndToEndTests(unittest.TestCase):
    """验证两种存储模式的转换入口与最终 OSS 产物。"""

    def test_continuous_conversion(self):
        """continuous 输入应产出 ion_image_zarr。"""
        self._run_conversion("continuous")

    def test_processed_conversion(self):
        """processed 输入应产出 spectra_zarr。"""
        self._run_conversion("processed")

    def _run_conversion(self, mode: str) -> None:
        from oss2.exceptions import OssError  # pylint: disable=import-outside-toplevel
        from requests import RequestException  # pylint: disable=import-outside-toplevel
        from Tea.exceptions import TeaException  # pylint: disable=import-outside-toplevel

        support.load_env_file()
        config = support.load_config()
        image, expected_sha = support.resolve_image(config)
        run_id = support.new_run_id()
        run_prefix = f"{support.run_prefix(config, run_id)}/imz-to-zarr/{mode}"
        output_path = f"{run_prefix}/output.zarr"
        manifest_key = f"{run_prefix}/{run_id}/manifest.json"
        metadata = {"hpc_e2e_run_id": run_id, "source": "hpc-conversion-test"}
        try:
            creds = support.assume_oss_role(
                support.make_sts_client(config["region"]), config, run_id,
                support.oss_session_policy(config["oss_bucket"], [run_prefix], run_prefix))
            bucket = support.make_bucket(creds, config)
            fixture = support.upload_imzml_fixture(bucket, f"{run_prefix}/input", mode)
            env_vars = support.credential_env_vars(creds, {
                "OSS_BUCKET_NAME": config["oss_bucket"],
                "OSS_ENDPOINT_URL": config["oss_endpoint"],
                "OSS_RESULTS_PREFIX": run_prefix,
            })
            request = support.build_create_job_request(
                config, image,
                job_name=f"kw-convert-{mode[0]}-{run_id}",
                description=f"Kawaru raw conversion E2E {mode} run {run_id}",
                command=["python", "/app/imz_to_zarr.py"],
                args=["--run-id", str(run_id), "--imzml-path", fixture["imzml_path"],
                      "--ibd-path", fixture["ibd_path"], "--output-path", output_path,
                      "--metadata-json", json.dumps(metadata)],
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
        self.assertEqual(manifest["run_id"], run_id)
        self.assertEqual(manifest["image_git_sha"], expected_sha)
        self.assertEqual(manifest["imzml_path"], fixture["imzml_path"])
        self.assertEqual(manifest["ibd_path"], fixture["ibd_path"])
        self.assertEqual(manifest["output_path"], output_path)
        expected_format = "ion_image_zarr" if mode == "continuous" else "spectra_zarr"
        self.assertEqual(manifest["output_format"], expected_format)
        self.assertGreater(manifest["output_size"], 0)
        outputs = support.list_prefix(bucket, output_path)
        self.assertGreater(len(outputs), 0)
        self.assertEqual(manifest["output_file_count"], len(outputs))

        root = support.get_object_json(bucket, f"{output_path}/zarr.json")
        self.assertEqual(root["attributes"]["write_state"], "complete")
        self.assertEqual(root["attributes"]["spatial_shape"], [10, 6])
        stored_metadata = support.get_object_json(bucket, f"{output_path}/metadata/zarr.json")
        for key, value in metadata.items():
            self.assertEqual(stored_metadata["attributes"][key], value)
        coordinates = support.get_object_json(bucket, f"{output_path}/axes/coordinates/zarr.json")
        self.assertEqual(coordinates["shape"], [fixture["spectrum_count"], 3])
        tic = support.get_object_json(bucket, f"{output_path}/stats/tic/zarr.json")
        self.assertEqual(tic["shape"], [fixture["spectrum_count"]])
        group = "ion_image" if mode == "continuous" else "spectra"
        layout = support.get_object_json(bucket, f"{output_path}/{group}/zarr.json")
        self.assertEqual(layout["attributes"]["row_axis"], "ion" if mode == "continuous" else "pixel")
        self.assertEqual(layout["attributes"]["encoding"], mode)
        intensity = support.get_object_json(bucket, f"{output_path}/{group}/intensity/zarr.json")
        self.assertEqual(intensity["shape"], [fixture["point_count"]])
        intensity_prefix = f"{output_path}/{group}/intensity/"
        self.assertTrue(any(key.startswith(intensity_prefix) and key != intensity_prefix + "zarr.json"
                            for key in outputs),
                        "intensity metadata exists but no data chunks were uploaded")
        if mode == "continuous":
            mean = support.get_object_json(bucket, f"{output_path}/stats/mean_spectrum/zarr.json")
            self.assertEqual(mean["shape"], [len(fixture["mz_axis"])])
        for key, etag in fixture["input_etags"].items():
            self.assertEqual(bucket.head_object(key).etag, etag, f"source was modified: {key}")
        print(json.dumps({"run_id": run_id, "job_id": job_id, "mode": mode,
                          "manifest": manifest_key, "output": output_path,
                          "output_file_count": len(outputs)}, indent=2))


if __name__ == "__main__":
    unittest.main()
