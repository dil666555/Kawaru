# -*- coding: utf-8 -*-
"""algorithm_processing 作业的镜像 E2E 测试：真实提交一条 INSTANT 作业，验证
workflow 构建并推送的镜像在云端能正常完成预处理。

链路：读本地凭证/配置 → STS AssumeRole（读权限只放开真实 imzML 所在目录与
本次 run 前缀、写权限只放开本次 run 前缀）→ 用镜像的不可变 sha-<commit> tag
提交 CreateJob → 轮询 GetJob 至终态 → 校验 OSS 上的 manifest.json 与上传的
输出 zarr。

输入数据：使用者预先上传到 OSS 的真实 imzML/ibd 对（同目录同名，如
ehpc-benchmark/test_data/sample.imzML 与 sample.ibd），key 填在
instant.local.json 的 imzml_dataset 字段；作业原位读取（不复制到 run 前缀，
作业不回写输入，STS 策略也无其写入权限）。未填该字段时本用例自动跳过。

用法（真实提交、真实计费；不设环境变量时用例自动跳过）：
    KAWARU_INSTANT_E2E=1 python HPC/tests/algorithm_instant_tests.py

门禁必须在 shell 里 export 且值恰为 "1"（=0/false 均不触发）；skipUnless 在
import 期求值，写进 .env 不生效——有意为之，避免每次 discover 都真实提交。
"""

import functools
import importlib.util
import json
import os
import sys
import unittest
from pathlib import PurePosixPath

import instant_support as support


@functools.cache
def job_module():
    """按路径加载 HPC/scripts/algorithm_processing.py 及其共享运行时：
    取其 ProcessingAlgorithmsConfig 构造 --config-json，保证提交的配置与作业模型同源。"""
    spec = importlib.util.spec_from_file_location(
        "algorithm_processing", support.TEST_DIR.parent / "scripts" / "algorithm_processing.py")
    assert spec and spec.loader, "failed to load ModuleSpec for algorithm_processing.py"
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(support.TEST_DIR.parent / "scripts"))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


@unittest.skipUnless(os.environ.get("KAWARU_INSTANT_E2E") == "1", "set KAWARU_INSTANT_E2E=1 to submit a real (billed) INSTANT job")
class AlgorithmEndToEndTests(unittest.TestCase):
    """真实提交：读取预上传的真实 imzML → CreateJob → 轮询 → manifest 与输出 zarr 校验。"""

    def test_algorithm_job_end_to_end(self):
        """完整链路：校验输入就位 → 提交 → 校验 manifest 与输出 zarr。"""
        support.load_env_file()
        config = support.load_config()
        if not config["imzml_dataset"]:
            self.skipTest("imzml_dataset not set in instant.local.json; "
                          "upload a real imzML/ibd pair to OSS and fill it in")
        image, expected_sha = support.resolve_image(config)
        run_id = support.new_run_id()
        run_prefix = support.run_prefix(config, run_id)
        imzml_key = config["imzml_dataset"]
        ibd_key = imzml_key[: -len(".imzML")] + ".ibd"  # 作业按同名推导 ibd
        input_dir = str(PurePosixPath(imzml_key).parent)
        output_path = f"{run_prefix}/output/{run_id}.zarr"
        manifest_key = f"{run_prefix}/{run_id}/manifest.json"
        job = job_module()
        try:
            # 会话策略放开：读 imzML 所在目录与本次 run 前缀（测试读回 manifest/输出），
            # 写（输出、manifest、失败清理）只放开本次 run 前缀——输入无写入权限
            sts = support.make_sts_client(config["region"])
            creds = support.assume_oss_role(
                sts, config, run_id,
                support.oss_session_policy(config["oss_bucket"], [input_dir, run_prefix], run_prefix))
            bucket = support.make_bucket(creds, config)
            # 提交前确认输入在位：否则会以一条计费作业的方式失败
            objects = support.list_prefix(bucket, input_dir)
            self.assertIn(imzml_key, objects,
                          f"{imzml_key} not found under oss://{config['oss_bucket']}")
            self.assertIn(ibd_key, objects, f"{ibd_key} not found (must sit alongside the imzML)")
            config_json = job.ProcessingAlgorithmsConfig(
                baseline_correction=job.BaselineCorrectionParams(),
                normalization=job.NormalizationParams(),
                peak_pick=job.PeakPickParams(),
            ).model_dump_json(exclude_none=True)
            env_vars = support.credential_env_vars(creds, {
                "OSS_BUCKET_NAME": config["oss_bucket"],
                "OSS_ENDPOINT_URL": config["oss_endpoint"],
                "OSS_RESULTS_PREFIX": run_prefix,
            })
            request = support.build_create_job_request(
                config, image,
                job_name=f"kawaru-algorithm-{run_id}",
                description=f"Kawaru algorithm E2E test run {run_id}",
                command=["python", "/app/algorithm_processing.py"],
                args=["--run-id", str(run_id), "--imzml-path", imzml_key,
                      "--config-json", config_json, "--output-path", output_path],
                env_vars=env_vars)
            job_id = support.submit_job(support.make_client(config["region"]), config, request)
        except Exception as exc: # pylint: disable=broad-except
            # from None 切断异常链：unittest 会把原异常作为上下文打印，SDK 报文可能回显 ACR 密码
            raise AssertionError(f"submission failed: {support.error_summary(exc)}") from None
        info = support.wait_for_job(support.make_client(config["region"]), job_id)
        state = info.get("Status")
        self.assertIn(state, support.SUCCESS_STATES,
                      f"job {job_id} ended in {state}: {support.failure_reasons(info)}")
        manifest = support.get_object_json(bucket, manifest_key)
        self.assertEqual(manifest["run_id"], run_id)
        self.assertTrue(manifest["is_success"],
                        f"error={manifest.get('error_type')}: {manifest.get('error_message')}")
        self.assertEqual(manifest["output_path"], output_path)
        self.assertGreater(manifest["output_size"], 0)
        self.assertEqual(manifest["image_git_sha"], expected_sha,
                         "镜像内嵌的构建 commit 与被测 tag 不符（镜像漂移？）")
        outputs = support.list_prefix(bucket, output_path)
        self.assertTrue(outputs, "output zarr was not uploaded")
        print(json.dumps({
            "run_id": run_id, "job_id": job_id, "state": state, "image": image,
            "input": f"oss://{config['oss_bucket']}/{imzml_key}",
            "manifest": f"oss://{config['oss_bucket']}/{manifest_key}",
            "output": f"oss://{config['oss_bucket']}/{output_path}",
            "output_objects": len(outputs),
            "output_size": manifest["output_size"],
        }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
