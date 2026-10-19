# -*- coding: utf-8 -*-
"""clustering_umap_kmeans 作业的镜像 E2E 测试：真实提交一条 INSTANT 作业，验证
workflow 构建并推送的镜像在云端能正常完成聚类。

链路：读本地凭证/配置 → STS AssumeRole（权限只放开基准数据集与本次 run 前缀）
→ 把基准 zarr 服务端复制到本次 run 前缀（聚类会把 UMAP 结果写回源 zarr 内部，
不能直接跑在共享数据集上）→ 用镜像的不可变 sha-<commit> tag 提交 CreateJob →
轮询 GetJob 至终态 → 校验 OSS 上的 manifest.json 与写回的 analysis/umap 结果。

用法（真实提交、真实计费；不设环境变量时用例自动跳过）：
    KAWARU_INSTANT_E2E=1 python HPC/tests/clustering_instant_tests.py

门禁必须在 shell 里 export 且值恰为 "1"（=0/false 均不触发）；skipUnless 在
import 期求值，写进 .env 不生效——有意为之，避免每次 discover 都真实提交。
"""

import json
import os
import unittest
from pathlib import PurePosixPath

import instant_support as support


@unittest.skipUnless(os.environ.get("KAWARU_INSTANT_E2E") == "1", "set KAWARU_INSTANT_E2E=1 to submit a real (billed) INSTANT job")
class ClusteringEndToEndTests(unittest.TestCase):
    """真实提交：数据集副本 → CreateJob → 轮询 → manifest 与写回结果校验。"""

    def test_clustering_job_end_to_end(self):
        """完整链路：数据集副本 → 提交 → 校验 manifest 与写回的 analysis 结果。"""
        support.load_env_file()
        config = support.load_config()
        image, expected_sha = support.resolve_image(config)
        run_id = support.new_run_id()
        run_prefix = support.run_prefix(config, run_id)
        dataset = config["zarr_dataset"]
        source = f"{run_prefix}/{PurePosixPath(dataset).name}"
        manifest_key = f"{run_prefix}/{run_id}/manifest.json"
        try:
            # 会话策略放开：读基准数据集 + 读写本次 run 前缀（含复制的 zarr 与 manifest）
            sts = support.make_sts_client(config["region"])
            creds = support.assume_oss_role(
                sts, config, run_id,
                support.oss_session_policy(config["oss_bucket"], [dataset, run_prefix], run_prefix))
            bucket = support.make_bucket(creds, config)
            copied = support.copy_prefix(bucket, dataset, source)
            self.assertGreater(copied, 0, f"no objects under {dataset}")
            # 基准 zarr 若自带 analysis/，会让写回断言变成假阳性：先清掉副本里的
            support.delete_prefix(bucket, f"{source}/analysis")
            env_vars = support.credential_env_vars(creds, {
                "OSS_BUCKET_NAME": config["oss_bucket"],
                "OSS_ENDPOINT_URL": config["oss_endpoint"],
                "OSS_RESULTS_PREFIX": run_prefix,
            })
            request = support.build_create_job_request(
                config, image,
                job_name=f"kawaru-clustering-{run_id}",
                description=f"Kawaru clustering E2E test run {run_id}",
                command=["python", "/app/clustering_umap_kmeans.py"],
                args=["--run-id", str(run_id), "--source-zarr-path", source],
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
        self.assertEqual(manifest["image_git_sha"], expected_sha,
                         "镜像内嵌的构建 commit 与被测 tag 不符（镜像漂移？）")
        self.assertGreater(manifest["peak_rss_mib"], 0)
        self.assertLess(manifest["peak_rss_mib"], config["resources"]["memory_gib"] * 1024)
        analysis = list(support.list_prefix(bucket, f"{source}/analysis"))
        self.assertTrue(analysis, "UMAP results were not written back into the zarr copy")
        print(json.dumps({
            "run_id": run_id, "job_id": job_id, "state": state, "image": image,
            "manifest": f"oss://{config['oss_bucket']}/{manifest_key}",
            "analysis_objects": len(analysis),
            "peak_rss_mib": manifest["peak_rss_mib"],
        }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
