# -*- coding: utf-8 -*-
"""clustering_umap_kmeans 作业的镜像 E2E 测试：真实提交一条 INSTANT 作业，验证
workflow 构建并推送的镜像在云端能正常完成聚类。

链路：读本地凭证/配置 → STS AssumeRole（权限放开基准数据集与本次 run 前缀）→
前置断言基准 analysis/ 为空 → 提交 CreateJob：作业原位跑基准数据集，UMAP 结果
写回其 analysis/，manifest 落 run 前缀 → 轮询 GetJob 至终态 → 校验 manifest 与
写回的 analysis 结果 → 删除 analysis/ 复原基准。

用法（真实提交、真实计费；不设环境变量时用例自动跳过）：
    KAWARU_INSTANT_E2E=1 python HPC/tests/clustering_instant_tests.py

门禁必须在 shell 里 export 且值恰为 "1"（=0/false 均不触发）；skipUnless 在
import 期求值，写进 .env 不生效——有意为之，避免每次 discover 都真实提交。
"""

import json
import os
import sys
import unittest

import instant_support as support


@unittest.skipUnless(os.environ.get("KAWARU_INSTANT_E2E") == "1", "set KAWARU_INSTANT_E2E=1 to submit a real (billed) INSTANT job")
class ClusteringEndToEndTests(unittest.TestCase):
    """真实提交：基准前置检查 → CreateJob 原位跑 → 轮询 → 校验 → 清理复原。"""

    def test_clustering_job_end_to_end(self):
        """完整链路：基准前置检查 → 原位提交 → 校验 manifest 与写回结果 → 清理复原。"""
        support.load_env_file()
        config = support.load_config()
        image, expected_sha = support.resolve_image(config)
        run_id = support.new_run_id()
        run_prefix = support.run_prefix(config, run_id)
        dataset = config["zarr_dataset"]
        manifest_key = f"{run_prefix}/{run_id}/manifest.json"
        try:
            # 会话策略放开：读基准数据集；作业写回与测试清理共用基准 analysis/；
            # manifest 落本次 run 前缀
            sts = support.make_sts_client(config["region"])
            creds = support.assume_oss_role(
                sts, config, run_id,
                support.oss_session_policy(config["oss_bucket"], [dataset, run_prefix],
                                           [f"{dataset}/analysis", run_prefix]))
            bucket = support.make_bucket(creds, config)
            # 前置：基准必须无 analysis/——上一轮残留会让写回断言假阳性、清理失去边界
            stale = support.list_prefix(bucket, f"{dataset}/analysis")
            self.assertFalse(
                stale, f"benchmark carries {len(stale)} stale analysis/ objects; clean them first")
            env_vars = support.credential_env_vars(creds, {
                "OSS_BUCKET_NAME": config["oss_bucket"],
                "OSS_ENDPOINT_URL": config["oss_endpoint"],
                "OSS_RESULTS_PREFIX": run_prefix,
                # 申请的内存配额透传给作业：容器内读不到 cgroup 限制时会退到 2048
                # 兜底，把采样预算错误压低数倍。单一事实来源是本次 CreateJob 的
                # TaskSpec.Resource.Memory，这里与配置保持一致
                "MEMORY_LIMIT_MB": str(int(config["resources"]["memory_gib"] * 1024)),
            })
            request = support.build_create_job_request(
                config, image,
                job_name=f"kawaru-clustering-{run_id}",
                description=f"Kawaru clustering E2E test run {run_id}",
                command=["python", "/app/clustering_umap_kmeans.py"],
                args=["--run-id", str(run_id), "--source-zarr-path", dataset],
                env_vars=env_vars)
            job_id = support.submit_job(support.make_client(config["region"]), config, request)
        except Exception as exc: # pylint: disable=broad-except
            # from None 切断异常链：unittest 会把原异常作为上下文打印，SDK 报文可能回显 ACR 密码
            raise AssertionError(f"submission failed: {support.error_summary(exc)}") from None
        info = support.wait_for_job(support.make_client(config["region"]), job_id)
        try:
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
            analysis = list(support.list_prefix(bucket, f"{dataset}/analysis"))
            self.assertTrue(analysis, "UMAP results were not written back into the benchmark zarr")
            print(json.dumps({
                "run_id": run_id, "job_id": job_id, "state": state, "image": image,
                "manifest": f"oss://{config['oss_bucket']}/{manifest_key}",
                "analysis_objects": len(analysis),
                "peak_rss_mib": manifest["peak_rss_mib"],
                "sampling": manifest.get("sampling"),
            }, indent=2, ensure_ascii=False))
        finally:
            # 基准复原：前置断言保证 analysis/ 原为空，此处内容必为本轮作业所写；
            # 清理失败不掩盖校验结果（残留会被下一轮前置断言拦下）。作业轮询超时
            # （仍在运行）不进此块，避免与活作业竞态删除
            try:
                removed = support.delete_prefix(bucket, f"{dataset}/analysis")
            except Exception as exc: # pylint: disable=broad-except
                print(f"WARNING: benchmark cleanup failed ({support.error_summary(exc)}); "
                      "next run will fail the precondition check", file=sys.stderr, flush=True)
            else:
                print(f"benchmark analysis/ restored: {removed} objects deleted", flush=True)


if __name__ == "__main__":
    unittest.main()
