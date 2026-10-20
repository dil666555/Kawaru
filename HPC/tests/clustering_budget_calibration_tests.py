# -*- coding: utf-8 -*-
"""clustering_umap_kmeans 作业的矩阵预算标定扫描：固定 UMAP_FULL_MATRIX_MB 逐档
提交 INSTANT 作业，采集生产平台的 (M, peak_rss) 点列，用于复核峰值模型系数
（815 / 5.2；背景见 umap_memory_measurement.md 与仓库根 context.md 的讨论）。

链路与 clustering_instant_tests.py 相同（STS 收窄 → 前置断言基准 analysis/ 为空 →
提交 → 轮询 → 校验 manifest → 清理复原），差异仅两点：
  - 作业额外注入 UMAP_FULL_MATRIX_MB=<档位>，绕过按内存反推的自动预算；
    manifest.sampling.budget_mib 必须等于档位值，否则视为注入失败
  - 档位自小到大逐档执行，任一档失败即停止——峰值随预算单调，更高档只会更糟；
    已成功档位的逐档 JSON 与汇总表在停止前照常输出

默认档位 1537/1756/2049 MiB（floor((8192×0.85−815)/B) 在 B=4.0/3.5/3.0 的反推
值），均低于基准数据集全量矩阵 4897.65 MiB，走抽样路径。OOM 即边界数据点，损失
仅该档 manifest（进程被杀时不上传）。

2026-10-20 首跑实测：1537 档即被 cgroup OOM kill（executor events: exitcode 137）。
真实边界含 4.4 GiB 下载 page cache，ru_maxrss 口径的斜率推算不适用（详见仓库根
context.md 第 7 节）——manifest 记 cgroup 指标 + 下载后清缓存的口径修复落地前，
重跑大概率仍撞同一堵墙；届时首档复现与否本身就是缓存项大小的直接测量。

用法（真实提交、真实计费，3 档约 1 小时；不设环境变量时用例自动跳过）：
    KAWARU_INSTANT_CALIB=1 python HPC/tests/clustering_budget_calibration_tests.py

门禁必须在 shell 里 export 且值恰为 "1"（=0/false 均不触发）；skipUnless 在
import 期求值，写进 .env 不生效——与 E2E 同理，避免每次 discover 都真实提交。
重跑部分档位或同档重复（估计运行噪声）：
    KAWARU_CALIB_BUDGETS=1756,2049 KAWARU_INSTANT_CALIB=1 python ...
（逗号分隔，升序执行；同档写两次即两次独立测量）
"""

import json
import os
import sys
import unittest

import instant_support as support

# 默认档位（MiB）：floor((8192×0.85−815)/B) 在 B=4.0/3.5/3.0 的反推值。
# 首档 1537 比自动预算 1182 仅高 30%，风险最低；末档 2049 按当前隐含 B≈3.0 预测
# 峰值 ≈6962 MiB（85% 配额），与 E2E 首跑同等余量
DEFAULT_BUDGETS_MB = (1537, 1756, 2049)
# 峰值模型系数，与 HPC/scripts/clustering_umap_kmeans.py 的 UMAP_MEM_BASE_MB /
# UMAP_MEM_PER_MATRIX_MB 对齐（作业脚本不能在测试侧 import：massflow 只在镜像内）；
# 仅用于汇总展示，不影响作业行为
MEM_BASE_MB = 815
MEM_PER_MATRIX_MB = 5.2


def _budgets() -> list[int]:
    """档位列表：默认值，或 KAWARU_CALIB_BUDGETS（逗号分隔）；升序执行。"""
    raw = os.environ.get("KAWARU_CALIB_BUDGETS", "").strip()
    if not raw:
        return list(DEFAULT_BUDGETS_MB)
    budgets = sorted(int(value) for value in raw.split(","))
    if budgets[0] <= 0:
        raise ValueError("KAWARU_CALIB_BUDGETS must list positive MiB values")
    return budgets


def _failure_detail(client, bucket, manifest_key: str, job_id: str) -> str:
    """失败档的诊断补充：失败 manifest 的 error_type/error_message（SIGKILL 时
    manifest 不存在，据此可判"死于终态上报前"）与 executor events 的平台侧错误
    （含退出码，如 OOM kill 的 137）。尽力而为：诊断自身失败不改变失败结论。"""
    parts = []
    try:
        manifest = support.get_object_json(bucket, manifest_key)
        parts.append(f"manifest: error={manifest.get('error_type')}: {manifest.get('error_message')}")
    except Exception: # pylint: disable=broad-except
        parts.append("no manifest (killed before terminal report?)")
    try:
        from alibabacloud_ehpcinstant20230701.models import ( # pylint: disable=import-outside-toplevel,E0401 # pyright: ignore[reportMissingImports]
            ListExecutorEventsRequest, ListExecutorEventsRequestFilter)
        events = client.list_executor_events(ListExecutorEventsRequest(
            filter=ListExecutorEventsRequestFilter(job_id=job_id), page_size=50)).body.to_map()
        parts.extend(f"event: {event.get('Content')}" for event in events.get("ExecutorEventList") or []
                     if event.get("Level") == "Error")
    except Exception: # pylint: disable=broad-except
        pass
    return "".join(f"\n  {part}" for part in parts)


@unittest.skipUnless(os.environ.get("KAWARU_INSTANT_CALIB") == "1",
                     "set KAWARU_INSTANT_CALIB=1 to submit real (billed) calibration jobs")
class ClusteringBudgetCalibrationTests(unittest.TestCase):
    """固定预算逐档提交，采集生产 (M, peak) 点；任一档失败即止。"""

    def test_budget_calibration_sweep(self):
        """逐档：前置检查 → 固定预算提交 → 校验并采集 → 清理复原 → 汇总。"""
        support.load_env_file()
        config = support.load_config()
        image, expected_sha = support.resolve_image(config)
        dataset = config["zarr_dataset"]
        client = support.make_client(config["region"])
        results, failure = [], None
        for budget_mb in _budgets():
            run_id = support.new_run_id()
            run_prefix = support.run_prefix(config, run_id)
            manifest_key = f"{run_prefix}/{run_id}/manifest.json"
            try:
                # 会话策略与 E2E 相同：读基准数据集；写回与清理共用基准 analysis/；
                # manifest 落本次 run 前缀
                sts = support.make_sts_client(config["region"])
                creds = support.assume_oss_role(
                    sts, config, run_id,
                    support.oss_session_policy(config["oss_bucket"], [dataset, run_prefix],
                                               [f"{dataset}/analysis", run_prefix]))
                bucket = support.make_bucket(creds, config)
                # 前置：基准必须无 analysis/——上一档残留会让写回断言假阳性
                stale = support.list_prefix(bucket, f"{dataset}/analysis")
                self.assertFalse(
                    stale, f"benchmark carries {len(stale)} stale analysis/ objects; clean them first")
                env_vars = support.credential_env_vars(creds, {
                    "OSS_BUCKET_NAME": config["oss_bucket"],
                    "OSS_ENDPOINT_URL": config["oss_endpoint"],
                    "OSS_RESULTS_PREFIX": run_prefix,
                    # 固定预算生效时它不参与预算反推，仅如实透传 cgroup 限制留作记录
                    "MEMORY_LIMIT_MB": str(int(config["resources"]["memory_gib"] * 1024)),
                    # 本档矩阵预算：绕过自动反推，manifest.sampling.budget_mib 应等于它
                    "UMAP_FULL_MATRIX_MB": str(budget_mb),
                })
                request = support.build_create_job_request(
                    config, image,
                    job_name=f"kawaru-calibration-{run_id}",
                    description=f"Kawaru UMAP budget calibration {run_id} ({budget_mb} MiB)",
                    command=["python", "/app/clustering_umap_kmeans.py"],
                    args=["--run-id", str(run_id), "--source-zarr-path", dataset],
                    env_vars=env_vars)
                job_id = support.submit_job(client, config, request)
            except Exception as exc: # pylint: disable=broad-except
                # from None 切断异常链：unittest 会把原异常作为上下文打印，SDK 报文可能回显 ACR 密码
                raise AssertionError(f"submission failed: {support.error_summary(exc)}") from None
            info = support.wait_for_job(client, job_id, timeout_seconds=2400)
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
                sampling = manifest["sampling"]
                # 注入校验：固定预算必须原样生效，否则测的是自动预算、点列失真
                self.assertEqual(sampling["budget_mib"], budget_mb,
                                 "UMAP_FULL_MATRIX_MB did not take effect (manifest budget != tier)")
                self.assertGreater(manifest["peak_rss_mib"], 0)
                self.assertLess(manifest["peak_rss_mib"],
                                config["resources"]["memory_gib"] * 1024)
                analysis = list(support.list_prefix(bucket, f"{dataset}/analysis"))
                self.assertTrue(analysis, "UMAP results were not written back into the benchmark zarr")
                proc_memory = manifest.get("proc_memory")
                # 剖析数据是标定的产出本体：缺失即仪表失效（旧镜像或 /proc 被遮蔽）
                self.assertIsNotNone(proc_memory,
                                     "proc_memory missing - memory instrumentation not running")
                results.append((budget_mb, sampling, manifest["peak_rss_mib"], proc_memory))
                print(json.dumps({
                    "budget_mib": budget_mb, "run_id": run_id, "job_id": job_id,
                    "state": state, "image": image,
                    "manifest": f"oss://{config['oss_bucket']}/{manifest_key}",
                    "sampling": sampling, "peak_rss_mib": manifest["peak_rss_mib"],
                    "proc_memory": proc_memory,
                    "major_faults": manifest.get("major_faults"),
                }, indent=2, ensure_ascii=False), flush=True)
            except Exception as exc: # pylint: disable=broad-except
                # 峰值随预算单调：本档失败（如 OOM）时更高档只会更糟，就此打住；
                # 失败详情（含失败 manifest 与平台退出码）收尾统一 fail，成功档位照常输出
                failure = (f"budget {budget_mb} MiB (run {run_id}, job {job_id}): {exc}"
                           + _failure_detail(client, bucket, manifest_key, job_id))
                break
            finally:
                # 基准复原（同 E2E 的边界）：轮询超时不进此块——超时在 wait_for_job
                # 直接抛出，不会走到这里，避免与活作业竞态删除
                try:
                    removed = support.delete_prefix(bucket, f"{dataset}/analysis")
                except Exception as exc: # pylint: disable=broad-except
                    print(f"WARNING: benchmark cleanup failed ({support.error_summary(exc)}); "
                          "next run will fail the precondition check", file=sys.stderr, flush=True)
                else:
                    print(f"benchmark analysis/ restored: {removed} objects deleted", flush=True)

        # --- 汇总：(M, peak) 点列与旧模型的偏离 -----------------------------------
        print(f"\ncalibration points: {len(results)} "
              f"(compared against model {MEM_BASE_MB} + {MEM_PER_MATRIX_MB} x M)", flush=True)
        print("| budget MiB | samples | ratio | M MiB | peak MiB | anon pk MiB | file pk MiB | model MiB | model/actual | implied B |",
              flush=True)
        print("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |", flush=True)
        for budget_mb, sampling, peak, proc in results:
            matrix_mib = sampling["sample_matrix_mib"]
            predicted = MEM_BASE_MB + MEM_PER_MATRIX_MB * matrix_mib
            print(f"| {budget_mb} | {sampling['samples']} | {sampling['ratio']:.4f} "
                  f"| {matrix_mib} | {peak} | {proc['anon_peak_mib']} | {proc['file_peak_mib']} "
                  f"| {predicted:.1f} | {predicted / peak:.2f} "
                  f"| {(peak - MEM_BASE_MB) / matrix_mib:.3f} |", flush=True)
        if failure:
            self.fail(f"calibration sweep stopped early: {failure} "
                      f"({len(results)} tier(s) succeeded; table above)")


if __name__ == "__main__":
    unittest.main()
