# HPC 镜像使用说明

镜像提供两个一次性计算命令：**预处理**（imzML/ibd → 新 Zarr）和 **UMAP**（读取 Zarr，结果写回同一 Zarr）。两者分别启动，不会自动串联，执行结束后容器退出。

## 1. 启动镜像

当前最新已构建并推送的版本为 `sha-7478a0c5f458bdce4b7a69e4975c3503b1b1c23c`（2026-10-20，[构建记录](https://github.com/dil666555/Kawaru/actions/runs/39388510413)）。预处理和 UMAP 使用同一个镜像。

E-HPC INSTANT 同地域 VPC 使用：

```bash
IMAGE='crpi-y6a776c9l4k3agmh-vpc.cn-hongkong.personal.cr.aliyuncs.com/kawaru/sxo:sha-7478a0c5f458bdce4b7a69e4975c3503b1b1c23c'
```

本地或公网环境使用：

```bash
IMAGE='crpi-y6a776c9l4k3agmh.cn-hongkong.personal.cr.aliyuncs.com/kawaru/sxo:sha-7478a0c5f458bdce4b7a69e4975c3503b1b1c23c'
```

后续命令直接使用上述 `IMAGE`。私有仓库需要先完成镜像拉取认证。运行环境需为 `linux/amd64`，CPU 兼容 `x86-64-v3` 指令集。

镜像没有 ENTRYPOINT，必须指定完整命令：

| 任务 | 容器启动命令 |
| --- | --- |
| 预处理 | `python /app/algorithm_processing.py <参数>` |
| UMAP | `python /app/clustering_umap_kmeans.py <参数>` |

在 E-HPC INSTANT 中，将 `python` 和脚本路径放入 `Command`，参数逐项放入 `Arg`。例如：

```json
{
  "Command": ["python", "/app/clustering_umap_kmeans.py"],
  "Arg": ["--run-id", "123", "--source-zarr-path", "processed/123.zarr"]
}
```

## 2. 注入环境变量

### 必填：两个命令通用

| 变量 | 说明 |
| --- | --- |
| `OSS_BUCKET_NAME` | 输入、输出和 manifest 所在的 bucket 名 |
| `OSS_ENDPOINT_URL` | 容器可达的 OSS endpoint，例如 `https://oss-cn-hongkong-internal.aliyuncs.com` |
| `ALIBABA_CLOUD_ACCESS_KEY_ID` | STS 临时凭证的 AccessKeyId |
| `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | 同次 STS 临时凭证的 AccessKeySecret |
| `ALIBABA_CLOUD_SECURITY_TOKEN` | 同次 STS 临时凭证的 SecurityToken；INSTANT 中按下述方式分块传入 |

STS 凭证由调用方提供，容器不会申请或刷新，需覆盖排队和整个任务执行时间。凭证需允许读取输入、写入结果和 manifest；预处理还需允许删除输出前缀内的对象，UMAP 需允许列举源 Zarr 前缀。

**INSTANT 的长 token 传法：**按每段不超过 256 字符拆成 `OSS_TOKEN_0`、`OSS_TOKEN_1`……，从 0 连续编号，不设置 `ALIBABA_CLOUD_SECURITY_TOKEN`。脚本启动后自动按顺序拼接。若同时设置了非空的完整 token，脚本优先使用完整 token。分块也占环境变量名额，提交时注意 INSTANT 的 20 个环境变量总数限制。

例如，本地 Docker 可使用一个 `job.env` 文件注入完整 token：

```dotenv
OSS_BUCKET_NAME=your-bucket
OSS_ENDPOINT_URL=https://oss-cn-hongkong-internal.aliyuncs.com
ALIBABA_CLOUD_ACCESS_KEY_ID=STS.<temporary-id>
ALIBABA_CLOUD_ACCESS_KEY_SECRET=<temporary-secret>
ALIBABA_CLOUD_SECURITY_TOKEN=<temporary-token>
```

endpoint 必须在容器网络中可达；在 VPC 外运行时应使用对应公网 endpoint。

### 可选

| 变量 | 适用任务 | 默认值 / 作用 |
| --- | --- | --- |
| `OSS_RESULTS_PREFIX` | 两者 | manifest 存放前缀；预处理默认 `hpc-algorithm`，UMAP 默认 `hpc-clustering`。传非空值，**不控制计算结果输出路径** |
| `WORK_ROOT` | 两者 | 本地工作目录父目录，默认 `/tmp`；自定义时使用可写的绝对路径 |
| `ALGORITHM_FC_CALLBACK_URL` | 预处理 | 完整回调 URL；不设则不回调。主服务路由为 `/callback/algorithm_callback` |
| `CLUSTERING_FC_CALLBACK_URL` | UMAP | 完整回调 URL；不设则不回调。主服务路由为 `/callback/clustering_callback` |
| `CALLBACK_TOKEN` | 两者 | 回调请求头 `X-FC-Token` 的值，接主服务时须与其 `FC_CALLBACK_TOKEN` 一致 |
| `MEMORY_LIMIT_MB` | UMAP | 实际申请的内存，单位 MiB，例如 8 GiB 填 `8192`。建议显式注入；未设时读取 cgroup 配额，读不到按 `2048` 处理 |
| `UMAP_FULL_MATRIX_MB` | UMAP | 拟合矩阵预算，单位 MiB；正整数时覆盖自动预算，未设或非正值时按内存自动计算。不是容器总内存限制，通常无需设置 |
| `OSS_DOWNLOAD_EXCLUDE_ARRAYS` | UMAP | 默认 `ion_image/intensity`，跳过该数组的数据块以减少下载，仍保留元数据。逗号分隔多个数组路径；设为空字符串则完整下载 |

镜像以 UID `10001` 运行，工作目录需允许该用户写入。系统盘需容纳输入、输出及中间文件，不能只按输入大小分配。

## 3. 预处理命令

```bash
docker run --rm --env-file job.env "$IMAGE" \
  python /app/algorithm_processing.py \
  --run-id 123 \
  --imzml-path 'inputs/sample.imzML' \
  --config-json '{"baseline_correction":{},"normalization":{},"peak_pick":{}}' \
  --output-path 'processed/123.zarr'
```

| 参数 | 说明 |
| --- | --- |
| `--run-id` | 必填，正整数；启用主服务回调时对应其已有 run 记录 |
| `--imzml-path` | 必填，bucket 内的 `.imzML` key；同目录必须有同名 `.ibd`，例如 `inputs/sample.ibd` |
| `--config-json` | 必填，算法配置 JSON 对象字符串，见下表；传 `{}` 不启用任何算法 |
| `--output-path` | 必填，本次任务独占的 OSS 输出前缀，通常以 `.zarr` 结尾 |
| `--metadata-json` | 可选，写入输出 Zarr 的附加元数据 JSON 对象，例如 `'{"source":"hpc"}'` |

所有数据路径都是 **bucket 内相对 key**，不带 `oss://bucket/` 或 endpoint。JSON 在 INSTANT 中作为一个完整的 `Arg` 值传入，不要把 shell 外层单引号也传进去。

算法按下表顺序执行；字段省略或设为 `null` 表示跳过，设为 `{}` 表示启用默认参数，也可在对象内指定参数：

| 配置字段 | 功能 | 默认方法 / 配置示例 |
| --- | --- | --- |
| `baseline_correction` | 基线校正 | `{"method":"locmin_numba"}` |
| `noise_reduction` | 降噪 | `{"method":"gaussian_numba","window":5}` |
| `normalization` | 归一化 | `{"method":"tic_numba"}` |
| `peak_pick` | 寻峰 | `{"method":"diff","snr":2.0,"width":5}` |
| `peak_align` | 峰对齐 | `{"units":"ppm","binratio":2.0,"min_frequency":0.01}` |

**输出前缀必须与输入和已有数据分开。预处理失败时，脚本会尝试删除整个 `--output-path/` 下的对象。** 成功后，完整 Zarr 上传到该前缀，输入 imzML/ibd 不被修改。

## 4. UMAP 命令

```bash
docker run --rm --env-file job.env -e MEMORY_LIMIT_MB=8192 "$IMAGE" \
  python /app/clustering_umap_kmeans.py \
  --run-id 123 \
  --source-zarr-path 'processed/123.zarr'
```

此处 `8192` 应与运行环境实际分配的内存一致，设置环境变量本身不会分配内存。

| 参数 | 说明 |
| --- | --- |
| `--run-id` | 必填，正整数；启用主服务回调时对应其已有 run 记录 |
| `--source-zarr-path` | 必填，bucket 内以 `.zarr` 结尾的源数据前缀 |

输入需为 MassFlow 支持的 Zarr 布局，且是共享 m/z 轴的 `continuous` 数据，至少有 5 条谱及非空 m/z 轴。寻峰后未峰对齐的预处理结果不一定满足该条件。

作业按内存预算选择全量或抽样 UMAP，结果增量写回**源 Zarr 的同一 OSS 前缀**，不会生成另一个输出路径。不要对同一 Zarr 并发写入；需要保留原数据时先制作副本。上传失败可能留下部分写回结果。

脚本生成的 `umap_image.jpg` 仅在容器本地，不会单独上传到 OSS。当前入口没有可配置的 KMeans 参数。

## 5. 判断任务结果

结果上传或失败清理后，脚本尝试写入终态文件：

```text
预处理默认：oss://<bucket>/hpc-algorithm/<run_id>/manifest.json
UMAP 默认： oss://<bucket>/hpc-clustering/<run_id>/manifest.json
自定义前缀：oss://<bucket>/<OSS_RESULTS_PREFIX>/<run_id>/manifest.json
```

查看 `is_success` 判断计算结果，失败原因在 `error_type` / `error_message`。预处理还包含 `output_path`、`output_size`（字节）；UMAP 包含 `source_zarr_path`、采样信息和内存指标。`image_git_sha` 标识镜像构建版本。

| 退出码 | 含义 |
| --- | --- |
| `0` | 计算成功，manifest 上传成功 |
| `2` | 计算管线失败；即使 manifest 也上传失败仍返回 2 |
| `3` | 计算成功，但 manifest 上传失败；结果可能已存在 |
| `64` | 参数、配置 JSON 或必填环境变量校验失败 |

参数校验失败、容器启动失败或进程被平台杀死时，可能没有 manifest，需结合平台状态和容器日志判断。

配置回调 URL 后，脚本 POST JSON，并附带 `X-FC-Token` 请求头：

- 通用字段：`run_id`、`is_success`、`error_type`、`error_message`。
- 预处理额外带 `out_path`、`output_size`，失败时这两项为 `null`。
- 最多尝试 2 次，仅 HTTP `200` 算成功；回调失败记日志，不改变计算结果和退出码。
