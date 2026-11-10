# HPC 镜像使用说明

本文供后端部署和作业提交使用。镜像提供四个独立任务，每次启动执行一个任务，完成后容器退出：

| 任务 | 输入 | 输出 | 容器启动命令 |
| --- | --- | --- | --- |
| 原始转换 | imzML / ibd | 新 Zarr，不启用预处理算法 | `python /app/imz_to_zarr.py <参数>` |
| 上传后预览 | imzML / ibd | UMAP、TIC、随机离子 JPEG 图，具体按模式决定 | `python /app/imzml_preview.py <参数>` |
| 预处理 | imzML / ibd | 算法处理后的新 Zarr | `python /app/algorithm_processing.py <参数>` |
| Zarr UMAP | Zarr | 分析结果写回源 Zarr | `python /app/clustering_umap_kmeans.py <参数>` |

任务不会自动串联。后端需创建对应的 run / file 记录、提交 HPC 作业，并通过回调或 manifest 接收结果。部署镜像不会自动将现有 FC 调用切换到 HPC。

## 1. 启动镜像

四个任务共用一个镜像。使用构建方交付的 `sha-<commit>` 标签，替换下面的 `<commit>`；原始转换和上传后预览需使用包含这两个入口的新镜像。

E-HPC INSTANT 同地域 VPC 使用：

```bash
IMAGE='crpi-y6a776c9l4k3agmh-vpc.cn-hongkong.personal.cr.aliyuncs.com/kawaru/sxo:sha-<commit>'
```

本地或公网环境使用：

```bash
IMAGE='crpi-y6a776c9l4k3agmh.cn-hongkong.personal.cr.aliyuncs.com/kawaru/sxo:sha-<commit>'
```

私有仓库需配置镜像拉取认证，与后续注入的 OSS 凭证分开。运行环境为 `linux/amd64`，CPU 需兼容 `x86-64-v3`（AVX2）。镜像以 UID `10001` 运行，工作目录需允许该用户写入；系统盘需容纳输入、输出及中间文件。

镜像未设置 ENTRYPOINT，提交时必须指定完整命令。在 E-HPC INSTANT 的容器配置中，将 `python` 和脚本路径放入 `Command`，参数逐项放入 `Arg`。例如：

```json
{
  "Command": ["python", "/app/clustering_umap_kmeans.py"],
  "Arg": ["--run-id", "123", "--source-zarr-path", "processed/123.zarr"]
}
```

下文使用 Docker 展示各任务的完整命令；提交 INSTANT 时按上例拆分即可。所有数据路径均为 **bucket 内相对 key**，不带 `oss://bucket/` 或 endpoint；JSON 参数作为一个完整的 `Arg` 值传入，不包含 shell 外层单引号。

## 2. 注入环境变量

### 必填：所有命令通用

| 变量 | 说明 |
| --- | --- |
| `OSS_BUCKET_NAME` | 输入、输出和 manifest 所在的 bucket 名 |
| `OSS_ENDPOINT_URL` | 容器可达的 OSS endpoint，例如 `https://oss-cn-hongkong-internal.aliyuncs.com` |
| `ALIBABA_CLOUD_ACCESS_KEY_ID` | STS 临时凭证的 AccessKeyId |
| `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | 同次 STS 临时凭证的 AccessKeySecret |
| `ALIBABA_CLOUD_SECURITY_TOKEN` | 同次 STS 临时凭证的 SecurityToken；INSTANT 中按下述方式分块传入 |

STS 凭证由后端提供，容器不会申请或刷新，有效期需覆盖排队和整个任务执行时间。按任务授予以下 OSS 权限：

| 范围 | 所需权限 |
| --- | --- |
| 所有任务的输入 | 读取对象；Zarr UMAP 还需列举源 Zarr 前缀 |
| 原始转换、上传后预览、预处理的输出前缀 | 写入、列举、删除对象，用于发布结果和失败清理 |
| Zarr UMAP 的源 Zarr 前缀 | 写入分析结果 |
| manifest 前缀 | 写入终态 JSON；后端需有读取权限 |

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
| `OSS_RESULTS_PREFIX` | 全部 | manifest 存放前缀；原始转换默认 `hpc-imz-to-zarr`，上传后预览默认 `hpc-preview`，预处理默认 `hpc-algorithm`，Zarr UMAP 默认 `hpc-clustering`。传非空相对 key，**不控制计算结果输出路径** |
| `WORK_ROOT` | 全部 | 本地工作目录父目录，默认 `/tmp`；自定义时使用可写的绝对路径 |
| `RAW_CONVERT_FC_CALLBACK_URL` | 原始转换 | 完整回调 URL；不设则不回调。沿用 FC 原始转换回调配置，可接主服务 `/callback/algorithm_callback` |
| `PUBLISH_MAX_WORKERS` | 原始转换、上传后预览 | OSS 上传线程数，默认 `8`，必须为正整数，通常无需修改 |
| `FEATURE_FC_CALLBACK_URL` | 上传后预览 feature 模式 | 完整回调 URL；不设则不回调。沿用 FC 变量名，当前主服务对应路由为 `/callback/umap_callback` |
| `TIC_FC_CALLBACK_URL` | 上传后预览 tic 模式 | 完整回调 URL；不设则不回调。主服务路由为 `/callback/tic_callback` |
| `ALGORITHM_FC_CALLBACK_URL` | 预处理 | 完整回调 URL；不设则不回调。主服务路由为 `/callback/algorithm_callback` |
| `CLUSTERING_FC_CALLBACK_URL` | UMAP | 完整回调 URL；不设则不回调。主服务路由为 `/callback/clustering_callback` |
| `CALLBACK_TOKEN` | 全部 | 回调请求头 `X-FC-Token` 的值，接主服务时须与其 `FC_CALLBACK_TOKEN` 一致 |
| `MEMORY_LIMIT_MB` | UMAP | 实际申请的内存，单位 MiB，例如 8 GiB 填 `8192`。建议显式注入；未设时读取 cgroup 配额，读不到按 `2048` 处理 |
| `UMAP_FULL_MATRIX_MB` | UMAP | 拟合矩阵预算，单位 MiB；正整数时覆盖自动预算，未设或非正值时按内存自动计算。不是容器总内存限制，通常无需设置 |
| `OSS_DOWNLOAD_EXCLUDE_ARRAYS` | UMAP | 默认 `ion_image/intensity`，跳过该数组的数据块以减少下载，仍保留元数据。逗号分隔多个数组路径；设为空字符串则完整下载 |

## 3. 原始转换命令

```bash
docker run --rm --env-file job.env "$IMAGE" \
  python /app/imz_to_zarr.py \
  --run-id 123 \
  --imzml-path 'inputs/sample.imzML' \
  --output-path 'raw/123.zarr' \
  --metadata-json '{"source":"hpc"}'
```

| 参数 | 说明 |
| --- | --- |
| `--run-id` | 必填，正整数；启用回调时对应已有的原始转换 run 记录 |
| `--imzml-path` | 必填，源 `.imzML` 文件在 bucket 内的相对 key |
| `--ibd-path` | 可选，源 `.ibd` 文件的相对 key；默认按 imzML 同目录同名推导 |
| `--output-path` | 必填，本次任务独占的相对 `.zarr` 前缀，可带尾部 `/` |
| `--metadata-json` | 可选，写入 Zarr 的附加元数据 JSON 对象，默认 `{}` |

continuous 输入输出 `ion_image_zarr`，其他输入输出 `spectra_zarr`，并计算平均谱和 TIC。

**发布前会清空整个输出前缀，发布失败会尝试清理部分输出。** 输出前缀必须由本次任务独占，不能包含输入、已有数据或 manifest，也不能并发写入。完整输出发布后才上报成功。

## 4. 上传后预览命令

直接读取原始 imzML/ibd：

```bash
docker run --rm --env-file job.env "$IMAGE" \
  python /app/imzml_preview.py \
  --file-id 123 \
  --imzml-path 'inputs/sample.imzML' \
  --image-dest 'images/<public_id>/' \
  --mode feature
```

| 参数 | 说明 |
| --- | --- |
| `--file-id` | 必填，源文件的正整数 ID；用于回调和 manifest，不是处理 run ID |
| `--imzml-path` | 必填，源 `.imzML` 在 bucket 内的相对 key |
| `--ibd-path` | 可选，源 `.ibd` 的相对 key；默认按 imzML 同目录同名推导 |
| `--image-dest` | 必填，本次任务独占的图片目录相对 key，可带尾部 `/` |
| `--mode` | 可选，默认 `feature`；`tic` 仅生成 TIC 图 |

| 模式 / 输入 | 图片目录内的输出 |
| --- | --- |
| `feature`，continuous 数据 | `preview.jpg` 为 UMAP，`preview_2.jpg` 为 TIC，`preview_3.jpg` 为随机离子图 |
| `feature`，非 continuous 数据 | 降级仅生成 TIC `preview.jpg` |
| `tic`，任意支持的 imzML 数据 | 仅生成 TIC `preview.jpg` |

feature 模式的 UMAP 固定抽样 10% 的谱拟合，再映射全部谱生成预览图；需按数据量分配内存，不使用 Zarr UMAP 的自动内存预算。

**发布前会清空图片目录，发布失败会尝试清理部分图片。** 目录必须由本次任务独占，不能包含输入、已有数据或 manifest，也不能并发写入。任一绘图失败按整个任务失败上报。feature 即使降级为 TIC，仍回调 `FEATURE_FC_CALLBACK_URL`；只有 `--mode tic` 使用 `TIC_FC_CALLBACK_URL`。

## 5. 预处理命令

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

算法按下表顺序执行；字段省略或设为 `null` 表示跳过，设为 `{}` 表示启用默认参数，也可在对象内指定参数：

| 配置字段 | 功能 | 默认方法 / 配置示例 |
| --- | --- | --- |
| `baseline_correction` | 基线校正 | `{"method":"locmin_numba"}` |
| `noise_reduction` | 降噪 | `{"method":"gaussian_numba","window":5}` |
| `normalization` | 归一化 | `{"method":"tic_numba"}` |
| `peak_pick` | 寻峰 | `{"method":"diff","snr":2.0,"width":5}` |
| `peak_align` | 峰对齐 | `{"units":"ppm","binratio":2.0,"min_frequency":0.01}` |

**输出前缀必须与输入和已有数据分开。预处理失败时，脚本会尝试删除整个 `--output-path/` 下的对象。** 成功后，完整 Zarr 上传到该前缀，输入 imzML/ibd 不被修改。

## 6. Zarr UMAP 命令

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

此命令只将分析结果写回 Zarr，不向 OSS 发布预览 JPEG。需要上传后预览图片时使用第 4 节命令。

## 7. 判断任务结果

结果上传或失败清理后，脚本尝试写入终态文件：

```text
原始转换：hpc-imz-to-zarr/<run_id>/manifest.json
上传后预览：hpc-preview/<mode>/<file_id>/manifest.json
预处理：hpc-algorithm/<run_id>/manifest.json
Zarr UMAP：hpc-clustering/<run_id>/manifest.json
```

以上是 `OSS_BUCKET_NAME` 内的相对 key。设置 `OSS_RESULTS_PREFIX` 时替换对应的默认前缀，后面的目录结构不变。相同任务 ID 会覆盖原来的 manifest，重试或新任务应注意区分 ID / 前缀。

读取 `is_success` 判断计算结果，失败原因在 `error_type` / `error_message`。转换和预处理的结果位置在 `output_path`，预览在 `image_path`，Zarr UMAP 在 `source_zarr_path`。`image_git_sha` 可用于核对镜像版本。

| 退出码 | 含义 |
| --- | --- |
| `0` | 计算成功，manifest 上传成功 |
| `2` | 计算管线失败；即使 manifest 也上传失败仍返回 2 |
| `3` | 计算成功，但 manifest 上传失败；结果可能已存在 |
| `64` | 参数、配置 JSON 或必填环境变量校验失败 |

参数校验失败、容器启动失败或进程被平台杀死时，可能没有 manifest，需结合平台状态和容器日志判断。

配置回调 URL 后，脚本 POST JSON，并附带 `X-FC-Token` 请求头：

| 任务 | ID 字段 | 结果字段 |
| --- | --- | --- |
| 原始转换、预处理 | `run_id` | `out_path`（目录，以 `/` 结尾）、`output_size`（字节） |
| 上传后预览 feature / tic | `file_id` | `image_path`（目录，以 `/` 结尾） |
| Zarr UMAP | `run_id` | 仅状态，结果位于提交时的源 Zarr |

所有回调均带 `is_success`、`error_type`、`error_message`；失败时结果字段为 `null`。回调中的目录仍是 bucket 内相对 key，例如 `processed/123.zarr/`。最多尝试 2 次，仅 HTTP `200` 算成功；回调失败记日志，不改变计算结果和退出码。后端应结合平台终态和 manifest 处理未收到回调的情况。
