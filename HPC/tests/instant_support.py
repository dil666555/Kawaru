# -*- coding: utf-8 -*-
"""
INSTANT 作业提交测试的共享支撑模块（本地运行，不进镜像）。

职责：加载本地凭证（.env）与提交配置（instant.local.json）、构造按 run 收窄的
STS 会话策略、把 STS token 分块为 OSS_TOKEN_0..n（与 HPC/scripts 两个作业脚本
内的 _reassemble_security_token 拼装成对）、构造并提交 CreateJob、轮询 GetJob、
以同一份 STS 凭证读写 OSS。阿里 SDK 与 oss2 惰性导入：本模块只在本地 venv
运行（不进镜像），装没装 SDK 在 import 期不报错。

本地环境（uv，约 1 分钟）：
    uv venv --python 3.12 /tmp/kawaru-hpc-venv
    uv pip install --python /tmp/kawaru-hpc-venv/bin/python \\
        oss2 requests pydantic numpy pyimzml \\
        alibabacloud-ehpcinstant20230701==4.1.0 alibabacloud-sts20150401==1.2.0 \\
        alibabacloud-credentials==1.0.12 alibabacloud-tea-openapi==0.4.6

配置与凭证（均 gitignore；模板见 instant.example.json 与 .env.example）：
    instant.local.json   region、镜像 VPC 地址（不含 tag）与不可变 sha-<commit> tag
                         （image_tag 必填，取 hpc-image.yml 推送的 tag）、网络与角色、
                         资源、基准数据集、algorithm 用的真实 imzML key（可选）
    .env                 RAM AccessKey（提交与 AssumeRole）+ ACR_PULL_*（镜像拉取）

E2E 层由 KAWARU_INSTANT_E2E=1 门禁：真实提交、真实计费。
"""

import copy
import datetime as dt
import json
import os
import re
import sys
import time
from functools import partial
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parent
CONFIG_PATH = TEST_DIR / "instant.local.json"
ENV_FILE_PATH = TEST_DIR / ".env"

SUCCESS_STATES = {"Succeed", "Succeeded"}
TERMINAL_STATES = SUCCESS_STATES | {"Failed", "Exception", "Expired", "Deleted"}
# INSTANT Container 限制（官方 SDK models 文档字符串）：环境变量 ≤20 个、单值
# ≤256 字符；Command ≤20 条、单条 ≤256；Arg ≤10 个（长度未文档化，保持短小）。
MAX_ENV_VARS = 20
ENV_VALUE_LIMIT = 256
MAX_COMMAND_ITEMS = 20
MAX_ARGS = 10
# 作业脚本约定的退出码（0 成功 / 2 算法失败 / 3 manifest 上传失败 / 64 配置或
# 凭证错误；1 为容器内未预期崩溃）。命中即不再重试。
NO_RETRY_EXIT_CODES = (1, 2, 3, 64)


# ---------------------------------------------------------------------------
# 配置与凭证加载
# ---------------------------------------------------------------------------

def load_env_file(path: Path = ENV_FILE_PATH) -> None:
    """把 KEY=VALUE 行注入环境；shell 里已导出的变量优先，空占位不遮蔽其它来源。"""
    if not path.is_file():
        return
    if path.stat().st_mode & 0o077:
        print(f"Warning: {path} is readable by other users; run: chmod 600 {path}", file=sys.stderr)
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = (part.strip() for part in line.removeprefix("export ").split("=", 1))
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        if value:
            os.environ.setdefault(key, value)


def load_config(path: Path = CONFIG_PATH) -> dict:
    """读取并校验 instant.local.json：缺文件、占位符、多余字段直接报错。"""
    if not path.is_file():
        raise FileNotFoundError(f"missing {path}; copy instant.example.json, fill it in, chmod 600")
    config = json.loads(path.read_text())
    required = {
        "region", "image", "image_tag", "vswitch_id", "security_group_id", "oss_role_arn",
        "oss_bucket", "oss_endpoint", "zarr_dataset",
    }
    allowed = required | {"test_prefix", "resources", "imzml_dataset"}
    missing, unknown = required - config.keys(), config.keys() - allowed
    if missing or unknown:
        raise ValueError(f"config keys invalid (missing={sorted(missing)} unknown={sorted(unknown)})")
    for key in required - {"region", "oss_role_arn"}:
        if not isinstance(config[key], str) or not config[key] or "<" in config[key] or "\n" in config[key]:
            raise ValueError(f"fill in a real value for {key!r} (no placeholders)")
    if not re.fullmatch(r"[a-z0-9-]+", config["region"]):
        raise ValueError("invalid region")
    if not re.fullmatch(r"acs:ram::\d+:role/[A-Za-z0-9.-]+", config["oss_role_arn"]):
        raise ValueError("oss_role_arn must look like acs:ram::<account-id>:role/<name>")
    if not config["zarr_dataset"].endswith(".zarr"):
        raise ValueError("zarr_dataset must be a .zarr prefix (clustering job input)")
    # 可选：algorithm E2E 用的真实 imzML key（作业按同名推导 .ibd，且必须带目录）；
    # 未填时 algorithm 用例自动跳过，不影响 clustering
    imzml = str(config.get("imzml_dataset", "")).strip().strip("/")
    if imzml and (not imzml.endswith(".imzML") or "/" not in imzml or "<" in imzml):
        raise ValueError(
            "imzml_dataset must be an OSS key like 'dir/sample.imzML' (with its .ibd alongside)")
    config["imzml_dataset"] = imzml
    if ":" in config["image"]:
        raise ValueError("image must not include a tag; configure image_tag instead")
    if not re.fullmatch(r"sha-[0-9a-f]{40}", config["image_tag"]):
        raise ValueError("image_tag must be the immutable sha-<commit> tag pushed by hpc-image.yml")
    config["zarr_dataset"] = config["zarr_dataset"].strip("/")
    config["test_prefix"] = str(config.get("test_prefix", "hpc-test")).strip("/") or "hpc-test"
    config["resources"] = config.get(
        "resources", {"cores": 4, "memory_gib": 8, "system_disk_gib": 100})
    _validate_resources(config["resources"])
    return config


def _validate_resources(resources: dict) -> None:
    """校验 resources 的数值、规格列表与回退开关。"""
    unknown = set(resources) - {"cores", "memory_gib", "system_disk_gib",
                                "instance_types", "fallback_any_type"}
    if unknown:
        raise ValueError(f"resources has unknown keys: {sorted(unknown)}")
    for key in ("cores", "memory_gib", "system_disk_gib"):
        value = resources.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ValueError(f"resources.{key} must be positive")
    types = resources.get("instance_types", [])
    if not isinstance(types, list) or len(types) > 5 or not all(
            isinstance(t, str) and re.fullmatch(r"[a-z0-9][a-z0-9.-]+", t) for t in types):
        raise ValueError("resources.instance_types must list at most 5 types like ecs.c9ae.xlarge")
    if not isinstance(resources.get("fallback_any_type", False), bool):
        raise ValueError("resources.fallback_any_type must be boolean")


def resolve_image(config: dict) -> tuple[str, str]:
    """返回 (镜像全地址, 被测 commit sha)。tag 只认配置的 image_tag（必须是
    hpc-image.yml 推送的不可变 sha-<commit> tag），不做任何 HEAD 推导——测哪个
    镜像由配置唯一决定；manifest 的 image_git_sha 与 tag 的 commit 闭环校验，
    镜像漂移/配错 tag 时给出明确失败。"""
    tag = config["image_tag"]
    return f"{config['image']}:{tag}", tag[4:]


def run_prefix(config: dict, run_id: int) -> str:
    """本次 run 在 OSS 上的根前缀，所有测试产物（输入副本、结果、manifest）都在其下。"""
    return f"{config['test_prefix']}/{run_id}"


def new_run_id() -> int:
    """以 UTC 时间戳作 run id（作业脚本要求正整数）。"""
    return int(dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S"))


# ---------------------------------------------------------------------------
# STS 会话策略与作业环境变量
# ---------------------------------------------------------------------------

def oss_session_policy(bucket: str, readable_prefixes: list[str],
                       writable_prefix: str | list[str]) -> dict:
    """按 run 收窄的 STS 会话策略：ListObjects 限定前缀、GetObject 限定数据源与
    本 run、PutObject/DeleteObject 限定可写前缀（clustering：基准 analysis/ 供
    作业写回与测试清理，加上 run 前缀供 manifest；algorithm：仅 run 前缀）。"""
    readable = [p.strip("/") for p in readable_prefixes]
    writable_sources = ((writable_prefix,) if isinstance(writable_prefix, str)
                        else writable_prefix)
    writables = [w.strip("/") for w in writable_sources]
    listable = list(dict.fromkeys((*readable, *writables)))  # 可读写前缀常相同，条件去重
    return {"Version": "1", "Statement": [
        {"Effect": "Allow", "Action": ["oss:ListObjects"],
         "Resource": [f"acs:oss:*:*:{bucket}"],
         "Condition": {"StringLike": {"oss:Prefix": [f"{p}/*" for p in listable]}}},
        {"Effect": "Allow", "Action": ["oss:GetObject"],
         "Resource": [f"acs:oss:*:*:{bucket}/{p}/*" for p in readable]},
        {"Effect": "Allow", "Action": ["oss:PutObject", "oss:DeleteObject"],
         "Resource": [f"acs:oss:*:*:{bucket}/{p}/*" for p in writables]},
    ]}


def credential_env_vars(credentials: dict, base_env: dict,
                        minimum_remaining_seconds: int = 600) -> list[dict]:
    """STS 凭证 + 作业基础环境变量 → INSTANT 环境变量列表。token 按 256 字符切成
    OSS_TOKEN_0..n（作业脚本 _reassemble_security_token 按序拼回），并整体校验
    INSTANT 限制（≤20 个变量、单值 ≤256、变量名合法）。"""
    creds = credentials.get("Credentials", credentials)
    for key in ("AccessKeyId", "AccessKeySecret", "SecurityToken", "Expiration"):
        if not isinstance(creds.get(key), str) or not creds[key]:
            raise ValueError(f"STS credentials are missing {key}")
    if not creds["AccessKeyId"].startswith("STS."):
        raise ValueError("expected temporary STS credentials (AccessKeyId starts with STS.)")
    expiration = dt.datetime.fromisoformat(creds["Expiration"].replace("Z", "+00:00"))
    remaining = (expiration - dt.datetime.now(dt.timezone.utc)).total_seconds()
    if remaining < minimum_remaining_seconds:
        raise ValueError(f"STS credentials valid for only {remaining:.0f}s, shorter than the job budget")
    pairs = dict(base_env)
    pairs["ALIBABA_CLOUD_ACCESS_KEY_ID"] = creds["AccessKeyId"]
    pairs["ALIBABA_CLOUD_ACCESS_KEY_SECRET"] = creds["AccessKeySecret"]
    token = creds["SecurityToken"]
    for index, start in enumerate(range(0, len(token), ENV_VALUE_LIMIT)):
        pairs[f"OSS_TOKEN_{index}"] = token[start:start + ENV_VALUE_LIMIT]
    if len(pairs) > MAX_ENV_VARS:
        raise ValueError(f"{len(pairs)} env vars exceed the INSTANT limit of {MAX_ENV_VARS}")
    for name, value in pairs.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"invalid env var name {name!r}")
        if len(str(value)) > ENV_VALUE_LIMIT:
            raise ValueError(f"env var {name!r} is {len(str(value))} chars (limit {ENV_VALUE_LIMIT})")
    return [{"Name": name, "Value": str(value)} for name, value in pairs.items()]


# ---------------------------------------------------------------------------
# CreateJob 请求构造
# ---------------------------------------------------------------------------

def build_create_job_request(config: dict, image: str, *, job_name: str, description: str,
                             command: list[str], args: list[str], env_vars: list[dict]) -> dict:
    """构造 CreateJob 请求 dict（字段与官方 SDK model 一一对应，提交前经 to_sdk_model 校验）。"""
    if len(command) > MAX_COMMAND_ITEMS or any(len(c) > ENV_VALUE_LIMIT for c in command):
        raise ValueError("container command exceeds INSTANT limits")
    if len(args) > MAX_ARGS:
        raise ValueError(f"{len(args)} args exceed the INSTANT limit of {MAX_ARGS}")
    user, password = os.environ.get("ACR_PULL_USERNAME"), os.environ.get("ACR_PULL_PASSWORD")
    if not user or not password:
        raise ValueError("private image requires ACR_PULL_USERNAME and ACR_PULL_PASSWORD (HPC/tests/.env)")
    container = {
        "Image": image,
        "Command": list(command),
        "Arg": list(args),
        "EnvironmentVars": env_vars,
        "ImageRegistryOptions": json.dumps({
            "ImageRegistryType": "https",
            "ImageRegistryServer": image.split("/")[0],
            "ImageRegistryUserName": user,
            "ImageRegistryPassword": password,
        }),
    }
    r = config["resources"]
    resource = {"Cores": r["cores"], "Memory": r["memory_gib"],
                "Disks": [{"Type": "System", "Size": r["system_disk_gib"]}]}
    if r.get("instance_types"):
        resource["InstanceTypes"] = r["instance_types"]
    return {
        "JobName": job_name,
        "JobDescription": description,
        "Tasks": [{
            "TaskName": "job",
            "TaskSustainable": False,
            "ExecutorPolicy": {"MaxCount": 1,
                               "ArraySpec": {"IndexStart": 0, "IndexEnd": 0, "IndexStep": 1}},
            "TaskSpec": {
                "TaskExecutor": [{"Container": container}],
                "VolumeMount": [],
                "Resource": resource,
                "RetryPolicy": {"RetryCount": 1, "ExitCodeActions": [
                    {"ExitCode": code, "Action": "Exit"} for code in NO_RETRY_EXIT_CODES]},
            },
        }],
        "DeploymentPolicy": {"AllocationSpec": "Standard", "Tag": [], "Network": {
            "Vswitch": [config["vswitch_id"]],
            "EnableExternalIpAddress": False,
        }},
        "SecurityPolicy": {"SecurityGroup": {"SecurityGroupIds": [config["security_group_id"]]}},
    }


def to_sdk_model(request: dict):
    """经官方 SDK model 校验并返回 model 对象；SDK 未安装时抛 ImportError。"""
    from alibabacloud_ehpcinstant20230701.models import CreateJobRequest # pylint: disable=import-outside-toplevel,E0401 # pyright: ignore[reportMissingImports]
    model = CreateJobRequest().from_map(request)
    model.validate()
    if model.to_map() != request:
        raise ValueError("SDK dropped an unsupported request field")
    return model


# ---------------------------------------------------------------------------
# INSTANT API：提交与等待
# ---------------------------------------------------------------------------

def _openapi_config(region: str, endpoint: str):
    # 本地身份走默认凭证链（.env 注入的环境变量优先）。
    from alibabacloud_credentials.client import Client as Credentials # pylint: disable=import-outside-toplevel,E0401 # pyright: ignore[reportMissingImports]
    from alibabacloud_tea_openapi.models import Config # pylint: disable=import-outside-toplevel,E0401 # pyright: ignore[reportMissingImports]
    return Config(credential=Credentials(), region_id=region, endpoint=endpoint, protocol="HTTPS",
                  connect_timeout=10000, read_timeout=30000)


def make_client(region: str):
    """INSTANT OpenAPI 客户端。"""
    from alibabacloud_ehpcinstant20230701.client import Client # pylint: disable=import-outside-toplevel,E0401 # pyright: ignore[reportMissingImports]
    return Client(_openapi_config(region, f"ehpcinstant.{region}.aliyuncs.com"))


def make_sts_client(region: str):
    """STS OpenAPI 客户端。"""
    from alibabacloud_sts20150401.client import Client # pylint: disable=import-outside-toplevel,E0401 # pyright: ignore[reportMissingImports]
    return Client(_openapi_config(region, "sts.aliyuncs.com"))


def assume_oss_role(sts_client, config: dict, run_id: int, policy: dict,
                    duration_seconds: int = 10800) -> dict:
    """以按 run 收窄的会话策略 AssumeRole，返回容器侧使用的 STS 凭证。
    默认 3 小时（角色允许的最大值，43200 已被拒）：大数据集的复制 + 作业运行 +
    读回验证总时长可远超 1 小时（max_1.zarr 顺序复制实测即 ~2.1h）。"""
    from alibabacloud_sts20150401.models import AssumeRoleRequest # pylint: disable=import-outside-toplevel,E0401 # pyright: ignore[reportMissingImports]
    response = sts_client.assume_role(AssumeRoleRequest(
        role_arn=config["oss_role_arn"], role_session_name=f"kawaru-test-{run_id}"[:64],
        duration_seconds=duration_seconds,
        policy=json.dumps(policy, separators=(",", ":"))))
    return response.body.credentials.to_map()


def sold_out(exc: Exception) -> bool:
    """是否规格售罄类拒绝：发生在请求阶段，无作业产生，可安全重提。"""
    inner = getattr(exc, "inner_exception", None) or exc
    return "SoldOut" in str(getattr(inner, "code", None) or "")


def submit_job(client, config: dict, request: dict) -> str:
    """CreateJob 并返回 JobId。不自动重试（API 无 ClientToken 幂等字段）；唯一例外
    是配置 fallback_any_type 且请求因规格售罄被拒（此时保证无作业产生）。"""
    def create(current: dict) -> str:
        job_id = client.create_job(to_sdk_model(current)).body.job_id
        if not job_id:
            raise RuntimeError("CreateJob returned no JobId")
        return job_id

    try:
        return create(request)
    except Exception as exc: # pylint: disable=broad-except
        resource = request["Tasks"][0]["TaskSpec"]["Resource"]
        if ("InstanceTypes" not in resource
                or not config["resources"].get("fallback_any_type")
                or not sold_out(exc)):
            raise
        print("instance types sold out; resubmitting without InstanceTypes", file=sys.stderr, flush=True)
        request = copy.deepcopy(request)
        del request["Tasks"][0]["TaskSpec"]["Resource"]["InstanceTypes"]
        return create(request)


def wait_for_job(client, job_id: str, timeout_seconds: int = 1800,
                 poll_seconds: int = 10) -> dict:
    """轮询 GetJob 至终态并返回 JobInfo；超时不取消作业，抛 TimeoutError。"""
    from alibabacloud_ehpcinstant20230701.models import GetJobRequest # pylint: disable=import-outside-toplevel,E0401 # pyright: ignore[reportMissingImports]
    # 先亮出 JobId：后续任何失败（含轮询超时）都能拿着它去 INSTANT 控制台善后
    print(f"job {job_id}: polling until terminal state", flush=True)
    deadline = time.monotonic() + timeout_seconds
    previous = None
    while True:
        info = client.get_job(GetJobRequest(job_id=job_id)).body.to_map().get("JobInfo", {})
        state = info.get("Status", "Unknown")
        if state != previous:
            print(f"job {job_id}: {state}", flush=True)
            previous = state
        if state in TERMINAL_STATES:
            return info
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"job {job_id} not terminal after {timeout_seconds}s (NOT cancelled; "
                "check the INSTANT console)")
        time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))


def failure_reasons(job_info: dict) -> list[str]:
    """终态为失败时，收集各 executor 的 StatusReason 供断言消息展示。"""
    reasons = []
    for task in job_info.get("Tasks") or []:
        for executor in task.get("ExecutorStatus") or []:
            if executor.get("StatusReason"):
                reasons.append(f"executor {executor.get('ArrayId')}: {executor['StatusReason']}")
    return reasons


def error_summary(exc: Exception) -> str:
    """异常的 类型[错误码] + 脱敏后的 message。SDK 报文可能回显请求参数，仓库
    密码 / RAM 密钥的值替换为 *** 后才拼接、截断到 500 字符（2026-10-20 的提交
    拒绝因看不到 message 盲查了一轮：UnretryableException 掩盖了服务端的真实
    拒绝理由）。"""
    inner = getattr(exc, "inner_exception", None) or exc
    code = getattr(inner, "code", None) or ""
    summary = f"{type(exc).__name__}[{code}]" if code else type(exc).__name__
    detail = str(exc)
    inner_text = str(inner) if inner is not exc else ""
    if inner_text and inner_text not in detail:
        detail = f"{detail} | {inner_text}"
    for name in ("ACR_PULL_PASSWORD", "ACR_PULL_USERNAME", "ALIBABA_CLOUD_ACCESS_KEY_SECRET"):
        secret = os.environ.get(name, "")
        if secret:
            detail = detail.replace(secret, "***")
    if detail:
        summary = f"{summary}: {detail[:500]}"
    return summary


# ---------------------------------------------------------------------------
# OSS 工具（用 AssumeRole 得到的同一份 STS 凭证）
# ---------------------------------------------------------------------------

def _retry_io(operation, attempts: int = 3, base_delay_seconds: float = 2.0):
    """幂等 OSS 操作的有界重试：仅网络层 RequestError 与 5xx 重试，4xx 快速失败。
    测试侧走公网 endpoint，数百个单发请求的 E2E 里瞬时抖动（超时/连接重置）是
    常态而非异常，oss2 对单对象操作没有内建重试。仅用于幂等操作（复制/删除/读）。"""
    from oss2.exceptions import RequestError, ServerError # pylint: disable=import-outside-toplevel
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except (RequestError, ServerError) as exc:
            transient = isinstance(exc, RequestError) or exc.status // 100 == 5
            if not transient or attempt == attempts:
                raise
            print(f"oss transient {type(exc).__name__}, retrying {attempt + 1}/{attempts}",
                  file=sys.stderr, flush=True)
            time.sleep(base_delay_seconds * attempt)


def make_bucket(credentials: dict, config: dict):
    """用容器同款 STS 凭证建 oss2 Bucket（测试侧读写数据集副本与 manifest）。"""
    import oss2 # pylint: disable=import-outside-toplevel
    auth = oss2.StsAuth(credentials["AccessKeyId"], credentials["AccessKeySecret"],
                        credentials["SecurityToken"])
    # internal endpoint 只有 VPC 内容器可达（本机 TLS 握手直接被重置）；
    # 容器继续吃配置原值，测试侧推导公网地址
    endpoint = config["oss_endpoint"].replace("-internal", "", 1)
    return oss2.Bucket(auth, endpoint, config["oss_bucket"], connect_timeout=15)


def list_prefix(bucket, prefix: str) -> list[str]:
    """列出前缀下全部对象 key。"""
    import oss2 # pylint: disable=import-outside-toplevel
    return [obj.key for obj in oss2.ObjectIterator(bucket, prefix=prefix.strip("/") + "/")]


def delete_prefix(bucket, prefix: str) -> int:
    """删除前缀下全部对象（凭证需 Delete 权限）；返回删除的对象数。"""
    keys = list_prefix(bucket, prefix)
    for key in keys:
        _retry_io(partial(bucket.delete_object, key))
    return len(keys)


def get_object_json(bucket, key: str) -> dict:
    """读取并解析对象上的 JSON。"""
    return json.loads(_retry_io(lambda: bucket.get_object(key).read())) # type: ignore
