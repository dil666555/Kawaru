FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MPLBACKEND=Agg

# numba 按宿主 CPU 名称/特性给编译缓存建档。固定为 x86-64-v3（AVX2）目标后，
# CI 构建机生成的缓存在 INSTANT 主机上直接命中，而不是每个作业重新编译；
# 要求实例 CPU 支持 AVX2（c9a 等当代规格均支持）。
ENV NUMBA_CACHE_DIR=/home/appuser/.cache/numba \
    NUMBA_CPU_NAME=x86-64-v3 \
    NUMBA_CPU_FEATURES=""

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# massflow 来自 PyPI；升级算法时改 HPC/image/requirements.txt 的版本号。
# gcc 只为编译 crcmod 的 C 扩展（oss2 CRC64 纯 Python 仅约 20 MiB/s），装完即删。
COPY HPC/image/requirements.txt /tmp/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev \
    && python -m pip install -r /tmp/requirements.txt \
    && apt-get purge -y --auto-remove gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt \
    && python -c "import crcmod._crcfunext"

RUN useradd --create-home --uid 10001 appuser

WORKDIR /app
COPY HPC/scripts/clustering_umap_kmeans.py /app/

USER appuser
# 构建期真实跑一遍合成数据 UMAP（clustering_umap_kmeans.py --warm-up）：
# numba 编译缓存与 matplotlib 字体缓存随镜像层发布；umap-learn 未标记可缓存的
# 函数仍会在作业内首次调用时编译。warm_up 自行切到临时目录（MassFlow 会往 cwd 写 logs/）。
RUN python /app/clustering_umap_kmeans.py --warm-up \
    && test -n "$(find "$NUMBA_CACHE_DIR" -name '*.nbi' -print -quit)"

# 记录构建 commit，作业日志与 manifest 会带上，便于结果对版本
ARG GIT_SHA=unknown
ENV IMAGE_GIT_SHA=$GIT_SHA

# 不设 ENTRYPOINT：INSTANT 作业提交时传完整命令，兼容 Command 被当作 entrypoint 或 CMD
CMD ["python", "/app/clustering_umap_kmeans.py", "--help"]
