FROM python:3.11-slim

WORKDIR /app

# 安装依赖（利用 Docker 层缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY dashboard.py proxy_pool.py warpgate_pool.py reconcile_controller.py aliyun_ecs.py aliyun_billing.py ./

# 以非 root 用户运行应用
RUN addgroup --system --gid 10001 proxypilot \
    && adduser --system --uid 10001 --gid 10001 --no-create-home proxypilot

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "from urllib.request import urlopen; urlopen('http://127.0.0.1:8765/healthz', timeout=3)"

# 配置通过环境变量传入，参考 .env.local 字段：
#   ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET / ALIYUN_REGION_ID 等
# 持久化设置文件挂载到 /app/dashboard_settings.json
USER proxypilot
CMD ["python", "dashboard.py", "--host", "0.0.0.0", "--port", "8765"]
