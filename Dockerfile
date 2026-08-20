FROM python:3.11-slim

WORKDIR /app

# 安装依赖（利用 Docker 层缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY dashboard.py proxy_pool.py reconcile_controller.py aliyun_ecs.py aliyun_billing.py ./

EXPOSE 8765

# 配置通过环境变量传入，参考 .env 字段：
#   ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET / ALIYUN_REGION_ID 等
# 持久化设置文件挂载到 /app/dashboard_settings.json
CMD ["python", "dashboard.py", "--host", "0.0.0.0", "--port", "8765"]
