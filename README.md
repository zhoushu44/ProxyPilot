# ProxyPilot

基于阿里云 ECS 的 SOCKS5 代理池自动化管理平台。

## 功能特性

- 自动扩缩容：根据配额/费用自动创建、释放阿里云 ECS 实例，维护代理池规模
- SOCKS5 代理池：内置代理池管理、健康检查与访问鉴权
- 云费用监控：对接阿里云账单（BSS OpenAPI），实时查看实例费用
- Web 管理面板：内置仪表盘（默认端口 `8765`）

## 目录结构

```
.
├── dashboard.py              # Web 管理面板与 API 入口
├── proxy_pool.py             # SOCKS5 代理池实现
├── reconcile_controller.py   # 扩缩容控制器（状态存储）
├── aliyun_ecs.py             # 阿里云 ECS 适配器
├── aliyun_billing.py         # 阿里云账单适配器
├── Dockerfile                # 容器镜像构建
├── .dockerignore             # Docker 构建忽略规则
└── requirements.txt          # Python 依赖
```

## 环境变量配置

通过环境变量（或本地 `.env` 文件，未提交到仓库）传入阿里云凭证等配置：

| 变量 | 说明 |
|---|---|
| `ALIBABA_CLOUD_ACCESS_KEY_ID` | 阿里云 AccessKey ID |
| `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | 阿里云 AccessKey Secret |
| `ALIYUN_REGION_ID` | 地域，如 `cn-hangzhou` |
| `ALIYUN_POOL_NAME` | 代理池名称，如 `proxy-main` |
| `SOCKS5_PORT` | SOCKS5 端口 |
| `SOCKS5_USERNAME` | SOCKS5 用户名 |
| `SOCKS5_PASSWORD` | SOCKS5 密码 |
| `SOCKS5_CHECK_HOST` / `SOCKS5_CHECK_PORT` | 代理健康检查目标 |
| `SOCKS5_TIMEOUT` | 代理健康检查超时 |
| `PROXY_API_BEARER_TOKEN` | 面板 API 访问令牌 |

> ⚠️ **安全提示**：`.env` 中含有阿里云 AccessKey，已通过 `.gitignore` / `.dockerignore` 排除，不会进入版本库或镜像。请勿在公开渠道粘贴真实凭证。

## 本地运行

```bash
pip install -r requirements.txt
cp .env .env.local          # 可选：本地覆盖示例
# 按需修改 .env 或通过 UI 配置面板设置
python dashboard.py --host 0.0.0.0 --port 8765
```

默认监听 `0.0.0.0:8765`。

## Docker

### 构建镜像

```bash
docker build -t proxypilot:2.0 .
```

### 运行容器

```bash
docker run -d \
  --name proxypilot \
  --restart unless-stopped \
  -p 8765:8765 \
  -e ALIBABA_CLOUD_ACCESS_KEY_ID=xxx \
  -e ALIBABA_CLOUD_ACCESS_KEY_SECRET=xxx \
  -e ALIYUN_REGION_ID=cn-hangzhou \
  -e ALIYUN_POOL_NAME=proxy-main \
  -v /path/to/dashboard_settings.json:/app/dashboard_settings.json \
  proxypilot:2.0
```

`-v`：挂载 `dashboard_settings.json`，持久化面板设置，防止容器重建后丢失。

## 镜像发布（GitHub Actions）

本项目使用 GitHub Actions 自动构建并推送 Docker 镜像，**本地不执行任何推送操作**。

- 工作流：`.github/workflows/docker.yml`
- 触发条件：推送到 `master` 或 `main` 分支
- 镜像仓库：`<DOCKER_HUB_USERNAME>/zhoushu1`
- 自动标签：
  - `latest` —— 指向最新构建
  - `2.0` —— 当前主版本号
  - `<sha>` —— 短提交哈希，便于回溯

### 所需 Secrets

在 GitHub 仓库 `Settings -> Secrets and variables -> Actions` 中配置：

| Secret | 说明 |
|---|---|
| `DOCKER_HUB_USERNAME` | Docker Hub 用户名 |
| `DOCKER_HUB_TOKEN` | Docker Hub Access Token（建议非密码） |

## 许可

仅供个人学习与合规使用，请遵守所在地区与目标服务的法律法规。
