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

通过环境变量（或本地 `.env.local` 文件，未提交到仓库）传入阿里云凭证等配置：

| 变量 | 说明 |
|---|---|
| `ALIBABA_CLOUD_ACCESS_KEY_ID` | 阿里云 AccessKey ID |
| `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | 阿里云 AccessKey Secret |
| `ALIYUN_REGION_ID` | 仅当 UI 配置文件没有地域时使用；已在 UI 选择地域后以 UI 保存值为准 |
| `ALIYUN_POOL_NAME` | 代理池名称，如 `proxy-main` |
| `SOCKS5_PORT` | SOCKS5 端口 |
| `SOCKS5_USERNAME` | SOCKS5 用户名 |
| `SOCKS5_PASSWORD` | SOCKS5 密码 |
| `SOCKS5_CHECK_HOST` / `SOCKS5_CHECK_PORT` | 代理健康检查目标 |
| `SOCKS5_TIMEOUT` | 代理健康检查超时 |
| `PROXY_API_BEARER_TOKEN` | 面板 API 访问令牌 |

阿里云地域以面板“阿里云设置”中的地域下拉选择为准。保存 UI 设置后，`.env.local` 中的 `ALIYUN_REGION_ID` 不会覆盖该选择。自动调和默认每 120 秒执行一次，负责按 UI 设置的目标节点数创建、健康检查、补充和释放 ECS；目标节点数修改后会持久化，服务重启后继续使用。

> ⚠️ **安全提示**：`.env.local` 中含有阿里云 AccessKey，已通过 `.gitignore` / `.dockerignore` 排除，不会进入版本库或镜像。请勿在公开渠道粘贴真实凭证。

## 本地运行

```bash
pip install -r requirements.txt
cp .env .env.local          # 创建本地配置副本（不要提交真实凭据）
# 按需修改 .env.local 或通过 UI 配置面板设置
python dashboard.py --host 0.0.0.0 --port 8765
```

默认监听 `0.0.0.0:8765`。

## API 使用

中控 API 默认地址为 `http://127.0.0.1:8765`。代理提取、调度控制和粘性代理接口需要配置的 `PROXY_API_BEARER_TOKEN`。

### 鉴权

推荐使用请求头传递 Token：

```bash
curl -H "Authorization: Bearer <PROXY_API_BEARER_TOKEN>" \
  http://127.0.0.1:8765/api/status
```

控制接口也兼容 `token` 查询参数，但不建议把 Token 放在 URL、日志或浏览器历史中。

### 代理提取接口

所有代理接口返回的是可直接交给 SOCKS5 客户端使用的代理地址。中控会把所有健康 ECS/WarpGate 节点合并成一个统一资源池，业务方不需要知道代理实际位于哪台 ECS。请求失败时不要把 ECS 的原生公网 IP 当作 WARP 出口 IP 使用；只有响应中的 `exit_ip` 才是实际代理出口。

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/api/proxies` | 获取当前健康代理列表，可筛选出口类型和协议 |
| `GET` | `/api/proxies?family=ipv4` | 只获取真实 IPv4 出口 |
| `GET` | `/api/proxies?family=ipv6` | 只获取真实 IPv6 出口 |
| `GET` | `/api/proxies?family=both` | 获取 IPv4 和 IPv6 两类出口，默认值 |
| `GET` | `/api/proxies/balanced?strategy=round-robin` | 按调度策略获取一个代理 |
| `GET` | `/api/proxies/sticky?session_id=...&family=ipv6` | 按业务会话获取或复用粘性代理 |
| `DELETE` | `/api/proxies/sticky/<URL编码后的session_id>?family=ipv6` | 主动释放粘性绑定 |

#### 1. 普通 SOCKS5 列表

```bash
# 所有类型，返回 SOCKS5 和 HTTP 格式（默认 family=both、protocol=both）
curl -H "Authorization: Bearer <PROXY_API_BEARER_TOKEN>" \\
  "http://127.0.0.1:8765/api/proxies"

# 只提取真实 IPv4 出口的 SOCKS5 代理
curl -H "Authorization: Bearer <PROXY_API_BEARER_TOKEN>" \\
  "http://127.0.0.1:8765/api/proxies?family=ipv4&protocol=socks5"

# 只提取真实 IPv6 出口，并返回 HTTP 代理格式
curl -H "Authorization: Bearer <PROXY_API_BEARER_TOKEN>" \\
  "http://127.0.0.1:8765/api/proxies?family=ipv6&protocol=http"
```

参数取值：`family` 只能是 `ipv4`、`ipv6`、`both`；`protocol` 只能是 `socks5`、`http`、`both`。普通列表适合业务方自行维护代理、批量导入或每次请求随机选择。它不保证同一业务标识后续仍拿到同一代理。

返回数据中的 `proxy` 是完整代理 URL，通常为 `socks5://用户名:密码@ECS地址:端口`；`server_ip` 是业务连接到的 ECS 地址，`port` 是 SOCKS5 端口，`node_id` 是所属 ECS，`exit_ip` 是 WARP/WarpGate 对外访问时的真实出口。`family` 必须按 `exit_ip` 的 IP 版本理解，而不是按 `server_ip` 理解。

#### 2. IPv4、IPv6 和 `family=both`

- `family=ipv4`：只返回 `exit_ip` 为 IPv4 的代理，适合目标网站只接受 IPv4、需要 IPv4 地址白名单或业务明确要求 IPv4 的场景。
- `family=ipv6`：只返回 `exit_ip` 为 IPv6 的代理，适合 IPv6 网络测试、IPv6 地址访问或需要 IPv6 出口的场景。
- `family=both`：返回两类代理，适合业务方自行判断和混合使用；它不能用于粘性接口，因为粘性绑定必须明确绑定哪一种地址族。

WarpGate 模式按 WARP 返回的真实 `exit_ip` 分类。ECS 原生公网 IPv4 只是连接中控节点的入口地址，即使它是 IPv4，也不会被伪装成 WARP IPv4 出口。IPv6 客户端还必须具备 IPv6 网络能力；代理 URL 中的连接目标仍是 `server_ip:port`，不要直接把带冒号的 `exit_ip` 拼成 SOCKS5 服务器地址。

#### 3. 平衡获取单个代理

```bash
curl -G -H "Authorization: Bearer <PROXY_API_BEARER_TOKEN>" \\
  --data-urlencode "strategy=least-connections" \\
  --data-urlencode "family=ipv4" \\
  --data-urlencode "protocol=socks5" \\
  http://127.0.0.1:8765/api/proxies/balanced
```

`strategy` 支持 `round-robin`（稳定轮询）、`random`（随机）、`least-connections`（按中控记录的分配次数较少者）和 `lowest-latency`（按记录延迟较低者）。这里的“连接数”是中控分配计数，不是业务 TCP 连接关闭后的实时连接数。平衡接口适合每次任务只需要一个代理且不要求会话固定的场景。

#### 4. 粘性 SOCKS5 代理

粘性绑定键固定为 `session_id + family`。同一 `session_id` 可以同时拥有一条 IPv4 和一条 IPv6 绑定，互不影响；`family=both` 不允许用于粘性接口。`session_id` 可以是邮箱、手机号、注册账号、订单号等非空业务标识，不要求特定格式。

默认 TTL 为 600 秒（10 分钟），从首次绑定开始计算。TTL 内重复请求返回相同的 `server_ip + port`，并返回 `reused: true`，不会因为重复请求而延长有效期。原代理失效时，中控会从所有健康 ECS 的统一资源池重新选择，并在响应中返回 `failover: true`。不同会话不会同时占用同一个代理端口。

```bash
# 获取或复用 IPv6 粘性 SOCKS5 代理
curl -G \\
  -H "Authorization: Bearer <PROXY_API_BEARER_TOKEN>" \\
  --data-urlencode "session_id=user@example.com" \\
  --data-urlencode "family=ipv6" \\
  http://127.0.0.1:8765/api/proxies/sticky
```

成功响应包含 `proxy`、`node_id`、`server_ip`、`port`、`exit_ip`、`expires_at`、`expires_in`、`reused`；故障切换时还会包含 `failover: true`。业务方应保存 `session_id`，每次任务开始时请求同一会话，而不是保存并长期硬编码旧代理地址。

主动释放时，路径中的 `session_id` 必须 URL 编码。释放成功和绑定不存在都返回 HTTP 200，因此可以安全地在任务结束、用户注销或异常清理时重复调用：

```bash
curl -X DELETE \\
  -H "Authorization: Bearer <PROXY_API_BEARER_TOKEN>" \\
  "http://127.0.0.1:8765/api/proxies/sticky/user%40example.com?family=ipv6"
```

释放后该代理立即回到公共资源池。中控重启会清空内存中的粘性绑定，但不会释放 ECS；业务方如需重启后继续使用原绑定，应在重启后重新获取代理。

#### 5. 在客户端使用 SOCKS5

```bash
# curl 通过 SOCKS5 代理访问目标；--socks5-hostname 让 DNS 也经代理解析
curl --socks5-hostname warpuser:password@47.238.139.26:10016 https://example.com

# Python requests（需 pip install requests[socks]）
import requests
proxy = "socks5h://warpuser:password@47.238.139.26:10016"
response = requests.get("https://example.com", proxies={"http": proxy, "https": proxy}, timeout=30)
```

优先使用 `socks5h` 或 `--socks5-hostname`，这样目标域名解析也通过代理完成，避免本地 DNS 泄漏。IPv6 出口不代表客户端连接入口必须是 IPv6；但客户端所在网络必须能访问中控 API，且不能把 `exit_ip` 替代 `server_ip`。

### API 状态码

- `200`：获取、查询或释放成功；DELETE 即使绑定不存在也返回 200
- `400`：参数错误，例如空 `session_id`、非法 `family`、非法 `protocol` 或 `family=both` 用于粘性接口
- `401`：Token 缺失或无效
- `409`：当前没有满足 IPv4/IPv6 条件的代理、代理资源已被占用或调和发生资源冲突
- `500`：中控内部错误

### 管理接口

- `GET /api/status`：节点健康状态、统计和事件
- `GET /api/settings`：配置概览，不返回密钥明文
- `GET /api/billing`：账户余额和账单
- `GET /api/resources`：镜像、规格、交换机和启动模板
- `GET /api/target?target=5`：设置目标 ECS 数量
- `GET /api/reconcile`：立即执行一次调和
- `GET /api/nodes/delete?instance_id=i-xxx`：下线并释放指定节点

管理接口同样需要 Bearer Token。中控重启后，粘性绑定会清空，但 ECS 节点和代理资源不会因此释放。


### 构建镜像

如需本地构建：

```bash
docker build -t proxypilot:4.0 .
```

如使用 GitHub Actions 已发布到 Docker Hub 的镜像：

```bash
docker pull zhoushu1/zhoushu1:4.0
# 也可以拉取始终指向最新构建的标签
docker pull zhoushu1/zhoushu1:latest
```

### 运行容器

先准备配置文件：

```bash
# 先在宿主机预创建配置文件，避免 Docker 将不存在的源路径创建为目录
if [ ! -f dashboard_settings.json ]; then touch dashboard_settings.json; fi
chmod 666 dashboard_settings.json  # 容器以 UID 10001 的非 root 用户运行，需要写入设置
```

使用 Docker Hub 的 `4.0` 镜像运行：

```bash
docker run -d \
  --name proxypilot \
  --restart unless-stopped \
  -p 8765:8765 \
  --env-file .env.local \
  -v "$(pwd)/dashboard_settings.json:/app/dashboard_settings.json" \
  zhoushu1/zhoushu1:4.0
```

如需始终使用最新构建，将最后一行改为：

```text
zhoushu1/zhoushu1:latest
```

## 镜像发布（GitHub Actions）

本项目使用 GitHub Actions 自动构建并推送 Docker 镜像，**本地不执行任何推送操作**。

- 工作流：`.github/workflows/docker.yml`
- 触发条件：推送到 `master` 或 `main` 分支
- 镜像仓库：`zhoushu1/zhoushu1`
- 自动标签：
  - `latest` —— 指向最新构建
  - `4.0` —— 当前主版本号，与 `latest` 指向同一个镜像构建
  - `<sha>` —— 短提交哈希，便于回溯

### 所需 Secrets

在 GitHub 仓库 `Settings -> Secrets and variables -> Actions` 中配置：

| Secret | 说明 |
|---|---|
| `DOCKER_HUB_USERNAME` | Docker Hub 用户名 |
| `DOCKER_HUB_TOKEN` | Docker Hub Access Token（建议非密码） |

## 许可

仅供个人学习与合规使用，请遵守所在地区与目标服务的法律法规。
