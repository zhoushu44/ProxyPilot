# 中控粘性代理调度需求

## 1. 背景

本项目运行在用户自有服务器的 Docker 中，作为统一中控。中控通过阿里云 ECS 管理多个 WARP/WarpGate 资源节点。业务方只访问中控 API，中控将所有 ECS 的健康代理合并为一个全局代理池。

## 2. 目标

新增按业务会话固定代理的 API：

- 同一个 `session_id` 可分别绑定一个 IPv4 和一个 IPv6 代理。
- 同一个 `session_id + family` 在固定 10 分钟内重复获取时返回原代理。
- 10 分钟从首次绑定开始计算，重复请求不刷新有效期。
- 到期后自动释放，代理立即回到公共池。
- 业务方可通过 DELETE API 主动释放。
- 绑定必须跨所有 ECS 统一调度，不区分代理来自哪台 ECS。
- 原有 `/api/proxies` 和 `/api/proxies/balanced` 行为保持不变。

## 3. API 契约

### 获取粘性代理

```http
GET /api/proxies/sticky?session_id=<业务标识>&family=ipv4|ipv6
Authorization: Bearer <PROXY_API_BEARER_TOKEN>
```

`session_id` 支持邮箱、手机号、注册账号等普通业务标识；只拒绝空值，不限制格式。接口不接受 `family=both`。

成功响应 HTTP 200：

```json
{
  "sticky": true,
  "session_id": "user@example.com",
  "family": "ipv6",
  "proxy": "socks5://warpuser:password@47.238.139.26:10016",
  "node_id": "ecs-instance-id",
  "server_ip": "47.238.139.26",
  "port": 10016,
  "exit_ip": "2a09:bac5:...",
  "expires_at": "2026-08-22T12:10:00+00:00",
  "expires_in": 600
}
```

重复请求在有效期内返回同一个 `server_ip + port`，并标记 `reused: true`。首次分配标记 `reused: false`。

### 主动释放

```http
DELETE /api/proxies/sticky/<URL编码后的session_id>?family=ipv4|ipv6
Authorization: Bearer <PROXY_API_BEARER_TOKEN>
```

找到并释放成功返回 HTTP 200；不存在也返回 HTTP 200，保证幂等。释放后代理立即回到公共池。

## 4. 调度与占用

- 资源池由全部已启用且健康的 ECS 节点组成。
- 代理唯一键为 `node_id + server_ip + port`，不能只使用端口。
- 一个代理端口同一时间只能被一个 `session_id + family` 独占。
- 首次分配按全局池轮询，跳过已占用端口。
- IPv4 与 IPv6 使用独立会话键，同一业务标识可以同时占用一条 IPv4 和一条 IPv6。
- 不足资源时返回 HTTP 409，不复用其他会话的端口。

## 5. 故障处理

获取请求发现原绑定已不在当前健康池时，从全部健康 ECS 资源中自动切换到未占用代理，并保持同一会话键。切换会改变出口 IP，响应标记 `reused: false` 和 `failover: true`。

## 6. 生命周期与并发

- TTL 固定 600 秒，从首次创建绑定时计算。
- 每次获取只读取剩余 TTL，不延长到期时间。
- 所有会话和占用表由中控进程内存维护；中控 Docker 重启会清空绑定，但不会影响 ECS 资源。
- 使用中控锁保证同一时间不会把一个代理端口分配给两个会话。
- 清理过期会话后再分配新代理。

## 7. 验收标准

1. 同一 `session_id + ipv6` 连续获取两次返回相同代理。
2. 同一 `session_id + ipv4` 可独立绑定另一条代理。
3. 不同 `session_id` 不会获得同一个代理端口。
4. DELETE 返回 200，释放后该端口可再次分配。
5. TTL 到期后重新获取会得到新绑定或明确返回资源不足。
6. `family=both`、空 `session_id`、缺失鉴权均被拒绝。
7. 真实中控连接当前多台 ECS，获取代理后通过 SOCKS5 完成 IPv4/IPv6 出口验证。
