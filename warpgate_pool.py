from __future__ import annotations

import json
import math
import random
import socket
import ssl
import struct
import threading
import time
from ipaddress import ip_address
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen


class WarpGateProxyPool:
    """通过 WarpGate API 聚合各 ECS 的白名单 SOCKS5 出口。"""

    def __init__(self, api_port: int = 4433, instance_count: int = 20,
                 ip_family: str = "both", timeout: float = 8.0,
                 cache_ttl: float = 5.0) -> None:
        if not 1 <= api_port <= 65535:
            raise ValueError("WarpGate API 端口无效")
        if not 1 <= instance_count <= 200:
            raise ValueError("WarpGate 实例数必须为 1-200")
        if ip_family not in ("ipv4", "ipv6", "both"):
            raise ValueError("IP 类型必须是 ipv4、ipv6 或 both")
        self.api_port = api_port
        self.instance_count = instance_count
        self.ip_family = ip_family
        self.timeout = timeout
        if cache_ttl <= 0:
            raise ValueError("WarpGate 缓存 TTL 必须大于 0")
        self.cache_ttl = cache_ttl
        self._nodes: dict[str, str] = {}
        self._proxies: list[dict] = []
        self._lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._rr_index = 0
        self._cache_time = 0.0
        self._latencies: dict[str, float] = {}
        self._connections: dict[str, int] = {}

    def enable(self, instance_id: str, public_ip: str) -> None:
        with self._lock:
            self._nodes[instance_id] = public_ip
            self._connections.setdefault(instance_id, 0)
            self._cache_time = 0.0

    def disable(self, instance_id: str) -> None:
        with self._lock:
            self._nodes.pop(instance_id, None)
            self._proxies = [p for p in self._proxies if p.get("instance_id") != instance_id]
            self._latencies.pop(instance_id, None)
            self._connections.pop(instance_id, None)
            self._cache_time = time.monotonic()

    def update_latency(self, instance_id: str, latency: float) -> None:
        if not math.isfinite(latency) or latency < 0:
            raise ValueError("延迟必须是非负有限数")
        with self._lock:
            if instance_id in self._nodes:
                self._latencies[instance_id] = latency

    def _fetch(self, public_ip: str) -> list[dict]:
        result = []
        for offset in range(0, self.instance_count, 20):
            query = urlencode({"num": min(20, self.instance_count - offset), "type": "json"})
            request = Request(f"http://{public_ip}:{self.api_port}/api/proxies?{query}")
            with urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            data = payload.get("data") or {}
            code = payload.get("code")
            # WarpGate 成功业务码为字符串 "00000"，兼容历史数字 200
            if code not in ("00000", 200):
                raise ValueError(f"WarpGate API 业务码无效: {code}")
            for item in data.get("proxies", []):
                exit_ip = item.get("exit_ip", "")
                try:
                    family = "ipv6" if ip_address(exit_ip).version == 6 else "ipv4"
                except ValueError:
                    continue
                if self.ip_family != "both" and family != self.ip_family:
                    continue
                result.append({**item, "family": family, "server_ip": public_ip})
        return result

    def refresh(self) -> int:
        collected = []
        with self._lock:
            nodes = list(self._nodes.items())
        for instance_id, public_ip in nodes:
            try:
                collected.extend({**item, "instance_id": instance_id} for item in self._fetch(public_ip))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
        with self._lock:
            unique: dict[tuple[str, str, int], dict] = {}
            for item in collected:
                proxy_url = item.get("proxy") or item.get("proxy_whitelist") or ""
                key = (item.get("instance_id", ""), proxy_url, int(item.get("port") or 0))
                unique.setdefault(key, item)
            self._proxies = list(unique.values())
            self._cache_time = time.monotonic()
            return len(self._proxies)

    def _refresh_if_expired(self) -> None:
        with self._lock:
            expired = time.monotonic() - self._cache_time >= self.cache_ttl
        if not expired:
            return
        with self._refresh_lock:
            with self._lock:
                expired = time.monotonic() - self._cache_time >= self.cache_ttl
            if expired:
                self.refresh()

    def health_check(self, public_ip: str) -> tuple[bool, float]:
        start = time.monotonic()
        try:
            items = self._fetch(public_ip)
        except (OSError, ValueError, json.JSONDecodeError):
            return False, 0.0
        if not items:
            return False, 0.0
        return True, round((time.monotonic() - start) * 1000, 1)

    def node_stats(self) -> dict[str, dict[str, int | float]]:
        self._refresh_if_expired()
        with self._lock:
            counts = {key: 0 for key in self._nodes}
            for proxy in self._proxies:
                counts[proxy.get("instance_id", "")] = counts.get(proxy.get("instance_id", ""), 0) + 1
            return {
                key: {
                    "public_ip": ip,
                    "proxy_count": counts.get(key, 0),
                    "connections": self._connections.get(key, 0),
                    "latency_ms": self._latencies.get(key, float("inf")),
                }
                for key, ip in self._nodes.items()
            }

    def proxies(self) -> list[dict]:
        self._refresh_if_expired()
        with self._lock:
            return list(self._proxies)

    def acquire(self, strategy: str = "round-robin") -> dict | None:
        strategies = {"round-robin", "random", "least-connections", "lowest-latency"}
        if strategy not in strategies:
            raise ValueError(f"未知代理调度策略: {strategy}")
        self._refresh_if_expired()
        with self._lock:
            if not self._proxies:
                return None
            items = sorted(self._proxies, key=lambda item: (
                item.get("instance_id", ""),
                item.get("proxy") or item.get("proxy_whitelist") or "",
                int(item.get("port") or 0),
            ))
            if strategy == "random":
                item = random.choice(items)
            elif strategy == "least-connections":
                item = min(items, key=lambda value: self._connections.get(value.get("instance_id", ""), 0))
            elif strategy == "lowest-latency":
                item = min(items, key=lambda value: self._latencies.get(value.get("instance_id", ""), float("inf")))
            else:
                item = items[self._rr_index % len(items)]
                self._rr_index = (self._rr_index + 1) % len(items)
            instance_id = item.get("instance_id", "")
            self._connections[instance_id] = self._connections.get(instance_id, 0) + 1
            return dict(item)

    @staticmethod
    def _recv_exact(sock: socket.socket, size: int) -> bytes:
        data = bytearray()
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                raise RuntimeError("SOCKS5 服务提前关闭连接")
            data.extend(chunk)
        return bytes(data)

    def test_item(self, item: dict, target_host: str, target_port: int = 80,
                  target_path: str = "/", timeout: float | None = None) -> dict[str, object]:
        timeout = timeout or self.timeout
        proxy_url = item.get("proxy") or item.get("proxy_whitelist") or ""
        parsed_proxy = urlparse(proxy_url)
        proxy_host = parsed_proxy.hostname or item.get("server_ip") or ""
        if proxy_host in ("127.0.0.1", "localhost", "0.0.0.0"):
            proxy_host = item.get("server_ip") or proxy_host
        proxy_port = parsed_proxy.port or int(item.get("port") or 0)
        username = (parsed_proxy.username or item.get("username") or "").encode()
        password = (parsed_proxy.password or item.get("password") or "").encode()
        start = time.monotonic()
        try:
            with socket.create_connection((proxy_host, proxy_port), timeout=timeout) as raw_sock:
                raw_sock.settimeout(timeout)
                raw_sock.sendall(b"\x05\x01\x02" if username else b"\x05\x01\x00")
                version, method = self._recv_exact(raw_sock, 2)
                expected_method = 2 if username else 0
                if version != 5 or method != expected_method:
                    raise RuntimeError("SOCKS5 认证方式不匹配")
                if method == 2:
                    raw_sock.sendall(
                        b"\x01" + bytes((len(username),)) + username
                        + bytes((len(password),)) + password
                    )
                    if self._recv_exact(raw_sock, 2) != b"\x01\x00":
                        raise RuntimeError("SOCKS5 认证失败")
                host = target_host.encode("idna")
                if len(host) > 255:
                    raise ValueError("目标主机名过长")
                raw_sock.sendall(
                    b"\x05\x01\x00\x03" + bytes((len(host),)) + host
                    + struct.pack("!H", target_port)
                )
                version, result, _, address_type = self._recv_exact(raw_sock, 4)
                if version != 5 or result != 0:
                    raise RuntimeError(f"SOCKS5 CONNECT 失败，错误码 {result}")
                address_size = {1: 4, 4: 16}.get(address_type)
                if address_size is None:
                    address_size = self._recv_exact(raw_sock, 1)[0]
                self._recv_exact(raw_sock, address_size + 2)

                sock = raw_sock
                if target_port == 443:
                    sock = ssl.create_default_context().wrap_socket(
                        raw_sock, server_hostname=target_host
                    )
                    sock.settimeout(timeout)
                request = (
                    f"GET {target_path} HTTP/1.1\r\nHost: {target_host}\r\n"
                    "Connection: close\r\nUser-Agent: ProxyPilot/1.0\r\n\r\n"
                ).encode()
                sock.sendall(request)
                response = self._recv_exact(sock, 12)
                status_line = response.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
                if not status_line.startswith("HTTP/"):
                    raise RuntimeError("目标未返回有效 HTTP 响应")
                status_code = int(status_line.split(" ", 2)[1])
                if not 200 <= status_code < 400:
                    raise RuntimeError(f"目标返回 HTTP {status_code}")
            return {
                "ok": True,
                "proxy": item,
                "status_line": status_line,
                "latency_ms": round((time.monotonic() - start) * 1000, 1),
            }
        except Exception as exc:
            return {
                "ok": False,
                "proxy": item,
                "error": str(exc),
                "latency_ms": round((time.monotonic() - start) * 1000, 1),
            }

    def test_proxy(self, public_ip: str, target_host: str, target_port: int = 80,
                   target_path: str = "/", timeout: float | None = None) -> dict[str, object]:
        candidates = [p for p in self.proxies() if p.get("server_ip") == public_ip]
        if not candidates:
            return {"ok": False, "error": "该节点没有可用 WarpGate 代理"}
        return self.test_item(candidates[0], target_host, target_port, target_path, timeout)
