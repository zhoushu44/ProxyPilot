from __future__ import annotations

import random
import socket
import struct
import threading
import time
from urllib.parse import quote


class Socks5ProxyPool:
    def __init__(
        self,
        port: int,
        username: str,
        password: str,
        check_host: str = "1.1.1.1",
        check_port: int = 80,
        timeout: float = 5.0,
        http_port: int = 8080,
    ) -> None:
        if not 1 <= port <= 65535 or not 1 <= http_port <= 65535 or not 1 <= check_port <= 65535:
            raise ValueError("代理端口必须为 1–65535")
        if not username or not password:
            raise ValueError("SOCKS5 用户名和密码不能为空")
        if len(username.encode()) > 255 or len(password.encode()) > 255:
            raise ValueError("SOCKS5 用户名和密码编码后不能超过 255 字节")
        self.port = port
        self.http_port = http_port
        self.username = username
        self.password = password
        self.check_host = check_host
        self.check_port = check_port
        self.timeout = timeout
        self._nodes: dict[str, str] = {}
        self._latencies: dict[str, float] = {}
        self._connections: dict[str, int] = {}
        self._bytes_sent: dict[str, int] = {}
        self._bytes_received: dict[str, int] = {}
        self._rr_index = 0
        self._lock = threading.Lock()

    @staticmethod
    def _recv_exact(sock: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = sock.recv(size - len(chunks))
            if not chunk:
                raise RuntimeError("SOCKS5 服务提前关闭连接")
            chunks.extend(chunk)
        return bytes(chunks)

    def health_check(self, public_ip: str) -> tuple[bool, float]:
        """返回 (是否健康, 延迟毫秒)；不健康时延迟为 0。"""
        username = self.username.encode()
        password = self.password.encode()
        start = time.monotonic()
        with socket.create_connection((public_ip, self.port), timeout=self.timeout) as sock:
            sock.settimeout(self.timeout)
            sock.sendall(b"\x05\x01\x02")
            if self._recv_exact(sock, 2) != b"\x05\x02":
                return False, 0.0
            sock.sendall(b"\x01" + bytes((len(username),)) + username + bytes((len(password),)) + password)
            if self._recv_exact(sock, 2) != b"\x01\x00":
                return False, 0.0
            try:
                address = socket.inet_pton(socket.AF_INET, self.check_host)
                destination = b"\x01" + address
            except OSError:
                host = self.check_host.encode("idna")
                if len(host) > 255:
                    raise ValueError("健康检查主机名过长")
                destination = b"\x03" + bytes((len(host),)) + host
            sock.sendall(b"\x05\x01\x00" + destination + struct.pack("!H", self.check_port))
            version, result, _reserved, address_type = self._recv_exact(sock, 4)
            if version != 5 or result != 0:
                return False, 0.0
            address_size = {1: 4, 4: 16}.get(address_type)
            if address_size is None:
                if address_type != 3:
                    return False, 0.0
                address_size = self._recv_exact(sock, 1)[0]
            self._recv_exact(sock, address_size + 2)
            latency = (time.monotonic() - start) * 1000
            return True, round(latency, 1)

    def enable(self, instance_id: str, public_ip: str) -> None:
        with self._lock:
            self._nodes[instance_id] = public_ip

    def disable(self, instance_id: str) -> None:
        with self._lock:
            self._nodes.pop(instance_id, None)
            self._latencies.pop(instance_id, None)

    def update_latency(self, instance_id: str, latency: float) -> None:
        with self._lock:
            if instance_id in self._nodes:
                self._latencies[instance_id] = latency

    def latencies(self) -> dict[str, float]:
        with self._lock:
            return dict(self._latencies)

    def record_connection(self, instance_id: str) -> None:
        with self._lock:
            self._connections[instance_id] = self._connections.get(instance_id, 0) + 1

    def record_traffic(self, instance_id: str, sent: int = 0, received: int = 0) -> None:
        with self._lock:
            self._bytes_sent[instance_id] = self._bytes_sent.get(instance_id, 0) + sent
            self._bytes_received[instance_id] = self._bytes_received.get(instance_id, 0) + received

    def node_stats(self) -> dict[str, dict[str, int | float]]:
        with self._lock:
            result = {}
            for instance_id, ip in self._nodes.items():
                result[instance_id] = {
                    "public_ip": ip,
                    "latency_ms": self._latencies.get(instance_id, 0),
                    "connections": self._connections.get(instance_id, 0),
                    "bytes_sent": self._bytes_sent.get(instance_id, 0),
                    "bytes_received": self._bytes_received.get(instance_id, 0),
                }
            return result

    def acquire(self, strategy: str = "round-robin", protocol: str = "socks5") -> str | None:
        """按策略获取一个代理 URL；返回 None 表示池为空。"""
        if protocol not in ("socks5", "http"):
            raise ValueError("protocol 必须是 socks5 或 http")
        with self._lock:
            if not self._nodes:
                return None
            items = sorted(self._nodes.items())
            if strategy == "random":
                instance_id, ip = random.choice(items)
            elif strategy == "least-connections":
                instance_id, ip = min(items, key=lambda x: self._connections.get(x[0], 0))
            elif strategy == "lowest-latency":
                instance_id, ip = min(items, key=lambda x: self._latencies.get(x[0], float("inf")))
            else:  # round-robin
                instance_id, ip = items[self._rr_index % len(items)]
                self._rr_index = (self._rr_index + 1) % len(items)
            self._connections[instance_id] = self._connections.get(instance_id, 0) + 1
        username = quote(self.username, safe="")
        password = quote(self.password, safe="")
        if protocol == "http":
            return f"http://{username}:{password}@{ip}:{self.http_port}"
        return f"socks5://{username}:{password}@{ip}:{self.port}"

    def proxies(self, protocol: str = "both") -> list[str]:
        if protocol not in ("socks5", "http", "both"):
            raise ValueError("protocol 必须是 socks5、http 或 both")
        username = quote(self.username, safe="")
        password = quote(self.password, safe="")
        with self._lock:
            addresses = sorted(self._nodes.values())
        result: list[str] = []
        for address in addresses:
            if protocol in ("socks5", "both"):
                result.append(f"socks5://{username}:{password}@{address}:{self.port}")
            if protocol in ("http", "both"):
                result.append(f"http://{username}:{password}@{address}:{self.http_port}")
        return result

    def test_proxy(self, public_ip: str, target_host: str, target_port: int = 80, target_path: str = "/", timeout: float | None = None) -> dict[str, object]:
        """通过指定代理节点向目标发起 HTTP GET，返回测试结果。"""
        timeout = timeout or self.timeout
        username = self.username.encode()
        password = self.password.encode()
        start = time.monotonic()
        try:
            with socket.create_connection((public_ip, self.port), timeout=timeout) as sock:
                sock.settimeout(timeout)
                # SOCKS5 握手
                sock.sendall(b"\x05\x01\x02")
                if self._recv_exact(sock, 2) != b"\x05\x02":
                    return {"ok": False, "error": "SOCKS5 握手失败"}
                sock.sendall(b"\x01" + bytes((len(username),)) + username + bytes((len(password),)) + password)
                if self._recv_exact(sock, 2) != b"\x01\x00":
                    return {"ok": False, "error": "SOCKS5 认证失败"}
                # CONNECT 目标
                try:
                    addr_bytes = socket.inet_pton(socket.AF_INET, target_host)
                    destination = b"\x01" + addr_bytes
                except OSError:
                    host = target_host.encode("idna")
                    destination = b"\x03" + bytes((len(host),)) + host
                sock.sendall(b"\x05\x01\x00" + destination + struct.pack("!H", target_port))
                version, result, _reserved, address_type = self._recv_exact(sock, 4)
                if version != 5 or result != 0:
                    return {"ok": False, "error": f"SOCKS5 CONNECT 失败，错误码 {result}"}
                addr_len = {1: 4, 4: 16}.get(address_type)
                if addr_len is None:
                    if address_type != 3:
                        return {"ok": False, "error": f"未知地址类型 {address_type}"}
                    addr_len = self._recv_exact(sock, 1)[0]
                self._recv_exact(sock, addr_len + 2)
                # 发送 HTTP 请求
                request = f"GET {target_path} HTTP/1.1\r\nHost: {target_host}\r\nConnection: close\r\nUser-Agent: ProxyPoolTest/1.0\r\n\r\n".encode()
                sock.sendall(request)
                response = b""
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    response += chunk
                    if len(response) > 65536:
                        break
            elapsed = round((time.monotonic() - start) * 1000, 1)
            status_line = response.split(b"\r\n", 1)[0].decode("ascii", errors="replace") if response else ""
            status_code = 0
            parts = status_line.split(" ", 2)
            if len(parts) >= 2 and parts[0].startswith("HTTP/"):
                try:
                    status_code = int(parts[1])
                except ValueError:
                    pass
            return {
                "ok": True,
                "status_code": status_code,
                "status_line": status_line,
                "latency_ms": elapsed,
                "response_size": len(response),
            }
        except Exception as exc:
            elapsed = round((time.monotonic() - start) * 1000, 1)
            return {"ok": False, "error": str(exc), "latency_ms": elapsed}
