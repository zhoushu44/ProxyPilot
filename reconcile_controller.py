from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol, Sequence


@dataclass(frozen=True)
class LaunchCandidate:
    region_id: str
    instance_type: str
    image_id: str
    vswitch_id: str
    security_group_id: str
    launch_template_id: str | None = None
    launch_template_version: str | None = None


@dataclass(frozen=True)
class CloudInstance:
    instance_id: str
    status: str
    public_ip: str | None
    created_at: datetime
    recycling: bool = False


@dataclass(frozen=True)
class ReconcileConfig:
    pool_name: str
    target_online: int
    candidates: Sequence[LaunchCandidate]
    batch_size: int = 5
    pending_timeout: timedelta = timedelta(minutes=5)
    release_excess: bool = True
    circuit_breaker_threshold: int = 3

    def __post_init__(self) -> None:
        if self.target_online < 0:
            raise ValueError("target_online 不能小于 0")
        if self.batch_size < 1:
            raise ValueError("batch_size 必须大于 0")
        if self.circuit_breaker_threshold < 1:
            raise ValueError("circuit_breaker_threshold 必须大于 0")
        if not self.candidates and self.target_online:
            raise ValueError("target_online 大于 0 时必须提供启动候选配置")


@dataclass(frozen=True)
class ReconcileResult:
    healthy: int
    pending: int
    draining: int
    created: tuple[str, ...]
    released: tuple[str, ...]
    errors: tuple[str, ...]
    circuit_broken: tuple[str, ...]


class ECSService(Protocol):
    def list_managed_instances(self, pool_name: str) -> list[CloudInstance]: ...

    def run_instance(
        self, pool_name: str, candidate: LaunchCandidate, instance_name: str
    ) -> str: ...

    def release_instance(self, instance_id: str) -> None: ...


class ProxyPool(Protocol):
    def health_check(self, public_ip: str) -> tuple[bool, float]: ...

    def enable(self, instance_id: str, public_ip: str) -> None: ...

    def disable(self, instance_id: str) -> None: ...

    def update_latency(self, instance_id: str, latency: float) -> None: ...


class SQLiteStateStore:
    def __init__(self, path: str) -> None:
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS reconcile_nodes (
                instance_id TEXT PRIMARY KEY,
                pool_name TEXT NOT NULL,
                state TEXT NOT NULL,
                public_ip TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS circuit_breaker (
                instance_id TEXT PRIMARY KEY,
                failure_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._db.commit()
        self._lock = threading.Lock()

    def known_ids(self, pool_name: str) -> set[str]:
        with self._lock:
            rows = self._db.execute(
                "SELECT instance_id FROM reconcile_nodes WHERE pool_name = ?",
                (pool_name,),
            ).fetchall()
        return {row[0] for row in rows}

    def save(self, pool_name: str, instance: CloudInstance, state: str) -> None:
        with self._lock:
            self._db.execute(
                """
                INSERT INTO reconcile_nodes(instance_id, pool_name, state, public_ip, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET
                    pool_name = excluded.pool_name,
                    state = excluded.state,
                    public_ip = excluded.public_ip,
                    updated_at = excluded.updated_at
                """,
                (
                    instance.instance_id,
                    pool_name,
                    state,
                    instance.public_ip,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            self._db.commit()

    def save_pending(self, pool_name: str, instance_id: str) -> None:
        self.save(
            pool_name,
            CloudInstance(
                instance_id=instance_id,
                status="Pending",
                public_ip=None,
                created_at=datetime.now(timezone.utc),
            ),
            "pending",
        )

    def delete(self, instance_id: str) -> None:
        with self._lock:
            self._db.execute(
                "DELETE FROM reconcile_nodes WHERE instance_id = ?", (instance_id,)
            )
            self._db.execute(
                "DELETE FROM circuit_breaker WHERE instance_id = ?", (instance_id,)
            )
            self._db.commit()

    def get_failure_count(self, instance_id: str) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT failure_count FROM circuit_breaker WHERE instance_id = ?",
                (instance_id,),
            ).fetchone()
        return row[0] if row else 0

    def increment_failure(self, instance_id: str) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT failure_count FROM circuit_breaker WHERE instance_id = ?",
                (instance_id,),
            ).fetchone()
            count = (row[0] if row else 0) + 1
            self._db.execute(
                """
                INSERT INTO circuit_breaker(instance_id, failure_count, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET
                    failure_count = excluded.failure_count,
                    updated_at = excluded.updated_at
                """,
                (instance_id, count, datetime.now(timezone.utc).isoformat()),
            )
            self._db.commit()
        return count

    def reset_failure(self, instance_id: str) -> None:
        with self._lock:
            self._db.execute(
                "DELETE FROM circuit_breaker WHERE instance_id = ?", (instance_id,)
            )
            self._db.commit()

    def close(self) -> None:
        self._db.close()


class ReconcileController:
    PENDING_STATUSES = {"Pending", "Starting", "Stopped"}

    def __init__(
        self,
        ecs: ECSService,
        proxy_pool: ProxyPool,
        state: SQLiteStateStore,
        config: ReconcileConfig,
        logger: logging.Logger | None = None,
    ) -> None:
        self.ecs = ecs
        self.proxy_pool = proxy_pool
        self.state = state
        self.config = config
        self.log = logger or logging.getLogger(__name__)
        self._lock = threading.Lock()
        self._candidate_cursor = 0

    def reconcile(self, now: datetime | None = None) -> ReconcileResult:
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("reconcile 已在运行")

        try:
            return self._reconcile(now or datetime.now(timezone.utc))
        finally:
            self._lock.release()

    def _reconcile(self, now: datetime) -> ReconcileResult:
        instances = self.ecs.list_managed_instances(self.config.pool_name)
        cloud_by_id = {item.instance_id: item for item in instances}
        created: list[str] = []
        released: list[str] = []
        errors: list[str] = []
        circuit_broken: list[str] = []
        healthy: list[CloudInstance] = []
        pending: list[CloudInstance] = []
        draining: list[CloudInstance] = []

        for missing_id in self.state.known_ids(self.config.pool_name) - cloud_by_id.keys():
            self._disable(missing_id, errors)
            self.state.delete(missing_id)

        for instance in instances:
            if instance.recycling:
                self._disable(instance.instance_id, errors)
                self.state.save(self.config.pool_name, instance, "draining")
                draining.append(instance)
                continue

            if instance.status == "Running" and instance.public_ip:
                try:
                    is_healthy, latency = self.proxy_pool.health_check(instance.public_ip)
                except Exception as exc:
                    errors.append(f"{instance.instance_id} 健康检查异常: {exc}")
                    failure_count = self.state.increment_failure(instance.instance_id)
                    self._disable(instance.instance_id, errors)
                    if failure_count >= self.config.circuit_breaker_threshold:
                        errors.append(
                            f"{instance.instance_id} 连续 {failure_count} 次健康检查失败，断路器触发自动释放"
                        )
                        circuit_broken.append(instance.instance_id)
                        if self._release(instance.instance_id, errors):
                            released.append(instance.instance_id)
                            self.state.delete(instance.instance_id)
                    elif now - instance.created_at >= self.config.pending_timeout:
                        if self._release(instance.instance_id, errors):
                            released.append(instance.instance_id)
                            self.state.delete(instance.instance_id)
                    else:
                        self.state.save(self.config.pool_name, instance, "pending")
                        pending.append(instance)
                    continue

                if is_healthy:
                    self.state.reset_failure(instance.instance_id)
                    try:
                        self.proxy_pool.enable(instance.instance_id, instance.public_ip)
                        self.proxy_pool.update_latency(instance.instance_id, latency)
                        self.state.save(self.config.pool_name, instance, "healthy")
                        healthy.append(instance)
                    except Exception as exc:
                        errors.append(f"{instance.instance_id} 加入代理池失败: {exc}")
                        self.state.save(self.config.pool_name, instance, "pending")
                        pending.append(instance)
                else:
                    failure_count = self.state.increment_failure(instance.instance_id)
                    self._disable(instance.instance_id, errors)
                    if failure_count >= self.config.circuit_breaker_threshold:
                        errors.append(
                            f"{instance.instance_id} 连续 {failure_count} 次健康检查失败，断路器触发自动释放"
                        )
                        circuit_broken.append(instance.instance_id)
                        if self._release(instance.instance_id, errors):
                            released.append(instance.instance_id)
                            self.state.delete(instance.instance_id)
                    elif now - instance.created_at >= self.config.pending_timeout:
                        if self._release(instance.instance_id, errors):
                            released.append(instance.instance_id)
                            self.state.delete(instance.instance_id)
                    else:
                        self.state.save(self.config.pool_name, instance, "pending")
                        pending.append(instance)
                continue

            if instance.status in self.PENDING_STATUSES:
                if now - instance.created_at >= self.config.pending_timeout:
                    self._disable(instance.instance_id, errors)
                    if self._release(instance.instance_id, errors):
                        released.append(instance.instance_id)
                        self.state.delete(instance.instance_id)
                else:
                    self.state.save(self.config.pool_name, instance, "pending")
                    pending.append(instance)
                continue

            self._disable(instance.instance_id, errors)
            self.state.save(self.config.pool_name, instance, "draining")
            draining.append(instance)

        excess = max(0, len(healthy) - self.config.target_online)
        if self.config.release_excess and excess:
            for instance in sorted(healthy, key=lambda item: item.created_at, reverse=True)[:excess]:
                self._disable(instance.instance_id, errors)
                if self._release(instance.instance_id, errors):
                    released.append(instance.instance_id)
                    self.state.delete(instance.instance_id)
                    healthy.remove(instance)

        effective_capacity = len(healthy) + len(pending)
        deficit = max(0, self.config.target_online - effective_capacity)
        create_count = min(deficit, self.config.batch_size)

        for index in range(create_count):
            instance_id = self._create_one(index, errors)
            if instance_id:
                created.append(instance_id)
                self.state.save_pending(self.config.pool_name, instance_id)

        return ReconcileResult(
            healthy=len(healthy),
            pending=len(pending) + len(created),
            draining=len(draining),
            created=tuple(created),
            released=tuple(released),
            errors=tuple(errors),
            circuit_broken=tuple(circuit_broken),
        )

    def _create_one(self, index: int, errors: list[str]) -> str | None:
        candidates = self.config.candidates
        for attempt in range(len(candidates)):
            position = (self._candidate_cursor + attempt) % len(candidates)
            candidate = candidates[position]
            name = f"{self.config.pool_name}-{index + 1}-{int(datetime.now().timestamp())}"
            try:
                instance_id = self.ecs.run_instance(
                    self.config.pool_name, candidate, name
                )
                self._candidate_cursor = (position + 1) % len(candidates)
                return instance_id
            except Exception as exc:
                errors.append(
                    f"候选 {candidate.region_id}/{candidate.vswitch_id}/"
                    f"{candidate.instance_type} 创建失败: {exc}"
                )
        return None

    def _disable(self, instance_id: str, errors: list[str]) -> None:
        try:
            self.proxy_pool.disable(instance_id)
        except Exception as exc:
            errors.append(f"{instance_id} 从代理池下线失败: {exc}")

    def _release(self, instance_id: str, errors: list[str]) -> bool:
        try:
            self.ecs.release_instance(instance_id)
            return True
        except Exception as exc:
            errors.append(f"{instance_id} 释放失败: {exc}")
            return False
