from __future__ import annotations

import base64
import ipaddress
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from alibabacloud_ecs20140526.client import Client as ECSClient
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_vpc20160428.client import Client as VPCClient
from alibabacloud_vpc20160428 import models as vpc_models
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models

from reconcile_controller import CloudInstance, LaunchCandidate


POOL_TAG_KEY = "reconcile-pool"
MANAGED_TAG_KEY = "managed-by"
MANAGED_TAG_VALUE = "reconcile-dashboard"
BUILTIN_DANTE_TEMPLATE_NAME = "ProxyPilot-Dante"
BUILTIN_WARPGATE_TEMPLATE_NAME = "ProxyPilot-WarpGate"
BUILTIN_TEMPLATE_NAME = BUILTIN_DANTE_TEMPLATE_NAME


@dataclass(frozen=True)
class AliyunNode:
    instance_id: str
    status: str
    public_ip: str | None
    created_at: datetime
    zone: str
    healthy: bool = True
    recycling: bool = False


class AliyunECSAdapter:
    """阿里云 ECS 适配器；只管理带双重标签的实例。"""

    def __init__(
        self,
        access_key_id: str,
        access_key_secret: str,
        region_id: str,
        launch_template_id: str,
        launch_template_version: str = "",
        resource_group_id: str = "",
        instance_charge_type: str = "PostPaid",
        spot_duration: int = 0,
        proxy_mode: str = "socks5",
        socks5_port: int = 1080,
        http_proxy_port: int = 8080,
        warpgate_api_port: int = 4433,
        warpgate_instances: int = 20,
        warpgate_whitelist_ip: str = "",
        client: Any | None = None,
        vpc_client: Any | None = None,
    ) -> None:
        if not all((access_key_id, access_key_secret, region_id)):
            raise ValueError("真实模式缺少 AccessKey 或地域配置")
        self.region_id = region_id
        self.launch_template_id = launch_template_id
        self.launch_template_version = launch_template_version
        self.resource_group_id = resource_group_id
        self.instance_charge_type = (
            "PostPaid" if instance_charge_type == "Spot" else instance_charge_type
        )
        self.spot_duration = spot_duration
        self.proxy_mode = proxy_mode
        self.socks5_port = socks5_port
        self.http_proxy_port = http_proxy_port
        self.warpgate_api_port = warpgate_api_port
        self.warpgate_instances = warpgate_instances
        self.warpgate_whitelist_ip = warpgate_whitelist_ip
        self._repaired_security_groups: dict[tuple[str, str], str] = {}
        self._include_ipv6_rules = False
        self.spot_strategy = "SpotAsPriceGo" if instance_charge_type == "Spot" else ""
        self._runtime = util_models.RuntimeOptions(connect_timeout=10000, read_timeout=30000)
        config = open_api_models.Config(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            region_id=region_id,
        )
        self._client = client or ECSClient(
            open_api_models.Config(
                access_key_id=access_key_id,
                access_key_secret=access_key_secret,
                region_id=region_id,
                endpoint=f"ecs.{region_id}.aliyuncs.com",
            )
        )
        config.endpoint = f"vpc.{region_id}.aliyuncs.com"
        self._vpc_client = vpc_client or client or VPCClient(config)
        self._pool_name = ""

    @staticmethod
    def _tags(pool_name: str, model: Any) -> list[Any]:
        return [
            model(key=POOL_TAG_KEY, value=pool_name),
            model(key=MANAGED_TAG_KEY, value=MANAGED_TAG_VALUE),
        ]

    @staticmethod
    def _parse_time(value: str | None) -> datetime:
        if not value:
            return datetime.now(timezone.utc)
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _public_ip(instance: Any) -> str | None:
        public = getattr(getattr(instance, "public_ip_address", None), "ip_address", None) or []
        if public:
            return public[0]
        eip = getattr(instance, "eip_address", None)
        return getattr(eip, "ip_address", None) or None

    @staticmethod
    def _is_recycling(instance: Any) -> bool:
        locks = getattr(getattr(instance, "operation_locks", None), "lock_reason", None) or []
        return any(getattr(item, "lock_reason", "") == "Recycling" for item in locks)

    def _describe_page(self, pool_name: str, page: int) -> Any:
        request = ecs_models.DescribeInstancesRequest(
            region_id=self.region_id,
            resource_group_id=self.resource_group_id or None,
            page_number=page,
            page_size=100,
            tag=self._tags(pool_name, ecs_models.DescribeInstancesRequestTag),
        )
        return self._client.describe_instances_with_options(request, self._runtime).body

    def snapshot(self, pool_name: str | None = None) -> list[AliyunNode]:
        pool = pool_name or self._pool_name
        if not pool:
            return []
        result: list[AliyunNode] = []
        page = 1
        while True:
            body = self._describe_page(pool, page)
            items = getattr(getattr(body, "instances", None), "instance", None) or []
            for item in items:
                result.append(
                    AliyunNode(
                        instance_id=item.instance_id,
                        status=item.status,
                        public_ip=self._public_ip(item),
                        created_at=self._parse_time(getattr(item, "creation_time", None)),
                        zone=getattr(item, "zone_id", "") or "",
                        recycling=self._is_recycling(item),
                    )
                )
            total = int(getattr(body, "total_count", 0) or 0)
            if page * 100 >= total:
                break
            page += 1
        return result

    def list_managed_instances(self, pool_name: str) -> list[CloudInstance]:
        self._pool_name = pool_name
        return [
            CloudInstance(n.instance_id, n.status, n.public_ip, n.created_at, n.recycling)
            for n in self.snapshot(pool_name)
        ]

    def _warpgate_whitelist(self, value: str) -> str:
        if not value:
            raise ValueError("WarpGate API 白名单不能为空")
        try:
            network = ipaddress.ip_network(value, strict=False)
        except ValueError as exc:
            raise ValueError("WarpGate API 白名单必须是有效的 IP 或 CIDR") from exc
        if network.prefixlen == 0:
            raise ValueError("WarpGate API 白名单禁止 IPv4/IPv6 全网")
        return str(network)

    def _warpgate_port_ranges(self) -> tuple[str, str]:
        if not 1 <= self.warpgate_instances <= 200:
            raise ValueError("WarpGate 实例数必须为 1-200")
        end = self.warpgate_instances - 1
        return f"10010/{10010 + end}", f"11010/{11010 + end}"

    def _required_security_rules(self) -> list[tuple[str, str, str]]:
        if self.proxy_mode == "warpgate":
            whitelist = self._warpgate_whitelist(self.warpgate_whitelist_ip)
            proxy_port_range, whitelist_port_range = self._warpgate_port_ranges()
            return [
                ("tcp", f"{self.warpgate_api_port}/{self.warpgate_api_port}", whitelist),
                ("tcp", proxy_port_range, "0.0.0.0/0"),
                ("tcp", whitelist_port_range, "0.0.0.0/0"),
            ]
        return [
            ("tcp", f"{self.socks5_port}/{self.socks5_port}", "0.0.0.0/0"),
            ("tcp", f"{self.http_proxy_port}/{self.http_proxy_port}", "0.0.0.0/0"),
        ]

    def _all_security_rules(self) -> list[tuple[str, str, str]]:
        rules = self._required_security_rules()
        ipv6_rules: list[tuple[str, str, str]] = []
        for protocol, port_range, source in rules:
            if source == "0.0.0.0/0":
                ipv6_source = "::/0"
            else:
                network = ipaddress.ip_network(source, strict=False)
                if isinstance(network, ipaddress.IPv4Network):
                    continue
                ipv6_source = str(network)
            ipv6_rules.append((protocol, port_range, ipv6_source))
        return rules + ipv6_rules

    def _authorize_rule(self, group_id: str, protocol: str, port_range: str, source: str, description: str) -> None:
        """授权安全组规则；IPv6 源网段必须走 ipv_6source_cidr_ip，IPv4 走 source_cidr_ip。"""
        kwargs: dict[str, Any] = dict(
            region_id=self.region_id, security_group_id=group_id,
            ip_protocol=protocol, port_range=port_range,
            policy="accept", priority="1", description=description,
            client_token=str(uuid4()),
        )
        try:
            is_ipv6 = ipaddress.ip_network(source, strict=False).version == 6
        except ValueError:
            is_ipv6 = False
        if is_ipv6:
            kwargs["ipv_6source_cidr_ip"] = source
        else:
            kwargs["source_cidr_ip"] = source
        self._client.authorize_security_group_with_options(
            ecs_models.AuthorizeSecurityGroupRequest(**kwargs), self._runtime,
        )


    def _ensure_security_group(self, candidate: LaunchCandidate) -> str:
        cache_key = (candidate.vswitch_id, candidate.security_group_id)
        cached_group_id = self._repaired_security_groups.get(cache_key)
        group_id = cached_group_id or candidate.security_group_id
        body = None
        if group_id:
            try:
                body = self._client.describe_security_groups_with_options(
                    ecs_models.DescribeSecurityGroupsRequest(
                        region_id=self.region_id, security_group_id=group_id,
                        page_number=1, page_size=10,
                    ), self._runtime,
                ).body
            except Exception as exc:
                message = str(exc).lower()
                if "notfound" not in message and "not found" not in message and "does not exist" not in message:
                    raise
        groups = getattr(getattr(body, "security_groups", None), "security_group", None) or []
        group = next((item for item in groups if getattr(item, "security_group_id", "") == group_id), None)
        if group is None:
            created = self.create_security_group(
                f"proxy-auto-{int(datetime.now().timestamp())}",
                "ProxyPilot auto repaired security group",
                candidate.vswitch_id,
            )
            group_id = created["security_group_id"]
            self._repaired_security_groups[cache_key] = group_id
        for protocol, port_range, source in (
            self._all_security_rules() if self._include_ipv6_rules else self._required_security_rules()
        ):
            try:
                self._authorize_rule(
                    group_id, protocol, port_range, source,
                    "ProxyPilot automatic security rule",
                )
            except Exception as exc:
                if "InvalidRule.Duplicate" not in str(exc) and "duplicate" not in str(exc).lower():
                    raise
        return group_id

    def run_instance(self, pool_name: str, candidate: LaunchCandidate, instance_name: str) -> str:
        self._pool_name = pool_name
        security_group_id = self._ensure_security_group(candidate)
        version = candidate.launch_template_version or self.launch_template_version
        kwargs: dict[str, Any] = dict(
            region_id=self.region_id,
            amount=1,
            min_amount=1,
            instance_charge_type=self.instance_charge_type,
            client_token=str(uuid4()),
            instance_name=instance_name,
            launch_template_id=candidate.launch_template_id or self.launch_template_id,
            launch_template_version=int(version) if version else None,
            v_switch_id=candidate.vswitch_id or None,
            security_group_id=security_group_id,
            instance_type=candidate.instance_type or None,
            resource_group_id=self.resource_group_id or None,
            tag=self._tags(pool_name, ecs_models.RunInstancesRequestTag),
        )
        if self.spot_strategy:
            kwargs["spot_strategy"] = self.spot_strategy
            kwargs["spot_duration"] = self.spot_duration
        if self.proxy_mode == "warpgate" and self._vswitch_supports_ipv6(candidate.vswitch_id):
            kwargs["ipv_6address_count"] = 1
        request = ecs_models.RunInstancesRequest(**kwargs)
        body = self._client.run_instances_with_options(request, self._runtime).body
        ids = getattr(getattr(body, "instance_id_sets", None), "instance_id_set", None) or []
        if not ids:
            raise RuntimeError("阿里云 RunInstances 未返回实例 ID")
        return ids[0]

    def release_instance(self, instance_id: str) -> None:
        if not self._pool_name:
            raise RuntimeError("未建立受管实例上下文，拒绝释放")
        managed_ids = {item.instance_id for item in self.snapshot(self._pool_name)}
        if instance_id not in managed_ids:
            raise RuntimeError("实例缺少控制器管理标签，拒绝释放")
        request = ecs_models.DeleteInstancesRequest(
            region_id=self.region_id,
            instance_id=[instance_id],
            force=True,
            client_token=str(uuid4()),
        )
        self._client.delete_instances_with_options(request, self._runtime)

    def _get_vswitch_vpc(self, v_switch_id: str) -> str:
        body = self._vpc_client.describe_vswitches_with_options(
            vpc_models.DescribeVSwitchesRequest(
                region_id=self.region_id,
                v_switch_id=v_switch_id,
                page_number=1,
                page_size=10,
            ),
            self._runtime,
        ).body
        switches = getattr(getattr(body, "v_switches", None), "v_switch", None) or []
        exact = [item for item in switches if getattr(item, "v_switch_id", "") == v_switch_id]
        if len(exact) != 1 or not getattr(exact[0], "vpc_id", ""):
            raise RuntimeError("目标 VSwitch 不存在或未返回唯一 VPC")
        return exact[0].vpc_id

    def _vswitch_supports_ipv6(self, v_switch_id: str) -> bool:
        if not v_switch_id:
            return False
        body = self._vpc_client.describe_vswitches_with_options(
            vpc_models.DescribeVSwitchesRequest(
                region_id=self.region_id, v_switch_id=v_switch_id,
                page_number=1, page_size=10,
            ),
            self._runtime,
        ).body
        switches = getattr(getattr(body, "v_switches", None), "v_switch", None) or []
        return any(getattr(item, "ipv_6cidr_block", "") for item in switches)

    def create_security_group(
        self,
        security_group_name: str,
        description: str,
        v_switch_id: str,
        vpc_id: str | None = None,
    ) -> dict[str, str]:
        """在目标 VSwitch 所属 VPC 创建普通安全组；本方法不创建任何访问规则。"""
        vpc_id = vpc_id or self._get_vswitch_vpc(v_switch_id)
        request = ecs_models.CreateSecurityGroupRequest(
            region_id=self.region_id,
            security_group_name=security_group_name,
            description=description,
            security_group_type="normal",
            service_managed=False,
            vpc_id=vpc_id,
            resource_group_id=self.resource_group_id or None,
            client_token=str(uuid4()),
        )
        body = self._client.create_security_group_with_options(request, self._runtime).body
        security_group_id = getattr(body, "security_group_id", "") or ""
        if not security_group_id:
            raise RuntimeError("阿里云 CreateSecurityGroup 未返回安全组 ID")
        return {"security_group_id": security_group_id, "vpc_id": vpc_id}

    def delete_test_security_group(
        self,
        security_group_id: str,
        expected_name: str,
        expected_vpc_id: str,
    ) -> None:
        """仅在名称/VPC 精确匹配且没有实例关联时删除测试安全组。"""
        groups_body = self._client.describe_security_groups_with_options(
            ecs_models.DescribeSecurityGroupsRequest(
                region_id=self.region_id,
                security_group_id=security_group_id,
                page_number=1,
                page_size=10,
            ),
            self._runtime,
        ).body
        groups = getattr(getattr(groups_body, "security_groups", None), "security_group", None) or []
        exact = [item for item in groups if getattr(item, "security_group_id", "") == security_group_id]
        if len(exact) != 1:
            raise RuntimeError("测试安全组不存在或查询结果不唯一，拒绝删除")
        group = exact[0]
        if (getattr(group, "security_group_name", "") != expected_name
                or getattr(group, "vpc_id", "") != expected_vpc_id):
            raise RuntimeError("安全组名称或 VPC 与预期不符，拒绝删除")
        instances_body = self._client.describe_instances_with_options(
            ecs_models.DescribeInstancesRequest(
                region_id=self.region_id,
                security_group_id=security_group_id,
                page_number=1,
                page_size=100,
            ),
            self._runtime,
        ).body
        instances = getattr(getattr(instances_body, "instances", None), "instance", None) or []
        if instances or int(getattr(instances_body, "total_count", 0) or 0):
            raise RuntimeError("安全组仍有关联实例，拒绝删除")
        request = ecs_models.DeleteSecurityGroupRequest(
            region_id=self.region_id,
            security_group_id=security_group_id,
        )
        self._client.delete_security_group_with_options(request, self._runtime)

    def _find_launch_template(self, name: str) -> dict[str, str] | None:
        body = self._client.describe_launch_templates_with_options(
            ecs_models.DescribeLaunchTemplatesRequest(
                region_id=self.region_id,
                launch_template_name=[name],
                page_number=1,
                page_size=100,
                template_resource_group_id=self.resource_group_id or None,
            ),
            self._runtime,
        ).body
        templates = getattr(getattr(body, "launch_template_sets", None), "launch_template_set", None) or []
        exact = [item for item in templates if getattr(item, "launch_template_name", "") == name]
        if not exact:
            return None
        item = exact[0]
        template_id = getattr(item, "launch_template_id", "") or ""
        version = getattr(item, "default_version_number", None) or getattr(item, "latest_version_number", None)
        if not template_id or version is None:
            raise RuntimeError("阿里云 DescribeLaunchTemplates 未返回模板 ID 或版本")
        return {"launch_template_id": template_id, "launch_template_version": str(version)}

    def _available_instance_type(self, v_switch_id: str) -> str:
        switch_body = self._vpc_client.describe_vswitches_with_options(
            vpc_models.DescribeVSwitchesRequest(
                region_id=self.region_id,
                v_switch_id=v_switch_id,
                page_number=1,
                page_size=10,
            ),
            self._runtime,
        ).body
        switches = getattr(getattr(switch_body, "v_switches", None), "v_switch", None) or []
        switch = next((item for item in switches if getattr(item, "v_switch_id", "") == v_switch_id), None)
        zone_id = getattr(switch, "zone_id", "") if switch else ""
        if not zone_id:
            raise RuntimeError("目标 VSwitch 未返回可用区，无法查询有库存的 ECS 规格")

        body = self._client.describe_available_resource_with_options(
            ecs_models.DescribeAvailableResourceRequest(
                region_id=self.region_id,
                zone_id=zone_id,
                destination_resource="InstanceType",
                instance_charge_type=self.instance_charge_type,
                network_category="vpc",
            ),
            self._runtime,
        ).body
        zones = getattr(getattr(body, "available_zones", None), "available_zone", None) or []
        resources: list[Any] = []
        for zone in zones:
            available = getattr(getattr(zone, "available_resources", None), "available_resource", None) or []
            resources.extend(available)
        supported: list[str] = []
        for resource in resources:
            nested = getattr(getattr(resource, "supported_resources", None), "supported_resource", None) or []
            supported.extend(
                getattr(item, "value", "")
                for item in nested
                if getattr(item, "value", "")
                and getattr(item, "status", "") == "Available"
                and getattr(item, "status_category", "") == "WithStock"
            )
        supported = list(dict.fromkeys(supported))
        if not supported:
            raise RuntimeError(f"可用区 {zone_id} 没有支持 VPC 且有库存的 ECS 规格")
        preferred = [
            "ecs.t6-c1m1.large",
            "ecs.t6-c1m2.large",
            "ecs.g7nex.large",
            "ecs.g7.large",
            "ecs.c7.large",
        ]
        for instance_type in preferred:
            if instance_type in supported:
                return instance_type
        modern = [
            item for item in supported
            if not item.startswith(("ecs.t1.", "ecs.s1.", "ecs.s2.", "ecs.s3."))
        ]
        if modern:
            return sorted(modern)[0]
        raise RuntimeError(f"可用区 {zone_id} 只有不支持 VPC 的旧 ECS 规格")

    def _ensure_builtin_network(self) -> tuple[str, str]:
        switches_body = self._vpc_client.describe_vswitches_with_options(
            vpc_models.DescribeVSwitchesRequest(
                region_id=self.region_id, v_switch_name="proxypilot", enable_ipv_6=True,
                page_number=1, page_size=50,
            ),
            self._runtime,
        ).body
        switches = getattr(getattr(switches_body, "v_switches", None), "v_switch", None) or []
        vpcs_body = self._vpc_client.describe_vpcs_with_options(
            vpc_models.DescribeVpcsRequest(
                region_id=self.region_id, vpc_name="proxypilot", enable_ipv_6=True,
                page_number=1, page_size=50,
            ),
            self._runtime,
        ).body
        vpcs = getattr(getattr(vpcs_body, "vpcs", None), "vpc", None) or []
        vpcs = [
            item for item in vpcs
            if getattr(item, "vpc_id", "")
            and (getattr(item, "enabled_ipv_6", False) or getattr(item, "ipv_6cidr_block", ""))
        ]
        vpc_id = vpcs[0].vpc_id if vpcs else ""
        if vpc_id:
            exact_switches = [
                item for item in switches
                if getattr(item, "v_switch_id", "")
                and getattr(item, "vpc_id", "") == vpc_id
                and getattr(item, "ipv_6cidr_block", "")
            ]
            if exact_switches:
                switch = exact_switches[0]
                return switch.v_switch_id, vpc_id
        if not vpc_id:
            created = self._vpc_client.create_vpc_with_options(
                vpc_models.CreateVpcRequest(
                    region_id=self.region_id, vpc_name="proxypilot", cidr_block="172.16.0.0/16",
                    enable_ipv_6=True,
                    description="ProxyPilot managed VPC", client_token=str(uuid4()),
                ),
                self._runtime,
            ).body
            vpc_id = getattr(created, "vpc_id", "") or ""
            if not vpc_id:
                raise RuntimeError("阿里云 CreateVpc 未返回 VPC ID")
            for _ in range(12):
                status_body = self._vpc_client.describe_vpcs_with_options(
                    vpc_models.DescribeVpcsRequest(
                        region_id=self.region_id, vpc_id=vpc_id, enable_ipv_6=True,
                        page_number=1, page_size=10,
                    ), self._runtime,
                ).body
                status_items = getattr(getattr(status_body, "vpcs", None), "vpc", None) or []
                if status_items and getattr(status_items[0], "status", "") == "Available":
                    break
                time.sleep(2)
            else:
                raise RuntimeError("新建 VPC 长时间未进入 Available 状态")
        vpc_ipv6 = ""
        for _ in range(12):
            detail_body = self._vpc_client.describe_vpcs_with_options(
                vpc_models.DescribeVpcsRequest(
                    region_id=self.region_id, vpc_id=vpc_id, enable_ipv_6=True,
                    page_number=1, page_size=10,
                ), self._runtime,
            ).body
            detail_items = getattr(getattr(detail_body, "vpcs", None), "vpc", None) or []
            vpc_ipv6 = getattr(detail_items[0], "ipv_6cidr_block", "") if detail_items else ""
            if vpc_ipv6:
                break
            time.sleep(2)
        if not vpc_ipv6:
            raise RuntimeError("VPC 未获取到自动分配的 IPv6 网段")
        exact_switches = [
            item for item in switches
            if getattr(item, "v_switch_id", "")
            and getattr(item, "vpc_id", "") == vpc_id
            and getattr(item, "ipv_6cidr_block", "")
        ]
        if exact_switches:
            switch = exact_switches[0]
            return switch.v_switch_id, vpc_id

        zones_body = self._client.describe_zones_with_options(
            ecs_models.DescribeZonesRequest(
                region_id=self.region_id, instance_charge_type=self.instance_charge_type,
                spot_strategy=self.spot_strategy or None,
            ),
            self._runtime,
        ).body
        zones = getattr(getattr(zones_body, "zones", None), "zone", None) or []
        zone_id = getattr(zones[0], "zone_id", "") if zones else ""
        if not zone_id:
            raise RuntimeError("目标地域没有可用区，无法创建内置模板交换机")
        created = self._vpc_client.create_vswitch_with_options(
            vpc_models.CreateVSwitchRequest(
                region_id=self.region_id, zone_id=zone_id, vpc_id=vpc_id,
                v_switch_name="proxypilot", cidr_block="172.16.0.0/24",
                vpc_ipv_6cidr_block=vpc_ipv6, ipv_6cidr_block=0,
                description="ProxyPilot managed VSwitch", client_token=str(uuid4()),
            ),
            self._runtime,
        ).body
        v_switch_id = getattr(created, "v_switch_id", "") or ""
        if not v_switch_id:
            raise RuntimeError("阿里云 CreateVSwitch 未返回交换机 ID")
        return v_switch_id, vpc_id

    def ensure_builtin_launch_template(
        self,
        name: str = BUILTIN_TEMPLATE_NAME,
        socks5_username: str = "",
        socks5_password: str = "",
    ) -> dict[str, str]:
        existing = self._find_launch_template(name)
        if existing:
            return existing
        v_switch_id, vpc_id = self._ensure_builtin_network()
        groups_body = self._client.describe_security_groups_with_options(
            ecs_models.DescribeSecurityGroupsRequest(
                region_id=self.region_id, vpc_id=vpc_id, security_group_name="proxypilot",
                network_type="vpc", service_managed=False, page_number=1, page_size=100,
                resource_group_id=self.resource_group_id or None,
            ),
            self._runtime,
        ).body
        groups = getattr(getattr(groups_body, "security_groups", None), "security_group", None) or []
        exact = [item for item in groups if getattr(item, "security_group_name", "") == "proxypilot"]
        if exact:
            security_group_id = exact[0].security_group_id
        else:
            security_group_id = self.create_security_group(
                "proxypilot", "ProxyPilot managed security group", v_switch_id, vpc_id,
            )["security_group_id"]

        images_body = self._client.describe_images_with_options(
            ecs_models.DescribeImagesRequest(
                region_id=self.region_id, image_owner_alias="system", status="Available",
                architecture="x86_64", ostype="linux", is_support_cloudinit=True,
                page_number=1, page_size=100,
            ),
            self._runtime,
        ).body
        images = getattr(getattr(images_body, "images", None), "image", None) or []
        ubuntu = [item for item in images if "ubuntu" in (getattr(item, "image_name", "") or "").lower()]
        image = (ubuntu or images)[0] if (ubuntu or images) else None
        if image is None:
            raise RuntimeError("目标地域没有可用的 Linux cloud-init 镜像")

        instance_type = self._available_instance_type(v_switch_id)
        self._include_ipv6_rules = True
        return self.create_launch_template(
            launch_template_name=name,
            description=f"ProxyPilot built-in {'WarpGate' if self.proxy_mode == 'warpgate' else 'Dante'} template",
            image_id=image.image_id,
            instance_type=instance_type,
            v_switch_id=v_switch_id,
            security_group_id=security_group_id,
            socks5_username=socks5_username,
            socks5_password=socks5_password,
            proxy_mode=self.proxy_mode,
            warpgate_instances=self.warpgate_instances,
            warpgate_whitelist_ip=self.warpgate_whitelist_ip,
        )

    def create_launch_template(
        self,
        launch_template_name: str,
        image_id: str,
        instance_type: str,
        v_switch_id: str,
        security_group_id: str,
        description: str = "",
        socks5_username: str = "",
        socks5_password: str = "",
        proxy_mode: str = "socks5",
        warpgate_instances: int = 20,
        warpgate_whitelist_ip: str = "0.0.0.0/0",
    ) -> dict[str, Any]:
        if proxy_mode not in ("socks5", "warpgate"):
            raise ValueError("代理模式必须是 socks5 或 warpgate")
        if proxy_mode == "warpgate":
            if not 1 <= warpgate_instances <= 200:
                raise ValueError("WarpGate 实例数必须为 1-200")
            whitelist = self._warpgate_whitelist(warpgate_whitelist_ip)
            user_data = f'''#cloud-config
package_update: true
packages:
  - docker.io
write_files:
  - path: /root/warp/config.json
    owner: root:root
    permissions: '0600'
    content: |
      {{"port":{self.warpgate_api_port},"bind_address":"0.0.0.0","base_port":10010,"resident_count":{warpgate_instances},"max_instances":200,"whitelist_port_offset":1000,"ip_whitelist":[{{"ip":{json.dumps(whitelist)},"remark":"ProxyPilot"}}]}}
runcmd:
  - [mkdir, -p, /root/warp-instances, /root/warp/logs]
  - [bash, -c, 'for attempt in 1 2 3; do timeout 900 docker pull docker.1ms.run/zhoushu1/warp-pool:latest && exit 0; sleep 15; done; exit 1']
  - [bash, -c, 'docker rm -f warp >/dev/null 2>&1 || true']
  - [docker, run, -d, --name, warp, --network, host, --restart, unless-stopped, --cap-add, NET_ADMIN, --cap-add, SYS_MODULE, --device, /dev/net/tun:/dev/net/tun, -v, /root/warp-instances:/root/warp-instances, -v, /root/warp/config.json:/app/config.json, -v, /root/warp/logs:/app/logs, docker.1ms.run/zhoushu1/warp-pool:latest]
'''
            proxy_port_range, whitelist_port_range = self._warpgate_port_ranges()
            proxy_ports = [f"{self.warpgate_api_port}/{self.warpgate_api_port}", proxy_port_range, whitelist_port_range]
            rules = self._all_security_rules() if self._include_ipv6_rules else self._required_security_rules()
            for protocol, port_range, source in rules:
                self._authorize_rule(
                    security_group_id, protocol, port_range, source, "WarpGate access",
                )
        else:
            if not socks5_username or not socks5_password:
                raise ValueError("创建 Dante 模板必须提供 SOCKS 用户名和密码")
            credentials = base64.b64encode(f"{socks5_username}:{socks5_password}".encode("utf-8")).decode("ascii")
            dante_config = """logoutput: syslog
internal: 0.0.0.0 port = """ + str(self.socks5_port) + """
external: eth0
socksmethod: username
clientmethod: none
user.privileged: root
user.unprivileged: nobody
client pass {
    from: 0.0.0.0/0 to: 0.0.0.0/0
    log: connect disconnect error
}
socks pass {
    from: 0.0.0.0/0 to: 0.0.0.0/0
    command: connect bind udpassociate
    log: connect disconnect error
    socksmethod: username
}
"""
            user_data = """#cloud-config
package_update: true
packages:
  - dante-server
  - tinyproxy
write_files:
  - path: /etc/danted.conf
    owner: root:root
    permissions: '0600'
    content: |
""" + "".join(f"      {line}\n" for line in dante_config.splitlines()) + f"""  - path: /etc/tinyproxy/tinyproxy.conf
    owner: root:root
    permissions: '0600'
    content: |
      User tinyproxy
      Group tinyproxy
      Port {self.http_proxy_port}
      Listen 0.0.0.0
      Timeout 600
      MaxClients 100
      BasicAuth {socks5_username} {socks5_password}
      Allow 0.0.0.0/0
      ConnectPort 443
      ConnectPort 563
runcmd:
  - [useradd, --create-home, --shell, /bin/bash, {json.dumps(socks5_username)}]
  - [bash, -c, 'echo {credentials} | base64 -d | chpasswd']
  - [systemctl, enable, --now, danted]
  - [systemctl, enable, --now, tinyproxy]
"""
            for port, description in (
                (self.socks5_port, "Dante SOCKS5 public access"),
                (self.http_proxy_port, "Tinyproxy HTTP public access"),
            ):
                sources = ("0.0.0.0/0", "::/0") if self._include_ipv6_rules else ("0.0.0.0/0",)
                for source in sources:
                    self._authorize_rule(
                        security_group_id, "tcp", f"{port}/{port}", source, description,
                    )
        template_kwargs: dict[str, Any] = dict(
            region_id=self.region_id,
            launch_template_name=launch_template_name,
            description=description or None,
            image_id=image_id,
            instance_type=instance_type,
            instance_charge_type=self.instance_charge_type,
            internet_charge_type="PayByTraffic",
            internet_max_bandwidth_out=1,
            v_switch_id=v_switch_id,
            security_group_id=security_group_id,
            user_data=base64.b64encode(user_data.encode("utf-8")).decode("ascii"),
            resource_group_id=self.resource_group_id or None,
            template_resource_group_id=self.resource_group_id or None,
        )
        if self.spot_strategy:
            template_kwargs["spot_strategy"] = self.spot_strategy
            template_kwargs["spot_duration"] = self.spot_duration
        request = ecs_models.CreateLaunchTemplateRequest(**template_kwargs)
        body = self._client.create_launch_template_with_options(request, self._runtime).body
        template_id = getattr(body, "launch_template_id", "") or ""
        version = getattr(body, "launch_template_version_number", None)
        if not template_id or version is None:
            raise RuntimeError("阿里云 CreateLaunchTemplate 未返回模板 ID 或版本")
        return {"launch_template_id": template_id, "launch_template_version": str(version)}

    def discover_launch_candidates(self) -> tuple[LaunchCandidate, ...]:
        if not self.launch_template_id:
            raise RuntimeError("尚未配置启动模板 ID")
        versions = [int(self.launch_template_version)] if self.launch_template_version else None
        request = ecs_models.DescribeLaunchTemplateVersionsRequest(
            region_id=self.region_id,
            launch_template_id=self.launch_template_id,
            launch_template_version=versions,
            detail_flag=True,
            page_size=50,
        )
        body = self._client.describe_launch_template_versions_with_options(request, self._runtime).body
        items = getattr(getattr(body, "launch_template_version_sets", None), "launch_template_version_set", None) or []
        candidates: list[LaunchCandidate] = []
        for item in items:
            data = getattr(item, "launch_template_data", None)
            if data is None:
                continue
            version = str(getattr(item, "version_number", "") or "")
            vswitch_id = getattr(data, "v_switch_id", "") or ""
            instance_type = getattr(data, "instance_type", "") or ""
            image_id = getattr(data, "image_id", "") or ""
            security_group_id = getattr(data, "security_group_id", "") or ""
            if vswitch_id:
                candidates.append(LaunchCandidate(
                    self.region_id, instance_type, image_id, vswitch_id, security_group_id,
                    self.launch_template_id, version,
                ))
        if not candidates:
            raise RuntimeError("启动模板未提供 VSwitchId，无法构造安全的启动候选")
        return tuple(candidates)

    def query_resources(self) -> dict[str, Any]:
        images_body = self._client.describe_images_with_options(
            ecs_models.DescribeImagesRequest(
                region_id=self.region_id, image_owner_alias="system", status="Available",
                architecture="x86_64", ostype="linux", page_number=1, page_size=100,
            ), self._runtime,
        ).body
        types_body = self._client.describe_instance_types_with_options(
            ecs_models.DescribeInstanceTypesRequest(max_results=100), self._runtime,
        ).body
        switches_body = self._vpc_client.describe_vswitches_with_options(
            vpc_models.DescribeVSwitchesRequest(region_id=self.region_id, page_number=1, page_size=50),
            self._runtime,
        ).body
        groups_body = self._client.describe_security_groups_with_options(
            ecs_models.DescribeSecurityGroupsRequest(
                region_id=self.region_id, page_number=1, page_size=100,
                network_type="vpc", service_managed=False,
                resource_group_id=self.resource_group_id or None,
            ), self._runtime,
        ).body
        templates_body = self._client.describe_launch_templates_with_options(
            ecs_models.DescribeLaunchTemplatesRequest(
                region_id=self.region_id, page_number=1, page_size=100,
                template_resource_group_id=self.resource_group_id or None,
            ), self._runtime,
        ).body

        images = getattr(getattr(images_body, "images", None), "image", None) or []
        instance_types = getattr(getattr(types_body, "instance_types", None), "instance_type", None) or []
        switches = getattr(getattr(switches_body, "v_switches", None), "v_switch", None) or []
        groups = getattr(getattr(groups_body, "security_groups", None), "security_group", None) or []
        templates = getattr(getattr(templates_body, "launch_template_sets", None), "launch_template_set", None) or []
        return {
            "region_id": self.region_id,
            "images": [{"id": item.image_id, "name": getattr(item, "image_name", "") or item.image_id} for item in images],
            "instance_types": [{
                "id": item.instance_type_id,
                "name": f"{item.instance_type_id} ({getattr(item, 'cpu_core_count', '?')} vCPU / {getattr(item, 'memory_size', '?')} GiB)",
            } for item in instance_types],
            "vswitches": [{
                "id": item.v_switch_id, "name": getattr(item, "v_switch_name", "") or item.v_switch_id,
                "zone_id": getattr(item, "zone_id", "") or "", "vpc_id": getattr(item, "vpc_id", "") or "",
            } for item in switches],
            "security_groups": [{
                "id": item.security_group_id,
                "name": getattr(item, "security_group_name", "") or item.security_group_id,
                "vpc_id": getattr(item, "vpc_id", "") or "",
            } for item in groups],
            "launch_templates": [{
                "id": item.launch_template_id,
                "name": getattr(item, "launch_template_name", "") or item.launch_template_id,
                "version": str(getattr(item, "default_version_number", "") or getattr(item, "latest_version_number", "") or ""),
            } for item in templates],
        }
