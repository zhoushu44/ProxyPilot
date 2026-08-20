from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from alibabacloud_ecs20140526.client import Client as ECSClient
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models

from reconcile_controller import CloudInstance, LaunchCandidate


POOL_TAG_KEY = "reconcile-pool"
MANAGED_TAG_KEY = "managed-by"
MANAGED_TAG_VALUE = "reconcile-dashboard"


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
        client: Any | None = None,
    ) -> None:
        if not all((access_key_id, access_key_secret, region_id)):
            raise ValueError("真实模式缺少 AccessKey 或地域配置")
        self.region_id = region_id
        self.launch_template_id = launch_template_id
        self.launch_template_version = launch_template_version
        self.resource_group_id = resource_group_id
        self.instance_charge_type = instance_charge_type
        self.spot_duration = spot_duration
        self.spot_strategy = "SpotAsPriceGo" if instance_charge_type == "Spot" else ""
        self._runtime = util_models.RuntimeOptions(connect_timeout=10000, read_timeout=30000)
        self._client = client or ECSClient(
            open_api_models.Config(
                access_key_id=access_key_id,
                access_key_secret=access_key_secret,
                region_id=region_id,
                endpoint=f"ecs.{region_id}.aliyuncs.com",
            )
        )
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

    def run_instance(self, pool_name: str, candidate: LaunchCandidate, instance_name: str) -> str:
        self._pool_name = pool_name
        version = candidate.launch_template_version or self.launch_template_version
        kwargs: dict[str, Any] = dict(
            region_id=self.region_id,
            amount=1,
            min_amount=1,
            instance_charge_type="PostPaid",
            client_token=str(uuid4()),
            instance_name=instance_name,
            launch_template_id=candidate.launch_template_id or self.launch_template_id,
            launch_template_version=int(version) if version else None,
            v_switch_id=candidate.vswitch_id or None,
            instance_type=candidate.instance_type or None,
            resource_group_id=self.resource_group_id or None,
            tag=self._tags(pool_name, ecs_models.RunInstancesRequestTag),
        )
        if self.spot_strategy:
            kwargs["spot_strategy"] = self.spot_strategy
            kwargs["spot_duration"] = self.spot_duration
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
        body = self._client.describe_vswitches_with_options(
            ecs_models.DescribeVSwitchesRequest(
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

    def create_security_group(
        self,
        security_group_name: str,
        description: str,
        v_switch_id: str,
    ) -> dict[str, str]:
        """在目标 VSwitch 所属 VPC 创建普通安全组；本方法不创建任何访问规则。"""
        vpc_id = self._get_vswitch_vpc(v_switch_id)
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
    ) -> dict[str, Any]:
        if not socks5_username or not socks5_password:
            raise ValueError("创建 Dante 模板必须提供 SOCKS 用户名和密码")
        credentials = base64.b64encode(
            f"{socks5_username}:{socks5_password}".encode("utf-8")
        ).decode("ascii")
        dante_config = """logoutput: syslog
internal: 0.0.0.0 port = 1080
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
write_files:
  - path: /etc/danted.conf
    owner: root:root
    permissions: '0600'
    content: |
""" + "".join(f"      {line}\n" for line in dante_config.splitlines()) + f"""runcmd:
  - [useradd, --create-home, --shell, /bin/bash, {json.dumps(socks5_username)}]
  - [bash, -c, 'echo {credentials} | base64 -d | chpasswd']
  - [systemctl, enable, --now, danted]
"""
        self._client.authorize_security_group_with_options(
            ecs_models.AuthorizeSecurityGroupRequest(
                region_id=self.region_id,
                security_group_id=security_group_id,
                ip_protocol="tcp",
                port_range="1080/1080",
                source_cidr_ip="0.0.0.0/0",
                policy="accept",
                priority="1",
                description="Dante SOCKS5 public access",
                client_token=str(uuid4()),
            ),
            self._runtime,
        )
        template_kwargs: dict[str, Any] = dict(
            region_id=self.region_id,
            launch_template_name=launch_template_name,
            description=description or None,
            image_id=image_id,
            instance_type=instance_type,
            instance_charge_type="PostPaid",
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
        switches_body = self._client.describe_vswitches_with_options(
            ecs_models.DescribeVSwitchesRequest(region_id=self.region_id, page_number=1, page_size=50),
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
