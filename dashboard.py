from __future__ import annotations

import argparse
import hmac
import ipaddress
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen
from urllib.error import URLError

from reconcile_controller import (
    CloudInstance,
    LaunchCandidate,
    ReconcileConfig,
    ReconcileController,
    ReconcileResult,
    SQLiteStateStore,
)
from aliyun_ecs import AliyunECSAdapter
from aliyun_billing import AliyunBillingAdapter
from proxy_pool import Socks5ProxyPool


ENV_SETTING_NAMES = {
    "ALIBABA_CLOUD_ACCESS_KEY_ID": "access_key_id",
    "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "access_key_secret",
    "ALIYUN_REGION_ID": "region_id",
    "ALIYUN_POOL_NAME": "pool_name",
    "SOCKS5_PORT": "socks5_port",
    "SOCKS5_USERNAME": "socks5_username",
    "SOCKS5_PASSWORD": "socks5_password",
    "SOCKS5_CHECK_HOST": "socks5_check_host",
    "SOCKS5_CHECK_PORT": "socks5_check_port",
    "SOCKS5_TIMEOUT": "socks5_timeout",
    "PROXY_API_BEARER_TOKEN": "proxy_api_bearer_token",
}


def load_dotenv_settings(path: str | Path) -> dict[str, str]:
    """读取受限的 .env 配置；进程环境变量始终优先，且不会记录变量值。"""
    dotenv: dict[str, str] = {}
    source = Path(path)
    if source.is_file():
        try:
            for raw_line in source.read_text(encoding="utf-8-sig").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                name, value = line.split("=", 1)
                name = name.strip()
                if name not in ENV_SETTING_NAMES:
                    continue
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                dotenv[name] = value
        except OSError:
            logging.warning("无法读取 .env，将仅使用系统环境变量")

    return {
        setting: os.environ[name] if name in os.environ else dotenv.get(name, "")
        for name, setting in ENV_SETTING_NAMES.items()
    }


HTML = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>节点调和控制台</title>
<style>
:root{color-scheme:light;--bg:#eef1f4;--panel:#fff;--line:#ccd3da;--line2:#e4e8ec;--text:#20262d;--muted:#66717d;--blue:#426783;--blue2:#e9f0f5;--green:#34715a;--amber:#97661c;--red:#a24747;--shadow:0 1px 2px #26374612;font-family:"Segoe UI","Microsoft YaHei UI",system-ui,sans-serif;font-size:13px}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);min-width:760px}button,input,select{font:inherit}button{height:28px;border:1px solid #aeb8c1;border-radius:3px;background:#f8f9fa;color:#26313a;padding:0 10px;cursor:pointer}button:hover{background:#edf1f4}button:active{background:#e1e7eb}button:focus-visible,input:focus-visible,select:focus-visible{outline:2px solid #6d99b9;outline-offset:1px}.primary{color:#fff;background:var(--blue);border-color:#365b76}.primary:hover{background:#365d79}.danger{color:#fff;background:var(--red);border-color:#873939}.danger:hover{background:#873939}.node-delete{height:24px;color:var(--red);padding:0 8px}button:disabled{opacity:.55;cursor:not-allowed}.app{height:100vh;display:grid;grid-template-rows:42px 1fr 24px}.toolbar{display:flex;align-items:center;gap:8px;padding:6px 12px;background:#f8f9fb;border-bottom:1px solid var(--line);box-shadow:var(--shadow)}.title{font-weight:600;font-size:14px;margin-right:10px;white-space:nowrap}.sep{width:1px;height:20px;background:var(--line);margin:0 3px}.field{display:flex;align-items:center;gap:6px;color:var(--muted);white-space:nowrap}.field input{width:64px;height:28px;border:1px solid #adb7c0;border-radius:3px;padding:0 7px;background:#fff}.spacer{flex:1}.refresh{display:flex;align-items:center;gap:5px;color:var(--muted);white-space:nowrap}.refresh select{height:28px;border:1px solid #adb7c0;border-radius:3px;background:#fff;padding:0 4px}.main{padding:10px 12px;overflow:auto;display:grid;grid-template-rows:auto minmax(280px,1fr);gap:10px}.summary{display:grid;grid-template-columns:1.35fr repeat(4,minmax(112px,1fr));gap:8px}.card{background:var(--panel);border:1px solid var(--line);border-radius:3px;box-shadow:var(--shadow);min-height:72px;padding:9px 11px}.runtime{display:flex;align-items:center;gap:10px}.led{width:10px;height:10px;border-radius:50%;background:var(--green);box-shadow:0 0 0 3px #34715a1d}.led.busy{background:var(--amber);animation:pulse 1s infinite}.led.error{background:var(--red)}@keyframes pulse{50%{opacity:.45}}.card-label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.3px}.card-value{font-size:22px;font-weight:600;line-height:27px;font-variant-numeric:tabular-nums}.billing{grid-column:1/-1;display:grid;grid-template-columns:repeat(7,1fr);gap:12px;min-height:64px}.billing .card-value{font-size:17px}.billing-warning{grid-column:1/-1;color:var(--amber);font-size:11px}.card-note{font-size:11px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.green{color:var(--green)}.amber{color:var(--amber)}.red{color:var(--red)}.workspace{display:grid;grid-template-columns:minmax(560px,1fr) 270px;gap:10px;min-height:0}.panel{background:var(--panel);border:1px solid var(--line);border-radius:3px;box-shadow:var(--shadow);min-height:0;display:flex;flex-direction:column}.panel-head{height:36px;display:flex;align-items:center;padding:0 10px;border-bottom:1px solid var(--line);font-weight:600}.panel-head small{font-weight:400;color:var(--muted);margin-left:7px}.table-wrap{overflow:auto;flex:1}table{width:100%;border-collapse:collapse;font-size:12px}th{position:sticky;top:0;background:#f4f6f8;color:#56616c;text-align:left;font-size:11px;font-weight:600;height:29px;border-bottom:1px solid var(--line);padding:0 9px;white-space:nowrap}td{height:34px;padding:0 9px;border-bottom:1px solid var(--line2);white-space:nowrap}tbody tr:hover{background:#f5f8fa}.mono{font-family:Consolas,"SFMono-Regular",monospace;font-size:11px}.badge{display:inline-flex;align-items:center;gap:5px;border-radius:10px;padding:2px 7px;background:#edf0f2;color:#58636d}.badge:before{content:"";width:6px;height:6px;border-radius:50%;background:#87919a}.badge.healthy{background:#e8f3ee;color:#286149}.badge.healthy:before{background:#3b8466}.badge.pending{background:#fbf2df;color:#805715}.badge.pending:before{background:#c48929}.badge.draining,.badge.error{background:#f7eaea;color:#883e3e}.badge.draining:before,.badge.error:before{background:#b95050}.empty{text-align:center;color:var(--muted);padding:36px!important}.side{display:grid;grid-template-rows:minmax(190px,1fr) minmax(130px,.65fr);gap:10px;min-height:0}.candidate-list,.event-list{overflow:auto}.candidate{padding:9px 10px;border-bottom:1px solid var(--line2)}.candidate-top{display:flex;justify-content:space-between;gap:6px}.candidate-name{font-weight:600}.candidate-meta{font-size:11px;color:var(--muted);margin-top:4px}.stock{font-size:11px;color:var(--green)}.event{padding:7px 10px;border-bottom:1px solid var(--line2);font-size:11px}.event time{display:block;color:var(--muted);margin-bottom:2px}.statusbar{display:flex;align-items:center;gap:12px;padding:0 12px;background:#e4e8ec;border-top:1px solid #c4cbd2;color:#56616b;font-size:11px}.statusbar .right{margin-left:auto}dialog{width:520px;max-width:calc(100vw - 30px);max-height:calc(100vh - 30px);border:1px solid #aeb8c1;border-radius:4px;padding:0;box-shadow:0 10px 35px #0005;color:var(--text)}dialog>form{max-height:calc(100vh - 32px);display:flex;flex-direction:column}dialog::backdrop{background:#26374655}.dialog-head{height:38px;display:flex;align-items:center;padding:0 12px;border-bottom:1px solid var(--line);font-weight:600}.settings-form{padding:10px 12px;display:grid;grid-template-columns:145px 1fr;gap:8px 10px;align-items:center;overflow-y:auto}.settings-form label{color:#4d5964}.settings-form input,.settings-form select{height:28px;border:1px solid #adb7c0;border-radius:3px;padding:0 7px;min-width:0;background:#fff}.settings-note{grid-column:1/-1;padding:7px 8px;background:var(--blue2);color:#536574;border:1px solid #d5e0e8;font-size:11px}.settings-help{grid-column:1/-1;border:1px solid var(--line);background:#fafbfc;padding:8px 10px;font-size:11px;color:#53606b}.settings-help-title{font-weight:600;color:#34424e;margin-bottom:5px}.settings-help ol{margin:0;padding-left:18px}.settings-help li{margin:4px 0;line-height:1.45}.settings-help a{color:#356b91;text-decoration:none}.settings-help a:hover{text-decoration:underline}.settings-help .warning{color:#8a5a16}.permission-box{margin:6px 0;padding:7px 8px;background:#f2f4f6;border:1px solid #d8dde2}.permission-box summary{cursor:pointer;font-weight:600;color:#3d4c58}.permission-box pre{margin:7px 0 0;max-height:190px;overflow:auto;padding:7px;background:#20272e;color:#e6edf2;font:10px/1.45 Consolas,monospace;white-space:pre-wrap}.permission-list{margin:5px 0 0!important;columns:2;column-gap:18px}.permission-list li{break-inside:avoid;font-family:Consolas,monospace;font-size:10px}.secret-state{font-size:11px;color:var(--muted)}.dialog-actions{height:42px;display:flex;align-items:center;justify-content:flex-end;gap:7px;padding:0 12px;border-top:1px solid var(--line);background:#f7f8f9}.toast{position:fixed;right:14px;bottom:34px;max-width:360px;background:#28333c;color:#fff;padding:8px 11px;border-radius:3px;box-shadow:0 4px 16px #0003;opacity:0;transform:translateY(5px);pointer-events:none;transition:.16s}.toast.show{opacity:1;transform:none}.toast.bad{background:#8a3e3e}@media(max-width:1000px){.summary{grid-template-columns:1.4fr repeat(2,1fr)}.workspace{grid-template-columns:1fr}.side{grid-template-columns:1fr 1fr;grid-template-rows:220px}.main{display:block}.summary,.workspace{margin-bottom:10px}}
</style>
</head>
<body><div class="app">
<header class="toolbar"><div class="title">节点调和控制台</div><div class="sep"></div><label class="field">目标节点数 <input id="target" type="number" min="0" max="1000" aria-label="目标节点数"></label><button id="saveTarget">应用</button><button id="reconcile" class="primary">立即调和</button><button id="openProxyTest">代理测试</button><button id="openBalanced">负载均衡</button><button id="openSettings">阿里云设置</button><button id="openSecurityGroup">创建安全组</button><button id="openTemplate">创建启动模板</button><a href="/help" style="color:#356b91;text-decoration:none;padding:5px 4px">使用说明</a><input id="tokenInput" type="password" maxlength="256" placeholder="API Token（远程操作需填写）" style="width:180px;font-size:12px;padding:3px 6px" title="填写 API Token 后可远程执行写操作"><button id="saveToken" title="保存 Token 到浏览器" style="padding:3px 8px;font-size:12px">解锁</button><div class="spacer"></div><label class="refresh"><input id="auto" type="checkbox" checked> 自动刷新</label><select id="interval" aria-label="刷新间隔"><option value="3">3 秒</option><option value="5" selected>5 秒</option><option value="10">10 秒</option><option value="30">30 秒</option></select><button id="refresh" title="刷新 (F5)">刷新</button></header>
<main class="main">
<section class="summary">
<div class="card runtime"><span id="led" class="led"></span><div><div class="card-label">运行状态</div><div id="runtime" style="font-size:16px;font-weight:600;margin:2px 0">正在连接</div><div id="runtimeNote" class="card-note">读取控制器状态</div></div></div>
<div class="card"><div class="card-label">健康</div><div id="healthy" class="card-value green">—</div><div class="card-note">实时 SOCKS5 探测</div></div>
<div class="card"><div class="card-label">启动中</div><div id="pending" class="card-value amber">—</div><div class="card-note">等待实例就绪</div></div>
<div class="card"><div class="card-label">回收中</div><div id="draining" class="card-value">—</div><div class="card-note">已从池中下线</div></div>
<div class="card"><div class="card-label">错误</div><div id="errors" class="card-value red">—</div><div class="card-note">最近一次调和</div></div>
<div class="card billing"><div><div class="card-label">账户余额（可用现金）</div><div id="billAvailableCash" class="card-value">—</div></div><div><div class="card-label">账户可用额度</div><div id="billBalance" class="card-value">—</div></div><div><div class="card-label">当月已花费</div><div id="billSpent" class="card-value">—</div></div><div><div class="card-label">信控额度</div><div id="billCredit" class="card-value">—</div></div><div><div class="card-label">币种</div><div id="billCurrency" class="card-value">—</div></div><div><div class="card-label">账期</div><div id="billCycle" class="card-value">—</div></div><div><div class="card-label">API 更新时间</div><div id="billUpdated" class="card-value">—</div></div><div id="billStatus" class="billing-warning">正在读取费用中心；账单非实时，以费用中心出账为准</div></div>
</section>
<section class="workspace"><div class="panel"><div class="panel-head">节点列表 <small id="nodeCount">0 个节点</small></div><div class="table-wrap"><table><thead><tr><th>状态</th><th>实例 ID</th><th>公网 IP</th><th>云状态</th><th>可用区</th><th>延迟</th><th>归属地</th><th>失败</th><th>创建时间</th><th>操作</th></tr></thead><tbody id="nodes"></tbody></table></div></div>
<aside class="side"><div class="panel"><div class="panel-head">候选可用区 <small>按顺序故障转移</small></div><div id="candidates" class="candidate-list"></div></div><div class="panel"><div class="panel-head">最近事件</div><div id="events" class="event-list"></div></div></aside></section>
</main><footer class="statusbar"><span>适配器：<b id="adapter">—</b></span><span>池：<b id="pool">—</b></span><span>阿里云：<b id="aliyunState">未配置</b></span><span class="right" id="updated">尚未刷新</span></footer></div><div id="toast" class="toast" role="status"></div>
<dialog id="settingsDialog"><form id="settingsForm"><div class="dialog-head">阿里云设置</div><div class="settings-form"><label for="akId">AccessKey ID</label><input id="akId" maxlength="128" autocomplete="off" placeholder="输入新 AccessKey ID"><label for="akSecret">AccessKey Secret</label><div><input id="akSecret" type="password" maxlength="256" autocomplete="new-password" placeholder="留空保持现有密钥" style="width:100%"><div id="secretState" class="secret-state">密钥未配置</div></div><label for="regionId">地域</label><select id="regionId" required><option value="">请选择地域</option><option value="cn-qingdao">华北 1（青岛） · cn-qingdao</option><option value="cn-beijing">华北 2（北京） · cn-beijing</option><option value="cn-zhangjiakou">华北 3（张家口） · cn-zhangjiakou</option><option value="cn-huhehaote">华北 5（呼和浩特） · cn-huhehaote</option><option value="cn-wulanchabu">华北 6（乌兰察布） · cn-wulanchabu</option><option value="cn-hangzhou">华东 1（杭州） · cn-hangzhou</option><option value="cn-shanghai">华东 2（上海） · cn-shanghai</option><option value="cn-nanjing">华东 5（南京） · cn-nanjing</option><option value="cn-fuzhou">华东 6（福州） · cn-fuzhou</option><option value="cn-shenzhen">华南 1（深圳） · cn-shenzhen</option><option value="cn-heyuan">华南 2（河源） · cn-heyuan</option><option value="cn-guangzhou">华南 3（广州） · cn-guangzhou</option><option value="cn-chengdu">西南 1（成都） · cn-chengdu</option><option value="cn-hongkong">中国香港 · cn-hongkong</option><option value="ap-southeast-1">新加坡 · ap-southeast-1</option><option value="ap-southeast-3">马来西亚（吉隆坡） · ap-southeast-3</option><option value="ap-southeast-5">印度尼西亚（雅加达） · ap-southeast-5</option><option value="ap-southeast-6">菲律宾（马尼拉） · ap-southeast-6</option><option value="ap-southeast-7">泰国（曼谷） · ap-southeast-7</option><option value="ap-northeast-1">日本（东京） · ap-northeast-1</option><option value="ap-northeast-2">韩国（首尔） · ap-northeast-2</option><option value="us-west-1">美国（硅谷） · us-west-1</option><option value="us-east-1">美国（弗吉尼亚） · us-east-1</option><option value="eu-central-1">德国（法兰克福） · eu-central-1</option><option value="eu-west-1">英国（伦敦） · eu-west-1</option><option value="me-east-1">阿联酋（迪拜） · me-east-1</option></select><label for="templateId">启动模板 ID</label><select id="templateId"><option value="">请选择启动模板</option></select><label for="templateVersion">启动模板版本（可选）</label><input id="templateVersion" maxlength="128"><label for="resourceGroupId">资源组 ID（可选）</label><input id="resourceGroupId" maxlength="128"><label for="poolName">实例标签池名称</label><select id="poolName" required></select><label for="socksUsername">SOCKS 用户名</label><input id="socksUsername" maxlength="32" autocomplete="username"><label for="socksPassword">SOCKS 密码</label><div><input id="socksPassword" type="password" maxlength="256" autocomplete="new-password" placeholder="留空保持现有密码" style="width:100%"><div id="socksPasswordState" class="secret-state">密码未配置</div></div><label for="proxyApiToken">API Token</label><div><input id="proxyApiToken" type="password" maxlength="256" autocomplete="new-password" placeholder="留空保持现有 Token" style="width:100%"><div id="proxyApiTokenState" class="secret-state">Token 未配置</div></div><label for="clearSecret">清除现有密钥</label><input id="clearSecret" type="checkbox" style="width:auto;justify-self:start"><label for="batchSize">调和批量大小</label><input id="batchSize" type="number" min="1" max="50" placeholder="3"><label for="pendingTimeout">启动超时（分钟）</label><input id="pendingTimeout" type="number" min="1" max="120" placeholder="5"><label for="autoReconcileInterval">自动调和间隔（秒）</label><input id="autoReconcileInterval" type="number" min="0" max="3600" placeholder="0=关闭"><label for="circuitBreakerThreshold">断路器阈值</label><input id="circuitBreakerThreshold" type="number" min="1" max="20" placeholder="3"><label for="instanceChargeType">实例计费方式</label><select id="instanceChargeType"><option value="PostPaid">按量付费</option><option value="Spot">抢占式</option></select><label for="spotDuration">抢占式保护期</label><select id="spotDuration"><option value="0">无保护期（价格最低）</option><option value="1">1 小时保护期</option><option value="6">6 小时保护期</option></select><div class="settings-note">可先保存非敏感配置。只有 AccessKey、地域、启动模板和池名称完整且校验成功后才激活阿里云 ECS；系统不会生成凭据。自动调和间隔设为 0 则关闭定时调和。断路器阈值达到后自动释放故障节点。</div><div class="settings-help"><div class="settings-help-title">获取方法（点击链接将在新标签页打开阿里云官方页面）</div><ol><li><b>AccessKey：</b>进入 <a href="https://ram.console.aliyun.com/users" target="_blank" rel="noopener noreferrer">RAM 用户控制台</a>，创建专用 RAM 用户并生成 AccessKey；Secret 只显示一次，请立即保存。随后在“权限管理 → 权限策略”创建自定义策略，并将策略授权给该用户。查看 <a href="https://help.aliyun.com/zh/ram/user-guide/accesskey-pair-management/" target="_blank" rel="noopener noreferrer">创建 AccessKey</a>、<a href="https://help.aliyun.com/zh/ram/create-a-custom-policy" target="_blank" rel="noopener noreferrer">创建自定义策略</a>和<a href="https://help.aliyun.com/zh/ram/user-guide/grant-permissions-to-the-ram-user" target="_blank" rel="noopener noreferrer">为用户授权</a>。<div class="permission-box"><b>本控制器建议的最小权限</b><ul class="permission-list"><li>ecs:RunInstances</li><li>ecs:DescribeInstances</li><li>ecs:DeleteInstances</li><li>ecs:CreateLaunchTemplate</li><li>ecs:DescribeLaunchTemplates</li><li>ecs:DescribeLaunchTemplateVersions</li><li>ecs:DescribeImages</li><li>ecs:DescribeSecurityGroups</li><li>ecs:CreateSecurityGroup</li><li>ecs:DeleteSecurityGroup（可选）</li><li>ecs:DescribeKeyPairs</li><li>ecs:DescribeTags</li><li>vpc:DescribeVpcs</li><li>vpc:DescribeVSwitches</li><li>ecs:DescribeInstanceHistoryEvents</li><li>bssapi:QueryAccountBalance</li><li>bssapi:QueryBillOverview</li></ul><span class="warning">BSS 两项只读权限用于余额与账单展示；最后一项 ECS 权限用于查询创建失败和系统事件，若只依赖 DescribeInstances 的 Recycling 锁可暂不授予。若启动模板引用实例 RAM 角色，还需 ram:PassRole。</span><details><summary>展开可复制的 RAM Policy JSON</summary><pre>{
  &quot;Version&quot;: &quot;1&quot;,
  &quot;Statement&quot;: [{
    &quot;Effect&quot;: &quot;Allow&quot;,
    &quot;Action&quot;: [
      &quot;ecs:RunInstances&quot;,
      &quot;ecs:DescribeInstances&quot;,
      &quot;ecs:DeleteInstances&quot;,
      &quot;ecs:CreateLaunchTemplate&quot;,
      &quot;ecs:DescribeLaunchTemplates&quot;,
      &quot;ecs:DescribeLaunchTemplateVersions&quot;,
      &quot;ecs:DescribeImages&quot;,
      &quot;ecs:DescribeSecurityGroups&quot;,
      &quot;ecs:CreateSecurityGroup&quot;,
      &quot;ecs:AuthorizeSecurityGroup&quot;,
      &quot;ecs:DeleteSecurityGroup&quot;,
      &quot;ecs:DescribeKeyPairs&quot;,
      &quot;ecs:DescribeTags&quot;,
      &quot;ecs:DescribeInstanceHistoryEvents&quot;,
      &quot;bssapi:QueryAccountBalance&quot;,
      &quot;bssapi:QueryBillOverview&quot;,
      &quot;vpc:DescribeVpcs&quot;,
      &quot;vpc:DescribeVSwitches&quot;
    ],
    &quot;Resource&quot;: &quot;*&quot;
  }]
}</pre></details></div></li><li><b>Region ID：</b>地域通常形如 <span class="mono">cn-hangzhou</span>，可在 ECS 控制台当前地域或 <a href="https://help.aliyun.com/zh/ecs/api-regions-describeregions" target="_blank" rel="noopener noreferrer">地域列表说明</a>中确认。</li><li><b>启动模板 ID / 版本：</b>进入 <a href="https://ecs.console.aliyun.com/" target="_blank" rel="noopener noreferrer">ECS 控制台</a>，切换到相同地域，在“部署与弹性 → 实例启动模板”中创建或复制模板 ID、版本号。查看 <a href="https://help.aliyun.com/zh/ecs/user-guide/launch-templates/" target="_blank" rel="noopener noreferrer">启动模板说明</a>。</li><li><b>资源组 ID：</b>可选。进入 <a href="https://resourcemanager.console.aliyun.com/resource-group" target="_blank" rel="noopener noreferrer">资源组控制台</a>复制 <span class="mono">rg-...</span>；不使用资源组可留空。</li></ol><div class="warning">安全提示：请使用最小权限 RAM 子账号，不要使用主账号 AccessKey，也不要通过聊天或截图发送 Secret。</div></div></div><div class="dialog-actions"><button id="cancelSettings" type="button">取消</button><button id="saveSettings" class="primary" type="submit">保存</button></div></form></dialog>
<dialog id="deleteDialog"><form id="deleteForm"><div class="dialog-head">确认删除节点</div><div class="settings-form"><div class="settings-note">此操作将先从代理池下线节点，再永久释放阿里云 ECS 实例，无法撤销。</div><label>实例 ID</label><strong id="deleteInstanceId" class="mono"></strong></div><div class="dialog-actions"><button id="cancelDelete" type="button">取消</button><button id="confirmDelete" class="danger" type="submit">删除并释放</button></div></form></dialog>
<dialog id="securityGroupDialog"><form id="securityGroupForm"><div class="dialog-head">创建普通 VPC 安全组</div><div class="settings-form"><label for="securityGroupName">安全组名称</label><input id="securityGroupName" maxlength="128" pattern="[A-Za-z][A-Za-z0-9._-]{2,127}" required><label for="securityGroupDescription">描述</label><input id="securityGroupDescription" maxlength="256" pattern="[A-Za-z0-9 .,_:/()\-]{1,256}" required><label for="securityGroupVSwitchId">目标交换机</label><select id="securityGroupVSwitchId" required></select><div class="settings-note">仅创建目标交换机所属 VPC 的普通安全组，不创建任何公网入站规则。名称和描述必须唯一且仅允许白名单字符。</div></div><div class="dialog-actions"><button id="cancelSecurityGroup" type="button">取消</button><button id="createSecurityGroup" class="primary" type="submit">创建</button></div></form></dialog>
<dialog id="templateDialog"><form id="templateForm"><div class="dialog-head">创建阿里云启动模板</div><div class="settings-form"><label for="templateName">模板名称</label><input id="templateName" maxlength="128" required><label for="templateDescription">描述（可选）</label><input id="templateDescription" maxlength="256"><label for="imageId">镜像</label><select id="imageId" required></select><label for="instanceType">实例规格</label><select id="instanceType" required></select><label for="vSwitchId">交换机</label><select id="vSwitchId" required></select><label for="securityGroupId">安全组</label><select id="securityGroupId" required></select><div class="settings-note">请先保存 AccessKey、地域及 SOCKS 凭据。模板将通过 Ubuntu cloud-init 安装并配置 Dante，同时为所选安全组开放公网 TCP 1080。创建成功后会自动回填模板 ID 和版本。</div></div><div class="dialog-actions"><button id="cancelTemplate" type="button">取消</button><button id="createTemplate" class="primary" type="submit">创建</button></div></form></dialog>
<dialog id="proxyTestDialog"><form id="proxyTestForm"><div class="dialog-head">代理测试</div><div class="settings-form"><label for="testInstanceId">节点实例 ID</label><select id="testInstanceId" required><option value="">请选择节点</option></select><label for="testTargetUrl">目标 URL</label><input id="testTargetUrl" type="url" maxlength="512" placeholder="https://httpbin.org/ip" required value="https://httpbin.org/ip"><div id="testResult" style="grid-column:1/-1;min-height:60px;max-height:200px;overflow:auto;padding:8px;background:#f2f4f6;border:1px solid #d8dde2;font-size:12px;font-family:Consolas,monospace;white-space:pre-wrap">等待测试</div><div class="settings-note">通过指定节点的 SOCKS5 代理向目标 URL 发起 HTTP GET 请求，返回状态码、延迟和响应大小。</div></div><div class="dialog-actions"><button id="cancelProxyTest" type="button">取消</button><button id="runProxyTest" class="primary" type="submit">执行测试</button></div></form></dialog>
<dialog id="balancedDialog"><form id="balancedForm"><div class="dialog-head">负载均衡获取代理</div><div class="settings-form"><label for="lbStrategy">调度策略</label><select id="lbStrategy"><option value="round-robin" selected>轮询 (round-robin)</option><option value="random">随机 (random)</option><option value="least-connections">最少连接 (least-connections)</option><option value="lowest-latency">最低延迟 (lowest-latency)</option></select><div id="lbResult" style="grid-column:1/-1;min-height:40px;padding:8px;background:#f2f4f6;border:1px solid #d8dde2;font-size:12px;font-family:Consolas,monospace;word-break:break-all">尚未获取</div><div class="settings-note">按所选策略从代理池中分配一个代理地址。需配置 API Token。</div></div><div class="dialog-actions"><button id="cancelBalanced" type="button">取消</button><button id="getBalanced" class="primary" type="submit">获取代理</button></div></form></dialog>
<script>
const $ = id => document.getElementById(id);
let timer = null;
let loading = false;
let lastTarget = null;
const esc=s=>String(s??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const labels={healthy:'健康',pending:'启动中',draining:'回收中',error:'错误'};
function toast(msg,bad=false){const e=$('toast');e.textContent=msg;e.className='toast show'+(bad?' bad':'');clearTimeout(e._t);e._t=setTimeout(()=>e.className='toast',2600)}
function render(d){$('reconcile').disabled=!d.reconcile_enabled;$('healthy').textContent=d.stats.healthy;$('pending').textContent=d.stats.pending;$('draining').textContent=d.stats.draining;$('errors').textContent=d.stats.errors;$('aliyunState').textContent=d.aliyun_configured?'已配置':'未配置';$('runtime').textContent=d.runtime.running?'调和执行中':(d.runtime.ok?'运行正常':'需要关注');$('runtimeNote').textContent=(d.runtime.last_reconcile?'上次调和 '+new Date(d.runtime.last_reconcile).toLocaleTimeString():'等待首次调和')+(d.runtime.auto_reconcile_interval>0?(d.runtime.next_auto_reconcile?' · 下次 '+new Date(d.runtime.next_auto_reconcile).toLocaleTimeString():' · 自动调和已启用'):'');$('led').className='led '+(d.runtime.running?'busy':(d.runtime.ok?'':'error'));if(document.activeElement!==$('target'))$('target').value=d.target;lastTarget=d.target;$('adapter').textContent=d.adapter;$('pool').textContent=d.pool;$('updated').textContent='更新于 '+new Date(d.generated_at).toLocaleTimeString();$('nodeCount').textContent=d.nodes.length+' 个节点';
$('nodes').innerHTML=d.nodes.length?d.nodes.map(n=>{const lat=n.latency_ms?n.latency_ms.toFixed(0)+'ms':'—';const geo=n.geo?`${esc(n.geo.country)} ${esc(n.geo.city||'')}`:'—';const fc=n.failure_count||0;return `<tr><td><span class="badge ${esc(n.state)}">${esc(labels[n.state]||n.state)}</span></td><td class="mono">${esc(n.instance_id)}</td><td class="mono">${esc(n.public_ip)}</td><td>${esc(n.cloud_status)}</td><td>${esc(n.zone)}</td><td class="mono">${lat}</td><td>${geo}</td><td>${fc>0?`<span class="red">${fc}</span>`:'0'}</td><td>${esc(new Date(n.created_at).toLocaleString())}</td><td><button class="node-delete" data-instance-id="${esc(n.instance_id)}" ${d.runtime.running?'disabled':''}>删除</button></td></tr>`}).join(''):'<tr><td class="empty" colspan="10">暂无节点</td></tr>';
$('candidates').innerHTML=d.candidates.map(c=>`<div class="candidate"><div class="candidate-top"><span class="candidate-name">${esc(c.zone)}</span><span class="stock">${esc(c.availability)}</span></div><div class="candidate-meta">${esc(c.region_id)} · ${esc(c.instance_type)}<br>${esc(c.vswitch_id)}</div></div>`).join('');$('events').innerHTML=d.events.length?d.events.map(e=>`<div class="event"><time>${esc(new Date(e.time).toLocaleString())}</time>${esc(e.message)}</div>`).join(''):'<div class="event">暂无操作记录</div>'}
async function api(path,opt){const c=new AbortController;const t=setTimeout(()=>c.abort(),60000);try{const tok=localStorage.getItem('pp_token')||'';const hdrs=Object.assign({},(opt&&opt.headers)||{});if(tok)hdrs['Authorization']='Bearer '+tok;const r=await fetch(path,{...opt,headers:hdrs,signal:c.signal});const d=await r.json().catch(()=>({error:'响应格式错误'}));if(!r.ok)throw Error(d.error||`HTTP ${r.status}`);return d}catch(e){if(e.name==='AbortError')throw Error('请求超时');throw e}finally{clearTimeout(t)}}
function renderBilling(d){$('billAvailableCash').textContent=d.available_cash_amount??'—';$('billBalance').textContent=d.balance??'—';$('billSpent').textContent=d.month_spent??'—';$('billCredit').textContent=d.credit_amount??'—';$('billCurrency').textContent=d.currency??'—';$('billCycle').textContent=d.billing_cycle??'—';$('billUpdated').textContent=d.updated_at?new Date(d.updated_at).toLocaleTimeString():'—';const bankCredit=d.mybank_credit_amount!=null?`网商银行信控 ${d.mybank_credit_amount}；`:'';$('billStatus').textContent=bankCredit+d.message+'；可用额度不等于 ECS 下单门槛，按量实例可能要求可用额度不少于 100 元；账单非实时，以费用中心出账为准'}
async function refreshBilling(){try{renderBilling(await api('/api/billing'))}catch(e){$('billStatus').textContent='账单状态读取失败；账单非实时，以费用中心出账为准'}}
async function refresh(silent=true){if(loading)return;loading=true;try{render(await api('/api/status'))}catch(e){$('runtime').textContent='连接失败';$('runtimeNote').textContent=e.message;$('led').className='led error';if(!silent)toast(e.message,true)}finally{loading=false}}
async function action(path,body,msg){try{const d=await api(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});render(d);toast(msg)}catch(e){toast(e.message,true)}finally{setBusy(false)}}
function setBusy(v){$('reconcile').disabled=v;$('saveTarget').disabled=v}
$('nodes').onclick=e=>{const button=e.target.closest('.node-delete');if(!button)return;$('deleteInstanceId').textContent=button.dataset.instanceId;$('deleteDialog').showModal()};$('cancelDelete').onclick=()=>$('deleteDialog').close();$('deleteDialog').onclick=e=>{if(e.target===$('deleteDialog'))$('deleteDialog').close()};$('deleteForm').onsubmit=async e=>{e.preventDefault();const instanceId=$('deleteInstanceId').textContent;try{$('confirmDelete').disabled=true;const d=await api('/api/nodes/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({instance_id:instanceId})});$('deleteDialog').close();render(d);toast(`节点 ${instanceId} 已删除`)}catch(e){toast(e.message,true)}finally{$('confirmDelete').disabled=false}};
function toggleSpotDuration(){const isSpot=$('instanceChargeType').value==='Spot';$('spotDuration').disabled=!isSpot;const op=isSpot?'1':'0.5';$('spotDuration').style.opacity=op;const lbl=document.querySelector('label[for="spotDuration"]');if(lbl)lbl.style.opacity=op}$('instanceChargeType').onchange=toggleSpotDuration;async function openSettings(){try{const d=await api('/api/settings');$('akId').value='';$('akId').placeholder=d.access_key_id_configured?'AccessKey ID 已配置；输入可替换':'输入 AccessKey ID';$('akSecret').value='';$('akSecret').disabled=false;$('secretState').textContent=d.secret_configured?'密钥已配置':'密钥未配置';$('socksUsername').value=d.socks5_username;$('socksPassword').value='';$('socksPasswordState').textContent=d.socks5_password_configured?'密码已配置':'密码未配置';$('proxyApiToken').value='';$('proxyApiTokenState').textContent=d.proxy_api_bearer_token_configured?'Token 已配置':'Token 未配置';const region=$('regionId');region.querySelector('option[data-current]')?.remove();if(d.region_id&&!Array.from(region.options).some(o=>o.value===d.region_id)){const option=new Option(`当前值 · ${d.region_id}`,d.region_id);option.dataset.current='';region.add(option)}region.value=d.region_id;const template=$('templateId');template.replaceChildren(new Option('请选择启动模板',''));[['lt-bp10ty3f6hvurdiyxn6u','1']].forEach(([id,v])=>{const o=new Option(`内置模板 · ${id}`,id);o.dataset.version=v;template.add(o)});if(d.access_key_id_configured&&d.secret_configured&&d.region_id){try{const resources=await api('/api/resources');resources.launch_templates.forEach(x=>{if(!Array.from(template.options).some(o=>o.value===x.id)){const o=new Option(`${x.name} · ${x.id}`,x.id);o.dataset.version=x.version;template.add(o)}})}catch(e){}}if(d.launch_template_id&&!Array.from(template.options).some(o=>o.value===d.launch_template_id))template.add(new Option(`当前值 · ${d.launch_template_id}`,d.launch_template_id));template.value=d.launch_template_id;$('templateVersion').value=d.launch_template_version;$('resourceGroupId').value=d.resource_group_id;const pn=$('poolName');pn.innerHTML=['proxy-main','proxy-backup','proxy-secondary','proxy-test'].map(n=>`<option value="${n}">${n}</option>`).join('');if(d.pool_name&&!Array.from(pn.options).some(o=>o.value===d.pool_name))pn.add(new Option(`当前值 · ${d.pool_name}`,d.pool_name));pn.value=d.pool_name;$('clearSecret').checked=false;$('batchSize').value=d.batch_size||'';$('pendingTimeout').value=d.pending_timeout_minutes||'';$('autoReconcileInterval').value=d.auto_reconcile_interval||'';$('circuitBreakerThreshold').value=d.circuit_breaker_threshold||'';$('instanceChargeType').value=d.instance_charge_type||'PostPaid';$('spotDuration').value=d.spot_duration||'0';toggleSpotDuration();$('settingsDialog').showModal()}catch(e){toast(e.message,true)}}
$('openSettings').onclick=openSettings;$('templateId').onchange=()=>{const version=$('templateId').selectedOptions[0]?.dataset.version;if(version)$('templateVersion').value=version};$('cancelSettings').onclick=()=>$('settingsDialog').close();$('settingsDialog').onclick=e=>{if(e.target===$('settingsDialog'))$('settingsDialog').close()};$('clearSecret').onchange=()=>{$('akSecret').disabled=$('clearSecret').checked;if($('clearSecret').checked)$('akSecret').value=''};$('akSecret').oninput=()=>{if($('akSecret').value)$('clearSecret').checked=false};$('settingsForm').onsubmit=async e=>{e.preventDefault();const body={region_id:$('regionId').value,launch_template_id:$('templateId').value,launch_template_version:$('templateVersion').value,resource_group_id:$('resourceGroupId').value,pool_name:$('poolName').value,access_key_secret:$('akSecret').value,socks5_username:$('socksUsername').value,socks5_password:$('socksPassword').value,proxy_api_bearer_token:$('proxyApiToken').value,clear_secret:$('clearSecret').checked,batch_size:$('batchSize').value,pending_timeout_minutes:$('pendingTimeout').value,auto_reconcile_interval:$('autoReconcileInterval').value,circuit_breaker_threshold:$('circuitBreakerThreshold').value,instance_charge_type:$('instanceChargeType').value,spot_duration:$('spotDuration').value};if($('akId').value)body.access_key_id=$('akId').value;try{$('saveSettings').disabled=true;await api('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});$('settingsDialog').close();toast('阿里云设置已保存');refresh(true)}catch(e){toast(e.message,true)}finally{$('saveSettings').disabled=false}};
const fillSelect=(id,items)=>{$(id).innerHTML=items.map(x=>`<option value="${esc(x.id)}">${esc(x.name)}</option>`).join('')};$('openSecurityGroup').onclick=async()=>{try{const r=await api('/api/resources');fillSelect('securityGroupVSwitchId',r.vswitches);$('securityGroupDialog').showModal()}catch(e){toast(e.message,true)}};$('cancelSecurityGroup').onclick=()=>$('securityGroupDialog').close();$('securityGroupDialog').onclick=e=>{if(e.target===$('securityGroupDialog'))$('securityGroupDialog').close()};$('securityGroupForm').onsubmit=async e=>{e.preventDefault();const body={security_group_name:$('securityGroupName').value,description:$('securityGroupDescription').value,v_switch_id:$('securityGroupVSwitchId').value};try{$('createSecurityGroup').disabled=true;const d=await api('/api/security-groups',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});$('securityGroupDialog').close();$('securityGroupForm').reset();toast(`安全组 ${d.security_group_id} 已创建`);await api('/api/resources')}catch(e){toast(e.message,true)}finally{$('createSecurityGroup').disabled=false}};$('openTemplate').onclick=async()=>{try{const r=await api('/api/resources');fillSelect('imageId',r.images);fillSelect('instanceType',r.instance_types);fillSelect('vSwitchId',r.vswitches);fillSelect('securityGroupId',r.security_groups);$('templateDialog').showModal()}catch(e){toast(e.message,true)}};$('cancelTemplate').onclick=()=>$('templateDialog').close();$('templateDialog').onclick=e=>{if(e.target===$('templateDialog'))$('templateDialog').close()};$('templateForm').onsubmit=async e=>{e.preventDefault();const body={launch_template_name:$('templateName').value,description:$('templateDescription').value,image_id:$('imageId').value,instance_type:$('instanceType').value,v_switch_id:$('vSwitchId').value,security_group_id:$('securityGroupId').value};try{$('createTemplate').disabled=true;const d=await api('/api/launch-templates',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});$('templateDialog').close();$('templateForm').reset();toast(`启动模板 ${d.launch_template_id} 已创建`);refresh(true)}catch(e){toast(e.message,true)}finally{$('createTemplate').disabled=false}};
$('openProxyTest').onclick=async()=>{try{const d=await api('/api/status');const sel=$('testInstanceId');sel.innerHTML='<option value="">请选择节点</option>';d.nodes.filter(n=>n.state==='healthy').forEach(n=>sel.add(new Option(`${n.instance_id} (${n.public_ip})`,n.instance_id)));$('testResult').textContent='等待测试';$('proxyTestDialog').showModal()}catch(e){toast(e.message,true)}};$('cancelProxyTest').onclick=()=>$('proxyTestDialog').close();$('proxyTestDialog').onclick=e=>{if(e.target===$('proxyTestDialog'))$('proxyTestDialog').close()};$('proxyTestForm').onsubmit=async e=>{e.preventDefault();const body={instance_id:$('testInstanceId').value,target_url:$('testTargetUrl').value};if(!body.instance_id){toast('请选择节点',true);return}try{$('runProxyTest').disabled=true;$('testResult').textContent='测试中...';const d=await api('/api/proxy-test',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});$('testResult').textContent=JSON.stringify(d,null,2)}catch(e){$('testResult').textContent='错误：'+e.message}finally{$('runProxyTest').disabled=false}};
$('openBalanced').onclick=()=>{$('lbResult').textContent='尚未获取';$('balancedDialog').showModal()};$('cancelBalanced').onclick=()=>$('balancedDialog').close();$('balancedDialog').onclick=e=>{if(e.target===$('balancedDialog'))$('balancedDialog').close()};$('balancedForm').onsubmit=async e=>{e.preventDefault();const strategy=$('lbStrategy').value;try{$('getBalanced').disabled=true;const d=await api('/api/proxies/balanced',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({strategy})});$('lbResult').textContent=d.proxy||'无可用代理'}catch(e){$('lbResult').textContent='错误：'+e.message}finally{$('getBalanced').disabled=false}};
$('saveTarget').onclick=()=>{const v=Number($('target').value);if(!Number.isInteger(v)||v<0||v>1000)return toast('目标节点数需为 0–1000 的整数',true);setBusy(true);action('/api/target',{target:v},'目标节点数已更新')};$('reconcile').onclick=()=>{setBusy(true);action('/api/reconcile',{},'调和已完成')};$('refresh').onclick=()=>{refresh(false);refreshBilling()};$('auto').onchange=schedule;$('interval').onchange=schedule;function schedule(){clearInterval(timer);if($('auto').checked)timer=setInterval(()=>refresh(true),Number($('interval').value)*1000)}document.addEventListener('keydown',e=>{if(e.key==='F5'){e.preventDefault();refresh(false);refreshBilling()}if(e.ctrlKey&&e.key==='Enter')$('reconcile').click()});$('saveToken').onclick=()=>{const v=$('tokenInput').value.trim();if(v){localStorage.setItem('pp_token',v);$('tokenInput').value='';$('tokenInput').placeholder='Token 已保存 ✓';toast('Token 已保存，写操作已解锁')}else{localStorage.removeItem('pp_token');$('tokenInput').placeholder='API Token（远程操作需填写）';toast('Token 已清除')}};(function(){const t=localStorage.getItem('pp_token');if(t)$('tokenInput').placeholder='Token 已保存 ✓'})();refresh(false);refreshBilling();schedule();
</script></body></html>'''


HELP_HTML = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>使用说明 · 节点调和控制台</title>
<style>
:root{color-scheme:light;--bg:#eef1f4;--panel:#fff;--line:#ccd3da;--text:#20262d;--muted:#66717d;--blue:#426783;--amber:#8a5a16;font-family:"Segoe UI","Microsoft YaHei UI",system-ui,sans-serif;font-size:14px}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text)}header{height:48px;display:flex;align-items:center;gap:18px;padding:0 max(18px,calc((100vw - 980px)/2));background:#f8f9fb;border-bottom:1px solid var(--line)}header strong{font-size:16px}a{color:#356b91}header a{text-decoration:none}.page{max-width:980px;margin:18px auto;padding:0 18px 32px}.intro,.section{background:var(--panel);border:1px solid var(--line);border-radius:4px;padding:18px 22px;margin-bottom:12px}.intro h1{margin:0 0 8px;font-size:24px}.intro p{margin:0;color:var(--muted)}h2{font-size:18px;margin:0 0 10px}h3{font-size:15px;margin:16px 0 6px}p,li{line-height:1.7}ol,ul{margin:6px 0;padding-left:24px}code{font-family:Consolas,monospace;background:#edf1f4;padding:1px 4px;border-radius:3px}pre{overflow:auto;background:#20272e;color:#e6edf2;padding:12px;border-radius:3px;font:12px/1.55 Consolas,monospace;white-space:pre-wrap}.warning{border-left:4px solid #b77b22;background:#fff8e8;padding:9px 12px;color:var(--amber)}nav{display:flex;flex-wrap:wrap;gap:7px 16px;margin-top:14px}@media(max-width:650px){header{padding:0 14px}.page{padding:0 10px}.section,.intro{padding:15px}}
</style>
</head>
<body><header><strong>节点调和控制台</strong><a href="/">返回控制台</a></header><main class="page">
<section class="intro"><h1>使用说明</h1><p>用于配置阿里云 ECS 节点、创建 Dante SOCKS5 启动模板，并安全地调和代理池规模。</p><nav aria-label="帮助目录"><a href="#configuration">配置</a><a href="#buttons">按钮说明</a><a href="#dante">Dante 模板</a><a href="#scaling">扩缩容</a><a href="#extract-socks">提取代理</a><a href="#proxy-test">代理测试</a><a href="#delete">删除</a><a href="#cost-security">费用与安全</a><a href="#troubleshooting">故障排查</a><a href="#spot-tutorial">抢占式教程</a><a href="#cost-tutorial">费用教程</a><a href="#api-control">API 控制</a><a href="#ai-prompt">AI 提示词模板</a></nav></section>
<section class="section" id="configuration"><h2>1. 配置</h2><ol><li>在“阿里云设置”中填写专用 RAM 用户的 AccessKey、地域、启动模板、实例标签池名称；资源组和模板版本可选。</li><li>填写 SOCKS 用户名、密码和代理 API Token。秘密字段留空会保留原值；只有勾选“清除现有密钥”才会清除 AccessKey Secret。</li><li>也可通过同目录 <code>.env</code> 或系统环境变量提供配置；优先级为系统环境变量 &gt; <code>.env</code> &gt; 已保存设置。<code>.env</code> 支持的全部变量如下：</li></ol><pre># 阿里云凭据与地域（必填）
ALIBABA_CLOUD_ACCESS_KEY_ID=LTAI...
ALIBABA_CLOUD_ACCESS_KEY_SECRET=...
ALIYUN_REGION_ID=cn-hangzhou

# 实例标签池名称（必填，默认 proxy-main）
ALIYUN_POOL_NAME=proxy-main

# SOCKS5 代理配置（必填，否则代理池不可用）
SOCKS5_PORT=1080
SOCKS5_USERNAME=myuser
SOCKS5_PASSWORD=mypassword
SOCKS5_CHECK_HOST=1.1.1.1
SOCKS5_CHECK_PORT=80
SOCKS5_TIMEOUT=5

# 代理 API 认证令牌（必填，用于提取代理和 API 控制）
PROXY_API_BEARER_TOKEN=your-secret-token</pre><ul><li>建议使用最小权限 RAM 用户。未完整配置或校验失败时，“立即调和”不可用。</li></ul></section>
<section class="section" id="buttons"><h2>2. 按钮功能说明</h2><p>工具栏从左到右依次排列以下按钮和控件：</p>
<h3>目标节点数 + 应用</h3><p>输入期望维持的 ECS 节点数量（0–1000 的整数），点“应用”保存。调和时会以此为基准：实际节点少则创建，多则释放。设为 <code>0</code> 表示回收全部受管节点。</p>
<h3>立即调和</h3><p>手动触发一次调和。系统会比较目标数与实际节点数，自动创建或释放 ECS 实例，并对运行中的节点执行 SOCKS5 健康检查。调和执行期间按钮禁用，快捷键 <code>Ctrl+Enter</code> 可等效触发。</p>
<h3>代理测试</h3><p>打开代理测试对话框，选择一个健康节点并指定目标 URL，通过该节点的 SOCKS5 代理向目标发起 HTTP GET 请求，返回状态码、延迟和响应大小，用于验证代理可用性。</p>
<h3>负载均衡</h3><p>打开负载均衡对话框，选择调度策略后获取一个代理地址。支持四种策略：轮询、随机、最少连接、最低延迟。控制台内调用无需 Token，适合快速获取单个代理。</p>
<h3>阿里云设置</h3><p>打开设置对话框，配置 AccessKey、地域、启动模板、池名称、SOCKS 凭据、API Token 及调和参数（批量大小、超时、自动调和间隔、断路器阈值）。修改后点“保存”生效。</p>
<h3>创建安全组</h3><p>打开创建安全组对话框，输入名称和描述，选择目标交换机后创建普通 VPC 安全组。仅创建安全组本身，不创建任何入站规则（创建启动模板时会自动为 TCP 1080 添加规则）。</p>
<h3>创建启动模板</h3><p>打开创建启动模板对话框，选择镜像、实例规格、交换机和安全组后，通过 cloud-init 自动安装并配置 Dante SOCKS5 服务，创建成功后自动回填模板 ID 和版本到设置中。</p>
<h3>使用说明</h3><p>打开本帮助页面。</p>
<h3>自动刷新 + 间隔选择</h3><p>勾选“自动刷新”后，页面会按所选间隔（3/5/10/30 秒）定时拉取最新节点状态。取消勾选则停止自动刷新。</p>
<h3>刷新</h3><p>立即手动刷新节点状态和账单信息，快捷键 <code>F5</code> 等效。</p>
<h3>节点列表 — 删除按钮</h3><p>每行节点右侧的“删除”按钮用于手动下线并释放指定 ECS 实例。点击后会弹出确认对话框，确认后先从代理池下线再永久释放。调和执行期间禁用。</p>
</section>
<section class="section" id="dante"><h2>3. Dante 启动模板</h2><p>先保存 AccessKey、地域和 SOCKS 凭据，再点“创建启动模板”。选择 Ubuntu 镜像、实例规格、交换机及安全组后，控制台通过 cloud-init 安装并配置 Dante，创建成功会自动回填模板 ID 与版本。</p><ul><li>模板开放 TCP <code>1080</code> 并使用已配置的 SOCKS 用户名和密码；客户端必须使用认证。</li><li>安全组入口应只允许可信客户端公网 IP，避免向全网开放代理端口。</li><li>自定义现有模板时，应保证 Ubuntu 可访问软件源、cloud-init 能执行、Dante 监听配置端口且实例具有公网 IP。</li></ul></section>
<section class="section" id="scaling"><h2>4. 扩缩容与调和</h2><ol><li>输入 0–1000 的目标节点数并点“应用”。</li><li>点“立即调和”：少于目标时按候选交换机顺序创建实例；多于目标时先将节点从代理池下线，再释放 ECS。</li><li>节点通过 SOCKS 健康检查后才进入“健康”状态。执行期间不要重复操作或手工修改带管理标签的实例。</li><li>新实例创建后需等待约 <strong>60 秒</strong>让 cloud-init 完成 Dante 安装与配置，再触发一次调和执行 SOCKS5 健康检查；提前检测会因服务未就绪而标记为 <code>error</code>。</li><li>目标设为 <code>0</code> 会在调和时回收全部受管节点，请先确认业务流量已迁移。</li></ol></section>
<section class="section" id="extract-socks"><h2>5. 提取 SOCKS 代理</h2><p>节点进入“健康”状态后，即可提取代理地址。系统提供三种提取方式：</p>
<h3>方式一：控制台“负载均衡”按钮（快速获取单个代理）</h3><ol><li>点击工具栏“负载均衡”按钮。</li><li>选择调度策略：轮询（默认）、随机、最少连接、最低延迟。</li><li>点“获取代理”，结果区显示一个完整的 SOCKS5 代理 URL。</li><li>控制台内调用无需 Token，适合手动复制使用。</li></ol>
<h3>方式二：API 获取全部代理列表</h3><p><code>GET /api/proxies</code> 返回当前所有健康代理，必须携带 Bearer Token：</p><pre>curl -H "Authorization: Bearer &lt;API_TOKEN&gt;" http://127.0.0.1:8765/api/proxies</pre><p>成功响应形如：</p><pre>{"proxies":["socks5://user:password@203.0.113.10:1080","socks5://user:password@203.0.113.11:1080"],"count":2}</pre>
<h3>方式三：API 按策略获取单个代理</h3><p><code>GET /api/proxies/balanced?strategy=round-robin</code> 按调度策略返回一个代理，需 Token 认证：</p><pre>curl "http://127.0.0.1:8765/api/proxies/balanced?strategy=lowest-latency&amp;token=&lt;API_TOKEN&gt;"</pre><p>成功响应形如 <code>{"proxy":"socks5://user:password@203.0.113.10:1080","strategy":"lowest-latency"}</code>。可选策略：<code>round-robin</code>（轮询）、<code>random</code>（随机）、<code>least-connections</code>（最少连接）、<code>lowest-latency</code>（最低延迟）。</p>
<h3>代理 URL 格式</h3><p>所有方式返回的代理 URL 统一格式为 <code>socks5://用户名:密码@公网IP:端口</code>，默认端口 <code>1080</code>。用户名和密码已做 URL 编码，可直接用于支持 SOCKS5 的客户端。</p>
<h3>客户端使用示例</h3><pre># curl 通过代理访问
curl --socks5 socks5://user:password@203.0.113.10:1080 https://httpbin.org/ip

# Python requests
import requests
proxies = {"http": "socks5://user:password@203.0.113.10:1080",
           "https": "socks5://user:password@203.0.113.10:1080"}
r = requests.get("https://httpbin.org/ip", proxies=proxies)

# 浏览器（以 Firefox 为例）
# 设置 → 网络设置 → 手动代理配置 → SOCKS 主机填 IP，端口 1080，选 SOCKS v5，勾选远程 DNS</pre><p class="warning">响应包含代理凭据，只应通过受信网络调用。Token 不应写入日志、聊天或前端公开代码。</p></section>
<section class="section" id="proxy-test"><h2>6. 代理测试与负载均衡</h2>
<h3>代理测试</h3><p>点击工具栏“代理测试”按钮，在对话框中选择一个健康节点并输入目标 URL（默认 <code>https://httpbin.org/ip</code>），点“执行测试”后系统会通过该节点的 SOCKS5 代理向目标发起 HTTP GET 请求，返回：<ul><li><code>ok</code> — 测试是否成功</li><li><code>status_code</code> — HTTP 状态码</li><li><code>latency_ms</code> — 端到端延迟（毫秒）</li><li><code>response_size</code> — 响应体大小（字节）</li></ul>用于快速验证某个代理节点是否可正常访问目标网站。</p>
<h3>负载均衡策略说明</h3><ul><li><b>轮询 (round-robin)</b> — 按固定顺序依次分配代理，适合均匀分担流量。</li><li><b>随机 (random)</b> — 随机选取一个代理，适合无状态场景。</li><li><b>最少连接 (least-connections)</b> — 优先分配当前连接数最少的代理，适合长连接场景。</li><li><b>最低延迟 (lowest-latency)</b> — 优先分配健康检查延迟最低的代理，适合对速度敏感的场景。</li></ul><p>每次通过 API 或负载均衡按钮获取代理时，系统会自动递增该节点的连接计数，供“最少连接”策略参考。</p>
</section>
<section class="section" id="delete"><h2>7. 删除节点</h2><p>节点列表的“删除”只对同时具备控制器双重管理标签的实例生效。确认后会先从代理池下线，再永久释放 ECS，且无法撤销；调和执行期间禁止删除。删除后若目标数未降低，下次调和可能创建替代节点，因此永久缩容应先降低目标数。</p></section>
<section class="section" id="cost-security"><h2>8. 费用与安全</h2><ul><li>创建 ECS、公网带宽和流量均可能计费；余额、可用额度和当月花费对应的账单非实时，以阿里云费用中心出账为准。按量实例还可能有最低可用额度要求。</li><li>定期检查目标节点数、账单与异常实例；不用时将目标降为 0 并调和，确认资源已释放。</li><li>不要使用主账号 AccessKey。定期轮换 AccessKey、SOCKS 密码和 API Token，并限制控制台监听地址及主机防火墙。</li><li>写操作仅接受本机请求，但若将服务暴露到公网，读接口和页面仍可能泄露运行信息；默认仅监听 <code>127.0.0.1</code>。</li></ul><p class="warning">切勿通过截图、工单或聊天发送 AccessKey Secret、SOCKS 密码或 API Token。</p></section>
<section class="section" id="troubleshooting"><h2>9. 故障排查</h2><h3>调和按钮不可用</h3><p>检查 AccessKey、地域、启动模板、池名称和代理配置是否完整；查看“最近事件”中的初始化错误，并确认 RAM 权限与地域一致。</p><h3>实例一直“启动中”</h3><p>检查 ECS 是否取得公网 IP、TCP 1080 安全组规则、cloud-init 日志、Dante 服务状态、SOCKS 凭据及健康检查目标是否可达。</p><h3>创建或删除失败</h3><p>确认账户余额/额度、实例库存、交换机可用 IP、配额、资源组及 <code>ecs:RunInstances</code>/<code>ecs:DeleteInstances</code> 权限。删除还要求双重管理标签且当前没有调和任务。</p><h3>代理 API 返回 401 或 409</h3><p>401 表示 Bearer Token 缺失或错误；409 通常表示真实代理池尚未配置。修改秘密后重新保存设置再重试。</p><h3>账单读取失败或延迟</h3><p>授予 <code>bssapi:QueryAccountBalance</code> 与 <code>bssapi:QueryBillOverview</code> 只读权限；账单延迟时直接到阿里云费用中心核对。</p></section>
<section class="section" id="spot-tutorial"><h2>10. 抢占式实例教程</h2><p>抢占式实例（Spot Instance）利用阿里云空闲计算资源，价格最高可比按量付费便宜 <strong>90%</strong>，非常适合代理池这类可容忍中断的工作负载。本节介绍如何在本控制台中启用和管理抢占式实例。</p>

<h3>10.1 什么是抢占式实例</h3><ul><li><b>按需获取</b> — 当有空闲资源时按当前市场价分配实例。</li><li><b>可能被回收</b> — 资源紧张时阿里云会自动回收实例，提前 <strong>5 分钟</strong>发出中断通知。</li><li><b>按秒计费</b> — 不足 60 秒按 60 秒计算，无最低消费。</li><li><b>保护期</b> — 可选择 1 小时或 6 小时保护期，保护期内不会被回收，但价格略高于无保护期。</li></ul>

<h3>10.2 适用场景评估</h3>
<table style="width:100%;border-collapse:collapse;margin:1em 0"><thead><tr style="border-bottom:2px solid #dee2e6"><th style="text-align:left;padding:8px">场景</th><th style="text-align:left;padding:8px">是否推荐抢占式</th><th style="text-align:left;padding:8px">原因</th></tr></thead><tbody><tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">代理池（多节点负载均衡）</td><td style="padding:8px;color:green">✓ 强烈推荐</td><td style="padding:8px">单节点被回收不影响整体可用性，调和会自动补节点</td></tr><tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">需要 7×24 不间断单节点</td><td style="padding:8px;color:red">✗ 不推荐</td><td style="padding:8px">单点故障无冗余，被回收即服务中断</td></tr><tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">大批量短时任务</td><td style="padding:8px;color:green">✓ 推荐</td><td style="padding:8px">短时任务可在保护期内完成，成本极低</td></tr><tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">有状态服务（数据库等）</td><td style="padding:8px;color:red">✗ 不推荐</td><td style="padding:8px">被回收导致数据丢失，需额外持久化方案</td></tr></tbody></table>

<h3>10.3 配置步骤</h3><ol><li>点击工具栏「阿里云设置」按钮打开设置对话框。</li><li>找到「实例计费方式」下拉框，从「按量付费」切换为「抢占式」。</li><li>在「抢占式保护期」下拉框中选择保护期时长：<ul><li><b>无保护期（价格最低）</b> — 随时可能被回收，价格最便宜，适合多节点代理池。</li><li><b>1 小时保护期</b> — 创建后 1 小时内不会被回收，适合需要短期稳定运行的任务。</li><li><b>6 小时保护期</b> — 创建后 6 小时内不会被回收，价格略高，适合需要较长时间运行的任务。</li></ul></li><li>点击「保存」。设置立即生效，后续创建的所有新实例均使用抢占式计费。</li></ol><p class="warning">切换计费方式只影响<strong>新创建</strong>的实例。已在运行的按量付费实例不会被自动转换，如需替换可手动删除旧节点让调和自动以抢占式重建。</p>

<h3>10.4 抢占式实例的生命周期管理</h3><p>本控制台自动处理抢占式实例的完整生命周期：</p>
<ol><li><b>创建</b> — 调和时按 <code>SpotAsPriceGo</code> 策略出价（跟随市场价），自动创建抢占式实例。</li><li><b>健康检查</b> — 与按量付费实例一样，创建后等待约 60 秒执行 SOCKS5 健康检查，通过后进入「健康」状态并加入代理池。</li><li><b>中断预警</b> — 阿里云在回收前会发出系统事件，控制台在刷新时检测到实例进入 <code>Stopping</code> / <code>Recycling</code> 状态会自动将其从代理池下线，避免向即将消失的节点分配流量。</li><li><b>自动补节点</b> — 被回收的实例在下次调和时会被检测为「缺失」，控制台自动创建新的抢占式实例补足目标节点数。如果已开启自动调和（间隔 &gt; 0），整个过程无需人工干预。</li><li><b>手动处理</b> — 也可以在节点列表中手动点击「删除」释放被标记为 <code>error</code> 的实例，调和会自动补节点。</li></ol>

<h3>10.5 推荐配置</h3><p>针对代理池场景，推荐以下抢占式配置组合：</p><ul><li><b>实例计费方式</b>：抢占式</li><li><b>保护期</b>：无保护期（价格最低，代理池有冗余节点可容忍单节点中断）</li><li><b>目标节点数</b>：建议 ≥ 3，保证单节点被回收后仍有可用代理</li><li><b>自动调和间隔</b>：建议 60–300 秒，确保被回收后能快速自动补节点</li><li><b>断路器阈值</b>：建议 3–5，避免库存不足时反复创建失败浪费 API 调用</li><li><b>实例规格</b>：选择 <code>t6</code> / <code>s6</code> 等入门级规格即可，代理转发对 CPU/内存要求低</li><li><b>地域</b>：多地域部署可分散被回收风险；单一地域建议选择库存充足的规格</li></ul>

<h3>10.6 成本对比示例</h3><p>以 <code>ecs.t6-c1m1.large</code>（2 vCPU / 2 GiB）华东1（杭州）为例（价格仅供参考，以阿里云官网为准）：</p>
<table style="width:100%;border-collapse:collapse;margin:1em 0"><thead><tr style="border-bottom:2px solid #dee2e6"><th style="text-align:left;padding:8px">计费方式</th><th style="text-align:right;padding:8px">小时单价</th><th style="text-align:right;padding:8px">月费（730h）</th><th style="text-align:right;padding:8px">相对按量</th></tr></thead><tbody><tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">按量付费</td><td style="text-align:right;padding:8px">¥0.17/h</td><td style="text-align:right;padding:8px">¥124</td><td style="text-align:right;padding:8px">100%</td></tr><tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">抢占式（无保护期）</td><td style="text-align:right;padding:8px">~¥0.02/h</td><td style="text-align:right;padding:8px">~¥15</td><td style="text-align:right;padding:8px;color:green">~12%</td></tr><tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">抢占式（1h 保护期）</td><td style="text-align:right;padding:8px">~¥0.03/h</td><td style="text-align:right;padding:8px">~¥22</td><td style="text-align:right;padding:8px;color:green">~18%</td></tr></tbody></table><p>5 个节点代理池使用抢占式（无保护期），月费约 <strong>¥75</strong>，而按量付费约 <strong>¥620</strong>，节省约 <strong>88%</strong>。</p>

<h3>10.7 常见问题</h3>
<h4>抢占式实例创建失败？</h4><p>常见原因：当前地域/规格库存不足、账户余额不足、超出抢占式实例配额。建议：选择其他规格或多规格配置、确认账户余额 &gt; ¥100、查看阿里云控制台的配额限制。</p>
<h4>节点频繁被回收怎么办？</h4><p>增加目标节点数（如从 3 调到 5）、缩短自动调和间隔（如 60 秒）、选择库存充足的规格和地域、考虑使用 1 小时保护期减少中断频率。多节点 + 快速自动调和是应对抢占式中断的最佳策略。</p>
<h4>保护期内会被收费吗？</h4><p>会。保护期只是保证不被回收，不是免费期。保护期内的费用按抢占式价格计算，通常仍远低于按量付费。</p>
<h4>已有的按量实例能转成抢占式吗？</h4><p>不能直接转换。切换计费方式后，手动删除旧节点，调和会自动以抢占式创建新节点替代。</p>
</section>
<section class="section" id="cost-tutorial"><h2>11. 费用教程</h2><p>本节详细说明代理池涉及的阿里云费用构成、控制台费用面板的读取方法、不同计费方式的成本对比、费用估算方法以及节省成本的最佳实践。</p>

<h3>11.1 费用构成</h3><p>运行代理池的阿里云费用由以下几部分组成：</p>
<table style="width:100%;border-collapse:collapse;margin:1em 0"><thead><tr style="border-bottom:2px solid #dee2e6"><th style="text-align:left;padding:8px">费用项</th><th style="text-align:left;padding:8px">说明</th><th style="text-align:left;padding:8px">计费方式</th></tr></thead><tbody>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">ECS 实例</td><td style="padding:8px">代理节点本身的计算资源（vCPU + 内存）</td><td style="padding:8px">按秒计费，不足 60 秒按 60 秒算</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">公网带宽</td><td style="padding:8px">实例的出/入公网流量带宽</td><td style="padding:8px">按固定带宽 或 按使用流量</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">公网流量费</td><td style="padding:8px">选择"按使用流量"时的实际流量费</td><td style="padding:8px">¥0.80/GB（国内），地域不同有差异</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">系统盘</td><td style="padding:8px">随实例创建的 ESSD/SSD 系统盘</td><td style="padding:8px">随实例生命周期，释放时自动删除</td></tr>
</tbody></table>
<p class="warning">代理池的主要成本是 ECS 实例费和公网流量费。代理转发流量较大时，流量费可能超过实例费本身。</p>

<h3>11.2 控制台费用面板</h3><p>控制台顶部的费用面板实时展示以下 7 个指标（数据来自阿里云 BSS OpenAPI，缓存 60 秒）：</p>
<table style="width:100%;border-collapse:collapse;margin:1em 0"><thead><tr style="border-bottom:2px solid #dee2e6"><th style="text-align:left;padding:8px">指标</th><th style="text-align:left;padding:8px">含义</th><th style="text-align:left;padding:8px">数据来源</th></tr></thead><tbody>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">账户余额（可用现金）</td><td style="padding:8px">账户中可用于消费的现金余额</td><td style="padding:8px">QueryAccountBalance.available_cash_amount</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">账户可用额度</td><td style="padding:8px">含信控额度的总可用额度</td><td style="padding:8px">QueryAccountBalance.available_amount</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">当月已花费</td><td style="padding:8px">当前账期（自然月）所有云产品消费汇总</td><td style="padding:8px">QueryBillOverview.pretax_amount 求和</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">信控额度</td><td style="padding:8px">阿里云授予的信用额度</td><td style="padding:8px">QueryAccountBalance.credit_amount</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">币种</td><td style="padding:8px">账户结算币种（通常为 CNY）</td><td style="padding:8px">QueryAccountBalance.currency</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">账期</td><td style="padding:8px">当前账单周期（如 2026-08）</td><td style="padding:8px">QueryBillOverview.billing_cycle</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">API 更新时间</td><td style="padding:8px">费用数据最近一次刷新的 UTC 时间</td><td style="padding:8px">控制台本地记录</td></tr>
</tbody></table>
<p>费用面板下方有一行状态提示，可能显示以下状态：</p>
<ul><li><b>费用中心数据已更新</b> — 当月账单已出账，数据为最新。</li><li><b>当月账单尚未出账或存在延迟</b> — 账单数据尚未生成，通常出现在月初或账单系统延迟时。此时余额仍然有效，但"当月已花费"可能为 ¥0 或不完整。</li><li><b>RAM 用户无 BSS 账单查询权限</b> — 需要给 RAM 用户授予 <code>bssapi:QueryAccountBalance</code> 和 <code>bssapi:QueryBillOverview</code> 只读权限。</li><li><b>费用中心暂时不可用</b> — BSS API 调用失败，稍后刷新重试。</li></ul>
<p class="warning">账单数据非实时，存在数小时延迟。费用面板仅供参考，以<a href="https://expense.console.aliyun.com/" target="_blank" rel="noopener noreferrer">阿里云费用中心</a>出账为准。</p>

<h3>11.3 计费方式对比</h3>
<table style="width:100%;border-collapse:collapse;margin:1em 0"><thead><tr style="border-bottom:2px solid #dee2e6"><th style="text-align:left;padding:8px">计费方式</th><th style="text-align:left;padding:8px">价格</th><th style="text-align:left;padding:8px">适合场景</th><th style="text-align:left;padding:8px">本控制台支持</th></tr></thead><tbody>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">按量付费 (PostPaid)</td><td style="padding:8px">基准价</td><td style="padding:8px">需要稳定不被中断的节点</td><td style="padding:8px;color:green">✓ 默认</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">抢占式 (Spot)</td><td style="padding:8px">基准价的 ~10%–30%</td><td style="padding:8px">代理池等多节点可容忍中断场景</td><td style="padding:8px;color:green">✓ 设置中选择</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">包年包月 (PrePaid)</td><td style="padding:8px">比按量便宜 ~30%–50%</td><td style="padding:8px">长期稳定运行，需提前规划</td><td style="padding:8px;color:red">✗ 不支持（本控制台仅创建按量/抢占式）</td></tr>
</tbody></table>
<p>详细抢占式配置参见<a href="#spot-tutorial">第 10 节 抢占式教程</a>。</p>

<h3>11.4 公网带宽计费方式</h3><p>创建启动模板时需要选择公网带宽计费方式，这直接影响流量费用：</p>
<table style="width:100%;border-collapse:collapse;margin:1em 0"><thead><tr style="border-bottom:2px solid #dee2e6"><th style="text-align:left;padding:8px">带宽计费</th><th style="text-align:left;padding:8px">计费规则</th><th style="text-align:left;padding:8px">适合场景</th></tr></thead><tbody>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">按使用流量</td><td style="padding:8px">¥0.80/GB（国内地域），用多少算多少</td><td style="padding:8px">代理流量不稳定或量级不大时最划算</td></tr>
<tr style="border-bottom:1px solid #dee2e6"><td style="padding:8px">按固定带宽</td><td style="padding:8px">按带宽峰值包月，如 5Mbps 约 ¥25/月</td><td style="padding:8px">流量稳定且大，超过约 31GB/月后更划算</td></tr>
</tbody></table>
<p><b>临界点估算</b>：按流量 ¥0.80/GB，固定带宽 5Mbps ¥25/月 → 临界点约 31GB/月。月流量 &lt; 31GB 选按流量，&gt; 31GB 选固定带宽。代理池通常流量波动大，建议默认选"按使用流量"。</p>

<h3>11.5 费用估算</h3><p>以下估算以华东 1（杭州）<code>ecs.t6-c1m1.large</code>（2 vCPU / 2 GiB）为例，价格为参考值，实际以<a href="https://www.aliyun.com/price/product?spm=5176.ecsgpu.0.0#/ecs/detail/calculator" target="_blank" rel="noopener noreferrer">阿里云官网</a>为准：</p>
<h4>场景一：3 节点代理池，按量付费，月流量 50GB</h4><pre>ECS 实例费：¥0.17/h × 730h × 3 = ¥372.30/月
公网流量费：¥0.80/GB × 50GB = ¥40.00/月
系统盘费：¥0.35/GB × 40GB × 3 = ¥42.00/月（ESSD PL0）
────────────────────────────────
合计约：¥454/月</pre>
<h4>场景二：3 节点代理池，抢占式（无保护期），月流量 50GB</h4><pre>ECS 实例费：~¥0.02/h × 730h × 3 = ~¥43.80/月
公网流量费：¥0.80/GB × 50GB = ¥40.00/月
系统盘费：¥0.35/GB × 40GB × 3 = ¥42.00/月
────────────────────────────────
合计约：¥126/月（节省 ~72%）</pre>
<h4>场景三：5 节点代理池，抢占式（无保护期），月流量 200GB</h4><pre>ECS 实例费：~¥0.02/h × 730h × 5 = ~¥73.00/月
公网流量费：¥0.80/GB × 200GB = ¥160.00/月
系统盘费：¥0.35/GB × 40GB × 5 = ¥70.00/月
────────────────────────────────
合计约：¥303/月（按量付费同等配置约 ¥793/月，节省 ~62%）</pre>
<p class="warning">系统盘费用在实例释放时自动停止。抢占式实例被回收后系统盘随之释放，不会产生额外费用。</p>

<h3>11.6 费用监控与告警</h3>
<ol><li><b>控制台实时监控</b> — 费用面板每次刷新自动更新，缓存 60 秒。关注"账户余额"和"当月已花费"两个核心指标。</li><li><b>阿里云费用告警</b> — 前往<a href="https://usercenter2.aliyun.com/home" target="_blank" rel="noopener noreferrer">用户中心</a> → "预算管理"创建预算告警，当月消费超过阈值时发送短信/邮件通知。</li><li><b>余额预警</b> — 阿里云默认在余额低于 100 元时发送预警。建议将预警阈值提高到能覆盖 3 天预估费用的金额。</li><li><b>自动调和兜底</b> — 将目标节点数设为 0 并调和，可立即停止所有 ECS 计费。配合自动调和间隔，即使抢占式实例被回收也能自动控制成本。</li></ol>

<h3>11.7 节省费用的最佳实践</h3>
<ul><li><b>使用抢占式实例</b> — 代理池最适合抢占式，成本可降至按量的 10%–30%。详见<a href="#spot-tutorial">抢占式教程</a>。</li><li><b>按需调整节点数</b> — 业务低峰期将目标节点数调低（如从 5 降到 2），高峰期再调高。通过 API 可定时自动调节：<pre># 低峰期降到 2 节点（每天 02:00）
curl "http://127.0.0.1:8765/api/target?target=2&token=&lt;API_TOKEN&gt;"
# 高峰期恢复 5 节点（每天 08:00）
curl "http://127.0.0.1:8765/api/target?target=5&token=&lt;API_TOKEN&gt;"</pre></li><li><b>选择入门级规格</b> — 代理转发是 I/O 密集型而非 CPU 密集型，<code>t6</code> / <code>s6</code> / <code>e-instance</code> 等突发性能实例即可满足需求，价格远低于计算型 <code>c6</code>。</li><li><b>合理设置公网带宽</b> — 代理流量波动大时选"按使用流量"计费；流量稳定且大时选"按固定带宽"。</li><li><b>及时释放闲置节点</b> — 不使用时将目标设为 0 并调和，确保所有实例已释放，避免忘记关机持续计费。</li><li><b>关注断路器</b> — 设置合理的断路器阈值（3–5），避免库存不足时反复创建失败浪费 API 调用和时间。</li><li><b>多地域分散部署</b> — 不同地域的抢占式价格和库存不同，多地域部署既能降低被回收风险，也可能获得更低的整体价格。</li></ul>
</section>
<section class="section" id="api-control"><h2>12. API 控制</h2><p>除控制台界面外，所有管理操作均可通过 HTTP API 调用。写操作需携带 API Token，可通过 <code>token</code> 查询参数或 <code>Authorization: Bearer</code> 请求头传递。</p><h3>认证方式</h3><pre>curl "http://127.0.0.1:8765/api/reconcile?token=&lt;API_TOKEN&gt;"
curl -H "Authorization: Bearer &lt;API_TOKEN&gt;" http://127.0.0.1:8765/api/reconcile</pre><h3>查询接口（GET）</h3><ul><li><code>GET /api/status</code> — 节点状态、统计、候选区、事件</li><li><code>GET /api/settings</code> — 当前配置概览（不返回密钥明文）</li><li><code>GET /api/billing</code> — 账户余额与当月账单</li><li><code>GET /api/resources</code> — 可用镜像、规格、交换机、安全组、启动模板</li><li><code>GET /api/proxies</code> — 当前健康代理列表（需 Token）</li><li><code>GET /api/proxies/balanced?strategy=round-robin</code> — 按策略获取单个代理（需 Token）</li></ul><h3>控制接口（GET，需 Token）</h3><ul><li><code>GET /api/reconcile</code> — 立即执行调和，返回操作后状态</li><li><code>GET /api/target?target=5</code> — 设置目标节点数（0–1000 整数），返回更新后状态</li><li><code>GET /api/nodes/delete?instance_id=i-xxx</code> — 下线并释放指定实例，返回操作后状态</li></ul><pre>curl "http://127.0.0.1:8765/api/target?target=6&token=&lt;API_TOKEN&gt;"
curl "http://127.0.0.1:8765/api/reconcile?token=&lt;API_TOKEN&gt;"
curl "http://127.0.0.1:8765/api/nodes/delete?instance_id=i-bp1xxx&token=&lt;API_TOKEN&gt;"</pre><p>控制接口成功时返回节点状态 JSON（与 <code>GET /api/status</code> 格式相同）。失败时返回 <code>{"error":"..."}</code> 及对应 HTTP 状态码：401（Token 无效或缺失）、400（参数错误）、409（操作冲突，如调和执行中）、500（内部错误）。</p><p class="warning">API Token 与控制台设置中的代理 API Token 相同。请通过受信网络调用，不要将 Token 写入日志或公开代码。</p></section>
<section class="section" id="ai-prompt"><h2>13. AI 提示词模板案例</h2><p>本节提供一套面向 AI 的结构化提示词，用于通过自然语言对话引导 AI 完成安全组、交换机、实例规格等核心参数的确认，最终生成可提交到控制台的完整资源配置模板。将以下提示词复制给 AI 助手即可开始交互式配置。</p>

<h3>13.1 使用方法</h3><ol><li>将下方"系统提示词"发送给 AI 助手。</li><li>AI 会逐步询问地域、安全组、交换机、实例规格等参数。</li><li>通过自然语言回答或调整，AI 会实时更新配置草案。</li><li>确认无误后，AI 输出完整的 JSON 配置模板。</li><li>将 JSON 中的参数填入控制台"阿里云设置"和"创建启动模板"对话框。</li></ol>

<h3>13.2 系统提示词</h3><pre>你是 ProxyPilot 代理池的资源配置助手。你的任务是通过自然语言对话，
帮助用户完成阿里云 ECS SOCKS5 代理池的资源配置。

你需要引导用户确认以下核心参数，并最终生成完整的资源配置模板。

## 需要确认的参数

### 1. 地域与可用区
- region_id：阿里云地域（如 cn-hangzhou、cn-shanghai、cn-beijing）
- zone_id：可用区（如 cn-hangzhou-j，可通过 GET /api/resources 查询）

### 2. 安全组
- security_group_id：已有安全组 ID，或通过"创建安全组"新建
- 安全组需开放 TCP 1080 入方向（SOCKS5 代理端口）
- 建议：仅允许可信客户端 IP，不向全网开放

### 3. 交换机（vSwitch）
- v_switch_id：交换机 ID，需与所选地域/可用区一致
- 可通过 GET /api/resources 查询可用交换机列表

### 4. 实例规格
- instance_type：ECS 实例规格
- 推荐：t6-c1m1.large（2vCPU/2GiB，代理转发足够）
- 可选：s6-c1m1.large、e-instance ecs.t6-c1m1.large 等
- 可通过 GET /api/resources 查询可用规格

### 5. 镜像
- image_id：Ubuntu 镜像 ID（cloud-init 依赖 Ubuntu）
- 推荐：ubuntu_22_04_x64 或 ubuntu_20_04_x64
- 可通过 GET /api/resources 查询可用镜像

### 6. SOCKS5 凭据
- socks5_username：代理认证用户名
- socks5_password：代理认证密码（至少 8 位，含大小写字母和数字）

### 7. 计费方式
- instance_charge_type：PostPaid（按量付费）或 Spot（抢占式）
- spot_duration：抢占式保护期（0=无、1=1小时、6=6小时）
- 代理池推荐：抢占式 + 无保护期（成本最低）

### 8. 调和参数
- target_nodes：目标节点数（建议 ≥ 2，保证可用性）
- batch_size：调和批量大小（默认 3）
- auto_reconcile_interval：自动调和间隔秒数（建议 60-300）
- circuit_breaker_threshold：断路器阈值（建议 3-5）

## 交互规则

1. 每次只问 1-2 个问题，不要一次列出所有参数。
2. 提供推荐值和说明，让用户可以简单回复"可以"或给出自定义值。
3. 用户说"用默认值"时，使用括号中的推荐值。
4. 收集完所有参数后，输出完整的 JSON 配置模板。
5. 如果用户提供的参数可能有问题（如地域与交换机不匹配），主动提醒。

## 对话示例

用户：帮我配置一个代理池
助手：好的！我先确认几个核心参数。

首先，你打算把代理节点部署在哪个地域？
- 华东1（杭州）· cn-hangzhou ← 推荐，资源丰富
- 华东2（上海）· cn-shanghai
- 华北2（北京）· cn-beijing
- 其他地域

回复地域名称或编号即可，也可以说"用默认"。

用户：用默认
助手：好的，使用华东1（杭州）cn-hangzhou。

接下来确认实例规格。代理转发是 I/O 密集型任务，入门级规格即可：
- ecs.t6-c1m1.large（2vCPU/2GiB）← 推荐，性价比最高
- ecs.s6-c1m1.large（2vCPU/2GiB）
- ecs.t6-c1m1.xlarge（4vCPU/4GiB）

你倾向哪个？也可以说"推荐"。

用户：推荐
助手：好的，使用 ecs.t6-c1m1.large。

关于计费方式，代理池适合抢占式实例（成本约按量的 10%-30%）：
- 抢占式（无保护期）← 推荐，价格最低，多节点可容忍中断
- 抢占式（1小时保护期），略贵但创建后1小时内不被回收
- 按量付费，稳定但成本高

你的选择？

用户：抢占式无保护期
助手：好的。

最后几个参数：
1. 目标节点数？（建议 3-5 个，保证单节点被回收后仍有可用代理）
2. SOCKS5 用户名和密码？（用于代理认证，密码至少8位含大小写和数字）

用户：3个节点，用户名 proxyuser，密码 MyProxy2026
助手：所有参数已确认。以下是完整的资源配置模板：

（输出 JSON）

请将以上参数填入控制台：
1. "阿里云设置"中填入 region_id、socks5_username、socks5_password、instance_charge_type 等。
2. "创建安全组"中创建安全组（如尚未有）。
3. "创建启动模板"中选择 image_id、instance_type、v_switch_id、security_group_id。
4. 设置目标节点数为 target_nodes，点击"立即调和"。</pre>

<h3>13.3 输出模板格式</h3><p>AI 确认所有参数后，会输出以下 JSON 格式的配置模板：</p><pre>{
  "settings": {
    "region_id": "cn-hangzhou",
    "access_key_id": "&lt;在控制台填写&gt;",
    "access_key_secret": "&lt;在控制台填写&gt;",
    "pool_name": "proxy-main",
    "socks5_username": "proxyuser",
    "socks5_password": "&lt;已确认的密码&gt;",
    "proxy_api_bearer_token": "&lt;在控制台填写&gt;",
    "instance_charge_type": "Spot",
    "spot_duration": "0",
    "auto_reconcile_interval": "60",
    "circuit_breaker_threshold": "3",
    "batch_size": "3",
    "pending_timeout_minutes": "5"
  },
  "launch_template": {
    "launch_template_name": "dante-proxy-&lt;时间戳&gt;",
    "description": "Dante SOCKS5 代理模板",
    "image_id": "ubuntu_22_04_x64",
    "instance_type": "ecs.t6-c1m1.large",
    "v_switch_id": "&lt;已确认的交换机 ID&gt;",
    "security_group_id": "&lt;已确认的安全组 ID&gt;"
  },
  "security_group": {
    "security_group_name": "proxy-sg-&lt;时间戳&gt;",
    "description": "SOCKS5 代理安全组",
    "v_switch_id": "&lt;与启动模板相同的交换机&gt;"
  },
  "target_nodes": 3,
  "post_create_steps": [
    "1. 在'阿里云设置'中填写 settings 部分的参数并保存",
    "2. 在'创建安全组'中创建安全组（如尚未有）",
    "3. 在'创建启动模板'中选择镜像、规格、交换机、安全组并创建",
    "4. 设置目标节点数为 3，点击'立即调和'",
    "5. 等待约 60 秒后再次调和，节点应进入健康状态"
  ]
}</pre>

<h3>13.4 快速启动提示词</h3><p>如果已有安全组和交换机，可以用以下精简提示词快速配置：</p><pre>你是 ProxyPilot 配置助手。请帮我快速配置代理池。

我已有的资源：
- 地域：cn-hangzhou
- 交换机 ID：vsw-bpxxxxxxxxxx
- 安全组 ID：sg-bpxxxxxxxxxx
- 目标节点数：3
- 计费方式：抢占式，无保护期
- SOCKS5 用户名：proxyuser
- SOCKS5 密码：Proxy2026!

请帮我确认实例规格和镜像选择，然后输出完整的启动模板参数和设置参数，
我直接填入控制台的'创建启动模板'和'阿里云设置'对话框。</pre>

<p class="warning">提示词中的密码和 Token 为示例值，实际使用时请替换为自己的真实凭据。切勿在公共 AI 平台上发送真实 AccessKey Secret。</p>
</section>
</main></body></html>'''


class UnavailableProxyPoolAdapter:
    def health_check(self, public_ip: str) -> tuple[bool, float]:
        raise RuntimeError("真实代理池尚未接入")

    def enable(self, instance_id: str, public_ip: str) -> None:
        raise RuntimeError("真实代理池尚未接入")

    def disable(self, instance_id: str) -> None:
        raise RuntimeError("真实代理池尚未接入")

    def update_latency(self, instance_id: str, latency: float) -> None:
        pass

    def proxies(self) -> list[str]:
        return []

    def node_stats(self) -> dict:
        return {}

    def acquire(self, strategy: str = "round-robin") -> str | None:
        return None

    def test_proxy(self, public_ip: str, target_host: str, target_port: int = 80, target_path: str = "/", timeout: float | None = None) -> dict:
        return {"ok": False, "error": "代理池未配置"}


def _validate_int(value: str, minimum: int, maximum: int, name: str) -> str:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是整数") from exc
    if not minimum <= parsed <= maximum:
        raise ValueError(f"{name} 必须在 {minimum}–{maximum} 之间")
    return str(parsed)


def _validate_charge_type(value: str) -> str:
    if value not in ("PostPaid", "Spot"):
        raise ValueError("instance_charge_type 必须是 PostPaid 或 Spot")
    return value


def _validate_spot_duration(value: str) -> str:
    if value not in ("0", "1", "6"):
        raise ValueError("spot_duration 必须是 0、1 或 6")
    return value


class DashboardService:
    SETTINGS_FIELDS = {
        "access_key_id",
        "access_key_secret",
        "region_id",
        "launch_template_id",
        "launch_template_version",
        "resource_group_id",
        "pool_name",
        "socks5_port",
        "socks5_username",
        "socks5_password",
        "socks5_check_host",
        "socks5_check_port",
        "socks5_timeout",
        "proxy_api_bearer_token",
        "clear_secret",
        "batch_size",
        "pending_timeout_minutes",
        "auto_reconcile_interval",
        "circuit_breaker_threshold",
        "instance_charge_type",
        "spot_duration",
    }
    SETTINGS_DEFAULTS = {
        "access_key_id": "",
        "access_key_secret": "",
        "region_id": "",
        "launch_template_id": "",
        "launch_template_version": "",
        "resource_group_id": "",
        "pool_name": "",
        "socks5_port": "1080",
        "socks5_username": "",
        "socks5_password": "",
        "socks5_check_host": "1.1.1.1",
        "socks5_check_port": "80",
        "socks5_timeout": "5",
        "proxy_api_bearer_token": "",
        "batch_size": "3",
        "pending_timeout_minutes": "5",
        "auto_reconcile_interval": "0",
        "circuit_breaker_threshold": "3",
        "instance_charge_type": "PostPaid",
        "spot_duration": "0",
    }
    HOT_CONFIG_FIELDS = {
        "batch_size": lambda v: _validate_int(v, 1, 50, "batch_size"),
        "pending_timeout_minutes": lambda v: _validate_int(v, 1, 120, "pending_timeout_minutes"),
        "auto_reconcile_interval": lambda v: _validate_int(v, 0, 3600, "auto_reconcile_interval"),
        "circuit_breaker_threshold": lambda v: _validate_int(v, 1, 20, "circuit_breaker_threshold"),
        "instance_charge_type": _validate_charge_type,
        "spot_duration": _validate_spot_duration,
    }
    TEMPLATE_FIELDS = {
        "launch_template_name", "description", "image_id", "instance_type",
        "v_switch_id", "security_group_id",
    }
    SECURITY_GROUP_FIELDS = {"security_group_name", "description", "v_switch_id"}
    IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:-]+$")
    RESOURCE_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{2,127}$")
    DESCRIPTION_PATTERN = re.compile(r"^[A-Za-z0-9 .,_:/()-]{1,256}$")
    HEALTH_CACHE_TTL = 8.0

    def __init__(
        self,
        settings_path: str | Path | None = None,
        env_path: str | Path | None = None,
    ) -> None:
        self.lock = threading.RLock()
        self.running = False
        self.settings_path = Path(settings_path) if settings_path is not None else Path(__file__).with_name("dashboard_settings.json")
        self.env_path = Path(env_path) if env_path is not None else Path(__file__).with_name(".env")
        self._settings = self._load_settings()
        self.last_reconcile: datetime | None = None
        self.last_result: ReconcileResult | None = None
        self.events: list[dict[str, str]] = []
        self._billing_cache: dict[str, Any] | None = None
        self._billing_cache_time = 0.0
        self._health_cache: dict[str, tuple[bool, float, float]] = {}
        self._geoip_cache: dict[str, dict[str, str]] = {}
        self._geoip_cache_time: dict[str, float] = {}
        self._auto_reconcile_timer: threading.Timer | None = None
        self._next_auto_reconcile: datetime | None = None
        self.ecs: AliyunECSAdapter | None = None
        self.proxy = self._make_proxy_pool()
        self.candidates: tuple[LaunchCandidate, ...] = ()
        self.store = SQLiteStateStore(":memory:")
        self.target = 4
        startup_error = ""
        if self._is_aliyun_configured():
            try:
                self._activate_adapter()
            except Exception as exc:
                startup_error = self.redact(exc)
                self.ecs = None
                self.candidates = ()
        self.controller = self._make_controller() if self.ecs else None
        self._event("阿里云 ECS 已就绪" if self.ecs else "阿里云未配置")
        if startup_error:
            self._event(f"阿里云初始化失败：{startup_error}")
        self._start_auto_reconcile()

    def _load_settings(self) -> dict[str, str]:
        settings = dict(self.SETTINGS_DEFAULTS)
        if self.settings_path.exists():
            try:
                data = json.loads(self.settings_path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("配置根节点不是对象")
                for key in settings:
                    value = data.get(key, settings[key])
                    if isinstance(value, str):
                        settings[key] = value
            except (OSError, ValueError, json.JSONDecodeError):
                logging.exception("无法读取阿里云设置，将使用环境配置")

        for key, value in load_dotenv_settings(self.env_path).items():
            if value:
                settings[key] = value
        return settings

    def _is_aliyun_configured(self) -> bool:
        required = ("access_key_id", "access_key_secret", "region_id", "launch_template_id", "pool_name")
        return all(self._settings[key] for key in required)

    def _make_proxy_pool(self) -> Socks5ProxyPool | UnavailableProxyPoolAdapter:
        required = ("socks5_username", "socks5_password", "proxy_api_bearer_token")
        if not all(self._settings[key] for key in required):
            return UnavailableProxyPoolAdapter()
        try:
            port = int(self._settings["socks5_port"])
            check_port = int(self._settings["socks5_check_port"])
            timeout = float(self._settings["socks5_timeout"])
        except ValueError as exc:
            raise ValueError("SOCKS5 端口或超时配置无效") from exc
        if timeout <= 0:
            raise ValueError("SOCKS5 超时必须大于 0")
        return Socks5ProxyPool(
            port, self._settings["socks5_username"], self._settings["socks5_password"],
            self._settings["socks5_check_host"], check_port, timeout,
        )

    def _adapter_name(self) -> str:
        return "阿里云 ECS" if self.ecs else "未配置"

    def proxy_list(self, authorization: str | None) -> dict[str, Any]:
        token = self._settings["proxy_api_bearer_token"]
        supplied = authorization[7:] if authorization and authorization.startswith("Bearer ") else ""
        if not token or not hmac.compare_digest(supplied, token):
            raise PermissionError("Bearer 凭据无效")
        if not isinstance(self.proxy, Socks5ProxyPool):
            raise RuntimeError("SOCKS5 代理池未配置")
        proxies = self.proxy.proxies()
        return {"proxies": proxies, "count": len(proxies)}

    def authenticate(self, token: str) -> bool:
        """验证 API 控制令牌"""
        configured = self._settings["proxy_api_bearer_token"]
        if not configured:
            return False
        return hmac.compare_digest(token, configured)

    def _build_adapter(self, settings: dict[str, str]) -> tuple[Any, tuple]:
        """Create adapter and discover candidates from a settings dict (no self mutation, may do network I/O)."""
        required = ("access_key_id", "access_key_secret", "region_id", "launch_template_id", "pool_name")
        if not all(settings.get(key) for key in required):
            return None, ()
        adapter = AliyunECSAdapter(
            settings["access_key_id"], settings["access_key_secret"],
            settings["region_id"], settings["launch_template_id"],
            settings["launch_template_version"], settings["resource_group_id"],
            settings.get("instance_charge_type", "PostPaid"),
            int(settings.get("spot_duration", "0")),
        )
        candidates = adapter.discover_launch_candidates()
        return adapter, candidates

    def _activate_adapter(self) -> None:
        self.ecs, self.candidates = self._build_adapter(self._settings)

    def redact(self, value: Any) -> str:
        text = str(value)
        with self.lock:
            secrets = tuple(
                secret for secret in (
                    self._settings["access_key_secret"], self._settings["socks5_password"],
                    self._settings["proxy_api_bearer_token"],
                ) if secret
            )
        for secret in secrets:
            text = text.replace(secret, "[REDACTED]")
        return text

    def settings(self) -> dict[str, Any]:
        with self.lock:
            return {
                "access_key_id_configured": bool(self._settings["access_key_id"]),
                "secret_configured": bool(self._settings["access_key_secret"]),
                "region_id": self._settings["region_id"],
                "launch_template_id": self._settings["launch_template_id"],
                "launch_template_version": self._settings["launch_template_version"],
                "resource_group_id": self._settings["resource_group_id"],
                "pool_name": self._settings["pool_name"],
                "proxy_configured": not isinstance(self.proxy, UnavailableProxyPoolAdapter),
                "socks5_port": self._settings["socks5_port"],
                "socks5_username": self._settings["socks5_username"],
                "socks5_password_configured": bool(self._settings["socks5_password"]),
                "proxy_api_bearer_token_configured": bool(self._settings["proxy_api_bearer_token"]),
                "configured": self._is_aliyun_configured(),
                "adapter": self._adapter_name(),
                "batch_size": self._settings.get("batch_size", "3"),
                "pending_timeout_minutes": self._settings.get("pending_timeout_minutes", "5"),
                "auto_reconcile_interval": self._settings.get("auto_reconcile_interval", "0"),
                "circuit_breaker_threshold": self._settings.get("circuit_breaker_threshold", "3"),
                "instance_charge_type": self._settings.get("instance_charge_type", "PostPaid"),
                "spot_duration": self._settings.get("spot_duration", "0"),
            }

    def _validate_setting(self, key: str, value: Any, required: bool = False) -> str:
        if not isinstance(value, str):
            raise ValueError(f"{key} 必须是字符串")
        value = value.strip()
        secret_fields = {"access_key_secret", "socks5_password", "proxy_api_bearer_token"}
        maximum = 256 if key in secret_fields else 128
        if len(value) > maximum:
            raise ValueError(f"{key} 长度不能超过 {maximum}")
        if required and not value:
            raise ValueError(f"{key} 不能为空")
        if value and key not in secret_fields and not self.IDENTIFIER_PATTERN.fullmatch(value):
            raise ValueError(f"{key} 包含不允许的字符")
        return value

    def save_settings(self, data: dict[str, Any]) -> None:
        unknown = set(data) - self.SETTINGS_FIELDS
        if unknown:
            raise ValueError(f"包含未知字段：{', '.join(sorted(unknown))}")
        clear_secret = data.get("clear_secret", False)
        if not isinstance(clear_secret, bool):
            raise ValueError("clear_secret 必须是布尔值")
        # Phase 1: validate under lock (quick, no network I/O)
        with self.lock:
            if self.running:
                raise RuntimeError("reconcile 执行期间不能修改阿里云设置")
            updated = dict(self._settings)
            secret_fields = {"access_key_secret", "socks5_password", "proxy_api_bearer_token"}
            for key in self.SETTINGS_DEFAULTS:
                if key in secret_fields or key not in data:
                    continue
                if key in self.HOT_CONFIG_FIELDS:
                    updated[key] = self.HOT_CONFIG_FIELDS[key](data[key])
                else:
                    updated[key] = self._validate_setting(key, data[key])
            if clear_secret:
                updated["access_key_secret"] = ""
            elif "access_key_secret" in data and data["access_key_secret"] != "":
                updated["access_key_secret"] = self._validate_setting("access_key_secret", data["access_key_secret"], required=True)
            for key in ("socks5_password", "proxy_api_bearer_token"):
                if key in data and data[key] != "":
                    updated[key] = self._validate_setting(key, data[key], required=True)

        # Phase 2: build adapter outside lock (network I/O, may be slow)
        adapter, candidates = self._build_adapter(updated)

        # Phase 3: commit under lock (quick)
        with self.lock:
            if self.running:
                raise RuntimeError("reconcile 执行期间不能修改阿里云设置")
            previous = self._settings
            previous_ecs, previous_candidates, previous_proxy = self.ecs, self.candidates, self.proxy
            self._settings = updated
            try:
                self.ecs = adapter
                self.candidates = candidates
                self.proxy = self._make_proxy_pool()
                controller = self._make_controller() if self.ecs else None
            except Exception:
                self._settings = previous
                self.ecs, self.candidates, self.proxy = previous_ecs, previous_candidates, previous_proxy
                raise
            try:
                self._write_settings(updated)
            except Exception:
                self._settings = previous
                self.ecs, self.candidates, self.proxy = previous_ecs, previous_candidates, previous_proxy
                self.controller = self._make_controller() if self.ecs else None
                raise
            self.controller = controller
            self._billing_cache = None
            self._billing_cache_time = 0.0
            self._stop_auto_reconcile()
            self._start_auto_reconcile()
            self._event(f"设置已更新，当前状态：{self._adapter_name()}")

    def _write_settings(self, settings: dict[str, str]) -> None:
        self.settings_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.settings_path.with_name(f".{self.settings_path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            temporary.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")
            try:
                os.chmod(temporary, 0o600)
            except OSError:
                pass
            os.replace(temporary, self.settings_path)
            try:
                os.chmod(self.settings_path, 0o600)
            except OSError:
                pass
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def _make_controller(self) -> ReconcileController:
        pool_name = self._settings["pool_name"] or "proxy-main"
        batch_size = int(self._settings.get("batch_size", "3") or "3")
        pending_timeout = timedelta(minutes=int(self._settings.get("pending_timeout_minutes", "5") or "5"))
        circuit_breaker_threshold = int(self._settings.get("circuit_breaker_threshold", "3") or "3")
        config = ReconcileConfig(
            pool_name, self.target, self.candidates,
            batch_size=batch_size,
            pending_timeout=pending_timeout,
            circuit_breaker_threshold=circuit_breaker_threshold,
        )
        return ReconcileController(self.ecs, self.proxy, self.store, config)

    def resources(self) -> dict[str, Any]:
        with self.lock:
            if not all(self._settings[key] for key in ("access_key_id", "access_key_secret", "region_id")):
                raise RuntimeError("查询资源前必须先保存 AccessKey 和地域")
            adapter = self.ecs or AliyunECSAdapter(
                self._settings["access_key_id"], self._settings["access_key_secret"],
                self._settings["region_id"], "", resource_group_id=self._settings["resource_group_id"],
            )
            return adapter.query_resources()

    def create_security_group(self, data: dict[str, Any]) -> dict[str, str]:
        unknown = set(data) - self.SECURITY_GROUP_FIELDS
        if unknown:
            raise ValueError(f"包含未知字段：{', '.join(sorted(unknown))}")
        if set(data) != self.SECURITY_GROUP_FIELDS:
            raise ValueError("security_group_name、description 和 v_switch_id 均为必填字段")
        name = data.get("security_group_name")
        description = data.get("description")
        v_switch_id = data.get("v_switch_id")
        if not isinstance(name, str) or not self.RESOURCE_NAME_PATTERN.fullmatch(name):
            raise ValueError("security_group_name 不符合严格白名单")
        if not isinstance(description, str) or not self.DESCRIPTION_PATTERN.fullmatch(description):
            raise ValueError("description 不符合严格白名单")
        v_switch_id = self._validate_setting("v_switch_id", v_switch_id, required=True)
        with self.lock:
            if self.running:
                raise RuntimeError("reconcile 执行期间不能创建安全组")
            required_settings = ("access_key_id", "access_key_secret", "region_id")
            if not all(self._settings[key] for key in required_settings):
                raise RuntimeError("创建安全组前必须先保存 AccessKey 和地域")
            adapter = self.ecs or AliyunECSAdapter(
                self._settings["access_key_id"], self._settings["access_key_secret"],
                self._settings["region_id"], "", resource_group_id=self._settings["resource_group_id"],
            )
            result = adapter.create_security_group(name, description, v_switch_id)
            self._event(f"已创建普通 VPC 安全组 {result['security_group_id']}")
            return result

    def create_launch_template(self, data: dict[str, Any]) -> dict[str, str]:
        unknown = set(data) - self.TEMPLATE_FIELDS
        if unknown:
            raise ValueError(f"包含未知字段：{', '.join(sorted(unknown))}")
        with self.lock:
            if self.running:
                raise RuntimeError("reconcile 执行期间不能创建启动模板")
            required_settings = ("access_key_id", "access_key_secret", "region_id")
            if not all(self._settings[key] for key in required_settings):
                raise RuntimeError("创建启动模板前必须先保存 AccessKey 和地域")
            values: dict[str, str] = {}
            for key in self.TEMPLATE_FIELDS:
                value = data.get(key, "")
                if key == "description":
                    if not isinstance(value, str):
                        raise ValueError("description 必须是字符串")
                    value = value.strip()
                    if len(value) > 256:
                        raise ValueError("description 长度不能超过 256")
                else:
                    value = self._validate_setting(key, value, required=True)
                values[key] = value
            if not self._settings["socks5_username"] or not self._settings["socks5_password"]:
                raise RuntimeError("创建启动模板前必须先配置 SOCKS5 用户名和密码")
            adapter = AliyunECSAdapter(
                self._settings["access_key_id"], self._settings["access_key_secret"],
                self._settings["region_id"], "", resource_group_id=self._settings["resource_group_id"],
                instance_charge_type=self._settings.get("instance_charge_type", "PostPaid"),
                spot_duration=int(self._settings.get("spot_duration", "0")),
            )
            result = adapter.create_launch_template(
                **values,
                socks5_username=self._settings["socks5_username"],
                socks5_password=self._settings["socks5_password"],
            )
            updated = dict(self._settings)
            updated.update(result)
            previous = self._settings
            previous_ecs, previous_candidates = self.ecs, self.candidates
            self._settings = updated
            try:
                adapter.launch_template_id = result["launch_template_id"]
                adapter.launch_template_version = result["launch_template_version"]
                self.candidates = adapter.discover_launch_candidates()
                self.ecs = adapter
                controller = self._make_controller()
                self._write_settings(updated)
            except Exception:
                self._settings = previous
                self.ecs, self.candidates = previous_ecs, previous_candidates
                raise
            self.controller = controller
            self._event(f"已创建并激活启动模板 {result['launch_template_id']} 版本 {result['launch_template_version']}")
            return result

    def billing(self) -> dict[str, Any]:
        with self.lock:
            if not self._settings["access_key_id"] or not self._settings["access_key_secret"]:
                return {"status": "unconfigured", "message": "未配置阿里云凭据", "balance": None, "month_spent": None, "currency": None, "billing_cycle": datetime.now(timezone.utc).strftime("%Y-%m"), "updated_at": None, "cached": False}
            now_monotonic = time.monotonic()
            if self._billing_cache is not None and now_monotonic - self._billing_cache_time < 60:
                return {**self._billing_cache, "cached": True}
            access_key_id = self._settings["access_key_id"]
            access_key_secret = self._settings["access_key_secret"]
            region_id = self._settings["region_id"]
        try:
            result = AliyunBillingAdapter(access_key_id, access_key_secret, region_id).query()
            status = "ok" if result.pop("bill_available") else "delayed"
            message = "费用中心数据已更新" if status == "ok" else "当月账单尚未出账或存在延迟"
            response = {"status": status, "message": message, **result, "updated_at": datetime.now(timezone.utc).isoformat(), "cached": False}
        except Exception as exc:
            text = self.redact(exc).lower()
            permission_markers = ("forbidden", "unauthorized", "permission", "accessdenied", "no permission", "没有权限", "无权限")
            status = "permission_denied" if any(marker in text for marker in permission_markers) else "unavailable"
            message = "RAM 用户无 BSS 账单查询权限" if status == "permission_denied" else "费用中心暂时不可用"
            response = {"status": status, "message": message, "balance": None, "month_spent": None, "currency": None, "billing_cycle": datetime.now(timezone.utc).strftime("%Y-%m"), "updated_at": datetime.now(timezone.utc).isoformat(), "cached": False}
        with self.lock:
            self._billing_cache = response
            self._billing_cache_time = time.monotonic()
        return response

    def _event(self, message: str) -> None:
        self.events.insert(0, {"time": datetime.now(timezone.utc).isoformat(), "message": self.redact(message)})
        del self.events[20:]

    def set_target(self, target: int) -> None:
        if isinstance(target, bool) or not isinstance(target, int) or not 0 <= target <= 1000:
            raise ValueError("目标节点数需为 0–1000 的整数")
        with self.lock:
            if self.running:
                raise RuntimeError("reconcile 执行期间不能修改目标节点数")
            self.target = target
            self.controller = self._make_controller() if self.ecs else None
            self._event(f"目标节点数调整为 {target}")

    def delete_node(self, data: dict[str, Any]) -> None:
        if set(data) != {"instance_id"}:
            raise ValueError("instance_id 是唯一必填字段")
        instance_id = self._validate_setting("instance_id", data.get("instance_id"), required=True)
        with self.lock:
            if self.running:
                raise RuntimeError("reconcile 执行期间不能删除节点")
            if self.ecs is None:
                raise RuntimeError("阿里云未配置，禁止删除节点")
            pool_name = self._settings["pool_name"]
            managed_ids = {node.instance_id for node in self.ecs.list_managed_instances(pool_name)}
            if instance_id not in managed_ids:
                raise RuntimeError("实例缺少当前池的双重管理标签，拒绝删除")
            self.proxy.disable(instance_id)
            self.ecs.release_instance(instance_id)
            self.store.delete(instance_id)
            self._event(f"已手动删除并释放节点 {instance_id}")

    def _geoip_lookup(self, ip: str) -> dict[str, str] | None:
        """通过 ip-api.com 免费接口查询 IP 归属地，带 10 分钟缓存。"""
        if not ip:
            return None
        now = time.monotonic()
        cached_time = self._geoip_cache_time.get(ip, 0)
        if ip in self._geoip_cache and now - cached_time < 600:
            return self._geoip_cache[ip]
        try:
            with urlopen(f"http://ip-api.com/json/{ip}?fields=status,country,regionName,city,isp&lang=zh-CN", timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if data.get("status") != "success":
                return None
            result = {
                "country": data.get("country", ""),
                "region": data.get("regionName", ""),
                "city": data.get("city", ""),
                "isp": data.get("isp", ""),
            }
        except (URLError, OSError, json.JSONDecodeError):
            return None
        with self.lock:
            self._geoip_cache[ip] = result
            self._geoip_cache_time[ip] = now
        return result

    def _start_auto_reconcile(self) -> None:
        """如果配置了自动调和间隔，启动后台定时器。"""
        interval = int(self._settings.get("auto_reconcile_interval", "0") or "0")
        if interval <= 0 or not self.controller:
            return
        def _auto_reconcile():
            try:
                self.reconcile()
            except RuntimeError:
                pass
            finally:
                self._start_auto_reconcile()
        with self.lock:
            self._auto_reconcile_timer = threading.Timer(interval, _auto_reconcile)
            self._auto_reconcile_timer.daemon = True
            self._auto_reconcile_timer.start()
            self._next_auto_reconcile = datetime.now(timezone.utc) + timedelta(seconds=interval)

    def _stop_auto_reconcile(self) -> None:
        with self.lock:
            if self._auto_reconcile_timer:
                self._auto_reconcile_timer.cancel()
                self._auto_reconcile_timer = None
            self._next_auto_reconcile = None

    def proxy_test(self, data: dict[str, Any]) -> dict[str, Any]:
        """通过指定代理节点向目标发起 HTTP GET 测试。"""
        if set(data) != {"instance_id", "target_url"}:
            raise ValueError("instance_id 和 target_url 是必填字段")
        instance_id = self._validate_setting("instance_id", data.get("instance_id"), required=True)
        target_url = data.get("target_url", "")
        if not isinstance(target_url, str) or not target_url:
            raise ValueError("target_url 不能为空")
        parsed = urlparse(target_url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("target_url 必须是 http 或 https URL")
        target_host = parsed.hostname or ""
        target_port = parsed.port or (443 if parsed.scheme == "https" else 80)
        target_path = parsed.path or "/"
        if parsed.query:
            target_path += "?" + parsed.query
        if not target_host:
            raise ValueError("target_url 缺少主机名")
        with self.lock:
            if isinstance(self.proxy, UnavailableProxyPoolAdapter):
                raise RuntimeError("代理池未配置")
            proxy = self.proxy
        stats = proxy.node_stats() if hasattr(proxy, "node_stats") else {}
        node_info = stats.get(instance_id)
        if not node_info:
            raise RuntimeError("节点不在代理池中或未启用")
        public_ip = node_info["public_ip"]
        result = proxy.test_proxy(public_ip, target_host, target_port, target_path)
        if result.get("ok") and hasattr(proxy, "record_traffic"):
            proxy.record_traffic(instance_id, 0, result.get("response_size", 0))
        return result

    def balanced_proxy(self, strategy: str, authorization: str | None) -> dict[str, Any]:
        """按负载均衡策略返回单个代理 URL。"""
        token = self._settings["proxy_api_bearer_token"]
        supplied = authorization[7:] if authorization and authorization.startswith("Bearer ") else ""
        if not token or not hmac.compare_digest(supplied, token):
            raise PermissionError("Bearer 凭据无效")
        with self.lock:
            if isinstance(self.proxy, UnavailableProxyPoolAdapter):
                raise RuntimeError("SOCKS5 代理池未配置")
            proxy = self.proxy
        if strategy not in ("round-robin", "random", "least-connections", "lowest-latency"):
            strategy = "round-robin"
        url = proxy.acquire(strategy) if hasattr(proxy, "acquire") else None
        if not url:
            raise RuntimeError("代理池为空")
        return {"proxy": url, "strategy": strategy}

    def balanced_proxy_local(self, strategy: str) -> dict[str, Any]:
        """控制台内部调用负载均衡，不需要 API Token。"""
        with self.lock:
            if isinstance(self.proxy, UnavailableProxyPoolAdapter):
                raise RuntimeError("SOCKS5 代理池未配置")
            proxy = self.proxy
        if strategy not in ("round-robin", "random", "least-connections", "lowest-latency"):
            strategy = "round-robin"
        url = proxy.acquire(strategy) if hasattr(proxy, "acquire") else None
        if not url:
            raise RuntimeError("代理池为空")
        return {"proxy": url, "strategy": strategy}

    def reconcile(self) -> None:
        with self.lock:
            if self.running:
                raise RuntimeError("reconcile 已在运行")
            if self.controller is None:
                raise RuntimeError("阿里云未配置，禁止调和")
            if isinstance(self.proxy, UnavailableProxyPoolAdapter):
                raise RuntimeError("真实代理池尚未接入，禁止调和以避免误判或误删实例")
            self.running = True
        try:
            result = self.controller.reconcile()
            with self.lock:
                self.last_result = result
                self.last_reconcile = datetime.now(timezone.utc)
                detail = f"调和完成：健康 {result.healthy}，启动中 {result.pending}"
                if result.created:
                    detail += f"，新建 {len(result.created)}"
                if result.released:
                    detail += f"，释放 {len(result.released)}"
                if result.circuit_broken:
                    detail += f"，断路器释放 {len(result.circuit_broken)}"
                self._event(detail)
                for error in result.errors:
                    self._event(f"错误：{error}")
        finally:
            with self.lock:
                self.running = False

    def _probe_health(self, nodes: list, proxy: Any) -> dict[str, tuple[bool, float]]:
        """对运行中有公网 IP 的节点执行 SOCKS5 探测；带短时缓存避免频繁探测。返回 {instance_id: (healthy, latency_ms)}"""
        if isinstance(proxy, UnavailableProxyPoolAdapter):
            return {}
        now = time.monotonic()
        result: dict[str, tuple[bool, float]] = {}
        to_check: list = []
        for node in nodes:
            if node.status == "Running" and node.public_ip and not node.recycling:
                cached = self._health_cache.get(node.instance_id)
                if cached and now - cached[2] < self.HEALTH_CACHE_TTL:
                    result[node.instance_id] = (cached[0], cached[1])
                else:
                    to_check.append(node)
        if not to_check:
            return result

        def _check(node: Any) -> tuple[str, bool, float]:
            try:
                healthy, latency = proxy.health_check(node.public_ip)
                return node.instance_id, healthy, latency
            except Exception:
                return node.instance_id, False, 0.0

        with ThreadPoolExecutor(max_workers=min(8, len(to_check))) as pool:
            for instance_id, healthy, latency in pool.map(_check, to_check):
                self._health_cache[instance_id] = (healthy, latency, time.monotonic())
                result[instance_id] = (healthy, latency)
        return result

    def status(self) -> dict[str, Any]:
        with self.lock:
            pool_name = self._settings["pool_name"] or "未配置"
            nodes = self.ecs.snapshot(pool_name) if self.ecs else []
            proxy = self.proxy

        health = self._probe_health(nodes, proxy)

        with self.lock:
            rows = []
            counts = {"healthy": 0, "pending": 0, "draining": 0, "errors": len(self.last_result.errors) if self.last_result else 0}
            for node in nodes:
                if node.recycling:
                    state = "draining"
                elif node.status in ReconcileController.PENDING_STATUSES:
                    state = "pending"
                elif node.status == "Running" and node.public_ip and health.get(node.instance_id, (False, 0))[0]:
                    state = "healthy"
                else:
                    state = "error"
                if state == "error":
                    counts["errors"] += 1
                elif state in counts:
                    counts[state] += 1
                latency_ms = health.get(node.instance_id, (False, 0))[1] if state == "healthy" else 0
                geo = self._geoip_lookup(node.public_ip) if node.public_ip and state == "healthy" else None
                rows.append({
                    "instance_id": node.instance_id, "public_ip": node.public_ip,
                    "cloud_status": node.status, "zone": node.zone, "state": state,
                    "created_at": node.created_at.isoformat(),
                    "latency_ms": latency_ms,
                    "geo": geo,
                    "failure_count": self.store.get_failure_count(node.instance_id),
                })
            traffic_stats = proxy.node_stats() if hasattr(proxy, "node_stats") else {}
            candidates = [{"region_id": c.region_id, "instance_type": c.instance_type, "vswitch_id": c.vswitch_id, "zone": c.vswitch_id, "availability": "真实模板"} for c in self.candidates]
            configured = self._is_aliyun_configured() and self.ecs is not None
            auto_interval = int(self._settings.get("auto_reconcile_interval", "0") or "0")
            return {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "adapter": self._adapter_name(), "pool": pool_name,
                "aliyun_configured": configured,
                "reconcile_enabled": configured and not isinstance(self.proxy, UnavailableProxyPoolAdapter),
                "proxy_configured": not isinstance(self.proxy, UnavailableProxyPoolAdapter),
                "target": self.target,
                "runtime": {
                    "running": self.running,
                    "ok": configured and counts["errors"] == 0,
                    "last_reconcile": self.last_reconcile.isoformat() if self.last_reconcile else None,
                    "auto_reconcile_interval": auto_interval,
                    "next_auto_reconcile": self._next_auto_reconcile.isoformat() if self._next_auto_reconcile else None,
                },
                "stats": counts, "nodes": rows, "candidates": candidates,
                "events": list(self.events),
                "traffic_stats": traffic_stats,
            }


SERVICE = DashboardService()


class Handler(BaseHTTPRequestHandler):
    server_version = "ReconcileDashboard/1.0"

    @property
    def service(self) -> DashboardService:
        return getattr(self.server, "service", SERVICE)

    def log_message(self, fmt: str, *args: Any) -> None:
        logging.info("%s - %s", self.address_string(), self.service.redact(fmt % args))

    def _json(self, data: Any, status: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _is_local_request(self) -> bool:
        try:
            address = ipaddress.ip_address(self.client_address[0])
        except ValueError:
            return False
        return address.is_loopback or (
            isinstance(address, ipaddress.IPv6Address)
            and address.ipv4_mapped is not None
            and address.ipv4_mapped.is_loopback
        )

    def _is_authorized(self) -> bool:
        """本机请求或携带有效 Bearer Token 的请求均允许写操作。"""
        if self._is_local_request():
            return True
        auth = self.headers.get("Authorization", "")
        supplied = auth[7:] if auth.startswith("Bearer ") else ""
        return self.service.authenticate(supplied)

    def _body(self) -> dict[str, Any]:
        content_length = self.headers.get("Content-Length")
        if content_length is None:
            raise ValueError("缺少 Content-Length")
        try:
            length = int(content_length)
        except ValueError as exc:
            raise ValueError("Content-Length 无效") from exc
        if length < 0:
            raise ValueError("Content-Length 无效")
        if length > 65536:
            raise ValueError("请求体过大")
        if not length:
            return {}
        data = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("JSON 请求体必须是对象")
        return data

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in {"/", "/help"}:
            body = (HTML if path == "/" else HELP_HTML).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'")
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/status":
            self._json(self.service.status())
        elif path == "/api/settings":
            self._json(self.service.settings())
        elif path == "/api/billing":
            self._json(self.service.billing())
        elif path == "/api/resources":
            self._json(self.service.resources())
        elif path == "/api/proxies":
            try:
                self._json(self.service.proxy_list(self.headers.get("Authorization")))
            except PermissionError as exc:
                self._json({"error": str(exc)}, 401)
            except RuntimeError as exc:
                self._json({"error": self.service.redact(exc)}, 409)
        elif path == "/api/proxies/balanced":
            params = parse_qs(urlparse(self.path).query)
            strategy = params.get("strategy", ["round-robin"])[0]
            supplied = params.get("token", [""])[0] if params.get("token") else ""
            if not supplied:
                auth = self.headers.get("Authorization", "")
                if auth.startswith("Bearer "):
                    supplied = auth[7:]
            if not self.service.authenticate(supplied):
                self._json({"error": "API token 无效或缺失"}, 401)
                return
            try:
                self._json(self.service.balanced_proxy(strategy, self.headers.get("Authorization")))
            except PermissionError as exc:
                self._json({"error": str(exc)}, 401)
            except RuntimeError as exc:
                self._json({"error": self.service.redact(exc)}, 409)
        elif path in {"/api/reconcile", "/api/target", "/api/nodes/delete"}:
            self._handle_control_get(path, parse_qs(urlparse(self.path).query))
        else:
            self._json({"error": "接口不存在"}, 404)

    def _handle_control_get(self, path: str, params: dict[str, list[str]]) -> None:
        """GET 方式的 API 控制（需 Token 认证）"""
        supplied = params.get("token", [""])[0] if params.get("token") else ""
        if not supplied:
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                supplied = auth[7:]
        if not self.service.authenticate(supplied):
            self._json({"error": "API token 无效或缺失"}, 401)
            return
        try:
            if path == "/api/reconcile":
                self.service.reconcile()
            elif path == "/api/target":
                raw = params.get("target", [""])[0] if params.get("target") else ""
                try:
                    target = int(raw)
                except ValueError:
                    raise ValueError("target 参数必须是 0–1000 的整数")
                self.service.set_target(target)
            elif path == "/api/nodes/delete":
                instance_id = params.get("instance_id", [""])[0] if params.get("instance_id") else ""
                if not instance_id:
                    raise ValueError("instance_id 参数缺失")
                self.service.delete_node({"instance_id": instance_id})
            self._json(self.service.status())
        except ValueError as exc:
            self._json({"error": self.service.redact(exc)}, 400)
        except RuntimeError as exc:
            self._json({"error": self.service.redact(exc)}, 409)
        except Exception:
            logging.error("API 控制执行失败")
            self._json({"error": "服务器内部错误"}, 500)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            if not self._is_authorized():
                self._json({"error": "仅允许本机操作或携带有效 Token"}, 403)
                return
            body = self._body()
            if path == "/api/target":
                self.service.set_target(body.get("target"))
                response = self.service.status()
            elif path == "/api/reconcile":
                self.service.reconcile()
                response = self.service.status()
            elif path == "/api/nodes/delete":
                self.service.delete_node(body)
                response = self.service.status()
            elif path == "/api/settings":
                self.service.save_settings(body)
                response = self.service.settings()
            elif path == "/api/security-groups":
                response = self.service.create_security_group(body)
            elif path == "/api/launch-templates":
                response = self.service.create_launch_template(body)
            elif path == "/api/proxy-test":
                response = self.service.proxy_test(body)
            elif path == "/api/proxies/balanced":
                strategy = body.get("strategy", "round-robin")
                response = self.service.balanced_proxy_local(strategy)
            else:
                self._json({"error": "接口不存在"}, 404)
                return
            self._json(response)
        except (ValueError, json.JSONDecodeError) as exc:
            self._json({"error": self.service.redact(exc)}, 400)
        except RuntimeError as exc:
            self._json({"error": self.service.redact(exc)}, 409)
        except Exception:
            logging.error("API 执行失败；异常详情已抑制以避免泄露敏感配置")
            self._json({"error": "服务器内部错误"}, 500)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main() -> None:
    parser = argparse.ArgumentParser(description="阿里云 ECS 节点调和 Web 管理界面")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = Server((args.host, args.port), Handler)
    print(f"节点调和控制台：http://{args.host}:{args.port}/")
    print("未完整配置时禁止调和；按 Ctrl+C 停止。")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已停止。")
    finally:
        SERVICE._stop_auto_reconcile()
        server.server_close()
        SERVICE.store.close()


if __name__ == "__main__":
    main()
