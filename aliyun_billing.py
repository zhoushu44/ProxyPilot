from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from alibabacloud_bssopenapi20171214.client import Client as BSSClient
from alibabacloud_bssopenapi20171214 import models as bss_models
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models


class AliyunBillingAdapter:
    def __init__(
        self,
        access_key_id: str,
        access_key_secret: str,
        region_id: str = "cn-hangzhou",
        client: Any | None = None,
    ) -> None:
        if not access_key_id or not access_key_secret:
            raise ValueError("账单查询缺少 AccessKey 配置")
        self._runtime = util_models.RuntimeOptions(connect_timeout=10000, read_timeout=30000)
        self._client = client or BSSClient(open_api_models.Config(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            region_id=region_id or "cn-hangzhou",
            endpoint="business.aliyuncs.com",
        ))

    @staticmethod
    def _decimal(value: Any) -> Decimal:
        try:
            return Decimal(str(value if value is not None else 0))
        except (InvalidOperation, ValueError):
            return Decimal(0)

    @staticmethod
    def _amount(value: Decimal) -> str:
        return format(value.quantize(Decimal("0.01")), "f")

    def query(self, now: datetime | None = None) -> dict[str, Any]:
        current = now or datetime.now(timezone.utc)
        cycle = current.strftime("%Y-%m")
        balance_body = self._client.query_account_balance_with_options(self._runtime).body
        overview_body = self._client.query_bill_overview_with_options(
            bss_models.QueryBillOverviewRequest(billing_cycle=cycle), self._runtime
        ).body
        if getattr(balance_body, "success", True) is False:
            raise RuntimeError("QueryAccountBalance 返回失败")
        if getattr(overview_body, "success", True) is False:
            raise RuntimeError("QueryBillOverview 返回失败")

        balance = getattr(balance_body, "data", None)
        overview = getattr(overview_body, "data", None)
        items = getattr(getattr(overview, "items", None), "item", None) or []
        currency = getattr(balance, "currency", "") or ""
        spent = Decimal(0)
        for item in items:
            spent += self._decimal(getattr(item, "pretax_amount", None))
            if not currency:
                currency = getattr(item, "currency", "") or getattr(item, "payment_currency", "") or ""
        returned_cycle = getattr(overview, "billing_cycle", "") or cycle
        return {
            "balance": self._amount(self._decimal(getattr(balance, "available_amount", None))),
            "available_cash_amount": self._amount(self._decimal(getattr(balance, "available_cash_amount", None))),
            "credit_amount": self._amount(self._decimal(getattr(balance, "credit_amount", None))),
            "mybank_credit_amount": self._amount(self._decimal(getattr(balance, "mybank_credit_amount", None))),
            "month_spent": self._amount(spent),
            "currency": currency or "未知",
            "billing_cycle": returned_cycle,
            "bill_available": bool(items),
        }
