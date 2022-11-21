import ujson as json
from typing import Optional
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.billing.v20180709 import models
from tencentcloud.billing.v20180709.billing_client import BillingClient


def describe_bill_summary_by_product(client: BillingClient, begin_time: str, end_time: str):
    """
    Describe bill summary by product - 获取产品汇总费用分布
    :param client: bill client
    :param begin_time: 2022-09
    :param end_time: 2022-09
    """
    try:
        req = models.DescribeBillSummaryByProductRequest()
        params = {
            "BeginTime": begin_time,
            "EndTime": end_time
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeBillSummaryByProduct(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe bill by product exception: {err}")


def describe_bill_summary_by_project(client: BillingClient, begin_time: str, end_time: str):
    """
    Describe bill summary by project - 获取按项目汇总费用分布
    :param client: bill client
    :param begin_time: 2022-09
    :param end_time: 2022-09
    """
    try:
        req = models.DescribeBillSummaryByProjectRequest()
        params = {
            "BeginTime": begin_time,
            "EndTime": end_time
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeBillSummaryByProject(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe bill by project exception: {err}")


def describe_bill_summary_by_region(client: BillingClient, begin_time: str, end_time: str):
    """
    Describe bill summary by region - 获取按地域汇总费用分布
    :param client: bill client
    :param begin_time: 2022-09
    :param end_time: 2022-09
    """
    try:
        req = models.DescribeBillSummaryByRegionRequest()
        params = {
            "BeginTime": begin_time,
            "EndTime": end_time
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeBillSummaryByRegion(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe bill by project exception: {err}")


def describe_bill_detail(client: BillingClient, bill_month: Optional[str] = None,
                         begin_time: Optional[str] = None, end_time: Optional[str] = None):
    """
    Describe bill detail - 查询账单明细数据
    :param client: bill client
    :param bill_month: 2022-9
    :param begin_time: 2022-09-01 00:00:00
    :param end_time: 2022-09-30 23:59:59
    """
    try:
        req = models.DescribeBillDetailRequest()
        params = {
            "Offset": 0,
            "Limit": 100,
            "PeriodType": "byUsedTime",
            "Month": bill_month,
            "BeginTime": begin_time,
            "EndTime": end_time
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeBillDetail(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe bill detail exception: {err}")


def describe_cost_summary_by_region(client: BillingClient, begin_time: str, end_time: str):
    """
    Describe cost summary by region - 获取按地域汇总消耗详情
    :param client: bill client
    :param begin_time: 2022-09
    :param end_time: 2022-09
    """
    try:
        req = models.DescribeCostSummaryByRegionRequest()
        params = {
            "BeginTime": begin_time,
            "EndTime": end_time,
            "Limit": 100,
            "Offset": 0
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeCostSummaryByRegion(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe cost summary by region exception: {err}")


def describe_cost_summary_by_project(client: BillingClient, begin_time: str, end_time: str):
    """
    Describe cost summary by project - 获取按项目汇总消耗详情
    :param client: bill client
    :param begin_time: 2022-09
    :param end_time: 2022-09
    :return:
    """
    try:
        req = models.DescribeCostSummaryByProjectRequest()
        params = {
            "BeginTime": begin_time,
            "EndTime": end_time,
            "Limit": 100,
            "Offset": 0
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeCostSummaryByProject(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe cost summary by project exception: {err}")


def describe_cost_summary_by_product(client: BillingClient, begin_time: str, end_time: str):
    """
    Describe cost summary by product - 获取按产品汇总消耗详情
    :param client: bill client
    :param begin_time: 2022-09
    :param end_time: 2022-09
    """
    try:
        req = models.DescribeCostSummaryByProductRequest()
        params = {
            "BeginTime": begin_time,
            "EndTime": end_time,
            "Limit": 100,
            "Offset": 0
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeCostSummaryByProduct(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe cost summary by product exception: {err}")


def describe_cost_detail(client: BillingClient, cost_month: Optional[str] = None,
                         begin_time: Optional[str] = None, end_time: Optional[str] = None):
    """
    Describe cost detail
    :param client: bill client
    :param cost_month: yyyy-mm - 2022-09
    :param begin_time: yyyy-mm-dd hh:ii:ss - 2022-09-01 00:00:00
    :param end_time: yyyy-mm-dd hh:ii:ss - 2022-09-30 23:59:59
    """
    try:
        req = models.DescribeCostDetailRequest()
        params = {
            "Limit": 100,
            "Offset": 0,
            "BeginTime": begin_time,
            "EndTime": end_time,
            "Month": cost_month,
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeCostDetail(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe cost detail exception: {err}")


def describe_account_balance(client: BillingClient):
    """
    Describe account balance - 获取云账户余额信息
    :param client: bill client
    """
    try:
        req = models.DescribeAccountBalanceRequest()
        params = {
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeAccountBalance(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe account balance exception: {err}")
