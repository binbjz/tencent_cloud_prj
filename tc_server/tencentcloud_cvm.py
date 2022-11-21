import ujson as json
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.cvm.v20170312 import models
from tencentcloud.cvm.v20170312.cvm_client import CvmClient


def describe_instance(client: CvmClient):
    """
    Describe instance
    :param client: cvm client
    """
    try:
        req = models.DescribeInstancesRequest()
        params = {
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeInstances(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe Instance Exception: {err}")


def describe_instance_status(client: CvmClient):
    """
    Describe instance Status
    :param client: cvm client
    """
    try:
        req = models.DescribeInstancesStatusRequest()
        params = {
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeInstancesStatus(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe Instance Status Exception: {err}")


def describe_instance_type_configs(client: CvmClient):
    """
    Describe instance type config - 用于查询实例机型配置
    :param client: cvm client
    """
    try:
        req = models.DescribeInstanceTypeConfigsRequest()
        params = {
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeInstanceTypeConfigs(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe Instance type Exception: {err}")
