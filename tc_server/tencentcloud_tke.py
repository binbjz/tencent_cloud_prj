import ujson as json
from typing import Optional
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.tke.v20180525 import models
from tencentcloud.tke.v20180525.tke_client import TkeClient


def describe_cluster(client: TkeClient):
    """
    Describe cluster - 查询集群列表
    :param client: tke client
    :return:
    """
    try:
        req = models.DescribeClustersRequest()
        params = {
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeClusters(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe Cluster Exception: {err}")


def describe_cluster_status(client: TkeClient, cluster_ids: Optional[list] = None):
    """
    Describe cluster status - 查看集群状态列表
    :param client: tke client
    :param cluster_ids:
    :return:
    """
    try:
        req = models.DescribeClusterStatusRequest()
        params = {
            "ClusterIds": cluster_ids
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeClusterStatus(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe Cluster Status Exception: {err}")


def describe_cluster_endpoint_status(client: TkeClient, cluster_id: str):
    """
    Describe cluster endpoint status
    查询集群访问端口状态(独立集群开启内网/外网访问，托管集群支持开启内网访问)
    :param client: tke client
    :param cluster_id:
    """
    try:
        req = models.DescribeClusterEndpointStatusRequest()
        params = {
            "ClusterId": cluster_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeClusterEndpointStatus(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe Cluster Endpoint Status Exception: {err}")


def describe_cluster_endpoints(client: TkeClient, cluster_id: str):
    """
    Describe cluster endpoint
    获取集群的访问地址，包括内网地址，外网地址，外网域名，外网访问安全策略
    :param client: tke client
    :param cluster_id:
    :return:
    """
    try:
        req = models.DescribeClusterEndpointsRequest()
        params = {
            "ClusterId": cluster_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeClusterEndpoints(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe Cluster Endpoint Exception: {err}")
