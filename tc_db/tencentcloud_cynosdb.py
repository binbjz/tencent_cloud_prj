import ujson as json
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.cynosdb.v20190107 import models
from tencentcloud.cynosdb.v20190107.cynosdb_client import CynosdbClient


def tdsql_c_cluster_list(client: CynosdbClient):
    """
    Describe TDSQL-C cluster list
    :param client: cynosdb client
    :return: describe tdsql-c cluster list
    Status: running、offlined、deleted
    """
    try:
        req = models.DescribeClustersRequest()
        params = {
            "Limit": 100,
            "OrderBy": "CREATETIME",
            "OrderByType": "DESC",
            "DbType": "MYSQL",
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeClusters(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe TDSQL-C cluster list exception: {err}")


def describe_cluster_detail(client: CynosdbClient, cluster_id: str):
    """
    Describe cluster detail
    :param client: cynosdb client
    :param cluster_id:
    """
    try:
        req = models.DescribeClusterDetailRequest()
        params = {
            "ClusterId": cluster_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeClusterDetail(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe TDSQL-C cluster detail exception: {err}")


def tdsql_c_instance_list(client: CynosdbClient):
    """
    Describe TDSQL-C instance list
    :param client: cynosdb client
    :return: describe tdsql-c instance list
    """
    try:
        req = models.DescribeInstancesRequest()
        params = {
            "Limit": 100,
            "OrderBy": "CREATETIME",
            "OrderByType": "DESC",
            "DbType": "MYSQL",
            "Status": "running"
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeInstances(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe TDSQL-C instance list exception: {err}")


def describe_instance_detail(client: CynosdbClient, db_instance_id: str):
    """
    Describe instance detail
    :param client: synosdb client
    :param db_instance_id:
    """
    try:
        req = models.DescribeInstanceDetailRequest()
        params = {
            "InstanceId": db_instance_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeInstanceDetail(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe TDSQL-C instance detail exception: {err}")
