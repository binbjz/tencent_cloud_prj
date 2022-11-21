import ujson as json
from tencentcloud.cdb.v20170320.cdb_client import CdbClient
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.cdb.v20170320 import models


def mysql_instance_list(client: CdbClient):
    """
    Describe mysql instance list
    :param client: cdb client
    :return: describe mysql instance list
    """
    try:
        req = models.DescribeDBInstancesRequest()
        params = {
            "DbType": "MYSQL",
            "Limit": 100,
            "OrderByType": "DESC"
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeDBInstances(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe DB instance list exception: {err}")


def describe_databases(client: CdbClient, db_instance_id: str):
    """
    Query the database information about cloud database instances.
    Only the primary instance and disaster recovery instance are supported.
    Read-only instances are not supported.
    :param client: cdb client
    :param db_instance_id:
    """
    try:
        req = models.DescribeDatabasesRequest()
        params = {
            "InstanceId": db_instance_id,
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeDatabases(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe DB info exception: {err}")


def describe_tables(client: CdbClient, db_instance_id: str, db_name: str):
    """
    Query the database table information of the cloud database instance.
    Only the primary instance and disaster recovery instance are supported.
    Read-only instances are not supported.
    :param client: cdb client
    :param db_instance_id:
    :param db_name:
    """
    try:
        req = models.DescribeTablesRequest()
        params = {
            "InstanceId": db_instance_id,
            "Database": db_name,
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeTables(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe Table info exception: {err}")


def modify_db_instance_ip_port(client: CdbClient, instance_id, dst_ip, dst_port=3306):
    """
    Modify DB instance ip and port
    :param client: cdb client
    :param instance_id:
    :param dst_ip:
    :param dst_port:
    :return: the response information for the modified instance
    """
    try:
        req = models.ModifyDBInstanceVipVportRequest()
        params = {
            "InstanceId": instance_id,
            "DstIp": dst_ip,
            "DstPort": dst_port,
            "UniqVpcId": "vpc-ia6g8bqe",
            "UniqSubnetId": "subnet-faia91n7"
        }
        req.from_json_string(json.dumps(params))
        resp = client.ModifyDBInstanceVipVport(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Modify DB instance exception: {err}")


def modify_db_instance_name(client: CdbClient, instance_id, instance_name):
    """
    Modify DB instance name
    :param client: cdb client
    :param instance_id:
    :param instance_name:
    :return: the response information for the modified instance
    """
    try:
        req = models.ModifyDBInstanceNameRequest()
        params = {
            "InstanceId": instance_id,
            "InstanceName": instance_name,
        }
        req.from_json_string(json.dumps(params))
        resp = client.ModifyDBInstanceName(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Modify DB instance exception: {err}")


def db_instance_time_window(client: CdbClient, instance_id):
    """
    Query the maintenance time window of the cloud database instance
    :param client: cdb client
    :param instance_id:
    """
    try:
        req = models.DescribeTimeWindowRequest()
        params = {
            "InstanceId": instance_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeTimeWindow(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Query DB instance timewindow exception: {err}")


def describe_db_accounts(client: CdbClient, instance_id):
    """
    Query the information about all accounts in the cloud database
    :param client: cdb client
    :param instance_id:
    """
    try:
        req = models.DescribeAccountsRequest()
        params = {
            "InstanceId": instance_id,
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeAccounts(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Query DB accounts exception: {err}")


def describe_db_instance_charset(client: CdbClient, db_instance_id):
    """
    Describe DB instance charset
    :param client:
    :param db_instance_id:
    :return: describe DB instance charset
    """
    try:
        req = models.DescribeDBInstanceCharsetRequest()
        params = {
            "InstanceId": db_instance_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeDBInstanceCharset(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe DB instance charset exception: {err}")
