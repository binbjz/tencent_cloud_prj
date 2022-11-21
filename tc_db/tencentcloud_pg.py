import ujson as json
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.postgres.v20170312 import models
from tencentcloud.postgres.v20170312.postgres_client import PostgresClient


def describe_db_instances(client: PostgresClient):
    """
    Describe DB instances
    :param client: PG client
    :return:
    """
    try:
        req = models.DescribeDBInstancesRequest()
        params = {
            "Limit": 100,
            "OrderBy": "CreateTime",
            "OrderByType": "desc"
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeDBInstances(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe PG instance exception: {err}")


def describe_db_instance_attribute(client: PostgresClient, db_instance_id: str):
    """
    Describe PG instance attribute
    :param client: PG client
    :param db_instance_id:
    :return:
    """
    try:
        req = models.DescribeDBInstanceAttributeRequest()
        params = {
            "DBInstanceId": db_instance_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeDBInstanceAttribute(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe PG instance attribute exception: {err}")


def describe_databases(client: PostgresClient, db_instance_id: str):
    """
    Pull PG DB list
    :param client: PG client
    :param db_instance_id:
    :return:
    """
    try:
        req = models.DescribeDatabasesRequest()
        params = {
            "DBInstanceId": db_instance_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeDatabases(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Pull PG DB list exception: {err}")


def describe_slow_query_list(client: PostgresClient, db_instance_id: str,
                             begin_time: str, end_time: str):
    """
    Describe slow query list
    :param client: PG client
    :param db_instance_id:
    :param begin_time: The default log retention period is 7 days.
           The start time cannot exceed the log retention period. (2022-09-25 00:00:00)
    :param end_time: end time (2022-09-29 23:59:59)
    """
    try:
        req = models.DescribeSlowQueryListRequest()
        params = {
            "DBInstanceId": db_instance_id,
            "StartTime": begin_time,
            "EndTime": end_time
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeSlowQueryList(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe slow query list exception: {err}")


def describe_slow_query_analysis(client: PostgresClient, db_instance_id: str,
                                 begin_time: str, end_time: str):
    """
    Describe slow query analysis list
    :param client: PG client
    :param db_instance_id:
    :param begin_time:
    :param end_time:
    """
    try:
        req = models.DescribeSlowQueryAnalysisRequest()
        params = {
            "DBInstanceId": db_instance_id,
            "StartTime": begin_time,
            "EndTime": end_time,
            "OrderBy": "CostTime",
            "OrderByType": "desc",
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeSlowQueryAnalysis(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe slow query analysis list exception: {err}")
