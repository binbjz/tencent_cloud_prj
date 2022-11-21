import requests
import ujson as json
from qcloud_cos import CosS3Client
from qcloud_cos.cos_exception import CosException


def list_buckets(client: CosS3Client):
    """
    Describe buckets list
    :param client: cos Client
    :return:
    """
    try:
        req = client.list_buckets()
        return req
    except CosException as err:
        raise Exception(f"Describe bucket list exception: {err}")


def head_bucket(client: CosS3Client, bucket_name: str):
    """
    Retrieves whether the bucket exists and has access permission
    :param client: cos Client
    :param bucket_name: bucket name - BucketName-APPID
    """
    try:
        req = client.head_bucket(
            Bucket=bucket_name
        )
        return req
    except CosException as err:
        raise Exception(f"Retrieves bucket exception: {err}")


def bucket_exists(client: CosS3Client, bucket_name: str):
    """
    Check whether the bucket exists
    :param client: cos Client
    :param bucket_name:
    """
    try:
        req = client.bucket_exists(
            Bucket=bucket_name
        )
        return req
    except CosException as err:
        raise Exception(f"Check bucket exists exception: {err}")


def list_objects(client: CosS3Client, bucket_name: str):
    """
    Describe all objects with specify bucket name
    :param client: cos Client
    :param bucket_name:
    """
    try:
        req = client.list_objects(Bucket=bucket_name)
        if "Contents" in req:
            for content in req["Contents"]:
                print(content["Key"])
    except CosException as err:
        raise Exception(f"List all objects exception: {err}")


def list_objects_paging(client: CosS3Client, bucket_name: str, obj_prefix=None):
    """
    Paging enumeration object with specify bucket name
    :param client: cos Client
    :param bucket_name:
    :param obj_prefix: object prefix
    By default, all objects are retrieved.
    If a prefix is specified, the object containing the prefix will be matched.
    """
    if not obj_prefix:
        obj_prefix = ""
    try:
        marker = ""
        while True:
            req = client.list_objects(Bucket=bucket_name, Prefix=obj_prefix,
                                      Marker=marker, MaxKeys=50)
            if "Contents" in req:
                for content in req["Contents"]:
                    print(content["Key"])
            if req["IsTruncated"] == "false":
                break
            marker = req["NextMarker"]
    except CosException as err:
        raise Exception(f"List all objects with paging exception: {err}")


def bucket_cors(client: CosS3Client, bucket_name: str):
    """
    Describe bucket cores config
    :param client: cos Client
    :param bucket_name:
    """
    try:
        req = client.get_bucket_cors(
            Bucket=bucket_name,
        )
        return req
    except CosException as err:
        raise Exception(f"Describe bucket cors exception: {err}")


def bucket_lifecycle(client: CosS3Client, bucket_name: str):
    """
    Describe bucket lifecycle
    :param client: cos Client
    :param bucket_name:
    """
    try:
        req = client.get_bucket_lifecycle(
            Bucket=bucket_name,
        )
        return req
    except CosException as err:
        raise Exception(f"Describe bucket lifecycle exception: {err}")


def get_object_url(client: CosS3Client, bucket_name: str, obj_key_name: str):
    """
    Obtain object access URL is used for anonymous download or distribution.
    :param bucket_name:
    :param client: cos client
    :param obj_key_name:  the unique identifier of the object in the bucket
    若对象的访问域名 api-okr-tool-1312894547.cos.ap-beijing.myqcloud.com/TB-project/axios.js 中,
    对象键为 TB-project/axios.js
    """
    try:
        url = client.get_object_url(
            Bucket=bucket_name,
            Key=obj_key_name
        )
        resp = requests.get(url).status_code
        return url, resp
    except CosException as err:
        raise Exception(f"Get bucket object url exception: {err}")


def get_bucket_policy(client: CosS3Client, bucket_name: str):
    """
    Query the permission policy of a specified bucket
    :param client: cos client
    :param bucket_name:
    :return:
    """
    try:
        reps = client.get_bucket_policy(
            Bucket=bucket_name,
        )
        policy = json.loads(reps["Policy"])
        return policy
    except CosException as err:
        raise Exception(f"Get bucket policy exception: {err}")


def get_bucket_acl(client: CosS3Client, bucket_name: str):
    """
    Query the ACL of a specified bucket
    :param client: cos client
    :param bucket_name:
    """

    try:
        reps = client.get_bucket_acl(
            Bucket=bucket_name
        )
        return reps
    except CosException as err:
        raise Exception(f"Get bucket acl exception: {err}")


def get_bucket_logging(client: CosS3Client, bucket_name: str):
    """
    Query the log config of a specified bucket
    :param client: cos client
    :param bucket_name:
    """
    try:
        reps = client.get_bucket_logging(
            Bucket=bucket_name
        )
        return reps
    except CosException as err:
        raise Exception(f"Get bucket logging exception: {err}")
