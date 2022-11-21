import ujson as json
import pandas as pd
from typing import Optional
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.dts.v20180330 import models
from tencentcloud.dts.v20180330.dts_client import DtsClient


def create_migrate_job(client: DtsClient, inst_region: str, dts_job_name: str,
                       src_supplier: str, src_access_key: str, src_db_type: str,
                       src_access_type: str, src_db_ip: str, src_db_user: str,
                       src_db_passwd: str, src_db_version: str, dst_db_type: str, dst_access_type: str,
                       dst_inst_id: str, dst_db_ip: str, dst_db_user: str, dst_db_passwd: str):
    """
    Create Data Transmission Service Job - 创建数据迁移任务
    :param client: dts client
    :param inst_region: instance region for Src and Dst
    :param dts_job_name: dts job name
    :param src_supplier: dts source supplier
    :param src_access_key: source access key
    :param src_db_type: source database type
    :param src_access_type: source access type
    :param src_db_ip: source database ip
    :param src_db_user: source database user
    :param src_db_passwd: source database password
    :param src_db_version: source database version
    :param dst_db_type: destination database type
    :param dst_access_type: destination database access type
    :param dst_inst_id: destination Instance id
    :param dst_db_ip: destination database ip
    :param dst_db_user: destination database user
    :param dst_db_passwd: destination database password
    :return: Migrate job response - including JobId、RequestId
    RunMode 任务运行模式，值包括: 1-立即执行，2-定时执行
    MigrateType 数据迁移类型, 值包括：1-结构迁移, 2-全量迁移, 3-全量+增量迁移
    MigrateObject 迁移对象, 1-整个实例，2-指定库表
    ConsistencyType 抽样数据一致性检测参数，1-未配置, 2-全量检测, 3-抽样检测, 4-仅校验不一致表, 5-不检测
    IsOverrideRoot 是否用源库Root账户覆盖目标库, 值包括:0-不覆盖, 1-覆盖, 选择库表或者结构迁移时应该为0
    默认接口请求频率限制：20次/秒
    """
    try:
        req = models.CreateMigrateJobRequest()
        params = {
            "JobName": dts_job_name,
            "MigrateOption": {
                "RunMode": 1,
                "MigrateType": 2,
                "MigrateObject": 1,
                "ConsistencyType": 5,
                "IsOverrideRoot": 0
            },
            "SrcDatabaseType": src_db_type,
            "SrcAccessType": src_access_type,
            "SrcInfo": {
                "AccessKey": src_access_key,
                "Ip": src_db_ip,
                "Port": 3306,
                "Region": inst_region,
                "Supplier": src_supplier,
                "User": src_db_user,
                "Password": src_db_passwd,
                "EngineVersion": src_db_version,
                "CcnId": "ccn-o6bj73lx",
                "VpcId": "vpc-ia6g8bqe",
                "SubnetId": "subnet-faia91n7"
            },
            "DstDatabaseType": dst_db_type,
            "DstAccessType": dst_access_type,
            "DstInfo": {
                "InstanceId": dst_inst_id,
                "Ip": dst_db_ip,
                "Port": 3306,
                "Region": inst_region,
                "ReadOnly": 0,
                "User": dst_db_user,
                "Password": dst_db_passwd
            }
        }
        req.from_json_string(json.dumps(params))
        resp = client.CreateMigrateJob(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Create migrate job exception: {err}")


def describe_migrate_job(client: DtsClient, job_id: str):
    """
    Query Data Transmission Service Job - 查询数据迁移任务并返回job状态
    :param client: dts client
    :param job_id: dts job id
    :return: describe migrate job response
    默认接口请求频率限制: 50次/秒
    """
    try:
        req = models.DescribeMigrateJobsRequest()
        params = {
            "JobId": job_id,
            "Order": "CreateTime",
            "OrderSeq": "DESC",
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeMigrateJobs(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe migrate job exception: {err}")


def get_all_migrate_job_id(client: DtsClient):
    """
    :param client: dts client
    :return: all migrate job count、job id and job request id
    """
    try:
        req = models.DescribeMigrateJobsRequest()
        params = {
            "Order": "CreateTime",
            "OrderSeq": "DESC",
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeMigrateJobs(req)
        job_total_account = resp.TotalCount
        job_id_list = (job.JobId for job in resp.JobList)
        job_request_id = resp.RequestId
    except TencentCloudSDKException as err:
        raise Exception(f"Describe migrate job id exception: {err}")
    return job_total_account, job_id_list, job_request_id


def check_migrate_job(client: DtsClient, job_id: Optional[str] = None):
    """
    Query Data Transmission Service Job - 查询数据迁移任务并输出结果
    :param client: dts client
    :param job_id: dts job id - If job_id is specified, the specified job is described;
            otherwise, all jobs are described.
    Status 任务状态,取值为: 1-创建中(Creating), 3-校验中(Checking), 4-校验通过(CheckPass),
    5-校验不通过(CheckNotPass), 7-任务运行(Running), 8-准备完成(ReadyComplete),
    9-任务成功(Success), 10-任务失败(Failed), 11-撤销中(Stopping), 12-完成中(Completing),
    2-任务创建完成,等待启动, 14-任务已终止, 6-任务准备运行
    StepInfo: Status/步骤状态, 0-默认值, 1-成功, 2-失败, 3-执行中, 4-未执行
    """
    try:
        req = models.DescribeMigrateJobsRequest()
        params = {
            "JobId": job_id,
            "Order": "CreateTime",
            "OrderSeq": "DESC",
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeMigrateJobs(req)

        # 若没有指定job id, 将输出所有migrate job信息, 并退出函数
        if job_id is None:
            # 输出json格式的字符串回包
            print(resp.to_json_string(indent=2))
            return

        # 若指定job id, 将输出指定的migrate job信息
        step_count = 0
        for job in resp.JobList:
            print(f"{'-' * 126}")
            print(f"Job ID: {job.JobId}, Job Name: {job.JobName}, Job Status: {job.Status}")
            print(f"Job Migrate Option: {job.MigrateOption}")
            jdsa = job.Detail.StepAll
            print(f"\nJob Detail Step All: {jdsa}")
            jdsi = job.Detail.StepInfo
            for step in jdsi:
                step_count += 1
                print(f"Step{step_count} Name: {step.StepName}, Step{step_count} Status: {step.Status}")
    except TencentCloudSDKException as err:
        raise Exception(f"Describe migrate job exception: {err}")


def create_migrate_check_job(client: DtsClient, job_id: str):
    """
    Create Data Transmission Service Check Job - 创建校验迁移任务
    :param client: dts client
    :param job_id: dts job id
    :return: create migrate check job response - including RequestId
    校验成功后,迁移任务若有修改, 则必须重新创建校验并通过后, 才能开始迁移
    默认接口请求频率限制：20次/秒
    """
    try:
        req = models.CreateMigrateCheckJobRequest()
        params = {
            "JobId": job_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.CreateMigrateCheckJob(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Create migrate check job exception: {err}")


def describe_migrate_check_job(client: DtsClient, job_id: str):
    """
    Check Data Transmission Service Check Job - 获取迁移校验结果
    :param client: dts client
    :param job_id: dts job id
    Status 校验任务状态: unavailable(当前不可用), starting(开始中)，running(校验中)，finished(校验完成)
    CheckFlag 校验是否通过: 0-fail，1-pass, 3-no-check
    :return: describe migrate check job response
    默认接口请求频率限制：20次/秒
    """
    try:
        req = models.DescribeMigrateCheckJobRequest()
        params = {
            "JobId": job_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeMigrateCheckJob(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe migrate check job exception: {err}")


def start_migrate_job(client: DtsClient, job_id: str):
    """
    Start Data Transmission Service Job - 启动数据迁移任务
    :param client: dts client
    :param job_id: dts job id
    :return: start migrate job response
    默认接口请求频率限制：20次/秒
    """
    try:
        req = models.StartMigrateJobRequest()
        params = {
            "JobId": job_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.StartMigrateJob(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Start migrate job exception: {err}")


def complete_migrate_job(client: DtsClient, job_id: str):
    """
    Complete Data Transmission Service Job - 完成数据迁移任务
    :param client: dts client
    :param job_id: dts job id
    :return: complete migrate job response
    仅支持旧版MySQL迁移任务 - waitForSync-等待主从差距为0才停止,immediately-立即完成，不会等待主从差距一致
    任务的状态为准备完成(status=8)时，此时可以调用本接口完成迁移任务
    默认接口请求频率限制：20次/秒
    """
    try:
        req = models.CompleteMigrateJobRequest()
        params = {
            "JobId": job_id,
            "CompleteMode": "waitForSync"
        }
        req.from_json_string(json.dumps(params))
        resp = client.CompleteMigrateJob(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Complete migrate job exception: {err}")


def modify_migrate_job(client: DtsClient, job_id: str):
    """
    Modify Data Transmission Service Job - 修改数据迁移任务
    :param client: dts client
    :param job_id: dts job id
    :return: modify migrate job response
    当迁移任务处于下述状态时，允许调用本接口修改迁移任务：迁移创建中(status=1)、校验成功(status=4)、
    校验失败(status=5)、迁移失败(status=10)。但源实例、目标实例类型和目标实例地域不允许修改
    默认接口请求频率限制：20次/秒
    """
    try:
        req = models.ModifyMigrateJobRequest()
        params = {
            "JobId": job_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.ModifyMigrateJob(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Modify migrate job exception: {err}")


def delete_migrate_job(client: DtsClient, job_id: str):
    """
    Delete Data Transmission Service Job - 删除数据迁移任务
    :param client: dts client
    :param job_id: dts job id
    :return: delete migrate job response
    任务的状态为: 检验中(status=3)、运行中(status=7)、准备完成(status=8)、
    撤销中(status=11)或者完成中(status=12)时，不允许删除任务
    """
    try:
        req = models.DeleteMigrateJobRequest()
        params = {
            "JobId": job_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.DeleteMigrateJob(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Delete migrate job exception: {err}")


def stop_migrate_job(client: DtsClient, job_id: str):
    """
    Stop Data Transmission Service Job - 撤销数据迁移任务
    :param client: dts client
    :param job_id: dts job id
    :return: stop migrate job response
    任务状态为运行中(status=7)或准备完成(status=8)时,才能撤销数据迁移任务
    """
    try:
        req = models.StopMigrateJobRequest()
        params = {
            "JobId": job_id
        }
        req.from_json_string(json.dumps(params))
        resp = client.StopMigrateJob(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Stop migrate job exception: {err}")


def parse_dts_parm_to_dict(fn: str, sn: str, nr: Optional[int] = None, spr: Optional[list] = None):
    """
    Parsing valid values to dict from specified work sheet
    :param fn: Any valid string path is acceptable
    :param sn: Strings are used for sheet names
    :param nr: Number of rows to parse - 24 or 66
    :param spr: Line numbers to skip (0-indexed) [1, 4, 8, 12, 14, 20]
    :return: valid dts parameters with dict type
    """
    dts_parms = {}
    df = pd.read_excel(fn, sheet_name=sn, nrows=nr, skiprows=lambda x: x in spr)
    for row in df.itertuples():
        dts_parms[getattr(row, "腾讯云实例id")] = {
            "ali_ds_instance_id": getattr(row, "原实例id"),
            "tc_ds_instance_id": getattr(row, "腾讯云实例id"),
            "tc_ds_version": getattr(row, "版本"),
            "ali_ds_intranet_host": getattr(row, "阿里云地址内网"),
            "ali_ds_extranet_host": getattr(row, "阿里云地址外网"),
            "tc_ds_intranet_host": getattr(row, "对标腾讯云连接地址"),
            "ali_ds_user": getattr(row, "阿里云高权限").split("/")[0],
            "ali_ds_passwd": getattr(row, "阿里云高权限").split("/")[1],
            "tc_ds_user": getattr(row, "腾讯云高权限").split("/")[0],
            "tc_ds_passwd": getattr(row, "腾讯云高权限").split("/")[1],
            "tc_ds_arch": getattr(row, "腾讯云架构"),
        }
    return dts_parms
