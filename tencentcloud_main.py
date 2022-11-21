import ujson as json
from pprint import pprint
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.dts.v20180330 import dts_client
from tencentcloud.dts.v20180330.dts_client import DtsClient
from tencentcloud.cdb.v20170320 import cdb_client
from tencentcloud.cdb.v20170320.cdb_client import CdbClient
from tencentcloud.cynosdb.v20190107 import cynosdb_client
from tencentcloud.cynosdb.v20190107.cynosdb_client import CynosdbClient
from tencentcloud.postgres.v20170312 import postgres_client
from tencentcloud.postgres.v20170312.postgres_client import PostgresClient
from tencentcloud.tke.v20180525 import tke_client
from tencentcloud.tke.v20180525.tke_client import TkeClient
from tencentcloud.cvm.v20170312 import cvm_client
from tencentcloud.cvm.v20170312.cvm_client import CvmClient
from qcloud_cos import CosS3Client, CosConfig
from tencentcloud.vpc.v20170312 import vpc_client
from tencentcloud.vpc.v20170312.vpc_client import VpcClient
from tencentcloud.billing.v20180709 import billing_client
from tencentcloud.billing.v20180709.billing_client import BillingClient
from tencent_cloud_prj.tc_db.tencentcloud_mysql import *
from tencent_cloud_prj.tc_db.tencentcloud_pg import *
from tencent_cloud_prj.tc_dts.tencentcloud_dts import *
from tencent_cloud_prj.tc_bill.tencentcloud_bill import *
from tencent_cloud_prj.tc_db.tencentcloud_cynosdb import *
from tencent_cloud_prj.tc_net.tencentcloud_net import *
from tencent_cloud_prj.tc_server.tencentcloud_cvm import *
from tencent_cloud_prj.tc_server.tencentcloud_tke import *
from tencent_cloud_prj.tc_cos.tencentcloud_cos import *
from tencent_cloud_prj.tc_utils.poll_task import TimeoutException, poll
from tencent_cloud_prj.tc_utils.tools_utils import from_yaml, name_gen


def dts_main(client: DtsClient):
    """
    Tencent Cloud dts main entry
    :param client: dts client
    """

    # DTS Params
    dts_job_name = name_gen("dts", "bin")
    # src_supplier = "aliyun"
    # src_secret_key = "SqavrS9GiKXqQLK8HQgAT8DkSgsTvB"
    # src_db_type = "mysql"
    # dst_db_type = "mysql"
    # src_access_type = "ccn"
    # dst_access_type = "cdb"

    # src_access_type = "extranet"
    # src_db_ip = "rm-2zeb90r6f2xukg8b30o.mysql.rds.aliyuncs.com"

    # src_db_ip = "rm-2ze6868ei1d6c605j.mysql.rds.aliyuncs.com"
    # src_db_user = "admin_user"
    # src_db_passwd = "zXIy2y*zZl&ensFk"
    # src_db_version = "8.0"
    # dst_inst_id = "cdb-ilc1lfny"
    # dst_db_ip = "10.111.0.106"
    # dst_db_user = "root"
    # dst_db_passwd = "Naxions123456--"

    # [Start] Migrate job workflow
    # 1. Create migrate job - note: the current SDK can only create dts job with Xlarge spec
    # create_reps = create_migrate_job(client, _region, dts_job_name, src_supplier, src_secret_key,
    #                                  src_db_type, src_access_type, src_db_ip, src_db_user,
    #                                  src_db_passwd, src_db_version, dst_db_type, dst_access_type,
    #                                  dst_inst_id, dst_db_ip, dst_db_user, dst_db_passwd)
    # print(f"Migrate Job has been created.\n{create_reps.to_json_string(indent=2)}")

    # 2. Describe migrate job (2->3->4)
    # mj_id = create_reps.JobId
    # desc_migrate_reps = describe_migrate_job(client, mj_id)
    # for job in desc_migrate_reps.JobList:
    #     if job.Status == 2:
    #         print(f"Migrate job ({mj_id}) status: {job.Status}")
    #     else:
    #         raise Exception(f"Migrate job ({mj_id}) create fail.")

    # 3. Create migrate check job
    # create_migrate_check_reps = create_migrate_check_job(client, mj_id)
    # print(f"Migrate check job ({mj_id}) has been created.\n"
    #       f"{create_migrate_check_reps.to_json_string(indent=2)}\n")

    # 4. Polling migrate check job status - (Status: finished, CheckFlag: 1)
    # try:
    #     poll(lambda: describe_migrate_check_job(client, mj_id).CheckFlag == 1, step=2, timeout=180)
    # except TimeoutException as te:
    #     raise Exception(f"Check migrate check job ({mj_id}) timeout: {te}")
    # desc_migrate_check_reps = describe_migrate_check_job(client, mj_id)
    # print(f"Checking migrate check job ({mj_id}).\n{desc_migrate_check_reps.to_json_string(indent=2)}")

    # 5. Start migrate job (6->7->9)
    # print(f"Start migrate job ({mj_id})")
    # start_migrate_reps = start_migrate_job(client, mj_id)
    # for job in describe_migrate_job(client, mj_id).JobList:
    #     print(f"Migrate job ({mj_id}) status: {job.Status}")
    # [End] Migrate job workflow

    # Complete migrate job - only to incremental migrate
    # complete_migrate_reps = complete_migrate_job(client, mj_id)
    # print(f"Complete migrate job.\n{complete_migrate_reps.to_json_string(indent=2)}")

    # Delete migrate job
    # not_allow_del_status = [3, 7, 8, 11, 12]
    # mj_list = ["dts-6r8uw6hv", "dts-nnp6s48f", "dts-1qr71yy7", "dts-ikytcq6d"]
    # for mj_id in mj_list:
    #     for job in describe_migrate_job(client, mj_id).JobList:
    #         if job.Status not in not_allow_del_status:
    #             print(f"[INFO] Deleting Migrate job ({mj_id}) status: {job.Status}")
    #             delete_migrate_job(client, mj_id)
    #         else:
    #             print(f"[WARN] Deleting Migrate job ({mj_id}) is not allowed for "
    #                   f"status: {job.Status}")

    # job数量及所有job id
    # job_infos = get_all_migrate_job_id(client)
    # jobs_count, jobs_id, job_request_id = job_infos[0], job_infos[1], job_infos[2]
    # print(f"Jobs Count: {jobs_count}, Jobs Request id: {job_request_id}")

    # job状态及其包含的所有步骤状态
    # for job_id in jobs_id:
    #     check_migrate_job(client, job_id)

    # job id及其状态
    # job_counter = 0
    # for job_id in jobs_id:
    #     job_counter += 1
    #     for job_info in describe_migrate_job(client, job_id).JobList:
    #         print(f"[{job_counter}] jod_id: {job_info.JobId}, job_status: {job_info.Status}")

    # Parsing work sheet for source dts data
    file_name = "resource/ali-to-tencent-resource-20220923.xlsx"
    sheet_name = "MySQL"
    number_of_rows = 23
    skip_rows = [1, 4, 8, 12, 14, 20]

    # dict with dts data
    dts_counter = 0
    dts_parm = parse_dts_parm_to_dict(file_name, sheet_name, number_of_rows, skip_rows)
    print(f"{len(dts_parm)} dts jobs will be processed..")
    pprint(dts_parm)
    print(f"{'-' * 62}")
    for k, v in dts_parm.items():
        dts_counter += 1
        print(f"[{dts_counter}] {k} - Architecture: {v['tc_ds_arch']}")


def cdb_main(client: CdbClient):
    """
    Tencent Cloud cdb main entry
    :param client: cdb client
    """
    # MySQL instance list
    # print(mysql_instance_list(client).to_json_string(indent=2))

    # MySQL DB info
    # db_instance_id = "cdb-imkepbos"
    # db_name = "oa_platform"
    # print(describe_databases(client, _db_instance_id).to_json_string(indent=2))

    # MySQL Table info
    # print(describe_tables(client, db_instance_id, db_name).to_json_string(indent=2))

    # mysql instance all accounts in the cloud database
    db_instance_id = "cdb-mr8rm9cy"
    print(describe_db_accounts(client, db_instance_id).to_json_string(indent=2))


def cynosdb_main(client: CynosdbClient):
    """
    Tencent Cloud tdsql-c main entry
    :param client: tdsql-c client
    """
    # tdsql-c cluster list
    # print(tdsql_c_cluster_list(client).to_json_string(indent=2))

    # tdsql-c instance list
    # print(tdsql_c_instance_list(client).to_json_string(indent=2))

    # tdsql-c cluster detail
    # db_cluster_id = "cynosdbmysql-cglgx86k"
    # print(describe_cluster_detail(client, db_cluster_id).to_json_string(indent=2))

    # tdsql-c instance detail
    db_instance_id = "cynosdbmysql-ins-kxrv2y35"
    print(describe_instance_detail(client, db_instance_id).to_json_string(indent=2))


def pg_main(client: PostgresClient):
    """
    Tencent Cloud pg main entry
    :param client: pg client
    """
    # PG instance list
    # print(describe_db_instances(client).to_json_string(indent=2))

    # PG instance attribute
    pg_instance_id = "postgres-p2t0ecp6"
    # print(describe_db_instance_attribute(client, pg_instance_id).to_json_string(indent=2))

    # Pull PG DB list
    # print(describe_databases(client, pg_instance_id).to_json_string(indent=2))

    # Describe PG slow query list
    begin_time = "2022-09-26 00:00:00"
    end_time = "2022-09-30 23:59:59"
    print(describe_slow_query_list(client, pg_instance_id, begin_time, end_time).
          to_json_string(indent=2))

    # Describe slow query analysis list
    # print(describe_slow_query_analysis(client, pg_instance_id, begin_time, end_time).
    #       to_json_string(indent=2))


def cvm_main(client: CvmClient):
    """
    Tencent Cloud cvm main entry
    :param client: cvm client
    """
    # cvm instance list
    # print(describe_instance(client).to_json_string(indent=2))

    # cvm instance status
    # print(describe_instance_status(client).to_json_string(indent=2))

    # cvm instance type config
    print(describe_instance_type_configs(client).to_json_string(indent=2))


def tke_main(client: TkeClient):
    """
    Tencent Cloud tke main entry
    :param client: tke client
    """
    # cluster list
    # print(describe_cluster(client).to_json_string(indent=2))

    # cluster status
    # print(describe_cluster_status(client).to_json_string(indent=2))

    # cluster endpoint
    cluster_id = "cls-43j28lsn"
    print(describe_cluster_endpoints(client, cluster_id).to_json_string(indent=2))

    # # cluster endpoint status
    print(describe_cluster_endpoint_status(client, cluster_id).to_json_string(indent=2))


def network_main(client: VpcClient):
    """
    Tencent Cloud network main entry
    :param client: network client
    """
    # Nat gateway
    # print(describe_nat_gateways(client).to_json_string(indent=2))

    # VPN gateway
    # print(describe_vpn_gateway(client).to_json_string(indent=2))

    # VPN gateway route
    # print(describe_vpn_gateway_route(client, "vpngw-ifnfq84w").to_json_string(indent=2))

    # Peer gateway vendor information object
    # print(describe_customer_gateways(client).to_json_string(indent=2))

    # VPCs - Private network cloud list
    print(describe_vpcs(client).to_json_string(indent=2))

    # VPC resource
    # vpc_ids = ["vpc-axc7u31k", "vpc-m4t03dls", "vpc-4tb5jn5y",
    #            "vpc-ia6g8bqe", "vpc-b0qal3xq", "vpc-02xgi2v6"]
    # print(describe_vpc_resource_dashboard(client, vpc_ids).to_json_string(indent=2))


def bill_main(client: BillingClient):
    """
    Tencent Cloud billing main entry
    :param client: billing client
    """
    time_tmpl = "2022-09"
    begin_time, end_time = time_tmpl, time_tmpl

    begin_time_detail = "2022-09-01 00:00:00"
    end_time_detail = "2022-09-30 23:59:59"

    # bill summary by product
    # print(describe_bill_summary_by_product(client, begin_time, end_time).to_json_string(indent=2))

    # bill summary by project
    # print(describe_bill_summary_by_project(client, begin_time, end_time).to_json_string(indent=2))

    # bill summary by region
    # print(describe_bill_summary_by_region(client, begin_time, end_time).to_json_string(indent=2))

    # bill detail
    # print(describe_bill_detail(client, time_tmpl, begin_time_detail,
    #                            end_time_detail).to_json_string(indent=2))

    # cost summary by product
    # print(describe_cost_summary_by_product(client, begin_time, end_time).to_json_string(indent=2))

    # cost summary by project
    # print(describe_cost_summary_by_project(client, begin_time, end_time).to_json_string(indent=2))

    # cost summary by region
    # print(describe_cost_summary_by_region(client, begin_time, end_time).to_json_string(indent=2))

    # cost detail
    print(describe_cost_detail(client, time_tmpl, begin_time_detail,
                               end_time_detail).to_json_string(indent=2))

    # account balance
    # print(describe_account_balance(client).to_json_string(indent=2))


def cos_main(client: CosS3Client):
    """
    Tencent Cloud cos main entry
    :param client: cos client
    """
    # Describe buckets list
    # pprint(list_buckets(client))

    # _bucket_name = "api-okr-tool-1312894547"
    _bucket_name = "sales-services-1312894547"
    # Retrieves whether the bucket exists and has access permission
    # head_bucket(client, _bucket_name)

    # Check whether the bucket exists
    # print(bucket_exists(client, _bucket_name))

    # Describe bucket cores config
    # print(bucket_cors(client, _bucket_name))

    # Describe bucket lifecycle
    # pprint(bucket_lifecycle(client, _bucket_name))

    # Describe all objects
    # list_objects(client, _bucket_name)

    # Paging enumeration object with specified bucket
    print(f"bucket name: {_bucket_name}\n{'-' * 39}")
    list_objects_paging(client, _bucket_name)

    # Paging enumeration object for all buckets
    # It Will output a large amount of data - not recommended
    # for bucket in list_buckets(client)["Buckets"]["Bucket"]:
    #     print(f"bucket name: {bucket['Name']}\n{'-' * 39}")
    #     list_objects_paging(client, bucket["Name"])
    #     print(f"{'-' * 39}\n")

    # Paging enumeration object with prefix
    # list_objects_paging(client, _bucket_name, obj_prefix="TB-project/")

    # Get bucket object access url
    # _object_key_name = "TB-project/axios.js"
    # _obj_resp = get_object_url(_cos_client, _bucket_name, _object_key_name)
    # print(f"Object URL: {_obj_resp[0]}\nStatus code: {_obj_resp[1]}")

    # Get bucket permission policy
    # print(get_bucket_policy(_cos_client, _bucket_name))

    # Get bucket ACL
    # print(f"{_bucket_name} ACL:\n", json.dumps(get_bucket_acl(_cos_client, _bucket_name), indent=2))

    # Get bucket logging
    # print(f"{_bucket_name} logging:\n", json.dumps(get_bucket_logging(
    #     _cos_client, _bucket_name), indent=2))


if __name__ == "__main__":
    # Note: Add project parent directory to PYTHONPATH for CLI
    # export PYTHONPATH=$PYTHONPATH:<ProjectParentDirectory>
    _data = from_yaml("resource/tencentcloud_conf.yml")
    _region = _data["tc_res"]["tc_region"]
    _secret_id = _data["tc_res"]["tc_secret_id"]
    _secret_key = _data["tc_res"]["tc_secret_key"]

    # 1. dts endpoint
    _dts_hp_endpoint = _data["tc_res"]["tc_dts_endpoint"]

    # 2. cdb endpoint
    _cdb_hp_endpoint = _data["tc_res"]["tc_cdb_endpoint"]

    # 3. cynosdb endpoint
    _cynosdb_hp_endpoint = _data["tc_res"]["tc_tdsql_c_endpoint"]

    # 4. pg endpoint
    _pg_hp_endpoint = _data["tc_res"]["tc_pg_endpoint"]

    # 5. cvm endpoint
    _cvm_hp_endpoint = _data["tc_res"]["tc_cvm_endpoint"]

    # 6. tke endpoint
    _tke_hp_endpoint = _data["tc_res"]["tc_tke_endpoint"]

    # 7. network endpoint
    _vpc_hp_endpoint = _data["tc_res"]["tc_vpc_endpoint"]

    # 8. bill endpoint
    _bill_hp_endpoint = _data["tc_res"]["tc_billing_endpoint"]

    # 实例化一个认证对象，入参需要传入腾讯云账户secretId，secretKey,此处还需注意密钥对的保密
    # 密钥可前往https://console.cloud.tencent.com/cam/capi网站进行获取
    cred = credential.Credential(_secret_id, _secret_key)

    # 实例化一个http选项，可选的，没有特殊需求可以跳过
    httpProfile = HttpProfile()
    httpProfile.endpoint = _bill_hp_endpoint
    # 实例化一个client选项，可选的，没有特殊需求可以跳过
    clientProfile = ClientProfile()
    clientProfile.httpProfile = httpProfile

    # 实例化要请求产品的client对象,clientProfile是可选的
    # 1. dts client
    _dts_client = dts_client.DtsClient(cred, _region, clientProfile)

    # 2. cdb client
    _cdb_client = cdb_client.CdbClient(cred, _region, clientProfile)

    # 3. cynosdb client
    _cynosdb_client = cynosdb_client.CynosdbClient(cred, _region, clientProfile)

    # 4. pg client
    _pg_client = postgres_client.PostgresClient(cred, _region, clientProfile)

    # 5. cvm client
    _cvm_client = cvm_client.CvmClient(cred, _region, clientProfile)

    # 6. tke client
    _tke_client = tke_client.TkeClient(cred, _region, clientProfile)

    # 7. network client
    _vpc_client = vpc_client.VpcClient(cred, _region, clientProfile)

    # 8. bill client
    _bill_client = billing_client.BillingClient(cred, "", clientProfile)

    # 9. cos client
    _cos_config = CosConfig(Region=_region, SecretId=_secret_id, SecretKey=_secret_key,
                            Token=None, Scheme="https")
    _cos_client = CosS3Client(_cos_config)

    try:
        cos_main(_cos_client)
    except KeyboardInterrupt:
        pass
