import ujson as json
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.vpc.v20170312 import models
from tencentcloud.vpc.v20170312.vpc_client import VpcClient


def describe_nat_gateways(client: VpcClient):
    """
    Describe Nat Gateway - 查询 NAT 网关
    :param client: vpc client
    """
    try:
        req = models.DescribeNatGatewaysRequest()
        params = {
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeNatGateways(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe Nat gateway exception: {err}")


def describe_subnets(client: VpcClient):
    """
    Describe subnets
    :param client: vpc client
    """
    try:
        req = models.DescribeSubnetsRequest()
        params = {
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeSubnets(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe subnets exception: {err}")


def describe_vpn_gateway(client: VpcClient):
    """
    Describe vpn gateway - 查询VPN网关列表
    :param client:  vpc client
    """
    try:
        req = models.DescribeVpnGatewaysRequest()
        params = {
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeVpnGateways(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe VPC gateway exception: {err}")


def describe_vpn_gateway_route(client: VpcClient, vpn_gateway_id):
    """
    Describe vpn gateway route - 查询路由型VPN网关的目的路由
    :param client: vpc client
    :param vpn_gateway_id:
    :return:
    """
    try:
        req = models.DescribeVpnGatewayRoutesRequest()
        params = {
            "VpnGatewayId": vpn_gateway_id,
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeVpnGatewayRoutes(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe VPN gateway route exception: {err}")


def describe_customer_gateways(client: VpcClient):
    """
    Describe customer gateways - 对端网关厂商信息对象 (查询对端网关列表)
    :param client: vpc client
    """
    try:
        req = models.DescribeCustomerGatewaysRequest()
        params = {
            "Limit": 100
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeCustomerGateways(req)
        return resp

    except TencentCloudSDKException as err:
        raise Exception(f"Describe Nat gateway exception: {err}")


def describe_vpcs(client: VpcClient):
    """
    Describe vpcs - 查询私有网络列表
    :param client: vpc client
    """
    try:
        req = models.DescribeVpcsRequest()
        params = {
            "Limit": "100"
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeVpcs(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe VPCs Exception: {err}")


def describe_vpc_resource_dashboard(client: VpcClient, vpc_ids: list):
    """
    Describe vpc resource dashboard - 查看VPC资源信息
    :param client:  vpc client
    :param vpc_ids: vpc id list
    """
    try:
        req = models.DescribeVpcResourceDashboardRequest()
        params = {
            "VpcIds": vpc_ids
        }
        req.from_json_string(json.dumps(params))
        resp = client.DescribeVpcResourceDashboard(req)
        return resp
    except TencentCloudSDKException as err:
        raise Exception(f"Describe VPC resource exception: {err}")
