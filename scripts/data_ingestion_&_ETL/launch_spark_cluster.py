'''
ETL 专用 EMR 集群启动脚本。
只安装 Hadoop + Spark，不含 JupyterHub/Livy。
Master 节点配置 200 GB 根磁盘，供 Phase 1 下载 7z 压缩档使用。

# 基本用法（使用默认配置：m5.2xlarge × 4 core nodes）：
python launch_spark_cluster.py \
    --pem_path "/Users/yuqiuyi/Desktop/2026 Spring/30123/vockey.pem"

# 自定义配置：
python launch_spark_cluster.py \
    --pem_path "/Users/yuqiuyi/Desktop/2026 Spring/30123/vockey.pem" \
    --core_count 4 \
    --instance_type "m5.2xlarge" \
    --cluster_name "ETL Cluster"
'''

import time
import boto3
import json
import argparse

emr = boto3.client('emr')
ec2 = boto3.client('ec2')


def launch_cluster(name='ETL Cluster', emr_release='emr-7.13.0',
                   instance_type='r8g.xlarge', primary_count=1, core_count=4,
                   ec2_key='vockey', additional_configs=[]):
    '''
    Launches a Spark-only EMR cluster for ETL workloads (spark-submit).
    No JupyterHub or Livy installed.
    All instances get a 100 GB root EBS volume (AWS Academy max) —
    sufficient for Phase 1 to download the ~22 GB Stack Overflow 7z archive.

    Default instance: m5.2xlarge (8 vCPU / 32 GB RAM), suited for
    pandas_udf workloads (BeautifulSoup + radon static analysis).
    '''
    apps = [{'Name': 'Hadoop'}, {'Name': 'Spark'}]
    configs = list(additional_configs)

    response = emr.run_job_flow(
        Name=name,
        ReleaseLabel=emr_release,
        Applications=apps,
        EbsRootVolumeSize=100,
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Primary node',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': instance_type,
                    'InstanceCount': primary_count,
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': core_count,
                }
            ],
            'Ec2KeyName': ec2_key,
            'KeepJobFlowAliveWhenNoSteps': True,
        },
        Configurations=configs,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )
    return response['JobFlowId']


def enable_ssh(cluster_id):
    '''
    Given an EMR Cluster ID, adds SSH inbound rule for all IPs on primary node
    security group.
    '''
    sg_id = None
    while True:
        cluster_info = emr.describe_cluster(ClusterId=cluster_id)['Cluster']
        sg_id = cluster_info.get('Ec2InstanceAttributes', {}) \
                            .get('EmrManagedMasterSecurityGroup')
        if sg_id:
            break
        time.sleep(5)

    try:
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpProtocol='tcp',
            FromPort=22,
            ToPort=22,
            CidrIp='0.0.0.0/0'
        )
        print('SSH access enabled for primary node')
    except ec2.exceptions.ClientError as e:
        if 'InvalidPermission.Duplicate' in str(e):
            print('SSH access already enabled for primary node.')
        else:
            raise e


def cluster_ready(cluster_id, pem_path='vockey.pem'):
    '''
    Waits until cluster is running, then prints the SSH command to access
    the master node for running Phase 1 and spark-submit (Phase 2).
    '''
    print(f'Waiting for cluster {cluster_id} to be ready...')
    waiter = emr.get_waiter('cluster_running')

    try:
        waiter.wait(ClusterId=cluster_id)

        cluster_info = emr.describe_cluster(ClusterId=cluster_id)['Cluster']
        public_dns = cluster_info['MasterPublicDnsName']

        print('Cluster is ready.')
        print('SSH into the primary node:')
        print(f'  ssh -i "{pem_path}" hadoop@{public_dns}')
        print()
        print('Next steps on the master node:')
        print('  1. export BUCKET="your-bucket-name"')
        print('  2. bash stream_posts_to_s3_batches.sh   # Phase 1')
        print('  3. spark-submit build_posts_cqi_batches.py ...  # Phase 2')

    except Exception as e:
        print(f'An error occurred while waiting for cluster to launch: {e}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Launch EMR cluster for Stack Overflow ETL pipeline.'
    )
    parser.add_argument('--pem_path', type=str,
                        default='/Users/yuqiuyi/Desktop/2026 Spring/30123/vockey.pem',
                        help='本地 PEM 文件完整路径 (default: vockey.pem in Desktop/2026 Spring/30123/)')
    parser.add_argument('--primary_count', type=int, default=1,
                        help='Number of primary (master) nodes (default: 1).')
    parser.add_argument('--core_count', type=int, default=4,
                        help='Number of core (worker) nodes (default: 4).')
    parser.add_argument('--instance_type', type=str, default='r8g.xlarge',
                        help='EC2 instance type (default: r8g.xlarge).')
    parser.add_argument('--ec2_key', type=str, default='vockey',
                        help='EC2 key pair name on AWS (default: vockey).')
    parser.add_argument('--cluster_name', type=str, default='ETL Cluster',
                        help='Name for the EMR cluster (default: ETL Cluster).')
    parser.add_argument('--emr_release', type=str, default='emr-7.13.0',
                        help='EMR release label (default: emr-7.13.0).')
    parser.add_argument('--additional_configs', nargs='+', type=str,
                        default=[],
                        help='Additional EMR configurations as JSON strings.')
    args = parser.parse_args()

    cluster_id = launch_cluster(
        name=args.cluster_name,
        emr_release=args.emr_release,
        instance_type=args.instance_type,
        primary_count=args.primary_count,
        core_count=args.core_count,
        ec2_key=args.ec2_key,
        additional_configs=[json.loads(c) for c in args.additional_configs]
    )
    print(f'Cluster {cluster_id} is launching with {args.primary_count} primary '
          f'and {args.core_count} core nodes ({args.instance_type}, emr-{args.emr_release}, 100 GB root disk).')

    enable_ssh(cluster_id)
    cluster_ready(cluster_id, pem_path=args.pem_path)
