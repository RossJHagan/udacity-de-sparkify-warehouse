import configparser
import boto3
import json


def create_iam_role(aws_config, role_name):
    """create a new iam role to allow redshift cluster access to AWS services

    Args:
        aws_config: expects a 'region', 'key', and 'secret' string on a Dict[str,str]
        role_name: string of name for IAM role to be created

    Returns:
        role_arn: string of the new role's resource identifier (ARN)

    """
    iam = boto3.client('iam',
                       region_name=aws_config.get('region'),
                       aws_access_key_id=aws_config.get('key'),
                       aws_secret_access_key=aws_config.get('secret'))

    try:
        print("[INFO] IAM Role: Creating a new IAM Role")
        iam.create_role(
            Path='/',
            RoleName=role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
        print("[INFO] IAM Role: Created new IAM Role")
    except Exception as e:
        print(f"[ERROR] IAM Role: Error creating role ({role_name})")
        print(e)

    print("[INFO] IAM Role: Attaching Policy")

    try:
        attach_result = iam.attach_role_policy(RoleName=role_name,
                                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                               )['ResponseMetadata']['HTTPStatusCode']

        print(f"[INFO] IAM Role: Attached Policy ({attach_result})")
    except Exception as e:
        print("[ERROR] IAM Role: Error attaching policy")
        print(e)

    print("[INFO] IAM Role: Get the IAM role ARN")
    role_arn = ''
    try:
        role_arn = iam.get_role(RoleName=role_name)['Role']['Arn']
        print("[INFO] IAM Role: Retrieved IAM role ARN")
    except Exception as e:
        print("[ERROR] IAM Role: Error retrieving ARN")
        print(e)

    return role_arn


def destroy_iam_role(aws_config, role_name):
    """destroys a given iam role by name

    This will detach the S3 Read Only policy before deletion.

    Args:
        aws_config: expects a 'region', 'key', and 'secret' string on a Dict[str,str]
        role_name: string name of the IAM role to be destroyed
    """
    iam = boto3.client('iam',
                       region_name=aws_config.get('region'),
                       aws_access_key_id=aws_config.get('key'),
                       aws_secret_access_key=aws_config.get('secret'))

    try:
        iam.detach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )

        iam.delete_role(
            RoleName=role_name
        )

        print(f"[INFO] IAM Role destroyed ({role_name})")
    except Exception as e:
        print("[ERROR] IAM Role could not be destroyed")
        print(e)


def create_redshift_cluster(aws_config, dwh_config, role_arn):
    """creates the redshift cluster

    Args:
        aws_config: expects a 'region', 'key', and 'secret' string on a Dict[str,str]
        dwh_config: expects a 'cluster_id', 'cluster_type', 'node_type', 'num_nodes', 'db_name', 'db_user', and 'db_password'
        role_arn: string of the IAM role ARN created for redshift to be able to assume
    """
    redshift = boto3.client('redshift',
                            region_name=aws_config.get('region'),
                            aws_access_key_id=aws_config.get('key'),
                            aws_secret_access_key=aws_config.get('secret')
                            )

    print(f"[INFO] Redshift: Creating cluster ({dwh_config.get('cluster_id')})")
    try:
        redshift.create_cluster(
            # HW
            ClusterType=dwh_config.get('cluster_type'),
            NodeType=dwh_config.get('node_type'),
            NumberOfNodes=int(dwh_config.get('num_nodes')),

            # Identifiers & Credentials
            DBName=dwh_config.get('db_name'),
            ClusterIdentifier=dwh_config.get('cluster_id'),
            MasterUsername=dwh_config.get('db_user'),
            MasterUserPassword=dwh_config.get('db_password'),

            # Roles (for s3 access)
            IamRoles=[role_arn]
        )
        print(f"[INFO] Redshift: Cluster creation in progress ({dwh_config.get('cluster_id')})")
    except Exception as e:
        print("[ERROR] Redshift: Error creating redshift cluster")
        print(e)


def fetch_cluster_props(aws_config, cluster_id):
    """fetches redshift cluster properties by cluster id

    Args:
        aws_config: expects a 'region', 'key', and 'secret' string on a Dict[str,str]
        cluster_id: string identifying the cluster for which to retrieve properties

    Returns:
        boto3 cluster properties, see [documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.describe_clusters)

    """
    redshift = boto3.client('redshift',
                            region_name=aws_config.get('region'),
                            aws_access_key_id=aws_config.get('key'),
                            aws_secret_access_key=aws_config.get('secret')
                            )
    return redshift.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]


def destroy_redshift_cluster(aws_config, cluster_id):
    """destroys the redshift cluster without final snapshot

    Args:
        aws_config: expects a 'region', 'key', and 'secret' string on a Dict[str,str]
        cluster_id: string identifier for the redshift cluster
    """
    redshift = boto3.client('redshift',
                            region_name=aws_config.get('region'),
                            aws_access_key_id=aws_config.get('key'),
                            aws_secret_access_key=aws_config.get('secret')
                            )

    print(f"[INFO] Redshift: Destroying cluster ({cluster_id})")
    try:
        redshift.delete_cluster(ClusterIdentifier=cluster_id,
                                SkipFinalClusterSnapshot=True)
        print(f"[INFO] Redshift: Cluster destruction imminent ({cluster_id})")
    except Exception as e:
        print(f"[ERROR] Redshift: Unable to delete redshift cluster ({cluster_id})")
        print(e)


def create_cluster_ingress(aws_config, vpc_id, port):
    """creates authorisation for ingress into the redshift cluster from public internet

    Args:
        aws_config: expecting a 'region', 'key', and 'secret' on a Dict[str,str]
        vpc_id: string of the ec2 vpc in which to add ingress rule
        port: string|int of the port to be opened for ingress
    """
    ec2 = boto3.resource('ec2',
                         region_name=aws_config.get('region'),
                         aws_access_key_id=aws_config.get('key'),
                         aws_secret_access_key=aws_config.get('secret'))

    print(f"[INFO] Creating cluster ingress rule (vpc: {vpc_id}, port: {port}")
    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        print(default_sg)
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
        print(f"[INFO] Cluster ingress rule created")
    except Exception as e:
        print(f"[ERROR] Cluster ingress rule could not be created")
        print(e)


def bootstrap_config():
    """bootstrap the raw .cfg file into application config

    Returns:
        aws_config, iam_role_config, dwh_config
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    iam_role_config = {
        "role_name": config.get('IAM_ROLE', 'NAME'),
        "ARN": config.get('IAM_ROLE', 'ARN')
    }

    aws_config = {
        "region": config.get('AWS', 'REGION'),
        "key": config.get('AWS', 'KEY'),
        "secret": config.get('AWS', 'SECRET'),
    }

    dwh_config = {
        "cluster_type": config.get('CLUSTER', 'CLUSTER_TYPE'),
        "num_nodes": config.get('CLUSTER', 'NUM_NODES'),
        "node_type": config.get('CLUSTER', 'NODE_TYPE'),
        "cluster_id": config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
        "db_name": config.get('CLUSTER', 'DB_NAME'),
        "db_user": config.get('CLUSTER', 'DB_USER'),
        "db_password": config.get('CLUSTER', 'DB_PASSWORD'),
        "db_port": config.get('CLUSTER', 'DB_PORT'),
    }

    return aws_config, iam_role_config, dwh_config


def main():
    aws_config, iam_role_config, dwh_config = bootstrap_config()

    redshift_role_arn = create_iam_role(aws_config, iam_role_config.get('role_name'))
    print(f"[INFO] Redshift Role ARN: {redshift_role_arn}")

    create_redshift_cluster(aws_config, dwh_config, redshift_role_arn)

    my_cluster_props = fetch_cluster_props(aws_config, dwh_config.get('cluster_id'))
    if my_cluster_props['ClusterStatus'] != 'available':
        print("[WARN] Redshift Cluster not available yet.  Wait and re-run the script to continue.")
        return

    print(f"[INFO] Redshift Endpoint \n{my_cluster_props['Endpoint']['Address']}")
    print(f"[INFO] IAM Role ARN \n{my_cluster_props['IamRoles'][0]['IamRoleArn']}")

    create_cluster_ingress(aws_config, my_cluster_props['VpcId'], dwh_config.get('db_port'))


if __name__ == "__main__":
    main()
