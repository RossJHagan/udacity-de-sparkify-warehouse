from initialise_infrastructure import bootstrap_config, destroy_iam_role, destroy_redshift_cluster

def main():
    aws_config, iam_role_config, dwh_config = bootstrap_config()
    destroy_iam_role(aws_config, iam_role_config.get('role_name'))
    destroy_redshift_cluster(aws_config, dwh_config.get('cluster_id'))

if __name__ == "__main__":
    main()
