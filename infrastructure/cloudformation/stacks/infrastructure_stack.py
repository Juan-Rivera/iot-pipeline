from aws_cdk import (
    Duration,
    Stack,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_kinesis as kinesis,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
)
from constructs import Construct
from constants import PROJECT_NAME


class InfrastructureStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.vpc = ec2.Vpc(
            self,
            f"{PROJECT_NAME}-vpc",
            vpc_name=PROJECT_NAME,
            max_azs=2,
            nat_gateways=1,
        )
        self.cluster = ecs.Cluster(
            self,
            f"{PROJECT_NAME}-cluster",
            cluster_name=PROJECT_NAME,
            vpc=self.vpc,
            container_insights_v2=ecs.ContainerInsights.ENHANCED,
        )
        self.repository = ecr.Repository(
            self,
            f"{PROJECT_NAME}-repository",
            repository_name=PROJECT_NAME,
            lifecycle_rules=[
                ecr.LifecycleRule(
                    description="Keep only last 10 images",
                    max_image_count=10,
                    rule_priority=1,
                )
            ],
        )
        self.kinesis_stream = kinesis.Stream(
            self,
            f"{PROJECT_NAME}-event-stream",
            stream_name=f"{PROJECT_NAME}-event-stream",
            shard_count=1,
            retention_period=Duration.hours(24),
        )
        self.checkpoint_db_table = dynamodb.Table(
            self,
            f"{PROJECT_NAME}-kinesis-checkpoints",
            partition_key=dynamodb.Attribute(
                name="shard_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.s3_data_bucket = s3.Bucket(
            self,
            f"{PROJECT_NAME}-data-bucket",
            bucket_name=f"{PROJECT_NAME}-data-bucket-7f2a4b".lower(),
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False,
        )
        self.s3_data_bucket.add_lifecycle_rule(
            id="expire-old-parquet",
            prefix="raw/",
            expiration=Duration.days(90),
        )
