from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_ecr_assets as ecr_assets,
    aws_kinesis as kinesis,
    aws_logs as logs,
    aws_s3 as s3,
    aws_iam as iam,
    RemovalPolicy,
)
from constructs import Construct
from cdk_ecr_deployment import ECRDeployment, DockerImageName

from constants import PROJECT_NAME


class ConsumerStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        repository: ecr.IRepository,
        cluster: ecs.ICluster,
        vpc: ec2.IVpc,
        kinesis_stream: kinesis.IStream,
        s3_bucket: s3.IBucket,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        service_name = f"{PROJECT_NAME}-consumer"

        localDockerImage = ecr_assets.DockerImageAsset(
            self,
            f"{service_name}-DockerAsset",
            directory="../../services/consumer",
            file="Dockerfile",
        )
        image_tag = f"consumer-{localDockerImage.asset_hash}"

        ECRDeployment(
            self,
            f"{service_name}-ImageDeployment",
            src=DockerImageName(localDockerImage.image_uri),
            dest=DockerImageName(f"{repository.repository_uri}:{image_tag}"),
        )

        log_group = logs.LogGroup(
            self,
            f"{service_name}-logs",
            log_group_name=f"/aws/ecs/{service_name}",
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.TWO_WEEKS,
        )

        task_def = ecs.FargateTaskDefinition(
            self,
            f"{service_name}-task_definition",
            cpu=512,
            memory_limit_mib=4096,
        )

        task_def.add_container(
            f"{service_name}-container",
            image=ecs.ContainerImage.from_ecr_repository(repository, tag=image_tag),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="consumer",
                log_group=log_group,
            ),
            environment={
                "KINESIS_STREAM": kinesis_stream.stream_name,
                "S3_BUCKET": s3_bucket.bucket_name,
                "APPLICATION_NAME": f"{service_name}-checkpoint",
                "DEDUP_TTL_DAYS": "30",
            },
        )

        s3_bucket.grant_put(task_def.task_role)
        kinesis_stream.grant_read(task_def.task_role)

        task_def.task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        # Additional Kinesis permissions required for KCL not covered by grant_read
        # RegisterStreamConsumer and SubscribeToShard are only needed for Enhanced Fan-Out
        task_def.task_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kinesis:DescribeStreamSummary",
                ],
                resources=[kinesis_stream.stream_arn],
            )
        )

        region = Stack.of(self).region
        account = Stack.of(self).account
        table_arn_prefix = (
            f"arn:aws:dynamodb:{region}:{account}:table/{service_name}-checkpoint"
        )

        task_def.task_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:CreateTable",
                    "dynamodb:DescribeTable",
                    "dynamodb:UpdateTable",
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:Scan",
                    "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:Query",
                ],
                resources=[
                    table_arn_prefix,
                    f"{table_arn_prefix}/index/*",
                    f"{table_arn_prefix}-*",
                ],
            )
        )

        ecs.FargateService(
            self,
            "ConsumerService",
            service_name=service_name,
            cluster=cluster,
            task_definition=task_def,
            desired_count=2,
            min_healthy_percent=0,
            max_healthy_percent=100,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            assign_public_ip=False,
            propagate_tags=ecs.PropagatedTagSource.SERVICE,
        )
