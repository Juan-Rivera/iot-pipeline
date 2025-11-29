from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_ecr_assets as ecr_assets,
    aws_kinesis as kinesis,
    aws_logs as logs,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_applicationautoscaling as autoscaling,
    RemovalPolicy,
    Duration,
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
        checkpoint_db_table: dynamodb.ITable,
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

        ECRDeployment(
            self,
            f"{service_name}-ImageDeployment",
            src=DockerImageName(localDockerImage.image_uri),
            dest=DockerImageName(f"{repository.repository_uri}:consumer"),
        )

        log_group = logs.LogGroup(
            self,
            f"{service_name}-logs",
            log_group_name=f"/aws/ecs/{service_name}",
            removal_policy=RemovalPolicy.DESTROY,
        )

        idempotency_table = dynamodb.Table(
            self,
            f"{PROJECT_NAME}-event-idempotency",
            partition_key=dynamodb.Attribute(
                name="event_key",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expire_at",
            removal_policy=RemovalPolicy.RETAIN,
        )

        task_def = ecs.FargateTaskDefinition(
            self,
            f"{service_name}-task_definition",
            cpu=512,
            memory_limit_mib=1024,
        )

        container = task_def.add_container(
            f"{service_name}-container",
            image=ecs.ContainerImage.from_ecr_repository(repository, tag="consumer"),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="consumer",
                log_group=log_group,
            ),
            environment={
                "KINESIS_STREAM": kinesis_stream.stream_name,
                "S3_BUCKET": s3_bucket.bucket_name,
                "IDEMPOTENCY_TABLE": idempotency_table.table_name,
                "CHECKPOINT_TABLE": checkpoint_db_table.table_name,
                "DEDUP_TTL_DAYS": "30",
            },
        )

        s3_bucket.grant_put(task_def.task_role)
        kinesis_stream.grant_read(task_def.task_role)
        idempotency_table.grant_read_write_data(task_def.task_role)
        checkpoint_db_table.grant_read_write_data(task_def.task_role)

        service = ecs.FargateService(
            self,
            "ConsumerService",
            service_name=service_name,
            cluster=cluster,
            task_definition=task_def,
            desired_count=1,
            min_healthy_percent=0,
            max_healthy_percent=100,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            assign_public_ip=False,
            propagate_tags=ecs.PropagatedTagSource.SERVICE,
        )

        scaling = service.auto_scale_task_count(
            min_capacity=1,
            max_capacity=5,
        )

        scaling.scale_on_metric(
            id=f"{service_name}-scale-based-on-iterator-age",
            metric=kinesis_stream.metric_get_records_iterator_age_milliseconds(
                statistic="Maximum"
            ),
            scaling_steps=[
                autoscaling.ScalingInterval(
                    upper=3000,
                    change=-1,
                ),
                autoscaling.ScalingInterval(
                    lower=7000,
                    change=+1,
                ),
            ],
            adjustment_type=autoscaling.AdjustmentType.CHANGE_IN_CAPACITY,
        )
