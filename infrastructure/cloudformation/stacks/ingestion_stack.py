from aws_cdk import (
    CfnOutput,
    Stack,
    aws_ecr as ecr,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_ecs_patterns as ecs_patterns,
    aws_logs as logs,
)
from constructs import Construct
from cdk_ecr_deployment import ECRDeployment, DockerImageName

from constants import PROJECT_NAME


class IngestionStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        repository: ecr.IRepository,
        cluster: ecs.ICluster,
        vpc: ec2.IVpc,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        localDockerImage = ecr_assets.DockerImageAsset(
            self,
            f"{PROJECT_NAME}-ingestion_api-DockerAsset",
            directory="../../services/ingestion_api",
            file="Dockerfile",
        )
        ECRDeployment(
            self,
            f"{PROJECT_NAME}-ingestion-api-ImageDeployment",
            src=DockerImageName(localDockerImage.image_uri),
            dest=DockerImageName(f"{repository.repository_uri}:ingestion-api"),
        )
        docker_image = ecs.ContainerImage.from_ecr_repository(
            repository=repository,
            tag="ingestion-api",
        )
        log_group = logs.LogGroup(
            self,
            f"{PROJECT_NAME}-ingestion_api-worker_logs",
            log_group_name="ingestion_api-worker_logs",
        )
        task_def = ecs.FargateTaskDefinition(
            self,
            f"{PROJECT_NAME}-ingestion_api-task_definition",
            cpu=256,
            memory_limit_mib=512,
        )
        container = task_def.add_container(
            f"{PROJECT_NAME}-ingestion_api-container",
            image=docker_image,
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix=PROJECT_NAME,
                log_group=log_group,
            ),
        )
        container.add_port_mappings(ecs.PortMapping(container_port=8000))
        service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            f"{PROJECT_NAME}-ingestion_api-service",
            service_name="ingestion_api-service",
            cluster=cluster,
            task_definition=task_def,
            desired_count=1,
            min_healthy_percent=100,
        )
        service.service.auto_scale_task_count(min_capacity=1, max_capacity=5)
        service.target_group.configure_health_check(
            path="/health",
            healthy_http_codes="200-399",
        )
        CfnOutput(
            self,
            f"{PROJECT_NAME}-ingestion_api-ingress_url",
            value=f"http://{service.load_balancer.load_balancer_dns_name}",
        )
