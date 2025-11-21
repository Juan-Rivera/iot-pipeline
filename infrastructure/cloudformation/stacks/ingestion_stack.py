import os

from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
    aws_ecr as ecr,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_ecs_patterns as ecs_patterns,
    aws_logs as logs,
    aws_route53 as route53,
    aws_certificatemanager as acm,
    aws_wafv2 as wafv2,
    aws_secretsmanager as secretsmanager,
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
        # Will be automatically rotated!!
        api_key_secret = secretsmanager.Secret(
            self,
            f"{PROJECT_NAME}-ingestion_api-key-secret",
            secret_name=f"{PROJECT_NAME}-ingestion_api-key",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                exclude_punctuation=True,
            ),
        )
        domain_name = os.environ["DOMAIN_NAME"]
        subdomain_name = f"api.{domain_name}"
        hosted_zone = route53.HostedZone.from_lookup(
            scope=self,
            id="HostedZone",
            domain_name=domain_name,
        )
        certificate = acm.DnsValidatedCertificate(
            self,
            f"{PROJECT_NAME}-ingestion-api-cert",
            domain_name=subdomain_name,
            hosted_zone=hosted_zone,
            region=self.region,
        )
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
            environment={
                "API_KEY_SECRET_ARN": api_key_secret.secret_arn,
            },
        )
        container.add_port_mappings(ecs.PortMapping(container_port=8000))
        api_key_secret.grant_read(task_def.task_role)
        service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            f"{PROJECT_NAME}-ingestion_api-service",
            service_name="ingestion_api-service",
            cluster=cluster,
            task_definition=task_def,
            desired_count=1,
            min_healthy_percent=50,
            max_healthy_percent=200,
            domain_name=subdomain_name,
            domain_zone=hosted_zone,
            certificate=certificate,
            redirect_http=True,
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
        )

        service.service.auto_scale_task_count(min_capacity=1, max_capacity=5)

        service.target_group.configure_health_check(
            path="/health",
            healthy_http_codes="200-399",
            interval=Duration.seconds(30),
            timeout=Duration.seconds(10),
            healthy_threshold_count=2,
            unhealthy_threshold_count=3,
        )

        web_acl = wafv2.CfnWebACL(
            self,
            f"{PROJECT_NAME}-web-acl",
            name=f"{PROJECT_NAME}-web-acl",
            default_action=wafv2.CfnWebACL.DefaultActionProperty(allow={}),
            scope="REGIONAL",
            visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                cloud_watch_metrics_enabled=True,
                metric_name=f"{PROJECT_NAME}-web-acl-metric",
                sampled_requests_enabled=True,
            ),
            rules=[
                wafv2.CfnWebACL.RuleProperty(
                    name="AWS-AWSManagedRulesCommonRuleSet",
                    priority=1,
                    override_action=wafv2.CfnWebACL.OverrideActionProperty(none={}),
                    statement=wafv2.CfnWebACL.StatementProperty(
                        managed_rule_group_statement=wafv2.CfnWebACL.ManagedRuleGroupStatementProperty(
                            vendor_name="AWS",
                            name="AWSManagedRulesCommonRuleSet",
                        )
                    ),
                    visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                        cloud_watch_metrics_enabled=True,
                        metric_name="AWSManagedRulesCommonRuleSet-metric",
                        sampled_requests_enabled=True,
                    ),
                )
            ],
        )
        wafv2.CfnWebACLAssociation(
            self,
            f"{PROJECT_NAME}-web-acl-association",
            resource_arn=service.load_balancer.load_balancer_arn,
            web_acl_arn=web_acl.attr_arn,
        )
        CfnOutput(
            self,
            f"{PROJECT_NAME}-ingestion_api-ingress_url",
            value=f"https://{subdomain_name}",
        )
