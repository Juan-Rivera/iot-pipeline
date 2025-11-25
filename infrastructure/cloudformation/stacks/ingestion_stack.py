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
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_apigatewayv2 as apigw,
    aws_apigatewayv2_integrations as apigw_integrations,
    aws_iam as iam,
    aws_route53_targets as route53_targets,
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

        # ===============================
        # Secrets Rotation Lambda
        # ===============================
        api_key_secret = secretsmanager.Secret(
            self,
            f"{PROJECT_NAME}-ingestion_api-key-secret",
            secret_name=f"{PROJECT_NAME}-ingestion_api-key",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                exclude_punctuation=True,
            ),
        )

        rotation_fn = _lambda.Function(
            self,
            f"{PROJECT_NAME}-ingestion_api-key-rotation-fn",
            function_name=f"{PROJECT_NAME}-ingestion_api-key-rotation-fn",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="index.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/secret_rotation"),
            architecture=_lambda.Architecture.ARM_64,
            timeout=Duration.seconds(30),
        )

        api_key_secret.grant_read(rotation_fn)
        api_key_secret.grant_write(rotation_fn)

        api_key_secret.add_rotation_schedule(
            f"{PROJECT_NAME}-ingestion_api-key-rotation-schedule",
            rotation_lambda=rotation_fn,
            automatically_after=Duration.days(1),
        )

        # ===============================
        # Domain / Certificate
        # ===============================
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

        # ===============================
        # Docker image build + push
        # ===============================
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

        # ===============================
        # ECS Log group
        # ===============================
        log_group = logs.LogGroup(
            self,
            f"{PROJECT_NAME}-ingestion_api-worker_logs",
            log_group_name="/aws/ecs/ingestion_api-worker_logs",
        )

        # ===============================
        # ECS Task Definition
        # ===============================
        task_def = ecs.FargateTaskDefinition(
            self,
            f"{PROJECT_NAME}-ingestion_api-task_definition",
            cpu=256,
            memory_limit_mib=512,
        )

        container = task_def.add_container(
            f"{PROJECT_NAME}-ingestion_api-container",
            image=ecs.ContainerImage.from_docker_image_asset(localDockerImage),
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

        # ===============================
        # ECS Service with ALB
        # ===============================
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

        alb = service.load_balancer

        service.service.auto_scale_task_count(min_capacity=1, max_capacity=5)

        service.target_group.configure_health_check(
            path="/health",
            healthy_http_codes="200-399",
            interval=Duration.seconds(30),
            timeout=Duration.seconds(10),
            healthy_threshold_count=2,
            unhealthy_threshold_count=3,
        )

        # ===============================
        # WAF
        # ===============================
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
            resource_arn=alb.load_balancer_arn,
            web_acl_arn=web_acl.attr_arn,
        )

        # ============================================================
        # STARTUP LAMBDA
        # ============================================================
        startup_lambda = _lambda.Function(
            self,
            f"{PROJECT_NAME}-startup-lambda",
            function_name=f"{PROJECT_NAME}-ingestion_api-startup-lambda-fn",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="index.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/start_backend"),
            timeout=Duration.seconds(10),
            environment={
                "CLUSTER_ARN": cluster.cluster_arn,
                "SERVICE_ARN": service.service.service_arn,
                "API_KEY_SECRET_ARN": api_key_secret.secret_arn,
            },
        )

        startup_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ecs:UpdateService"],
                resources=["*"],
            )
        )

        control_api = apigw.HttpApi(
            self,
            f"{PROJECT_NAME}-control-api",
            api_name=f"{PROJECT_NAME}-control-api",
        )

        control_api.add_routes(
            path="/backend/start",
            methods=[apigw.HttpMethod.POST],
            integration=apigw_integrations.HttpLambdaIntegration(
                "StartBackendIntegration",
                startup_lambda,
            ),
        )
        api_key_secret.grant_read(startup_lambda)

        # ============================================================
        # SHUTDOWN LAMBDA
        # ============================================================

        stop_lambda = _lambda.Function(
            self,
            f"{PROJECT_NAME}-stop-lambda",
            function_name=f"{PROJECT_NAME}-ingestion_api-stop-lambda-fn",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="index.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/stop_backend"),
            timeout=Duration.seconds(10),
            environment={
                "CLUSTER_ARN": cluster.cluster_arn,
                "SERVICE_ARN": service.service.service_arn,
                "API_KEY_SECRET_ARN": api_key_secret.secret_arn,
            },
        )

        stop_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ecs:ListTasks",
                    "ecs:StopTask",
                    "ecs:UpdateService",
                    "ecs:DescribeServices",
                ],
                resources=["*"],
            )
        )

        control_api.add_routes(
            path="/backend/stop",
            methods=[apigw.HttpMethod.POST],
            integration=apigw_integrations.HttpLambdaIntegration(
                "StopBackendIntegration",
                stop_lambda,
            ),
        )
        api_key_secret.grant_read(stop_lambda)

        # ============================================================
        # IDLE REAPER LAMBDA
        # ============================================================
        idle_reaper = _lambda.Function(
            self,
            f"{PROJECT_NAME}-idle-reaper",
            function_name=f"{PROJECT_NAME}-ingestion_api-idle-reaper-fn",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="index.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/ecs_reaper"),
            timeout=Duration.seconds(15),
            environment={
                "CLUSTER_ARN": cluster.cluster_arn,
                "SERVICE_ARN": service.service.service_arn,
                "LB_FULL_NAME": alb.load_balancer_full_name,
                "IDLE_HOURS": "3",
                "API_KEY_SECRET_ARN": api_key_secret.secret_arn,
            },
        )

        idle_reaper.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:GetMetricStatistics"],
                resources=["*"],
            )
        )

        idle_reaper.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ecs:UpdateService", "ecs:DescribeServices"],
                resources=["*"],
            )
        )

        events.Rule(
            self,
            f"{PROJECT_NAME}-ingestion_api-idle-schedule",
            rule_name=f"{PROJECT_NAME}-ingestion_api-idle-schedule-rule",
            schedule=events.Schedule.rate(Duration.minutes(15)),
            targets=[targets.LambdaFunction(idle_reaper)],
        )

        # ============================================================
        # CONTROL API CUSTOM DOMAIN (control.<domain>)
        # ============================================================

        control_subdomain = f"control.{domain_name}"

        control_cert = acm.DnsValidatedCertificate(
            self,
            f"{PROJECT_NAME}-control-api-cert",
            certificate_name=f"{PROJECT_NAME}-ingestion_api-control-api-cert",
            domain_name=control_subdomain,
            hosted_zone=hosted_zone,
            region=self.region,
        )

        control_domain = apigw.DomainName(
            self,
            f"{PROJECT_NAME}-control-api-domain",
            domain_name=control_subdomain,
            certificate=control_cert,
        )

        apigw.ApiMapping(
            self,
            f"{PROJECT_NAME}-control-api-mapping",
            api=control_api,
            domain_name=control_domain,
            stage=control_api.default_stage,
        )

        route53.ARecord(
            self,
            f"{PROJECT_NAME}-control-api-dns",
            zone=hosted_zone,
            record_name=control_subdomain,
            target=route53.RecordTarget.from_alias(
                route53_targets.ApiGatewayv2DomainProperties(
                    control_domain.regional_domain_name,
                    control_domain.regional_hosted_zone_id,
                )
            ),
        )

        # ============================================================
        # Outputs
        # ============================================================
        CfnOutput(
            self,
            f"{PROJECT_NAME}-ingestion_api-ingress_url",
            value=f"https://{subdomain_name}",
        )

        CfnOutput(
            self,
            f"{PROJECT_NAME}-control-api-url",
            value=f"https://{control_subdomain}",
        )
