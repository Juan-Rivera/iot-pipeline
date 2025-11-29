import os
from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_lambda as _lambda,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_certificatemanager as acm,
    aws_route53 as route53,
    aws_route53_targets as targets,
    aws_logs as logs,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct
from constants import PROJECT_NAME


class IngestionStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        kinesis_stream: kinesis.IStream,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)
        service_name = f"{PROJECT_NAME}-ingestion_api"

        api_key_secret = secretsmanager.Secret(
            self,
            f"{service_name}-key-secret",
            secret_name=f"{service_name}-key",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                exclude_punctuation=True,
            ),
        )

        rotation_fn = _lambda.Function(
            self,
            f"{service_name}-key-rotation-fn",
            function_name=f"{service_name}-key-rotation-fn",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="index.lambda_handler",
            code=_lambda.Code.from_asset("lambdas/secret_rotation"),
            architecture=_lambda.Architecture.ARM_64,
            timeout=Duration.seconds(30),
        )

        api_key_secret.grant_read(rotation_fn)
        api_key_secret.grant_write(rotation_fn)

        api_key_secret.add_rotation_schedule(
            f"{service_name}-key-rotation-schedule",
            rotation_lambda=rotation_fn,
            automatically_after=Duration.days(1),
        )

        domain_name = os.environ["DOMAIN_NAME"]
        subdomain = f"api.{domain_name}"

        zone = route53.HostedZone.from_lookup(self, "Zone", domain_name=domain_name)

        cert = acm.DnsValidatedCertificate(
            self,
            "IngestionCert",
            domain_name=subdomain,
            hosted_zone=zone,
            region=self.region,
        )

        ingestion_lambda = _lambda.Function(
            self,
            "IngestionLambda",
            function_name=f"{service_name}-fn",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="app.lambda_handler",
            code=_lambda.Code.from_asset("../../services/ingestion_lambda"),
            timeout=Duration.seconds(10),
            environment={
                "KINESIS_STREAM": kinesis_stream.stream_name,
                "API_KEY_SECRET_ARN": api_key_secret.secret_arn,
            },
        )

        kinesis_stream.grant_write(ingestion_lambda)
        api_key_secret.grant_read(ingestion_lambda)

        ingestion_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        http_api = apigwv2.HttpApi(
            self,
            "IngestionHttpApi",
            api_name=service_name,
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_headers=["*"],
                allow_methods=[apigwv2.CorsHttpMethod.POST],
                allow_origins=["*"],
            ),
        )

        integration = integrations.HttpLambdaIntegration(
            "EventsIntegration", ingestion_lambda
        )

        http_api.add_routes(
            path="/events",
            methods=[apigwv2.HttpMethod.POST],
            integration=integration,
        )

        log_group = logs.LogGroup(
            self,
            "IngestionHttpApiLogs",
            log_group_name=f"/aws/lambda/{service_name}-lambda-fn",
        )

        http_api.default_stage.node.default_child.access_log_settings = apigwv2.CfnStage.AccessLogSettingsProperty(
            destination_arn=log_group.log_group_arn,
            format='{"requestId":"$context.requestId","httpMethod":"$context.httpMethod","path":"$context.path","status":"$context.status"}',
        )

        domain = apigwv2.DomainName(
            self,
            "CustomDomain",
            domain_name=subdomain,
            certificate=cert,
        )

        apigwv2.ApiMapping(
            self,
            "ApiMapping",
            api=http_api,
            domain_name=domain,
            stage=http_api.default_stage,
        )

        route53.ARecord(
            self,
            "ApiDNS",
            zone=zone,
            record_name=subdomain,
            target=route53.RecordTarget.from_alias(
                targets.ApiGatewayv2DomainProperties(
                    domain.regional_domain_name, domain.regional_hosted_zone_id
                )
            ),
        )

        CfnOutput(
            self,
            "IngestionApiUrl",
            value=f"https://{subdomain}/events",
        )
