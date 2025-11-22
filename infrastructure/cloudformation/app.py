#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.infrastructure_stack import InfrastructureStack
from stacks.ingestion_stack import IngestionStack
from stacks.consumer_stack import ConsumerStack
from constants import PROJECT_NAME

env = cdk.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"],
)

app = cdk.App()

infra = InfrastructureStack(
    scope=app,
    construct_id=f"{PROJECT_NAME}-infrastructure-stack",
    env=env,
)

ingestion = IngestionStack(
    scope=app,
    construct_id=f"{PROJECT_NAME}-ingestion-stack",
    kinesis_stream=infra.kinesis_stream,
    env=env,
)

consumer = ConsumerStack(
    scope=app,
    construct_id=f"{PROJECT_NAME}-consumer-stack",
    repository=infra.repository,
    cluster=infra.cluster,
    vpc=infra.vpc,
    kinesis_stream=infra.kinesis_stream,
    s3_bucket=infra.s3_data_bucket,
    env=env,
)

app.synth()
