#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.infrastructure_stack import InfrastructureStack
from stacks.ingestion_stack import IngestionStack
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
    repository=infra.repository,
    cluster=infra.cluster,
    vpc=infra.vpc,
    env=env,
)

app.synth()
