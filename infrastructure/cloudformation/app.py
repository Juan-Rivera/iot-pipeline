#!/usr/bin/env python3
# import os

import aws_cdk as cdk
from stacks.network_stack import NetworkStack
from constants import PROJECT_NAME


app = cdk.App()

NetworkStack(app, f"{PROJECT_NAME}-network-stack")

app.synth()
