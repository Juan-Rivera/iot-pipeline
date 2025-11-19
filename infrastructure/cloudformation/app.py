#!/usr/bin/env python3
# import os

import aws_cdk as cdk
from stacks.network_stack import NetworkStack


app = cdk.App()

NetworkStack(app, "iot-pipeline-network-stack")

app.synth()
