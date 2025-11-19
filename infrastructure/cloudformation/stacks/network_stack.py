from aws_cdk import Stack, aws_ec2 as ec2, aws_ecs as ecs
from constructs import Construct
from constants import PROJECT_NAME


class NetworkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.vpc = ec2.Vpc(
            self,
            f"{PROJECT_NAME}-vpc",
            vpc_name=PROJECT_NAME,
            max_azs=2,
            nat_gateways=1,
        )
        self.ecs = ecs.Cluster(
            self,
            f"{PROJECT_NAME}-cluster",
            cluster_name=PROJECT_NAME,
            vpc=self.vpc,
            container_insights_v2=ecs.ContainerInsights.ENHANCED,
        )
