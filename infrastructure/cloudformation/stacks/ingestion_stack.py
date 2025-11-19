from aws_cdk import (
    Stack,
    aws_ecr as ecr,
    aws_ecr_assets as ecr_assets,
)
from constructs import Construct
from cdk_ecr_deployment import ECRDeployment, DockerImageName

from constants import PROJECT_NAME


class IngestionStack(Stack):

    def __init__(
        self, scope: Construct, construct_id: str, repository: ecr.IRepository, **kwargs
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
