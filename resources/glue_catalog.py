from typing import Generator

import boto3

from resources.aws_resource import AWSResource


class GlueCatalog(AWSResource):

    def __init__(self, glue_catalog: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {"arn": "GlueCatalogArn"}
        super().__init__("GlueCatalog", glue_catalog, should_dump_json)


def retrieve_glue_catalog() -> Generator[GlueCatalog, None, None]:

    region: str = boto3.session.Session().region_name
    account_id: str = boto3.client("sts").get_caller_identity().get("Account")
    yield GlueCatalog({
        "GlueCatalogArn": f"arn:aws:glue:{region}:{account_id}:catalog"
    })
