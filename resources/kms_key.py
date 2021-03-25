from typing import Generator

import boto3

from resources.aws_resource import AWSResource


class KMSKey(AWSResource):

    def __init__(self, kms_key: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {"arn": "KeyArn"}
        super().__init__("KMSKey", kms_key, should_dump_json)


def retrieve_kms_keys() -> Generator[KMSKey, None, None]:
    kms = boto3.client("kms")
    paginator = kms.get_paginator("list_keys")
    for response in paginator.paginate(PaginationConfig={"PageSize": 50}):
        for key in response["Keys"]:
            yield KMSKey(key)
