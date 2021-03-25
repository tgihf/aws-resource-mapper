from typing import Generator

import boto3

from resources.aws_resource import AWSResource


class IAMRole(AWSResource):

    def __init__(self, iam_role: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {
            "arn": "Arn",
            "name": "RoleName"
        }
        super().__init__("IAMRole", iam_role, should_dump_json)


def retrieve_iam_roles() -> Generator[IAMRole, None, None]:
    iam = boto3.client("iam")
    paginator = iam.get_paginator("list_roles")
    for response in paginator.paginate(PaginationConfig={"PageSize": 50}):
        for role in response["Roles"]:
            yield IAMRole(role)
