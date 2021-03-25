from typing import Generator, List

import boto3

from aws_resource import AWSResource


class DynamoDBTable(AWSResource):

    def __init__(self, s3_bucket: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {
            "name": "TableName",
            "arn": "TableArn",
        }
        super().__init__("DynamoDBTable", s3_bucket, should_dump_json)


def retrieve_dynamodb_tables() -> Generator[DynamoDBTable, None, None]:
    ddb = boto3.client("dynamodb")
    paginator = ddb.get_paginator("list_tables")
    for response in paginator.paginate(PaginationConfig={"PageSize": 50}):
        table_names: List[str] = response["TableNames"]
        for table_name in table_names:
            table = ddb.describe_table(TableName=table_name)
            yield DynamoDBTable(table["Table"])
