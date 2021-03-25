from typing import Generator

import boto3

from aws_resource import AWSResource


class GlueDatabase(AWSResource):

    def __init__(self, glue_database: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {
            "name": "Name",
            "arn": "GlueDatabaseArn",
            "description": "Description",
        }
        super().__init__("GlueDatabase", glue_database, should_dump_json)


def retrieve_glue_databases() -> Generator[GlueDatabase, None, None]:

    region: str = boto3.session.Session().region_name
    account_id: str = boto3.client("sts").get_caller_identity().get("Account")

    glue = boto3.client("glue")
    paginator_dbs = glue.get_paginator("get_databases")
    for response_dbs in paginator_dbs.paginate(PaginationConfig={"PageSize": 50}):
        for database in response_dbs["DatabaseList"]:
            arn_db: str = f"arn:aws:glue:{region}:{account_id}:database/{database['Name']}"
            database["GlueDatabaseArn"] = arn_db
            yield GlueDatabase(database)
