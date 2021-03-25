from typing import Generator

import boto3

from resources.aws_resource import AWSResource


class GlueTable(AWSResource):

    def __init__(self, glue_table: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {
            "name": "Name",
            "arn": "GlueTableArn",
        }
        super().__init__("GlueTable", glue_table, should_dump_json)


def retrieve_glue_tables() -> Generator[GlueTable, None, None]:

    region: str = boto3.session.Session().region_name
    account_id: str = boto3.client("sts").get_caller_identity().get("Account")

    glue = boto3.client("glue")
    paginator_dbs = glue.get_paginator("get_databases")
    for response_dbs in paginator_dbs.paginate(PaginationConfig={"PageSize": 50}):
        for database in response_dbs["DatabaseList"]:
            paginator_tables = glue.get_paginator("get_tables")
            for response_tables in paginator_tables.paginate(
                DatabaseName=database["Name"],
                PaginationConfig={"PageSize": 50},
            ):
                for table in response_tables["TableList"]:
                    table["GlueTableArn"] = f"arn:aws:glue:{region}:{account_id}:table/{database['Name']}/{table['Name']}"
                    yield GlueTable(table)
