import json
from typing import Generator, List

import botocore
import boto3

from resources.aws_resource import AWSResource


class S3Bucket(AWSResource):

    def __init__(self, s3_bucket: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {
            "name": "BucketName",
            "arn": "BucketArn",
        }
        super().__init__("S3Bucket", s3_bucket, should_dump_json)
        self.policies: List[dict] = s3_bucket["Policies"]


def retrieve_s3_buckets() -> Generator[S3Bucket, None, None]:
    s3 = boto3.client("s3")
    for b in s3.list_buckets()["Buckets"]:
        bucket = {"BucketName": b["Name"]}
        try:
            response = s3.get_bucket_policy(Bucket=b["Name"])
        except botocore.exceptions.ClientError:
            bucket["Policies"] = []
        else:
            bucket["Policies"] = json.loads(response["Policy"])["Statement"]

        bucket["BucketArn"] = f"arn:aws:s3:::{b['Name']}"
        yield S3Bucket(bucket)
