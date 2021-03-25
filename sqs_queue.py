from typing import Generator

import boto3

from aws_resource import AWSResource


class SQSQueue(AWSResource):

    def __init__(self, sqs_queue: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {"arn": "QueueArn"}
        super().__init__("SQSQueue", sqs_queue, should_dump_json)


def retrieve_sqs_queues() -> Generator[SQSQueue, None, None]:
    sqs = boto3.client("sqs")
    paginator = sqs.get_paginator("list_queues")
    for response in paginator.paginate(PaginationConfig={"PageSize": 50}):
        for queue_url in response["QueueUrls"]:
            r = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
            yield SQSQueue({"QueueArn": r["Attributes"]["QueueArn"]})
