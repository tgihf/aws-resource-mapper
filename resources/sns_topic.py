from typing import Dict, Generator

import boto3

from resources.aws_resource import AWSResource


class SNSTopic(AWSResource):

    def __init__(self, sns_topic: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {"arn": "TopicArn"}
        super().__init__("SNSTopic", sns_topic, should_dump_json)
        self.subscriptions = sns_topic["Subscriptions"]

    def create_neo4j_relationships(self, tx, aws_resources: Dict[str, Dict[str, dict]]) -> None:
        for subscription in self.subscriptions:
            resource_arns, resource_type, extra = AWSResource.extract_base_arns(subscription["Endpoint"], aws_resources)
            for resource_arn in resource_arns:
                if not AWSResource.is_existing_resource(resource_arn, resource_type, aws_resources):
                    print(f"[!] Flagging non-existing resource: {resource_arn}, {resource_type}, {extra}")
                AWSResource.create_neo4j_relationship(tx, self.arn, self.aws_resource_type, "sns:notifies", resource_arn, resource_type, extra)


def retrieve_sns_topics() -> Generator[SNSTopic, None, None]:

    sns_topics = {}
    sns = boto3.client("sns")

    # Retrieve topics
    topics_response = sns.list_topics()
    for topic in topics_response["Topics"]:
        sns_topics[topic["TopicArn"]] = {
            "TopicArn": topic["TopicArn"],
            "Subscriptions": []
        }

    # Attach subscriptions to topics
    subscriptions_response = sns.list_subscriptions()
    for subscription in subscriptions_response["Subscriptions"]:
        try:
            sns_topics[subscription["TopicArn"]]["Subscriptions"].append(subscription)
        except KeyError:
            pass  # pass any errors due to dangling subscriptions

    for topic_arn in sns_topics:
        yield SNSTopic(sns_topics[topic_arn])
