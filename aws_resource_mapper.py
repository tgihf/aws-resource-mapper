import argparse
import itertools
from typing import Dict

from neo4j import GraphDatabase

from dynamodb_table import retrieve_dynamodb_tables
from glue_catalog import retrieve_glue_catalog
from glue_database import retrieve_glue_databases
from glue_table import retrieve_glue_tables
from iam_role import retrieve_iam_roles
from kms_key import retrieve_kms_keys
from lmbda import retrieve_lambda_functions
from s3_bucket import retrieve_s3_buckets
from sns_topic import retrieve_sns_topics
from sqs_queue import retrieve_sqs_queues


parser = argparse.ArgumentParser(
    description="Map AWS resources in a Neo4j Graph Database"
)
parser.add_argument("--url", metavar="url", type=str, help="URL of Neo4j Database to map AWS resources in", default="bolt://localhost:7687")
parser.add_argument("--user", metavar="user", type=str, help="Neo4j user to map AWS resources as", default="neo4j")
parser.add_argument("--password", metavar="password", type=str, help="Password of Neo4j user to map AWS resources as", default="neo4j")
args = parser.parse_args()

driver = GraphDatabase.driver(
    args.url,
    auth=(args.user, args.password)
)

aws_resource_generator = itertools.chain(
    retrieve_dynamodb_tables(),
    retrieve_glue_catalog(),
    retrieve_glue_databases(),
    retrieve_glue_tables(),
    retrieve_iam_roles(),
    retrieve_kms_keys(),
    retrieve_s3_buckets(),
    retrieve_sns_topics(),
    retrieve_sqs_queues(),
    retrieve_lambda_functions()
)
aws_resources: Dict[str, Dict[str, dict]] = {
    "DynamoDBTable": {},
    "GlueCatalog": {},
    "GlueDatabase": {},
    "GlueTable": {},
    "IAMRole": {},
    "KMSKey": {},
    "Lambda": {},
    "S3Bucket": {},
    "SNSTopic": {},
    "SQSQueue": {},
}
for aws_resource in aws_resource_generator:
    aws_resources[aws_resource.aws_resource_type][aws_resource.arn] = aws_resource

with driver.session() as session:
    for aws_resource_category in aws_resources:
        for resource_arn in aws_resources[aws_resource_category]:
            session.write_transaction(aws_resources[aws_resource_category][resource_arn].create_neo4j_node)

with driver.session() as session:
    for aws_resource_category in aws_resources:
        for resource_arn in aws_resources[aws_resource_category]:
            session.write_transaction(aws_resources[aws_resource_category][resource_arn].create_neo4j_relationships, aws_resources)

driver.close()
