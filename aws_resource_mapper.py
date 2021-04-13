import argparse
import getpass
import itertools
from typing import Dict
import sys

import boto3
from botocore.exceptions import NoCredentialsError
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError

from resources.dynamodb_table import retrieve_dynamodb_tables
from resources.glue_catalog import retrieve_glue_catalog
from resources.glue_database import retrieve_glue_databases
from resources.glue_table import retrieve_glue_tables
from resources.iam_role import retrieve_iam_roles
from resources.kms_key import retrieve_kms_keys
from resources.lmbda import retrieve_lambda_functions
from resources.s3_bucket import retrieve_s3_buckets
from resources.sns_topic import retrieve_sns_topics
from resources.sqs_queue import retrieve_sqs_queues


parser = argparse.ArgumentParser(
    description="Map AWS resources in a Neo4j Graph Database"
)
parser.add_argument("--url", metavar="url", type=str, help="URL of Neo4j Database to map AWS resources in", default="bolt://localhost:7687")
parser.add_argument("--user", metavar="neo4j_user", type=str, help="Neo4j user to map AWS resources as", default="neo4j")
args = parser.parse_args()
password: str = getpass.getpass(f"[*] Password of Neo4j user {args.neo4j_user} to map AWS resources as: ")

print("[*] Attempting to authenticate to Neo4j...")
try:
    driver = GraphDatabase.driver(
        args.url,
        auth=(args.neo4j_user, password)
    )
except AuthError:
    print(f"[!] Neo4j credentials ({args.neo4j_user}, {password}) are invalid! Exiting...")
    sys.exit(1)
print("[*] Authentication to Neo4j successful!")

sts = boto3.client("sts")
try:
    sts.get_caller_identity()
except NoCredentialsError:
    print("[!] Unable to locate AWS credentials. Exiting...")
    sys.exit(1)

print("[*] Attempting to gather resource information from AWS using boto3 credentials...")
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
print("[*] AWS resource collection complete")

print("[*] Attempting to ingest AWS resources into Neo4j...")
with driver.session() as session:
    for aws_resource_category in aws_resources:
        for resource_arn in aws_resources[aws_resource_category]:
            session.write_transaction(aws_resources[aws_resource_category][resource_arn].create_neo4j_node)

with driver.session() as session:
    for aws_resource_category in aws_resources:
        for resource_arn in aws_resources[aws_resource_category]:
            session.write_transaction(aws_resources[aws_resource_category][resource_arn].create_neo4j_relationships, aws_resources)

driver.close()
print("[*] Neo4j ingestion complete!")
