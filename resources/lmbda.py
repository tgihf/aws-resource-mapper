import json
from typing import Dict, List, Generator

import boto3

from resources.aws_resource import AWSResource


class LambdaFunction(AWSResource):

    def __init__(self, lambda_function: dict, should_dump_json: bool = False) -> None:
        self.attr_map = {
            "name": "FunctionName",
            "arn": "FunctionArn",
            "runtime": "Runtime"
        }
        super().__init__("Lambda", lambda_function, should_dump_json)
        self.policies: Dict[dict] = lambda_function["Policies"]
        self.triggers: List[dict] = lambda_function["EventSourceMappings"]

    def __str__(self) -> str:
        d = json.loads(super().__str__())
        d["policies"] = self.policies
        d["triggers"] = self.triggers
        return json.dumps(d, default=str)

    def __repr__(self) -> str:
        return self.__str__()

    def create_neo4j_relationships(self, tx, aws_resources: Dict[str, Dict[str, dict]]) -> None:

        # Connect Lambda to triggers
        for trigger in self.triggers:
            source_arns, resource_type, extra = AWSResource.extract_base_arns(trigger["EventSourceArn"], aws_resources)
            for source_arn in source_arns:
                # if not AWSResource.is_existing_resource(source_arn, resource_type, aws_resources):
                #     print(f"[!] Flagging non-existing resource: {source_arn}, {resource_type}, {extra}")
                AWSResource.create_neo4j_relationship(tx, source_arn, resource_type, "TRIGGERS", self.arn, self.aws_resource_type)

        # Connect Lambda to accessible resources
        for policy_name in self.policies:
            statements = self.policies[policy_name]
            for statement in statements:
                actions = statement["Action"] if isinstance(statement["Action"], list) else [statement["Action"]]
                resources = statement["Resource"] if isinstance(statement["Resource"], list) else [statement["Resource"]]
                for resource in resources:
                    resource_arns, resource_type, extra = AWSResource.extract_base_arns(resource, aws_resources)
                    for resource_arn in resource_arns:
                        for action in actions:
                            # if not AWSResource.is_existing_resource(resource_arn, resource_type, aws_resources):
                                # print(f"[!] Flagging non-existing resource: {resource_arn}, {resource_type}, {extra}")
                            AWSResource.create_neo4j_relationship(tx, self.arn, self.aws_resource_type, action, resource_arn, resource_type, extra)


def retrieve_lambda_functions() -> Generator[LambdaFunction, None, None]:

    lmbda = boto3.client("lambda")
    iam = boto3.client("iam")

    # Paginate through Lambdas
    paginator = lmbda.get_paginator("list_functions")
    for response in paginator.paginate(PaginationConfig={"PageSize": 50}):
        functions = response["Functions"]
        for function in functions:

            # Save function event source mappings
            response = lmbda.list_event_source_mappings(FunctionName=function["FunctionName"])
            function["EventSourceMappings"] = response["EventSourceMappings"]

            # Save function policies
            function["Policies"]: Dict[dict] = {}
            role_name: str = function["Role"].split("/")[-1]

            # Inline role policies
            response = iam.list_role_policies(RoleName=role_name)
            policy_names: List[str] = response["PolicyNames"]
            for policy_name in policy_names:
                response = iam.get_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name
                )
                function["Policies"][policy_name] = response["PolicyDocument"]["Statement"]

            # Managed role policies
            response = iam.list_attached_role_policies(
                RoleName=role_name,
            )
            if "AttachedPolicies" in response:
                for policy in response["AttachedPolicies"]:
                    policy = iam.get_policy(PolicyArn=policy["PolicyArn"])
                    if isinstance(policy, dict) and "Policy" in policy and "Arn" in policy["Policy"] and "DefaultVersionId" in policy["Policy"]:
                        policy_version = iam.get_policy_version(
                            PolicyArn=policy["Policy"]["Arn"],
                            VersionId=policy["Policy"]["DefaultVersionId"]
                        )
                        if isinstance(policy_version, dict) and "PolicyVersion" in policy_version and "Document" in policy_version["PolicyVersion"] and "Statement" in policy_version["PolicyVersion"]["Document"]:
                            function["Policies"][policy["Policy"]["PolicyName"]] = policy_version["PolicyVersion"]["Document"]["Statement"]

            yield LambdaFunction(function)
