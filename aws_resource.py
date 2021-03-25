import json
import re
from typing import Dict, List, Tuple


class AWSResource:

    def __init__(self, resource_type: str, resource: dict, should_dump_json: bool = False) -> None:
        self.aws_resource_type: str = resource_type
        self.should_dump_json = should_dump_json
        assert self.attr_map is not None and isinstance(self.attr_map, dict)
        for attr in self.attr_map:
            setattr(self, attr, resource[self.attr_map[attr]])
        self.filename = f"aws-{resource_type}.json"

    def __del__(self) -> None:
        if self.should_dump_json:
            self.dump_json()

    def create_neo4j_node(self, tx) -> None:
        tx.run(
            f"CREATE (n:{self.aws_resource_type}) " + " ".join([f"SET n.{attr} = ${attr} " for attr in self.attr_map]) + "RETURN n",
            **{attr: getattr(self, attr) for attr in self.attr_map}
        )

    def create_neo4j_relationships(self, tx, aws_resources: Dict[str, Dict[str, dict]]) -> None:
        pass

    def create_neo4j_relationship(tx, source_arn: str, source_resource_type: str, relationship: str, dst_arn: str, dst_resource_type: str, extra: str = None) -> None:
        print(f"[*] source resource: {source_arn}")
        print(f"[*] source_resource_type: {source_resource_type}")
        print(f"[*] relationship: {relationship}")
        print(f"[*] dst resource: {dst_arn}")
        print(f"[*] dst_resource_type: {dst_resource_type}")
        source_resource_type: str = source_resource_type.replace("-", "_")
        relationship: str = relationship.replace(":", "_").replace("-", "_").replace("*", "_WILDCARD_").upper()
        dst_resource_type: str = dst_resource_type.replace("-", "_")
        tx.run(
            f"MERGE (src:{source_resource_type} {{arn: '{source_arn}'}}) "
            "RETURN src.arn"
        )
        tx.run(
            f"MERGE (dst:{dst_resource_type} {{arn: '{dst_arn}'}}) "
            "RETURN dst.arn"
        )
        tx.run(
            f"MATCH (src:{source_resource_type}),(dst:{dst_resource_type}) "
            "WHERE src.arn = $source_arn AND dst.arn = $dst_arn "
            f"MERGE (src)-[r:{relationship} {{extra: '{extra if extra is not None else ''}'}}]->(dst) "
            "RETURN src.arn, type(r), dst.arn",
            source_arn=source_arn,
            dst_arn=dst_arn
        )

    def expand_arn(arn: str, resources: Dict[str, dict]) -> List[str]:

        # If the ARN doesn't contain any wildcards, return it as is
        if "*" not in arn:
            return [arn]

        # Return the ARN with wildcards, along with all existing ARNs in the resource category that match the pattern
        regex: re.Pattern = re.compile(arn.replace("/", "\\/").replace("*", ".*"))
        return [arn] + [resource_arn for resource_arn in resources if re.match(regex, resource_arn)]

    def extract_base_arns(arn: str, aws_resources: Dict[str, Dict[str, dict]]) -> Tuple[List[str], str, str]:
        if arn == "*":
            return ["*"], "WILDCARD_AWS_RESOURCE", None

        elements: List[str] = arn.split(":")

        if elements[2] == "lambda":
            return AWSResource.expand_arn(arn, aws_resources["Lambda"]), "Lambda", ""

        if elements[2] == "s3":
            elements: List[str] = arn.split("/")
            base_arn: str = elements[0].replace("*", "")
            extra: str = "/" + "/".join(elements[1:])
            return [base_arn], "S3Bucket", extra

        if elements[2] == "dynamodb" and "/stream" in arn:
            elements: List[str] = arn.split("/stream")
            base_arn: str = elements[0]
            extra: str = "/stream" + elements[1]
            return [base_arn], "DynamoDBTable", extra

        if elements[2] == "dynamodb":
            return AWSResource.expand_arn(arn, aws_resources["DynamoDBTable"]), "DynamoDBTable", ""

        if elements[2] == "glue":
            elements: List[str] = arn.split(":")
            if elements[5][:5] == "table":
                regex: re.Pattern = re.compile(arn.replace("/", "\\/").replace("*", ".*"))
                return [arn] + [glue_resource_arn for glue_resource_arn in aws_resources["GlueTable"] if re.match(regex, glue_resource_arn)], "GlueTable", None

            if elements[5][:8] == "database":
                regex: re.Pattern = re.compile(arn.replace("/", "\\/").replace("*", ".*"))
                return [arn] + [glue_resource_arn for glue_resource_arn in aws_resources["GlueDatabase"] if re.match(regex, glue_resource_arn)], "GlueDatabase", None

            return [arn], "GlueCatalog", None

        if elements[2] == "sqs":
            return AWSResource.expand_arn(arn, aws_resources["SQSQueue"]), "SQSQueue", None

        if elements[2] == "sns":
            return AWSResource.expand_arn(arn, aws_resources["SNSTopic"]), "SNSTopic", None

        if elements[2] == "iam":
            return AWSResource.expand_arn(arn, aws_resources["IAMRole"]), "IAMRole", None

        if elements[2] == "kms":
            return AWSResource.expand_arn(arn, aws_resources["KMSKey"]), "KMSKey", None

        return [arn], "UNKNOWN_AWS_RESOURCE", None

    def is_existing_resource(
        resource_arn: str,
        resource_type: str,
        aws_resources: Dict[str, Dict[str, dict]]
    ) -> bool:
        if resource_arn == "*":
            return True
        if resource_type in aws_resources:
            return resource_arn in aws_resources[resource_type]
        return False

    def dump_json(self) -> None:
        with open(self.filename, "a") as f:
            f.write(self.__str__() + "\n")

    def __str__(self) -> str:
        return json.dumps(
            {attr: getattr(self, attr) for attr in self.attr_map},
            default=str
        )

    def __repr__(self) -> str:
        return self.__str__()
