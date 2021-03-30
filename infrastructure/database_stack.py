import os

from aws_cdk import core as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_dynamodb as ddb


class DatabaseStack(cdk.Stack):
    def _create_ddb_table(self, table_name):
        return ddb.Table(self,
            table_name,
            table_name=table_name,
            partition_key=ddb.Attribute(
                name="PK",
                type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(
                name="SK",
                type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            time_to_live_attribute="TTL",
            stream=ddb.StreamViewType.NEW_AND_OLD_IMAGES)

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.offers_table = self._create_ddb_table('Offers')
