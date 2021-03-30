import os
from pathlib import Path

from aws_cdk import core as cdk
import aws_cdk.aws_amplify as amplify
import aws_cdk.aws_apigatewayv2 as gateway
import aws_cdk.aws_apigatewayv2_integrations as gateway_integrations
import aws_cdk.aws_codecommit as code
import aws_cdk.aws_ecr as ecr
import aws_cdk.aws_ecr_assets as ecr_assets
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_lambda_event_sources as lambda_event_sources
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_logs as logs
import aws_cdk.aws_dynamodb as ddb

from database_stack import DatabaseStack


class AppStack(cdk.Stack):
    def _lambda_function_from_asset(self,
            lambda_asset, function_name, function_cmd, environment,
            *,
            reserved_concurrent_executions=2,
            timeout_minutes=15,
            memory_size=128,
            max_event_age_minutes=60
            ):
        return _lambda.DockerImageFunction(self,
            function_name,
            function_name=function_name,
            environment=environment,
            log_retention=logs.RetentionDays.ONE_WEEK,
            memory_size=memory_size,
            reserved_concurrent_executions=reserved_concurrent_executions,
            timeout=cdk.Duration.minutes(timeout_minutes),
            tracing=_lambda.Tracing.ACTIVE,
            max_event_age=cdk.Duration.minutes(max_event_age_minutes),
            code=_lambda.DockerImageCode.from_ecr(
                lambda_asset.repository,
                cmd=[function_cmd],
                tag=lambda_asset.image_uri.split(':')[1]))

    def __init__(self, scope: cdk.Construct, construct_id: str, db_stack: DatabaseStack, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # Enrichment Queue
        enrichment_queue = sqs.Queue(self,
                "CrawlerEnrichmentQueue",
                queue_name='CrawlerEnrichmentQueue',
                retention_period=cdk.Duration.days(1),
                visibility_timeout=cdk.Duration.minutes(15))

        # Environment
        env_default = {'APP_LOGGING_LEVEL': 'ERROR'}
        env_table = {'APP_OFFERS_TABLE': db_stack.offers_table.table_name}
        env_queue_url = {'APP_OFFERS_QUEUE_URL': enrichment_queue.queue_url}


        # Base Lambda ECR image asset
        lambda_asset = ecr_assets.DockerImageAsset(self,
                'CrawlerLambdaImage',
                directory=os.path.join(os.getcwd(), 'src', 'crawler'),
                repository_name='crawler')

        # Crawler Lambda
        lambda_crawler = self._lambda_function_from_asset(
                lambda_asset,
                'LambdaCrawler',
                'lambda_handler.crawler',
                {**env_default, **env_table, **env_queue_url})
        rule = events.Rule(self,
                'CrawlerCallingRule',
                rule_name='CrawlerCallingRule',
                schedule=events.Schedule.rate(cdk.Duration.hours(1)))
        rule.add_target(targets.LambdaFunction(lambda_crawler))
        db_stack.offers_table.grant_write_data(lambda_crawler)
        enrichment_queue.grant_send_messages(lambda_crawler)

        # Enrichment Lambda
        lambda_enrichment    = self._lambda_function_from_asset(
                lambda_asset,
                'LambdaEnrichment',
                'lambda_handler.enrichment',
                {**env_default, **env_table})
        lambda_enrichment.add_event_source(
            lambda_event_sources.SqsEventSource(enrichment_queue))
        db_stack.offers_table.grant_write_data(lambda_enrichment)

        lambda_search   = self._lambda_function_from_asset(
                lambda_asset,
                'LambdaSearch',
                'lambda_handler.search',
                {**env_default, **env_table},
                reserved_concurrent_executions=10,
                timeout_minutes=1,
                memory_size=128,
                max_event_age_minutes=1)
        db_stack.offers_table.grant_read_data(lambda_search)

        personal_token = open(os.path.join(str(Path.home()), '.github/personal_token.txt'), 'r').read()

        # Frontend entrypoin
        amplify_app =  amplify.App(self,
                'CrawlerFrontend',
                app_name='CrawlerFrontend',
                auto_branch_creation=amplify.AutoBranchCreation(
                    auto_build=True),
                source_code_provider=amplify.GitHubSourceCodeProvider(
                    owner='jaswdr',
                    repository='aws-cdk-crawler-frontend-example',
                    oauth_token=cdk.SecretValue(personal_token)))

        # Backend entrypoint
        search_entrypoint = gateway.HttpApi(self,
                'CrawlerSearchApiEntrypoint',
                api_name='CrawlerSearchApiEntrypoint',
                cors_preflight=gateway.CorsPreflightOptions(
                    allow_headers=['*'],
                    allow_methods=[gateway.HttpMethod.GET],
                    allow_origins=['*'],
                    max_age=cdk.Duration.hours(2)),
                description='Crawler Search API Entrypoint')
        search_entrypoint.add_routes(
                path='/search',
                methods=[gateway.HttpMethod.GET],
                integration=gateway_integrations.LambdaProxyIntegration(
                        handler=lambda_search,
                        payload_format_version=gateway.PayloadFormatVersion.VERSION_2_0))
        static_data_bucket = s3.Bucket(self,
            'CrawlerStaticDataBucket',
            versioned=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            bucket_name='crawler-static-data')
