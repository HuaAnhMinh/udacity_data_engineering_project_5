from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries
import logging


class StageToRedshiftOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(
            aws_conn_id=self.aws_credentials_id,
        )
        logging.info("Getting AWS credentials")
        credentials = aws_hook.get_credentials()
        logging.info(f"Credentials: {credentials}")

        logging.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        logging.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))

        logging.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        logging.info(f"S3_path: {s3_path}")
        formatted_sql = SqlQueries.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
        )
        redshift.run(formatted_sql)
