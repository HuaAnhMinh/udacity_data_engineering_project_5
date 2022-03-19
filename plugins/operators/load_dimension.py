import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    template_fields = ["mode"]
    # { "mode": "delete-load" } or { "mode": "append-only" }

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 mode="delete-load",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        logging.info(f"Load dimension mode: {self.mode})
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == 'delete-load':
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        redshift.run("INSERT INTO {} ({})".format(self.table, self.sql_query))
