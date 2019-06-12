from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 should_truncate = True,
                 sql_stat = "",
                 target_table = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.should_truncate = should_truncate
        self.sql_stat = sql_stat
        self.target_table = target_table
    def execute(self, context):
        self.log.info("LoadDimensionOperator executing")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.should_truncate:
            redshift_hook.run( 
                SqlQueries.truncate_table.format(self.target_table)
            )
        redshift_hook.run(self.sql_stat)
        self.log.info("Done inserting data into dimension table: {}".format(self.target_table))