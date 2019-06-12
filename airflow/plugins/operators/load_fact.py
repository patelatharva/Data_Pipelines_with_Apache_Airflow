from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 sql_stat = "",
                 target_table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stat = sql_stat
        self.target_table = target_table
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)        
        redshift_hook.run(self.sql_stat)
        self.log.info("Done inserting data into fact table: {}".format(self.target_table))
