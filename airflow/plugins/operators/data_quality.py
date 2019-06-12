from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 sql_stats_tests = [],
                 expected_result = 0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stats_tests = sql_stats_tests
        self.expected_result = expected_result
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for (sql_stat, expected_result) in self.sql_stats_tests:
            row = redshift_hook.first(sql_stat)
            if row is not None:
                if row.result == expected_result:
                    self.log.info("Passed Test: {}\nResult == {}\n===================================".format(sql_stat, expected_result))
                else:
                    raise ValueError("Failed Test: {}\nResult != {}\n===================================".format(sql_stat, expected_result))