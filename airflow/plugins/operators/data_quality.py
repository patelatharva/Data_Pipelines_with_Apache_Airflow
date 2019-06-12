from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
    This operator is able to perform quality checks of the data in tables.
    It accepts list of pairs of sql statement and expected value as arguments.
    For each SQL statement, it executes it on Redshift and 
    compares the retrieved result with expected value.
    In case of mismatch, it raises exception to indicate failure of test.
"""
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    """
        Inputs:
        * redshift_conn_id Redshift connection ID of Airflow connector for Redshift
        * sql_stats_tests list of pairs of SQL statement and expected value to be test for equality after query execution
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 sql_stats_tests = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stats_tests = sql_stats_tests
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for (sql_stat, expected_result) in self.sql_stats_tests:
            row = redshift_hook.get_first(sql_stat)
            if row is not None:
                if row[0] == expected_result:
                    self.log.info("Passed Test: {}\nResult == {}\n===================================".format(sql_stat, expected_result))
                else:
                    raise ValueError("Failed Test: {}\nResult != {}\n===================================".format(sql_stat, expected_result))