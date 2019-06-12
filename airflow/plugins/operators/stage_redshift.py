from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_conn_id="aws_credentials",
                 source_location="",
                 target_table="sample_table",
                 file_type="json",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)        
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.source_location = source_location
        self.target_table = target_table
        self.file_type = file_type

    def execute(self, context):        
        if self.file_type in ["json", "csv"]:            
            redshift_hook = PostgresHook(self.redshift_conn_id)
            aws_hook = AwsHook(self.aws_conn_id)
            credentials = aws_hook.get_credentials()
            self.log.info(f'StageToRedshiftOperator will start loading files at: {self.source_location} to staging table: {self.target_table}')
            if self.file_type == "json":
                redshift_hook.run (
                    SqlQueries.copy_json_to_redshift.format (
                    self.target_table,
                    self.source_location,
                    credentials.access_key, 
                    credentials.secret_key)
                )
            elif self.file_type == "csv":
                redshift_hook.run (
                    SqlQueries.copy_csv_to_redshift.format (
                    self.target_table,
                    self.source_location,
                    credentials.access_key, 
                    credentials.secret_key)
                )

            self.log.info(f'StageToRedshiftOperator has completed loading files at: {self.source_location} to staging table: {self.target_table}')
        else:
            raise ValueError("file_type param must be either json or csv")


