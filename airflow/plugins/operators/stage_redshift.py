from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
"""
    This operator is able to load given dataset in JSON or CSV format 
    from specified location on S3 into target Redshift table    
"""
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    
    """
        Inputs:
        * redshift_conn_id Redshift connection ID in Airflow connectors
        * aws_conn_id AWS credentials connection ID in Airflow connectors
        * source_location location of dataset on S3
        * target_table name of the table to loaded from dataset in Redshift
        * file_type format of the dataset files to be read from S3. Supported values "json" or "csv"
        * json_path location of file representing the schema of dataset in JSON format
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_conn_id="aws_credentials",
                 source_location="",
                 target_table="sample_table",
                 file_type="json",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)        
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.source_location = source_location
        self.target_table = target_table
        self.file_type = file_type
        self.json_path = json_path
    def execute(self, context):        
        if self.file_type in ["json", "csv"]:            
            redshift_hook = PostgresHook(self.redshift_conn_id)
            aws_hook = AwsHook(self.aws_conn_id)
            credentials = aws_hook.get_credentials()
            self.log.info(f'StageToRedshiftOperator will start loading files at: {self.source_location} to staging table: {self.target_table}')
            redshift_hook.run(SqlQueries.truncate_table.format(self.target_table))
            if self.file_type == "json":
                if self.json_path != "":
                    redshift_hook.run (
                        SqlQueries.copy_json_with_json_path_to_redshift.format (
                            self.target_table,
                            self.source_location,
                            credentials.access_key, 
                            credentials.secret_key,
                            self.json_path
                        )
                    )
                else:
                    redshift_hook.run (
                        SqlQueries.copy_json_to_redshift.format (
                            self.target_table,
                            self.source_location,
                            credentials.access_key, 
                            credentials.secret_key
                        )
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


