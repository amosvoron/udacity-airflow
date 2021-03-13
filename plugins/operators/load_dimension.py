from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Operator class for loading data into the dimension table.
    
    Args:
        redshift_conn_id (str): The Redshift Airflow Conn Id.
        table (str): The Redshift staging table name.        
        sql (str): The SQL code to execute. 
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)     
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info(f'Insert data into dimension table: {self.table}')        
        redshift = PostgresHook(self.redshift_conn_id)      
        redshift.run(self.sql)