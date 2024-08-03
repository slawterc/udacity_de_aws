from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,redshift_conn_id='',table ='',*args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(self.redshift_conn_id)
        records = redshift.get_records(f"select count(*) from {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data error: {self.table} returned no records")
        record_count = records[0][0]
        if record_count < 1:
            raise ValueError(f"Data error: {self.table} has no rows")
        self.log.info(f"All data check passed for {self.table}. Total record count: {record_count}." )