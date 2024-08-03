from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class TableCreateDelete(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_list= "",
                 drop=False,
                 *args, **kwargs):

        super(TableCreateDelete, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.drop = drop
        self.sql_list = sql_list

    def execute(self, context):
        if self.drop:
            self.log.info(f"Dropping Tables")
        else:
            self.log.info(f"Creating Tables")
        
        redshift = PostgresHook(self.redshift_conn_id)
        for item in self.sql_list:
            self.log.info(f"Processing {item}")
            redshift.run(item)
            