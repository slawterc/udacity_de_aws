from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql = "",
                 append=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.append = append

    def execute(self, context):
        self.log.info(f"Loading Fact Table {self.table}")
        redshift = PostgresHook(self.redshift_conn_id)
        try:
            redshift.run(f"select count(*) from {self.table}")
        except Exception:
            raise Exception(f"{self.table} can not be found.")
        
        if self.table != '' and not self.append:
            self.log.info("Deleting data")
            redshift.run(f"DELETE FROM {self.table}")
            self.log.info(f"Dimension table {self.table} has been cleared of data")

        redshift.run(self.sql)
        self.log.info(f"Fact Table {self.table} has been loaded into redshift")
