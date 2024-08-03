from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_creds="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 format="auto",
                 execution_date=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_creds = aws_creds
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key= s3_key
        self.format = format
        self.execustion_date = execution_date

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator')
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_creds)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.ingo('Getting Data from S3')
        s3_dir = self.s3_key
        if self.execution_date:
            year = str(self.execution_date.strftime("%Y"))
            month = str(self.execution_date.strftime("%m"))
            s3_dir = s3_dir.formate(year,month)
        s3_path = """s3://{}/{}""".format(self.s3_bucket,s3_dir)

        sql ="""
        COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' TIMEFORMAT as 'epochmillisecs' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL JSON '{}'
        """
        formatted_sql = sql.format(self.table,s3_path,aws_connection.login,aws_connection.password,self.json_format)
        self.log.info(f"Running sql: {formatted_sql}")
        redshift.run(formatted_sql) 





