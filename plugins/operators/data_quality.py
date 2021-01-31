from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''
This is a data quality check operator which is used to run checks on the data itself.  It will run a test with an expected result and check against test results
'''
        
class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks=dq_checks        
        
        
    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        for check in dq_checks:
            sql = check['check_sql']
            exp = check['expected_result']
            records = redshift_hook.get_records(sql)
            num_records = records[0][0]
            if num_records != exp:
                raise ValueError(f'Quality Check Failed.')
            else:
                self.log.info(f'Quality Check Passed.')