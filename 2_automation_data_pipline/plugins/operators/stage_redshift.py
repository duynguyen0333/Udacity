from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, *args, **kwargs): super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        

    def execute(self, context):
        self.log.info('StageToRedshiftOperator is not implemented')





