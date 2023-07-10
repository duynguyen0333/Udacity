from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs): super(DataQualityOperator, self).__init__(*args, **kwargs)    

    def execute(self, context):
        self.log.info('DataQualityOperator is not implemented')