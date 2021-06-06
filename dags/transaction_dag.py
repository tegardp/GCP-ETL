from datetime import datetime
from airflow.operators.python import BranchPythonOperator, PythonOperator
from google.cloud.exceptions import NotFound

from google.cloud import bigquery
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/tegardp/.keys/academi-314607-b12916885b12.json"

global_config = Variable.get("global", deserialize_json=True)
week2_config = Variable.get("week2", deserialize_json=True) 
PROJECT_ID = global_config["PROJECT_ID"]
COMPOSER_BUCKET = global_config["COMPOSER_BUCKET"]
SOURCE_TABLE = week2_config["SOURCE_TABLE"]
DESTINATION_TABLE = week2_config["DESTINATION_TABLE"]
DESTINATION_BUCKET = week2_config["DESTINATION_BUCKET"]
POSTGRE_HOST = week2_config["POSTGRE_HOST"]
POSTGRE_USER = week2_config["POSTGRE_USER"]
POSTGRE_PASSWORD = week2_config["POSTGRE_PASSWORD"]
PY_FILE = f'gs://{COMPOSER_BUCKET}/dataflow_functions/bq_to_postgre.py'
PY_FILE = './airflow/dataflow_function/bq_to_postgre.py'

default_args = {
    'owner': 'tegardp',
    "start_date": datetime(2021,3,20),
    "end_date": datetime(2021,3,29),
}


@dag(default_args=default_args, schedule_interval='@daily', tags=['academi', 'blankspace', 'week2'], catchup=False)
def transaction_etl():

    def check_if_bq_table_exist(**kwargs):
        client = bigquery.Client()
        table_id = f"{kwargs['destination_table']}"
        
        try:
            client.get_table(table_id)  # Make an API request.
            return "table_exist"
        except NotFound:
            return "table_not_exist"

    def create_bq_table(**kwargs):
        client = bigquery.Client(kwargs['project_id'])
        table_id = f"{kwargs['destination_table']}"

        schema = [
                {
                    "mode": "NULLABLE",
                    "name": "transaction_id",
                    "type": "INTEGER"
                },
                {
                    "mode": "NULLABLE",
                    "name": "transaction_detail_id",
                    "type": "INTEGER"
                },
                {
                    "mode": "NULLABLE",
                    "name": "transaction_number",
                    "type": "STRING"
                },
                {
                    "mode": "NULLABLE",
                    "name": "transaction_datetime",
                    "type": "TIMESTAMP"
                },
                {
                    "mode": "NULLABLE",
                    "name": "purchase_quantity",
                    "type": "INTEGER"
                },
                {
                    "mode": "NULLABLE",
                    "name": "purchase_amount",
                    "type": "INTEGER"
                },
                {
                    "mode": "NULLABLE",
                    "name": "purchase_payment_method",
                    "type": "STRING"
                },
                {
                    "mode": "NULLABLE",
                    "name": "purchase_source",
                    "type": "INTEGER"
                },
                {
                    "mode": "NULLABLE",
                    "name": "product_id",
                    "type": "INTEGER"
                },
                {
                    "mode": "NULLABLE",
                    "name": "user_id",
                    "type": "INTEGER"
                },
                {
                    "mode": "NULLABLE",
                    "name": "state",
                    "type": "STRING"
                },
                {
                    "mode": "NULLABLE",
                    "name": "city",
                    "type": "STRING"
                },
                {
                    "mode": "NULLABLE",
                    "name": "created_at",
                    "type": "TIMESTAMP"
                },
                {
                    "mode": "NULLABLE",
                    "name": "ext_created_at",
                    "type": "INTEGER"
                }
                ]

        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",  # name of column to use for partitioning
        )
        table = client.create_table(table)  # Make an API request.
        print(f"Created table {table_id}")

    check_if_table_exist = BranchPythonOperator(
        task_id='check_if_table_exist',
        python_callable=check_if_bq_table_exist,
        op_kwargs={'destination_table':DESTINATION_TABLE},
    )

    create_table = PythonOperator(
        task_id = 'create_bq_table',
        python_callable=create_bq_table,
        op_kwargs={'project_id':PROJECT_ID, 'destination_table': DESTINATION_TABLE},
    )
    

    bq_to_bq = BigQueryOperator(
        task_id='bq_write_transfer',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        allow_large_results=True,
        sql=f'''
            SELECT
                t1.transaction_id,
                t1.transaction_detail_id,
                t1.transaction_number,
                t1.transaction_datetime,
                t1.purchase_quantity,
                t1.purchase_amount,
                t1.purchase_payment_method,
                NULL AS purchase_source,
                t1.product_id,
                t1.user_id,
                t1.state,
                t1.city,
                t2.created_at,
                NULL AS ext_created_at
            FROM (
                SELECT
                    ep1.value.int_value AS transaction_id,
                    ep2.value.int_value AS transaction_detail_id,
                    ep3.value.string_value AS transaction_number,
                    event_datetime AS transaction_datetime,
                    ep4.value.int_value AS purchase_quantity,
                    ep5.value.float_value AS purchase_amount,
                    ep6.value.string_value AS purchase_payment_method,
                    NULL AS purchase_source,
                    ep8.value.int_value AS product_id,
                    user_id,
                    state,
                    city,
                FROM
                    `{SOURCE_TABLE}`
                CROSS JOIN
                    UNNEST(event_params) ep1
                CROSS JOIN
                    UNNEST(event_params) ep2
                CROSS JOIN
                    UNNEST(event_params) ep3
                CROSS JOIN
                    UNNEST(event_params) ep4
                CROSS JOIN
                    UNNEST(event_params) ep5
                CROSS JOIN
                    UNNEST(event_params) ep6
                CROSS JOIN
                    UNNEST(event_params) ep8
                WHERE
                    event_name = 'purchase_item'
                    AND ep1.key='transaction_id'
                    AND ep2.key='transaction_detail_id'
                    AND ep3.key='transaction_number'
                    AND ep4.key='purchase_quantity'
                    AND ep5.key='purchase_amount'
                    AND ep6.key='purchase_payment_method'
                    AND ep8.key='product_id') as t1
            INNER JOIN (
                SELECT
                    user_id, created_at
                FROM `{SOURCE_TABLE}`
                WHERE 
                    event_name = 'register' 
                ) as t2 ON t1.user_id = t2.user_id
            WHERE t1.transaction_datetime BETWEEN ''' + ''' '{{ds}}' AND '{{tomorrow_ds}}' 
            '''
            ,    
        destination_dataset_table=f'{DESTINATION_TABLE}',
        trigger_rule='none_failed',
    )

    bq_to_postgre = BeamRunPythonPipelineOperator(
        task_id="bq_to_postgre",
        py_file=PY_FILE,
        runner='DataflowRunner',
        py_requirements=['apache-beam[gcp]==2.26.0'],
        py_interpreter='python3',
        py_system_site_packages=True,
        pipeline_options={
            'inputFile': DESTINATION_TABLE,
            'batch-date':'{{ds}}',
            'postgre-host': POSTGRE_HOST,
            'postgre-user': POSTGRE_USER,
            'postgre-password': POSTGRE_PASSWORD,
        },
        dataflow_config=DataflowConfiguration(
            project_id=PROJECT_ID, 
            wait_until_finished=True
        )
    )

    table_exist = DummyOperator(task_id='table_exist')
    table_not_exist = DummyOperator(task_id='table_not_exist')
    
    
    check_if_table_exist >> [table_exist, table_not_exist] 
    table_not_exist >> create_table
    [table_exist , create_table] >> bq_to_bq >> bq_to_postgre
    

transaction_dag = transaction_etl()