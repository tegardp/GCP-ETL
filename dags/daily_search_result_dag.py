from datetime import datetime

from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import BranchPythonOperator

global_config = Variable.get("global", deserialize_json=True)
PROJECT_ID = global_config["PROJECT_ID"]
GCE_REGION = global_config["GCE_REGION"]   
COMPOSER_BUCKET = global_config["COMPOSER_BUCKET"]
week2_config = Variable.get("week2", deserialize_json=True)
SOURCE_BUCKET = week2_config["SOURCE_BUCKET"]
DWH_DATASET = week2_config["DWH_DATASET"]
TABLE_NAME = 'daily_search_history'
PY_FILE=f'gs://{COMPOSER_BUCKET}/dataflow_functions/gcs_to_bq.py'

default_args = {
    'owner': 'tegardp',
    "start_date": datetime(2021,3,10),
    "end_date": datetime(2021,3,15),
}


@dag(default_args=default_args, schedule_interval='@daily', tags=['academi', 'blankspace', 'week2'], catchup=False)
def daily_search_result_etl():


    gcs_to_bq = BeamRunPythonPipelineOperator(
        task_id="gcs_to_bq",
        py_file=PY_FILE,
        runner='DataflowRunner',
        py_requirements=['apache-beam[gcp]==2.26.0'],
        py_interpreter='python3',
        py_system_site_packages=True,
        pipeline_options={
            'inputFile': f'gs://{SOURCE_BUCKET}/'+'keyword_search_search_{{ds_nodash}}.csv',
            'output': TABLE_NAME,
            'extra-package': 'psycopg2'
        },
        dataflow_config=DataflowConfiguration(
            project_id=PROJECT_ID, 
            wait_until_finished=True
        )
    )

    bq_transform = BigQueryOperator(
        task_id='bq_write_to_most_searched',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        sql=f'''
            SELECT t.search_keyword, t.total, t.created_at
                FROM(
                    SELECT
                        max(total) as total, created_at
                    FROM
                        (SELECT search_keyword, SUM(search_result_count) as total, created_at
                        FROM `{DWH_DATASET}.{TABLE_NAME}`
                        GROUP BY created_at,search_keyword
                        ORDER BY total DESC
                        )
                    GROUP BY created_at
                    ORDER BY created_at ASC
                ) as x
                INNER JOIN (
                    SELECT search_keyword, SUM(search_result_count) as total, created_at
                    FROM `{DWH_DATASET}.{TABLE_NAME}`
                    GROUP BY created_at,search_keyword
                    ORDER BY total DESC
                    ) as t 
                ON t.total = x.total AND t.created_at = x.created_at
            ''',    
        destination_dataset_table=f'{PROJECT_ID}.staging.most_searched_keywords',
        trigger_rule='none_failed'
    )


    gcs_to_bq >> bq_transform


daily_search_result_etl_dag = daily_search_result_etl()
