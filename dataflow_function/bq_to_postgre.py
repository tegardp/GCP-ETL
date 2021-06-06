import argparse
import logging
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
#from beam_nuggets.io import relational_db


import psycopg2

class QueryPostgresFn(beam.DoFn):

  def __init__(self, **server_config):
    self.config = server_config

  def process(self, query):
    con = psycopg2.connect(**self.config)
    cur = con.cursor()

    cur.execute(query)
    return cur.fetchall()


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_file',
        dest='input',
        required=True,
        help='The file path for the input file to process.')
    parser.add_argument(
        '--batch-date', 
        dest='batch_date', 
        required=True,
        help='The project ID for your Google Cloud Project.')
    parser.add_argument(
        '--postgre-host', 
        dest='postgre_host', 
        required=True,
        help='The project ID for your Google Cloud Project.')
    parser.add_argument(
        '--postgre-user', 
        dest='postgre_user', 
        required=True,
        help='The project ID for your Google Cloud Project.')
    parser.add_argument(
        '--postgre-password', 
        dest='postgre_password', 
        required=True,
        help='The project ID for your Google Cloud Project.')
    parser.add_argument(
        '--project', 
        dest='project_id', 
        required=True,
        help='The project ID for your Google Cloud Project.')
    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_options = PipelineOptions(
            pipeline_args, 
            project=known_args.project_id
    )
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


    with beam.Pipeline(options=pipeline_options) as pipeline:
        

        query = f"""
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
                    `{known_args.input}`
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
                FROM `{known_args.input}`
                WHERE 
                    event_name = 'register' 
                ) as t2 ON t1.user_id = t2.user_id
            WHERE t1.transaction_datetime BETWEEN ''' + ''' '{known_args.batch_date}' AND '{datetime.datetime.strptime(known_args.batch_date, "%Y-%M-%d") + datetime.timedelta(days=1)}' 
            '''
        """


        # source_config = relational_db.SourceConfiguration(
        #     drivername='postgresql+pg8000',
        #     host=f'{known_args.postgre_host}',
        #     port=5432,
        #     username=f'{known_args.postgre_user}',
        #     password=f'{known_args.postgre_password}',
        #     database='backup',
        #     create_if_missing=True,
        # )
        # table_config = relational_db.TableConfiguration(
        #     name='transaction',
        #     create_if_missing=True
        # )

        # output = ( pipeline 
        #     | 'Read BQ' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        #     | 'Connect to Postgre' >> beam.ParDo(QueryPostgresFn(host=known_args.postgre_host, user=known_args))
        #     | 'Writing to DB' >> relational_db.Write(
        #         source_config=source_config,
        #         table_config=table_config
        #     )
        # )

        output = ( pipeline 
            | 'Read BQ' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
            | 'Connect to Postgre' >> beam.ParDo(QueryPostgresFn(host=known_args.postgre_host, user=known_args.postgre_user, password=known_args.postgre_password))
            | 'Writing to DB' >> beam.Reshuffle()
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
