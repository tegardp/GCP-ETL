import argparse
import logging


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class LineExtractDoFn(beam.DoFn):
    def __init__(self, delimiter=','):
        self.delimiter = delimiter


    def process(self, line):
        import csv
        from datetime import datetime

        csv_object = csv.reader(line.splitlines(), quotechar='"', delimiter=self.delimiter, quoting=csv.QUOTE_ALL)

        user_id, search_keyword, search_result_count, created_at = next(csv_object)
        created_at = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S %Z').strftime('%Y-%m-%d')
        
        return [{
                 'user_id': int(user_id), 
                 'search_keyword': search_keyword, 
                 'search_result_count': int(search_result_count), 
                 'created_at': created_at
                }]


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_file',
        dest='input',
        required=True,
        help='The file path for the input file to process.')
    parser.add_argument(
        '--output-path',
        dest='output',
        required=True,
        help='BigQuery table to write results to.')
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
        

        table_schema = {
            'fields' : [
                {'name': 'user_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'search_keyword', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'search_result_count', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'created_at', 'type': 'DATE', 'mode': 'REQUIRED'},
            ]
        }

        lines = pipeline | 'Read' >> ReadFromText(known_args.input, skip_header_lines=1)

        output = (
            lines
            | 'Transform Data' >> beam.ParDo(LineExtractDoFn())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table = f'{known_args.project_id}:dwh.{known_args.output}',
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
