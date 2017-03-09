import json
import os
import time
from pprint import pprint

from google.cloud import bigquery

def write_to_file(filename, json_data):
    with open(filename, 'w', encoding="utf-8") as snippet_file:
        json.dump(json_data, snippet_file)

def load_data_from_file(dataset_name, table_name, source_file_name):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)

    table.reload()

    with open(source_file_name, 'rb') as source_file:
        job = table.upload_from_file(
            source_file, source_format='NEWLINE_DELIMITED_JSON',
            ignore_unknown_values=True)

    wait_for_job(job, source_file_name)

    print('Loaded {} rows into {}:{} from {}.'.format(
        job.output_rows, dataset_name, table_name, source_file_name))


def wait_for_job(job, source_file_name):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)

def import_directory_into_big_query(directory, big_query_dataset_name, big_query_table_name):
    for filename in os.listdir(directory):
        if filename.endswith('.xml.json'):
            json_filename = os.path.join(directory, filename)
            snippet_filename = json_filename + '.snippet.json'
            with open(json_filename) as json_file:
                article = json.load(json_file)
                write_to_file(snippet_filename, article["snippet"])
                load_data_from_file(big_query_dataset_name, big_query_table_name, snippet_filename)
            continue
        else:
            continue

if __name__ == '__main__':
    directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'json')
    import_directory_into_big_query(directory, 'articles',  'snippets_staging')
