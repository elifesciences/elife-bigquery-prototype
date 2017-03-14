import json
import os
import time
import argparse
from pprint import pprint

from google.cloud import bigquery

def write_to_file(filename, json_data):
    with open(filename, 'a', encoding="utf-8") as snippet_file:
        json.dump(json_data, snippet_file)
        snippet_file.write('\n')

def upload_to_bigquery_from_file(dataset_name, table_name, source_file_name):
    print('Uploading ' + source_file_name + ' to ' + dataset_name + '.' + table_name)
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
    print('Waiting for job to load...')
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)

def import_directory_into_big_query(directory, big_query_dataset_name, big_query_table_name):
    count = 0
    number_of_files_made = 0
    split_every = 500
    snippet_filename = '0.snippet.json'

    for filename in os.listdir(directory):
        if filename.endswith('.xml.json'):
            count += 1
            json_filename = os.path.join(directory, filename)

            if count % split_every == 0:
                upload_to_bigquery_from_file(big_query_dataset_name, big_query_table_name, snippet_filename)
                snippet_filename = str(count) + '.snippet.json'

            with open(json_filename) as json_file:
                article = json.load(json_file)
                write_to_file(snippet_filename, article["snippet"])

            continue
        else:
            continue

    # always upload the last one
    upload_to_bigquery_from_file(big_query_dataset_name, big_query_table_name, snippet_filename)

if __name__ == '__main__':

    #usage: -s json -d articles -t snippets
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source", help="a directory contain json files")
    parser.add_argument("-d", "--dataset", help="the destination dataset")
    parser.add_argument("-t", "--table", help="the destination table")

    args = parser.parse_args()
    source_directory = args.source
    destination_dataset = args.dataset
    desitnation_table = args.table

    directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), source_directory)
    import_directory_into_big_query(directory, destination_dataset,  desitnation_table)
