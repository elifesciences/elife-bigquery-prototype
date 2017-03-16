# elife-bigquery-prototype
Various scripts and schema for prototyping eLife JSON data in Google BigQuery

This is an attempt by eLife to easily add summary information about published
eLife articles to Google BigQuery for the purposes of generating analytics.

It is intended to work with eLife JSON [https://github.com/elifesciences/elife-article-json]

## Uploading Article Snippets to BigQuery
The script `load_directory.py` will extract `snippet` elements from eLife JSON
files, batch them into line delimted JSON files (default is 500 per file) and
attempt to upload them to the specific Google BigQuery table.

Each batched file creates a new import job on BigQuery and the script waits for
the job to return a result before attempting the next batch.

## Usage
You need to install and authenticate `gcloud` [https://cloud.google.com/sdk/]
for this script to work. The most simple method is to authenticate locally with
a personal Google account using OAuth but servers will require an appropriate
key to be added.

Run `load_directory.py` with Python 3 passing in args for the directory
name containing the eLife JSON files, the dataset name of an existing dataset
in BigQuery and the table name of an existing table in BigQuery

The following will upload a directory `json` to the table `articles.snippets`
```{r, engine='bash', usage}
python load_directory.py -s json -d articles -t snippets
```
