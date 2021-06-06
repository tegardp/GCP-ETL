![alt Google Cloud](https://www.pointstar.co.id/wp-content/uploads/2018/11/Google-Cloud-P.png)

# GCP-ETL

We're going to set up a Google Cloud Composer
environment and solve several **batch-processing** cases
by creating DAGs to run ETL jobs in the cloud. The data
processing consists of ETLs with data going in and from
GCS to BigQuery and the other is BigQuery to PostgreSQL

## Project Overview
We have two kind of task at hand here.

1. Move data from several csv files in Google Cloud Storage to BigQuery

2. Transform transactional events from the unified user events bigquery
table, into a transaction table in bigquery. Then move it to PostgreSQL

## Tech Stack
We're going to use some services from Google Cloud Platform here:
1. Google Cloud Storage (GCS)
2. Google Cloud Dataflow
3. Google Cloud Composer
4. BigQuery
5. Cloud SQL

## Installation
requirements: you sould have Airflow installed. If you don't know how to install it, [read here](https://airflow.apache.org/docs/apache-airflow/stable/installation.html)

first init your database
```
airflow db init
```

then create admin user
```
$ airflow users create \
          --username admin \
          --firstname FIRST_NAME \
          --lastname LAST_NAME \
          --role Admin \
          --email admin@example.org
```

after that you can run airflow web ui via
```
airflow webserver
```

and in another terminal command below to schedule your dag

```
airflow scheduler
```

You can access Airflow UI in `localhost:8080` on your favorite web browser. Input username and password `airflow` and `airflow` if asked

Don't forget to setup Composer variables. You can look at `config/env-example.json` for example. Fill it then upload it on composer variables.

# Glossary

## Batch Processing
Batch processing is the processing of transactions in a group or batch. No user interaction is required once batch processing is underway. This differentiates batch processing from transaction processing, which involves processing transactions one at a time and requires user interaction.

Put simply, batch processing is the process by which a computer completes batches of jobs, often simultaneously, in non-stop, sequential order. It’s also a command that ensures large jobs are computed in small parts for efficiency during the debugging process.

While batch processing can be carried out at any time, it is particularly suited to end-of-cycle processing, such as for processing a bank's reports at the end of a day or generating monthly or biweekly payrolls.