# Data pipelines with Airflow
This is an Apache Airflow project for the Udacity Data Engineering Nanodegree program.

## Project Overview
In this project we'll create an ETL pipeline that extracts data from the Amazon's S3 store and loads it into the Amazon Redshift PostreSQL database using Apache Airflow for scheduling and monitoring the ETL workflow.

### DAG
The Airflow DAG will be configurated according to the following guidelines:
 - The DAG does not have dependencies on past runs
 - On failure, the task are retried 3 times
 - Retries happen every 5 minutes
 - Catchup is turned off
 - Do not email on retry
 
### Tasks (by dependency groups)

 1. **2 staging tasks** to extract log events and songs from S3 JSON files.
 2. Then **load** into *songplays* fact table.
 3. Then **load** into 4 dimension tables: *users*, *songs*, *artists*, *time*.
 4. Then **data quality checks**.
 
For each dependency group, that is, for staging the data, transforming the data to load it into fact table and dimension tables, and running checks on data quality, a custom Airflow operator will be used. The operators will be parameterized as much as possible.

## Installation
### Clone
```sh
$ git clone https://github.com/amosvoron/udacity-airflow.git
```

## License

MIT


