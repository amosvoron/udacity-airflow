from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import CreateSchemaOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator
from helpers import SqlSchema, SqlQueries

# Default DAG args
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

# DAG object
dag = DAG('s3_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup = False,
          max_active_runs=1
        )

# Task: Start
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Task: Create Redshift tables
create_schema = CreateSchemaOperator(
    task_id="Create_schema",
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlSchema.create,
    skip=False
)

# Task: copy log data from S3 to Redshift (staging)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',   
    region='us-west-2',
    table='public.staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    s3_format="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

# Task: copy song data from S3 to Redshift (staging)
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',   
    region='us-west-2',
    table='public.staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    s3_format="JSON 'auto'"
)

# Task: load songplays fact table
load_songplays = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.songplays',
    sql=SqlQueries.songplays_insert
)

# Task: load users dimension table
load_users = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.users',
    sql=SqlQueries.users_insert,
    insert_mode='with truncate'
)

# Task: load songs dimension table
load_songs = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.songs',
    sql=SqlQueries.songs_insert,
    insert_mode='with truncate'
)

# Task: load artists dimension table
load_artists = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.artists',
    sql=SqlQueries.artists_insert,
    insert_mode='with truncate'
)

# Task: load time dimension table
load_time = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.time',
    sql=SqlQueries.time_insert,
    insert_mode='with truncate'
)

# Task: Run quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

# Task: End
end_operator = DummyOperator(task_id='End_execution', dag=dag)

# Task dependencies
start_operator >> create_schema
create_schema >> stage_events_to_redshift >> load_songplays
create_schema >> stage_songs_to_redshift >> load_songplays
load_songplays >> load_users >> run_quality_checks
load_songplays >> load_songs >> run_quality_checks
load_songplays >> load_artists >> run_quality_checks
load_songplays >> load_time >> run_quality_checks
run_quality_checks >> end_operator
