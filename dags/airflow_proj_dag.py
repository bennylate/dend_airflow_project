from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

'''
Setting default args per project specifications
'''

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 1, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'email_on_retry': False,
    'catchup' : False
}

'''
Creating the DAG
'''

dag = DAG('airflow_proj_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@hourly',
          max_active_runs = 1
        )


start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

'''
Task for staging events log data to Redshift
'''
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_events',
    provide_context = False,
    dag = dag,
    table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    s3_path = 's3://udacity-dend/log_data/A/A/A/',
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials',
    region = 'us-west-2',
    extra_params = "'format as json's3://udacity-dend/log_json_path.json'"
)


'''
Task for staging song data data to Redshift
'''
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_songs',
    provide_context = False,
    dag = dag,
    table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    s3_path = 's3://udacity-dend/song_data',
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials',
    region = 'us-west-2',
    extra_params = "json 'auto' compupdate offregion 'us-west-2'"
    
)


'''
Task for populating the songplay (fact) table in Redshift
'''
load_songplays_table = LoadFactOperator(
    task_id = 'load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'songplays',
    query = SqlQueries.songplay_table_insert,
    append_only = False
)


'''
Tasks for populating the users, song, artist, time tables (dimension) in Redshift
'''
load_user_dimension_table = LoadDimensionOperator(
    task_id = 'load_user_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    query = SqlQueries.user_table_insert,
    append_only = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'load_song_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    query = SqlQueries.song_table_insert,
    append_only = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'load_artist_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    query = SqlQueries.artist_table_insert,
    append_only = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'load_time_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    query = SqlQueries.time_table_insert,
    append_only = False
)


'''
Data quality checks
'''

dq_checks = [{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result':0},
             {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}]

run_quality_checks = DataQualityOperator(
    task_id = 'run_data_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    dq_checks = dq_checks
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag = dag)

'''
Task Dependencies
'''
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator

