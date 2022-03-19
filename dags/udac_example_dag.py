from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries
from helpers.unit_test import UnitTest

REDSHIFT_CONN_ID = 'redshift'
AWS_CONN_ID = 'aws_credentials'

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now() - timedelta(days=1),
    'schedule_interval': '@hourly',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'udacity_project_5',
    description='Load and transform data in Redshift with Airflow',
    default_args=default_args,
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events_to_redshift',
    dag=dag,
    table='staging_events',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CONN_ID,
    s3_bucket='udacity-dend',
    s3_key='log_data',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_to_redshift',
    dag=dag,
    table='staging_songs',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CONN_ID,
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A',
)

load_songplays_table_task = LoadFactOperator(
    task_id='load_songplays_table_task',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert,
)

load_songs_table_task = LoadDimensionOperator(
    task_id="load_songs_table",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="songs",
    sql_query=SqlQueries.song_table_insert,
    mode="{{dag_run.conf.mode}}",
)

load_artists_table_task = LoadDimensionOperator(
    task_id="load_artists_table",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="artists",
    sql_query=SqlQueries.artist_table_insert,
    mode="{{dag_run.conf.mode}}",
)

load_users_table_task = LoadDimensionOperator(
    task_id="load_users_table",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="users",
    sql_query=SqlQueries.user_table_insert,
    mode="{{dag_run.conf.mode}}",
)

load_time_table_task = LoadDimensionOperator(
    task_id="load_time_table",
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table="time",
    sql_query=SqlQueries.time_table_insert,
    mode="{{dag_run.conf.mode}}",
)

data_quality_task = DataQualityOperator(
    task_id='data_quality_check',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    python_callable=UnitTest.run,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table_task
stage_songs_to_redshift >> load_songplays_table_task
load_songplays_table_task >> load_songs_table_task
load_songplays_table_task >> load_artists_table_task
load_songplays_table_task >> load_users_table_task
load_songplays_table_task >> load_time_table_task
load_songs_table_task >> data_quality_task
load_artists_table_task >> data_quality_task
load_users_table_task >> data_quality_task
load_time_table_task >> data_quality_task
data_quality_task >> end_operator
