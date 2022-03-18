from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now() - timedelta(days=1),
    'schedule_interval': '@hourly',
    'depends_on_past': False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'udacity_project_5',
    description='Load and transform data in Redshift with Airflow',
    default_args=default_args,
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_staging_events_table = PostgresOperator(
    task_id='create_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_staging_events_table,
)

create_staging_songs_table = PostgresOperator(
    task_id='create_staging_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.create_staging_songs_table,
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events_to_redshift',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_to_redshift',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A',
)

create_fact_dim_tables_task = DummyOperator(task_id='create_fact_dim_tables', dag=dag)

create_songs_table_task = PostgresOperator(
    task_id="create_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_songs_table,
)

create_artists_table_task = PostgresOperator(
    task_id="create_artists_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_artists_table,
)

create_users_table_task = PostgresOperator(
    task_id="create_users_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_users_table,
)

create_time_table_task = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_time_table,
)

create_songplays_table_task = PostgresOperator(
    task_id="create_songplays_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_songplays_table,
)

load_songplays_table_task = LoadFactOperator(
    task_id='load_songplays_table_task',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert,
)

load_songs_table_task = LoadDimensionOperator(
    task_id="load_songs_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_query=SqlQueries.song_table_insert,
)

load_artists_table_task = LoadDimensionOperator(
    task_id="load_artists_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_query=SqlQueries.artist_table_insert,
)

load_users_table_task = LoadDimensionOperator(
    task_id="load_users_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_query=SqlQueries.user_table_insert,
)

load_time_table_task = LoadDimensionOperator(
    task_id="load_time_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_query=SqlQueries.time_table_insert,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_staging_events_table
start_operator >> create_staging_songs_table
create_staging_events_table >> stage_events_to_redshift
create_staging_songs_table >> stage_songs_to_redshift
stage_events_to_redshift >> create_fact_dim_tables_task
stage_songs_to_redshift >> create_fact_dim_tables_task
create_fact_dim_tables_task >> create_songs_table_task
create_fact_dim_tables_task >> create_artists_table_task
create_fact_dim_tables_task >> create_users_table_task
create_fact_dim_tables_task >> create_time_table_task
create_fact_dim_tables_task >> create_songplays_table_task
create_songs_table_task >> load_songplays_table_task
create_artists_table_task >> load_songplays_table_task
create_users_table_task >> load_songplays_table_task
create_time_table_task >> load_songplays_table_task
create_songplays_table_task >> load_songplays_table_task
load_songplays_table_task >> load_songs_table_task
load_songplays_table_task >> load_artists_table_task
load_songplays_table_task >> load_users_table_task
load_songplays_table_task >> load_time_table_task
load_songs_table_task >> end_operator
load_artists_table_task >> end_operator
load_users_table_task >> end_operator
load_time_table_task >> end_operator
