# How to run project

## Requirements:

1. Docker
2. Running Redshift cluster

## Steps:

1. Clone repository from https://github.com/HuaAnhMinh/udacity_data_engineering_project_5
2. Open the terminal and cd to project folder.
3. Run the following command for the first run: `docker-compose up airflow-init`. If you have already run the command, the next time you run the command `docker-compose up`.
4. Wait for airflow to be ready.
5. Open browser and go to http://localhost:8080/ to see the UI.
6. Go to Admin -> Connections page to add the following connection:
   1. Conn Id: aws_credentials, Connection Type: Amazon Web Services, Login: AKIAVM4CLAILYNCUGLC3, Password: Eru/Bip47Tb7uEUvHBrB/YkzsnXV3XliW9IXSR9Y
   2. Conn Id: redshift, Connection Type: Redshift, Host: <connection_string>, Schema: <database_name>, Port: 5439, Login: <username>, Password: <password>
7. Go to list DAGs, find DAG: udacity_project_5 and click on it.
8. Click on the Run button, choose Trigger DAG w/ config.
9. You can type in these modes to run:
   1. `{ "mode": "delete-load" }`: Delete data from dimension tables and load all.
   2. `{ "mode": "append-only" }`: Append data to dimension tables.
10. Wait for DAG to finish. 
11. **Note: when loading data from staging_songs, due to the s3://udacity-dend/song_data is too large so PostgresHook might have error if it is reading too long. You can resize it by using prefix to load a subset: song_data/A/A.**