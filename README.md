## Project Description:
A music streaming startup, Sparky, has grown its user base and song database and wants to move its processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, and a guide with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3 stage them in Redshift and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and comparing your results with their expected results

## Project Structure

```
Cloud Data Warehouse
|___create_tables.py    # database/table creation&droping
|___etl.py              # ELT process
|___sql_queries.py      # SQL query collections
|___dwh.cfg             # AWS configuration file that has credentials
```


## Database schema design

#### Staging Tables
- staging_events
- staging_songs

####  Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong - 
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
- users - users in the app - 
*user_id, first_name, last_name, gender, level*
- songs - songs in music database - 
*song_id, title, artist_id, year, duration*
- artists - artists in music database - 
*artist_id, name, location, lattitude, longitude*
- time - timestamps of records in songplays broken down into specific units - 
*start_time, hour, day, week, month, year, weekday*



## How to Run python script 

1- To run this project you will need to fill the following information, and save it as dwh.cfg in the project root folder.
```
[CLUSTER]
HOST=''

DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=''

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSON_PATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

2- Run create_tables.py :-> This script drops existing tables and creates new ones.

3- Run etl.py :-> This script uses data in s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into DB.