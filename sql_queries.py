import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
    (
        artist        VARCHAR, 
        auth          VARCHAR,
        firstname     VARCHAR,
        gender        VARCHAR,
        itemInSession INT, 
        lastName      VARCHAR,
        length        FLOAT,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  VARCHAR, 
        sessionId     INT,
        song          VARCHAR,
        status        INT, 
        ts            BIGINT,
        userAgent     VARCHAR,
        userId        INT
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs         INTEGER,
        artist_id         VARCHAR,
        artist_latitude   FLOAT,
        artist_longitude  FLOAT,
        artist_location   TEXT,
        artist_name       VARCHAR,
        song_id           VARCHAR,
        title             VARCHAR,
        duration          FLOAT,
        year              INTEGER      
    )
""")

# In Id int identity(1,1), the first 1 means the starting value of ID 
# and the second1 means the increment value of ID. It will increment like 1,2,3,4..
# If it was (5,2), then, it starts from 5 and increment by 2 like, 5,7,9,11,...
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id   INTEGER       NOT NULL  PRIMARY KEY    IDENTITY(0,1),
        start_time    TIMESTAMP     NOT NULL  SORTKEY, 
        user_id       INTEGER       NOT NULL  DISTKEY,
        level         VARCHAR,
        song_id       VARCHAR       NOT NULL ,
        artist_id     VARCHAR       NOT NULL ,
        session_id    INTEGER       NOT NULL,
        location      VARCHAR           NULL,
        user_agent    VARCHAR     
    )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
    (
        user_id     INTEGER PRIMARY KEY  DISTKEY,
        first_name  VARCHAR,
        last_name   VARCHAR,
        gender      VARCHAR,
        level       VARCHAR
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
    (
        song_id    VARCHAR  PRIMARY KEY   SORTKEY,
        title      VARCHAR,
        artist_id  VARCHAR NOT NULL,
        year       INTEGER,
        duration   FLOAT
    )
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   VARCHAR    PRIMARY KEY  SORTKEY,
        name        VARCHAR    NOT NULL,
        location    VARCHAR,
        latitude    FLOAT,
        logitude    FLOAT
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
    (
        start_time   TIMESTAMP   PRIMARY KEY   SORTKEY,
        hour         INTEGER     NOT NULL,
        day          INTEGER     NOT NULL, 
        week         INTEGER     NOT NULL,
        month        INTEGER     NOT NULL,
        year         INTEGER     NOT NULL,
        weekday      INTEGER     NOT NULL
    )
""")


# STAGING TABLES


# staging_events COPY 

# read the credentials from dwh.cfg file 
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSON_PATH")

staging_events_copy = ("""
    copy staging_events
    from {0}
    iam_role {1}
    json {2}
    """).format(LOG_DATA, ARN, LOG_JSON_PATH)

# staging_songs COPY
SONG_DATA = config.get('S3', 'SONG_DATA')
staging_songs_copy = ("""
    copy staging_events
    from {0}
    iam_role {1}
    json 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
    (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT DISTINCT 
        timestamp with time zone 'epoch' + staging_events.ts/1000 * interval '1 second',
        staging_events.userId,
        staging_events.level,
        staging_songs.song_id,
        staging_songs.artist_id,
        staging_events.sessionId,
        staging_events.location,
        staging_events.userAgent
    FROM staging_events 
    INNER JOIN staging_songs 
    ON staging_events.song = staging_songs.title
    AND staging_events.artist = staging_songs.artist_name 
    AND staging_events.length = staging_songs.duration
    WHERE staging_events.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users 
    (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT 
        staging_events.userId,
        staging_events.firstName,
        staging_events.lastName,
        staging_events.gender,
        staging_events.level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs 
    (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT  DISTINCT 
        song_id ,
        title,
        artist_id,
        year,
        duration        
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists 
    (
        artist_id,
        name,
        location,
        latitude,
        logitude
    )
    SELECT  DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time, 
           extract(hour from start_time), 
           extract(day from start_time), 
           extract(week from start_time), 
           extract(month from start_time), 
           extract(year from start_time), 
           extract(weekday from start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
