import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS st_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS st_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS st_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId BIGINT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INT 
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS st_songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR,
        year INT,
        duration FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY sortkey,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy st_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {};
""").format(
    config["S3"]["LOG_DATA"],
    config["IAM_ROLE"]["ARN"],
    config["S3"]["LOG_JSONPATH"],
    )

staging_songs_copy = ("""
    COPY st_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
""").format(
    config["S3"]["SONG_DATA"],
    config["IAM_ROLE"]["ARN"],
    )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
        timestamp 'epoch' + ste.ts/1000 * interval '1 second' AS start_time,
        ste.user_id AS user_id,
        ste.level AS level,
        sts.song_id AS song_id,
        sts.artist_id AS artist_id,
        ste.sessionId AS session_id,
        ste.location AS location,
        ste.user_agent AS user_agent
FROM st_events ste
LEFT  JOIN st_songs sts ON ste.song = sts.title AND ste.artist = sts.artist_name AND ste.length = sts.duration
WHERE ste.page  =  'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
		DISTINCT
        ste.user_id AS user_id,
        ste.firstName AS first_name,
        ste.lastName AS last_name,
        ste.gender AS gender,
        ste.level AS level
FROM st_events ste
WHERE ste.page  =  'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
	DISTINCT
	sts.song_id AS song_id, 
    sts.title AS title, 
    sts.artist_id AS artist_id, 
    sts.year AS year, 
    sts.duration AS duration
FROM st_songs sts;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
		DISTINCT
        sts.artist_id AS artist_id,
        sts.artist_name AS name,
        sts.artist_location AS location,
        sts.artist_latitude AS latitude,
        sts.artist_longitude AS longitude
FROM st_songs sts;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
	DISTINCT
	ts.start_time AS start_time, 
    DATE_PART (HOUR, ts.start_time) AS hour, 
    DATE_PART (DAY, ts.start_time) AS day, 
    DATE_PART (WEEK, ts.start_time) AS week, 
    DATE_PART (MONTH, ts.start_time) AS month, 
    DATE_PART (YEAR, ts.start_time) AS year, 
    DATE_PART (WEEKDAY, ts.start_time) AS weekday
FROM (SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time FROM st_events) ts ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
