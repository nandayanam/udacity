import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events";
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs";
songplay_table_drop = "DROP TABLE IF EXISTS songplays_table";
user_table_drop = "DROP TABLE IF EXISTS user_table";
song_table_drop = "DROP TABLE IF EXISTS song_table";
artist_table_drop = "DROP TABLE IF EXISTS artist_table";
time_table_drop = "DROP TABLE IF EXISTS time_table";

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    );
""")

staging_songs_table_create = ("""
   CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_location VARCHAR,
        artist_longitude FLOAT,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INT,
        song_id VARCHAR,
        title VARCHAR,
        year INT
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays_table (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender CHAR(1),
    level VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")


# STAGING TABLES

staging_events_table_copy = ("""
    COPY staging_events
    FROM 's3://dwh-udacity/log_data/'
    IAM_ROLE 'myRedshiftRole'
    REGION 'us-east-1'
    FORMAT AS JSON 'auto'
    TIMEFORMAT 'epochmillisecs';

""").format()


staging_songs_copy = ("""
 COPY staging_songs
    FROM 's3://dwh-udacity/song_udacity/'
    IAM_ROLE 'myRedshiftRole'
    REGION 'us-east-1'
    FORMAT AS JSON 'auto'
    TIMEFORMAT 'epochmillisecs';

""").format()

# FINAL TABLES

songplay_table_insert = ("""
 INSERT INTO songplays_table (
    songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT
    ROW_NUMBER() OVER (ORDER BY start_time, user_id) AS songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
FROM
    (
        SELECT
            TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
            e.userId AS user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId AS session_id,
            e.location,
            e.userAgent AS user_agent
        FROM
            staging_events e
        JOIN
            staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)
        WHERE
            e.page = 'NextSong'
    );

""")

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level)
SELECT
    CAST(userId AS INT) AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM
    staging_events
WHERE
    userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM
    staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
SELECT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM
    staging_songs;
""")

time_table_insert = ("""
INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
SELECT
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(HOUR FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS hour,
    EXTRACT(DAY FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS day,
    EXTRACT(WEEK FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS week,
    EXTRACT(MONTH FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS month,
    EXTRACT(YEAR FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS year,
    EXTRACT(DOW FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS weekday
FROM
    staging_events
WHERE
    ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
