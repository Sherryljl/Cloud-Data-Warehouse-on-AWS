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
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR,
        auth VARCHAR,
        firstname VARCHAR,
        gender VARCHAR,
        iteminsession INT,
        lastname VARCHAR,
        length NUMERIC,
        level VARCHAR NOT NULL,
        location VARCHAR,
        method VARCHAR NOT NULL,
        page VARCHAR,
        registration VARCHAR,
        sessionid INT,
        song VARCHAR,
        status INT NOT NULL,
        ts BIGINT,
        useragent text,
        userid INT)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INT,
        artist_id VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC,
        location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year INT)
""")

songplay_table_create = ("""
     CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INT IDENTITY(1,1) NOT NULL,
        start_time TIMESTAMP WITHOUT TIME ZONE,
        user_id INT,
        level VARCHAR NOT NULL,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR)
""")

user_table_create = ("""
     CREATE TABLE IF NOT EXISTS users(
        user_id INT NOT NULL,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR(1),
        level VARCHAR)
""")

song_table_create = ("""
     CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT NOT NULL,
        duration NUMERIC NOT NULL)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR NOT NULL,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        hour INT NOT NULL, 
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json {}
    REGION 'us-west-2'
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json 'auto'
    REGION 'us-west-2'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,userid,level, song_id, artist_id, sessionid, e.location, useragent
    FROM staging_events AS e
    JOIN staging_songs AS s
    ON (e.artist = s.artist_name)
    AND (e.song = s.title)
    AND (e.length = s.duration)
    WHERE e.page = 'NextSong';
    """)

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender,level) 
    SELECT userid,firstname,lastname,gender,level
    FROM staging_events s1
    WHERE userid IS NOT NULL AND 
    ts=(SELECT max(ts) 
        FROM staging_events s2 
        WHERE s2.userid=s1.userid);  
    
    """)

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,title,artist_id,year,duration
    FROM staging_songs;
    """)

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT artist_id,artist_name,location,latitude,longitude
    FROM staging_songs;
    """)

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
    SELECT DISTINCT start_time,
    EXTRACT(HOUR FROM start_time) as hour,
    EXTRACT(DAY FROM start_time) as day,
    EXTRACT(WEEK FROM start_time) as week,
    EXTRACT(MONTH FROM start_time) as month,
    EXTRACT(YEAR FROM start_time) as year,
    EXTRACT(DOW FROM start_time) as weekday
    FROM
    (SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
    FROM staging_events) temp
    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
