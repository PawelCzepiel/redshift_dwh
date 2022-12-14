import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('../dwh.cfg')
ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                                (   id BIGINT IDENTITY(0,1),
                                    artist VARCHAR(MAX),
                                    auth VARCHAR,
                                    firstName VARCHAR,
                                    gender VARCHAR,
                                    itemInSession VARCHAR,
                                    lastName VARCHAR,
                                    length VARCHAR, 
                                    level VARCHAR,
                                    location VARCHAR,
                                    method VARCHAR,
                                    page VARCHAR,
                                    registration VARCHAR,
                                    sessionId INT,
                                    song VARCHAR(MAX),
                                    status INT,
                                    ts BIGINT,
                                    userAgent VARCHAR,
                                    userID INT
                                )
                            """)


staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (
                                    num_song INT,
                                    artist_id VARCHAR,
                                    artist_latitude VARCHAR,
                                    artist_longitude VARCHAR,
                                    artist_location VARCHAR,
                                    artist_name VARCHAR(MAX),
                                    song_id VARCHAR,
                                    title VARCHAR(MAX),
                                    duration DECIMAL,
                                    year INT
                                )        
                            """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                            (
                                songplay_id INT  IDENTITY(0,1) NOT NULL SORTKEY PRIMARY KEY,
                                start_time TIMESTAMP NOT NULL,
                                user_id VARCHAR NOT NULL DISTKEY,
                                level VARCHAR NOT NULL,
                                song_id VARCHAR NOT NULL,
                                artist_id VARCHAR NOT NULL,
                                session_id INT NOT NULL,
                                location VARCHAR,
                                user_agent VARCHAR
                            )
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                        (
                            user_id INT NOT NULL SORTKEY PRIMARY KEY,
                            first_name VARCHAR,
                            last_name VARCHAR,
                            gender VARCHAR,
                            level VARCHAR
                        )diststyle all;
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                        (
                            song_id VARCHAR NOT NULL PRIMARY KEY,
                            title VARCHAR(MAX) NOT NULL,
                            artist_id VARCHAR NOT NULL DISTKEY,
                            year INT NOT NULL,
                            duration DECIMAL NOT NULL
                        )COMPOUND SORTKEY(title, duration)
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                        (
                            artist_id VARCHAR NOT NULL PRIMARY KEY,
                            name VARCHAR(MAX) SORTKEY,
                            location VARCHAR,
                            latitude VARCHAR,
                            longitude VARCHAR
                        )diststyle all;
                    """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                        (
                            start_time TIMESTAMP NOT NULL SORTKEY PRIMARY KEY,
                            hour INT,
                            day INT,
                            week INT,
                            month INT,
                            year INT,
                            weekday INT
                        )diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
                            credentials 'aws_iam_role={}'
                            format as json {}
                            STATUPDATE ON
                            region 'us-west-2';
""").format(LOG_DATA, ARN,LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {}
                            credentials 'aws_iam_role={}'
                            format as json 'auto'
                            STATUPDATE ON
                            region 'us-west-2';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays
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
                                TIMESTAMP 'epoch' + se.ts/1000 \
                                * INTERVAL '1 second' AS start_time,
                                se.userID AS user_id,
                                se.level AS level,
                                ss.song_id AS song_id,
                                ss.artist_id AS artist_id,
                                se.sessionId AS session_id,
                                se.location AS location,
                                se.userAgent AS user_agent
                            FROM staging_events AS se
                            JOIN staging_songs AS ss 
                            ON (se.artist = ss.artist_name)
                            AND (se.song = ss.title)
                            AND (se.length = ss.duration)
                            WHERE se.page = 'NextSong';
                        """)

user_table_insert = ("""INSERT INTO users
                        (
                            user_id,
                            first_name,
                            last_name,
                            gender,
                            level
                        )
                        SELECT DISTINCT
                            se.userID AS user_id,
                            se.firstName AS first_name,
                            se.lastName AS last_name,
                            se.gender AS gender,
                            se.level AS level
                        FROM staging_events AS se
                        WHERE se.UserID is NOT NULL
                        AND se.page = 'NextSong';                 
                    """)

song_table_insert = ("""INSERT INTO songs
                        (
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration                        
                        )
                        SELECT DISTINCT
                            ss.song_id AS song_id,
                            ss.title AS title,
                            ss.artist_id AS artist_id,
                            ss.year AS year,
                            ss.duration AS duration
                        FROM staging_songs AS ss
                    """)

artist_table_insert = ("""INSERT INTO artists
                            (
                                artist_id,
                                name,
                                location,
                                latitude,
                                longitude                              
                            )
                            SELECT DISTINCT
                                ss.artist_id AS artist_id,
                                ss.artist_name AS name,
                                ss.artist_location AS location,
                                ss.artist_latitude AS latitude,
                                ss.artist_longitude AS longitude
                            FROM staging_songs AS ss
                        """)
        

time_table_insert = ("""INSERT INTO time
                        (
                            start_time,
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday                           
                        )
                        SELECT 
                            TIMESTAMP 'epoch' + se.ts/1000 \
                            * INTERVAL '1 second' AS start_time,
                            EXTRACT(HOUR FROM start_time) AS hour,
                            EXTRACT(DAY FROM start_time) AS day,
                            EXTRACT(WEEK FROM start_time) AS week,
                            EXTRACT(MONTH FROM start_time) AS month,
                            EXTRACT(YEAR FROM start_time) AS year,
                            EXTRACT(WEEKDAY FROM start_time) AS weekday
                        FROM staging_events AS se
                        WHERE se.page = 'NextSong';
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,artist_table_drop, time_table_drop,song_table_drop,user_table_drop,songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
