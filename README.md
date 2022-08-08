# 1. PURPOSE

Sparkify data resides in AWS S3 in 2 directories containing:
- logs on user activity on the app
- metadata on the songs in the app

In order to improve data analytics, it is advised to build Redshift
data warehouse together with ETL pipeline extracting data from S3 bucket
and eventually creating a star schema architecture allowing for easy quering.

# 2. MANUAL

In order to perform inteded actions, as describe in Purpose section, 2 python files 
need to be executed by the user and STRICTLY in the following order: create_tables.py and etl.py. 
To do so:
1. Open command line terminal
2. Navigate the terminal to the directory where the script is located using the cd command.
3. Type "python create_tables.py" in the terminal to execute the script.
4. Confirm create_tables.py did not return any errors.
5. Type "python etl.py" in the terminal to execute the script and make sure no errors were returned.

# 3. STRUCTURE

The repository consists of the following:

## 3.1 sql_queries.py

This python script stores all queries that are being called by create_tables.py and etl.py.
The scope covers connection to aws, creation of tables, inserting the data from JSON files
into staging tables and finally building star schema with fact table and dimension tables.

## 3.2 create_tables.py

The script is ultimately responsible for creation of tables defined in sql_queries.
To do so, it follows the steps below:
- establishes connection with AWS and gets cursor to it
- drops the existing tables (if those exist)
- creates new sparkify database with staging and final tables

## 3.3 etl.py

ETL script extracts data from JSON files, loads it into staging tables followed by 
moving the data to final star schema.

## 3.4 dwh.cfg

Stores Redshift database and IAM role info. 

## 3.5 README.md

Yes. 

# 4. SCHEMA & PIPELINE

Sparkify Redshift database Schema is of a Star type, providing best results for OLAP.
This architecture shall allow for fast aggregations and simple queries (also those not yet foreseen).

## 4.1 Staging Tables
Before the data lands in the final tables, it is initially loaded inside the staging tables:
- staging_events
            artist, auth, firstName, gender, itemInSession, lastName, length, level, location,
            method, page, registration, ressionId, song, status, ts, userAgent, userId

-staging_songs
            num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, 
            song_id, title, duration, year

## 4.2 Final Schema

The final schema consists of:
- centre Fact table:
    songplays - records in event data associated with song plays i.e. records with page NextSong
                songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- 4 dimension tables:
    users - users in the app
                user_id, first_name, last_name, gender, level
    songs - songs in music database
                song_id, title, artist_id, year, duration
    artists - artists in music database
                  artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
                  start_time, hour, day, week, month, year, weekday

# 5. EXAMPLE QUERIES

# 5.1 Getting song title and count of its plays

SELECT title, COUNT(*) FROM songplays sp 
JOIN songs s ON sp.song_id = s.song_id
GROUP BY title