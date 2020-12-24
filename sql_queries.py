import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS "staging_events" (
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSession smallint,
lastName varchar,
length double precision,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
sessionId integer,
song varchar,
status smallint,
ts timestamp,
userAgent varchar,
userId integer 
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS "staging_songs" (
artist_id varchar,
artist_latitude double precision,
artist_location varchar(max),
artist_longitude double precision,
artist_name varchar(max),
duration double precision,
num_songs smallint,
song_id varchar,
title varchar(max),
year smallint
)
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
copy "staging_events"
from {}
iam_role '{}' 
json {}
TIMEFORMAT AS 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy "staging_songs"
from {}
iam_role '{}'
json 'auto'; 
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
