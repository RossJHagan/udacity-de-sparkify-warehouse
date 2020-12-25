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
time_table_drop = "DROP TABLE IF EXISTS times;"

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
artist_location varchar,
artist_longitude double precision,
artist_name varchar(1000),
duration double precision,
num_songs smallint,
song_id varchar,
title varchar(1000),
year smallint
) DISTSTYLE even;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS "songplays" (
    songplay_id bigint IDENTITY(0,1) NOT NULL,
    songplay_timestamp timestamp NOT NULL,
    user_id varchar NOT NULL,
    user_level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id int,
    location varchar,
    user_agent varchar,
    foreign key(user_id) references users(user_id),
    foreign key(song_id) references songs(song_id),
    foreign key(artist_id) references artists(artist_id),
    foreign key(songplay_timestamp) references times(start_time)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS "users" (
    user_id varchar sortkey distkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar,
    primary key(user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS "songs" (
    song_id varchar sortkey distkey,
    title varchar(1000),
    artist_id varchar,
    year smallint,
    duration double precision,
    primary key(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS "artists" (
    artist_id varchar sortkey distkey,
    name varchar(1000),
    location varchar(1000),
    latitude double precision,
    longitude double precision,
    primary key(artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS "times" (
    start_time timestamp sortkey distkey,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint,
    primary key(start_time)
);
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
json 'auto'
truncatecolumns
compupdate off statupdate off; 
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO "songplays" (songplay_timestamp, user_id, user_level, song_id, artist_id, session_id, location, user_agent)
SELECT se1.ts, se1.userid, se1.level, s.song_id, a.artist_id, se1.sessionid, se1.location, se1.useragent
FROM staging_events se1
JOIN artists a ON a.name = se1.artist
JOIN songs s ON s.title = se1.song AND s.duration = se1.length
WHERE userid is NOT NULL
AND page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO "users" (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid, firstName, lastName, gender, level
FROM staging_events se1
WHERE userid IS NOT NULL
AND page = 'NextSong'
AND ts = (SELECT max(ts) FROM staging_events se2 WHERE page = 'NextSong' AND se2.userid = se1.userid)
GROUP BY userid, firstName, lastName, gender, level;
""")

song_table_insert = ("""
INSERT INTO "songs" (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs ss1
GROUP BY song_id, title, artist_id, year, duration;
""")

artist_table_insert = ("""
INSERT INTO "artists" (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs ss1
WHERE year = (SELECT MAX(year) FROM staging_songs ss2 WHERE ss1.artist_id = ss2.artist_id )
AND duration = (SELECT MAX(duration) FROM staging_songs ss2 WHERE ss1.artist_id = ss2.artist_id AND ss1.year = ss2.year)
GROUP BY artist_id, artist_name, artist_location, artist_latitude, artist_longitude;
""")

time_table_insert = ("""
INSERT INTO "times" (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
EXTRACT(hour FROM ts),
EXTRACT(day FROM ts),
EXTRACT(week FROM ts),
EXTRACT(month FROM ts),
EXTRACT(year FROM ts),
EXTRACT(weekday FROM ts)
FROM staging_events se1
WHERE se1.page = 'NextSong'
GROUP BY se1.ts;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop,  songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, staging_songs_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
