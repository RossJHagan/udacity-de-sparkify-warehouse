# Sparkify Song Plays ETL pipeline

This pipeline receives json formatted song data and json formatted song play log events, transforming them to a
queryable data store optimised for song play analysis.

## Why

Sparkify needs to be able to analyse what songs users are listening to.  This presents the data in a format that allows
a more flexible structure in fact and dimension tables for analysis.

## Transformation Assumptions

Artist data - to achieve uniqueness of record, we use the most recent data
for a given artist selected by year and longest song duration from the staging songs table.

User data - we use the most recent user data by log event timestamp when extracting a single
user.

## Schema Design

A star schema revolving around facts and dimensions is used here to achieve a data store
that is more flexible for analysis.

The fact table `songplays` can be joined with the dimensions: `time`, `artists`, `songs`, and `users` to constrain
data for analysis.

## Pipeline

We have pre-existing data sources in the form of `json` files in AWS S3 storage.

Our destination is an AWS redshift data warehouse.

Populating these will follow these steps:

- Extract using `COPY` from s3 files into redshift staging tables.
- Use the `INSERT ... SELECT FROM ...` pattern to transform and load data from staging into the star schema.

## Querying

```sql
-- Query song titles for songs played on thanksgiving day 2018 (22nd November 2018)
SELECT songs.title AS song_title
FROM songplays JOIN songs ON songplays.song_id = songs.song_id 
WHERE songplay_timestamp >= '2018-11-22 00:00:00' AND songplay_timestamp < '2018-11-23 00:00:00';
```

## Dependencies

Python 3.7 or later.  Python 3.8.6 was used in development.

## Run it!

`python create_tables.py && python etl.py`

## Data Quality - Verify uniqueness

The below queries can be used to verify the respective dimension tables have unique entries in order to be sure
the primary key description reflects reality of the underlying data given redshift does not enforce this constraint.

```redshift
SELECT DISTINCT song_id, COUNT(song_id) AS num_song_ids
FROM songs
GROUP BY song_id
ORDER BY num_song_ids DESC;

SELECT DISTINCT artist_id, COUNT(artist_id) AS num_artist_ids
FROM artists
GROUP BY artist_id
ORDER BY num_artist_ids DESC;

SELECT DISTINCT start_time, COUNT(start_time) as num_start_times
FROM times
GROUP BY start_time
ORDER BY num_start_times DESC;

SELECT DISTINCT user_id, COUNT(user_id) AS num_user_ids
FROM users
GROUP BY user_id
ORDER BY num_user_ids DESC;
```

