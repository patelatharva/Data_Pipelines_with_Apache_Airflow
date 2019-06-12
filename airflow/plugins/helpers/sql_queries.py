class SqlQueries:
    
    truncate_table = ("""
        TRUNCATE {}
    """)
    
    copy_csv_to_redshift = ("""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1
        DELIMITER ','
        region 'us-west-2'
    """)
    copy_json_to_redshift = ("""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as json 'auto'
        region 'us-west-2'
    """)
    copy_json_with_json_path_to_redshift = ("""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
        region 'us-west-2'
    """)
    songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
                events.start_time, 
                events.userid as user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid as session_id, 
                events.location, 
                events.useragent as user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong' AND userid IS NOT NULL) events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT distinct userid as user_id,
        firstname as first_name,
        lastname as last_name,
        gender, 
        level
        FROM staging_events
        WHERE page='NextSong' AND userid IS NOT NULL
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT distinct artist_id, 
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude, 
            artist_longitude as longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time,
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time),        
            extract(month from start_time),
            extract(year from start_time), 
            extract(dayofweek from start_time) as weekday
        FROM songplays
    """)
    
    count_of_nulls_in_songs_table = ("""
        SELECT count(*) as result
        FROM songs
        WHERE NULL in (song_id)
    """)
    
    count_of_nulls_in_artists_table = ("""
        SELECT count(*) as result
        FROM artists
        WHERE NULL in (artist_id)
    """)
    
    count_of_nulls_in_users_table = ("""
        SELECT count(*) as result
        FROM users
        WHERE NULL in (user_id)
    """)
    
    count_of_nulls_in_time_table = ("""
        SELECT count(*) as result
        FROM time
        WHERE NULL in (start_time, "hour", "month", "year", "day", "weekday")
    """)
    
    count_of_nulls_in_songplays_table = ("""
        SELECT count(*) as result
        FROM songplays
        WHERE NULL in (songplay_id)
    """)