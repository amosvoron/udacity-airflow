class SqlQueries:
    """Class that contains the SQL code for inserting data."""
    
    songplays_insert = ("""
        INSERT INTO public.songplays
        (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT 
            TIMESTAMP 'epoch' + (A.ts/1000)::BIGINT * INTERVAL '1 second'
            , A.userid::INTEGER
            , A.level
            , B.song_id
            , B.artist_id
            , A.sessionid
            , A.location
            , A.useragent
        FROM staging_events AS A
        INNER JOIN staging_songs AS B
            ON A.song = B.title
            AND A.artist = B.artist_name
        WHERE A.page = 'NextSong'
    """)
    
    users_insert = ("""
        INSERT INTO public.users
        (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT A.userid::INTEGER, A.firstname, A.lastname, A.gender, A.level
        FROM staging_events AS A
        INNER JOIN (
            SELECT userid, MAX(ts) AS ts_last
            FROM staging_events
            GROUP BY userid
        ) AS B
            ON A.userid = B.userid
            AND A.ts = B.ts_last
        WHERE A.page = 'NextSong'
            AND A.userid IS NOT NULL;             
    """)

    songs_insert = ("""
        INSERT INTO public.songs
        (song_id, title, artist_id, year, duration)
        SELECT song_id, MIN(title), MIN(artist_id), MIN(year), MIN(duration)
        FROM staging_songs
        GROUP BY song_id;            
    """)

    artists_insert = ("""
        INSERT INTO public.artists
        (artist_id, name, location, latitude, longitude)
        SELECT artist_id, MIN(artist_name), MIN(artist_location), MIN(artist_latitude), MIN(artist_longitude)
        FROM staging_songs
        GROUP BY artist_id;
    """)

    time_insert = ("""    
        INSERT INTO public.time
        (start_time, hour, day, week, month, year, weekday)
        SELECT A.start_time
            , EXTRACT(hour FROM A.start_time)
            , EXTRACT(day FROM A.start_time)
            , EXTRACT(week FROM A.start_time)
            , EXTRACT(month FROM A.start_time)
            , EXTRACT(year FROM A.start_time)
            , EXTRACT(weekday FROM A.start_time)        
        FROM (
            SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000)::BIGINT 
                * INTERVAL '1 second' AS start_time
            FROM staging_events
            WHERE page = 'NextSong'
        ) AS A
    """)