import configparser


# config

config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# drop tables

staging_events_table_drop = "drop table if exists stg_events"
staging_songs_table_drop = "drop table if exists stg_songs"
songplay_table_drop = "drop table if exists f_songplays"
user_table_drop = "drop table if exists d_users"
song_table_drop = "drop table if exists d_songs"
artist_table_drop = "drop table if exists d_artists"
time_table_drop = "drop table if exists d_time"

# create tables

staging_events_table_create= ("""create table if not exists stg_events(
                                 artist text,
                                 auth text,
                                 first_name text,
                                 gender char(1),
                                 item_session integer,
                                 last_name text,
                                 length numeric,
                                 level text,
                                 location text,
                                 method text,
                                 page text,
                                 registration numeric,
                                 session_id integer,
                                 song text,
                                 status integer,
                                 ts bigint,
                                 user_agent text,
                                 user_id integer
                            )
""")

staging_songs_table_create = ("""create  table if not exists stg_songs(
                                 num_songs integer,
                                 artist_id text,
                                 artist_latitude numeric,
                                 artist_longitude numeric,
                                 artist_location text,
                                 artist_name text,
                                 song_id text,
                                 title text,
                                 duration numeric,
                                 year integer
                            )
""")


songplay_table_create = ("""create table if not exists f_songplays(
                            songplay_id int identity(1,1) primary key,
                            start_time timestamp not null,
                            user_id integer not null,
                            level text,
                            song_id text not null,
                            artist_id text not null,
                            session_id integer not null,
                            location text,
                            user_agent text
                        )
""")

user_table_create = ("""create table if not exists d_users(
                        user_id integer primary key,
                        first_name text not null,
                        last_name text not null,
                        gender char(1),
                        level text
                    )
""")

song_table_create = ("""create table if not exists d_songs(
                        song_id text primary key,
                        title text,
                        artist_id text not null,
                        year integer,
                        duration numeric
                    )
""")

artist_table_create = ("""create table if not exists d_artists(
                          artist_id text primary key,
                          name text,
                          location text,
                          latitude numeric,
                          longitude numeric
                       )
""")

time_table_create = ("""create table if not exists d_time(
                        start_time timestamp primary key,
                        hour integer,
                        day integer,
                        week integer,
                        month integer,
                        year integer,
                        weekday integer
                    )
""")

# copy data from s3 into stage tables
staging_events_copy = (f"""copy stg_events 
                           from {log_data}
                           iam_role {iam_role}
                           json {log_jsonpath};
                       """)

staging_songs_copy = (f"""copy stg_songs 
                          from {song_data} 
                          iam_role {iam_role}
                          json 'auto';
                      """)

# load data to final tables

songplay_table_insert = ("""insert into f_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            select  timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.user_id, se.level, 
                                    ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
                            from stg_events se, stg_songs ss
                            where se.page = 'NextSong' and
                            se.song =ss.title and
                            se.artist = ss.artist_name and
                            se.length = ss.duration
                        """)

user_table_insert = ("""insert into d_users(user_id, first_name, last_name, gender, level)
                        select distinct  user_id, first_name, last_name, gender, level
                        from stg_events
                        where page = 'NextSong'
""")

song_table_insert = ("""insert into d_songs(song_id, title, artist_id, year, duration)
                        select song_id, title, artist_id, year, duration
                        from stg_songs
                        where song_id is not null
""")

artist_table_insert = ("""insert into d_artists(artist_id, name, location, latitude, longitude)
                          select distinct artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
                          from stg_songs
                          where artist_id is not null
""")

time_table_insert = ("""insert into d_time(start_time, hour, day, week, month, year, weekday)
                        select start_time, extract(hour from start_time), extract(day from start_time),
                               extract(week from start_time), extract(month from start_time),
                               extract(year from start_time), extract(dayofweek from start_time)
                        from f_songplays
""")

# query lists

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]