# DROP TABLES
class DeleteCreateTables:
    staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
    staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
    songplay_table_drop = "DROP TABLE IF EXISTS songplays"
    user_table_drop = "DROP TABLE IF EXISTS users"
    song_table_drop = "DROP TABLE IF EXISTS songs"
    artist_table_drop = "DROP TABLE IF EXISTS artists"
    time_table_drop = "DROP TABLE IF EXISTS time"

    # CREATE TABLES

    staging_events_table_create= """
    CREATE TABLE IF NOT EXISTS staging_events(
            artist TEXT,
            auth TEXT,
            firstName TEXT,
            gender TEXT,
            ItemInSession INT,
            lastName TEXT,
            length FLOAT,
            level TEXT,
            location TEXT,
            method TEXT,
            page TEXT,
            registration TEXT,
            sessionId INT,
            song TEXT,
            status INT,
            ts BIGINT, 
            userAgent TEXT, 
            userId INT
    )
    """

    staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs(
            song_id TEXT PRIMARY KEY,
            artist_id TEXT,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location TEXT,
            artist_name VARCHAR(255),
            duration FLOAT,
            num_songs INT,
            title TEXT,
            year INT
        )
    """

    songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays(
            songplay_id         integer identity(0,1) primary key,
            start_time          timestamp not null sortkey distkey,
            user_id             integer not null,
            level               varchar,
            song_id             varchar not null,
            artist_id           varchar not null,
            session_id          integer,
            location            varchar,
            user_agent          varchar
        )
    """

    user_table_create = """
    CREATE TABLE IF NOT EXISTS users(
            user_id VARCHAR PRIMARY KEY NOT NULL,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            level VARCHAR
        )
    """

    song_table_create = """
    CREATE TABLE IF NOT EXISTS songs(
            song_id VARCHAR PRIMARY KEY NOT NULL,
            title VARCHAR NOT NULL,
            artist_id VARCHAR NOT NULL,
            year INT,
            duration FLOAT
        )
    """

    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
            artist_id VARCHAR PRIMARY KEY NOT NULL,
            name VARCHAR,
            location VARCHAR,
            latitude VARCHAR,
            longitude VARCHAR
        )
    """)

    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
        (
            start_time  timestamp not null distkey sortkey primary key,
            hour        integer not null,
            day         integer not null,
            week        integer not null,
            month       integer not null,
            year        integer not null,
            weekday     varchar not null
        ) 
    """)

    create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
    drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
