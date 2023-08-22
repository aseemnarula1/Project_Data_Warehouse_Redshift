import configparser

print("Entering the SQL_Queries module")

# Config File 
config = configparser.ConfigParser()
config.read('dwh.cfg')


# Getting and fetching the S3 bukcets details and IAM role details into the local variables
LOG_DATA  = config.get("S3", "LOG_DATA")
LOG_PATH  = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE  = config.get("IAM_ROLE","ARN")

# Printing the values of the variables for S3 bucket and IAM role details
print("Log Data--->",LOG_DATA);
print("Song Data--->",SONG_DATA);
print("IAM Role---->",IAM_ROLE);
print("Log Path---->",LOG_PATH);

# Dropping the Facts and Dimension tables if already exist, this also include the staging events and songs table as well

print("Starting the dropping and re-creating the facts and dimension tables")

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

print("Finishing the dropping and re-creating the facts and dimension tables")

# Creating the Stage Events Table

print("Creating the Staging Events table")

staging_events_table_create= ("""

CREATE TABLE staging_events
(
        artist            VARCHAR(450),
        auth              VARCHAR(450),
        firstName         VARCHAR(450),
        gender            VARCHAR(50),
        ItemInSession     INTEGER,
        lastName          VARCHAR(450),
        length            FLOAT,
        level             VARCHAR(450),
        location          VARCHAR(450),
        method            VARCHAR(450),
        page              VARCHAR(450),
        registration      VARCHAR(450),
        sessionId         INTEGER,
        song              VARCHAR(450),
        status            INTEGER,
        ts                BIGINT, 
        userAgent         VARCHAR(450), 
        userId            INTEGER     
)                     

""")

# Creating the Stage Songs Table

print("Creating the Staging Songs table")

staging_songs_table_create = ("""

CREATE TABLE staging_songs
(
        song_id            VARCHAR(256) PRIMARY KEY,
        artist_id          VARCHAR(256),
        artist_latitude    FLOAT,
        artist_longitude   FLOAT,
        artist_location    VARCHAR(450),
        artist_name        VARCHAR(256),
        duration           FLOAT,
        num_songs          INT,
        title              VARCHAR(500),
        year               INT
)

""")

# Creating the Songs Play Fact Table

print("Creating the Songs Play Fact table")

songplay_table_create = ("""

CREATE TABLE songplays
(

        songplay_id         INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time          TIMESTAMP NOT NULL sortkey distkey,
        user_id             INTEGER NOT NULL,
        level               VARCHAR(50),
        song_id             VARCHAR(500) NOT NULL,
        artist_id           VARCHAR(500) NOT NULL,
        session_id          INTEGER,
        location            VARCHAR(500),
        user_agent          VARCHAR(500)
)
    
""")


# Creating Users Dimension Table

print("Creating the Users Dimension table")

user_table_create = ("""

CREATE TABLE users
(
        user_id            VARCHAR(500) PRIMARY KEY NOT NULL,
        first_name         VARCHAR(500),
        last_name          VARCHAR(500),
        gender             VARCHAR(50),
        level              VARCHAR(500)
)

""")

# Creating the Songs Dimension Table

print("Creating the Songs Dimension table")

song_table_create = ("""

CREATE TABLE songs
(
        song_id           VARCHAR(500) PRIMARY KEY NOT NULL,
        title             VARCHAR(500) NOT NULL,
        artist_id         VARCHAR(500) NOT NULL,
        year              INTEGER,
        duration          FLOAT NOT NULL
)

""")

# Creating the Artists Dimension Table 

print("Creating the Artists Dimension table")

artist_table_create = ("""

CREATE TABLE artists
(
        artist_id         VARCHAR(500) PRIMARY KEY NOT NULL,
        name              VARCHAR(500),
        location          VARCHAR(500),
        latitude          VARCHAR(500),
        longitude         VARCHAR(500)
)

""")

# Creating the Time Dimension Table 

print("Creating the Time Dimension table")

time_table_create = ("""

CREATE TABLE time
(
         start_time      TIMESTAMP PRIMARY KEY,
         hour            INTEGER, 
         day             INTEGER, 
         week            INTEGER, 
         month           INTEGER, 
         year            INTEGER,
         weekday         INTEGER
)

""")

# Staging Events Copy Table

print("Creating the Staging Events Copy table")

staging_events_copy = ("""

    copy staging_events from {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format       as JSON {path}
    timeformat   as 'epochmillisecs'

""").format(bucket=LOG_DATA, role=IAM_ROLE, path=LOG_PATH)
#format( config.get( 'S3','LOG_DATA'), 
#              config.get('IAM_ROLE', 'ARN'),
#              config.get('S3', 'LOG_JSONPATH'))
#.format(bucket=LOG_DATA, role=IAM_ROLE, path=LOG_PATH)

# Staging Songs Copy Table

print("Creating the Staging Songs Copy table")


staging_songs_copy = ("""

    copy staging_songs from {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format       as JSON 'auto'
    TRUNCATECOLUMNS 
    BLANKSASNULL 
    EMPTYASNULL

""").format(bucket=SONG_DATA, role=IAM_ROLE)
#.format( config.get( 'S3','LOG_DATA'), 
#              config.get('IAM_ROLE', 'ARN'),
#              config.get('S3', 'LOG_JSONPATH'))
#.format(bucket=SONG_DATA, role=IAM_ROLE)


# Songplay Table Insert Statements

print("Inserting table data into the Songplays table")

songplay_table_insert = ("""

INSERT INTO songplays
(
        start_time
        ,user_id
        ,level
        ,song_id
        ,artist_id
        ,session_id
        ,location
        ,user_agent
)      
SELECT   se.ts
        ,se.userid
        ,se.level
        ,ss.song_id
        ,ss.artist_id
        ,se.sessionid
        ,se.location
        ,se.useragent
FROM    staging_events  se
JOIN    staging_songs   ss  
ON      ss.artist_name  =   se.artist
AND     ss.title    =   se.song


""")

#User Table Insert Statements

print("Inserting data into the users table for page 'NextSong' ")

user_table_insert = ("""

INSERT INTO users
(
         user_id
        ,first_name
        ,last_name
        ,gender
        ,level
) 
SELECT
         se3.userid
        ,se3.firstname
        ,se3.lastname
        ,se3.gender
        ,se3.level
FROM    
(   
SELECT  se.userid
        ,MAX(se.ts) as  max_time_stamp
FROM    staging_events se
WHERE   se.page =   'NextSong'
GROUP BY    se.userid           
)       se2
JOIN    staging_events  se3 
ON      se3.userid  =   se2.userid
AND     se3.ts      =   se2.max_time_stamp
AND     se3.page    =   'NextSong'

""")

#Song Table Insert Statements

print("Inserting data into the users table for page 'NextSong' ")


song_table_insert = ("""

INSERT INTO songs
(     
       song_id
      ,title
      ,artist_id
      ,year
      ,duration
) 
SELECT
       ss.song_id
       ,ss.title
       ,ss.artist_id
       ,ss.year
       ,ss.duration
FROM    
      staging_songs ss
""")

#Artist Table Insert Statements

#### Fetching the de-duplicated "DISTINCT" records from the staging songs tables #####

print(" Fetching the de-duplicated 'DISTINCT' records from the staging songs tables ")

artist_table_insert = ("""

INSERT INTO artists
    ( artist_id  
      ,name
      ,location   
      ,latitude   
      ,longitude
    )SELECT DISTINCT 
            ss.artist_id
            ,ss.artist_name
            ,ss.artist_location
            ,ss.artist_latitude
            ,ss.artist_longitude
    FROM    staging_songs ss

""")

# Time Table Insert Statements

####  Extracting the Hour, Day, Week, Month, Year, Day of the Week each separately from the timestamp column from staging events table
####  Also, selecting the "DISTINCT" de-duplicated records from the staging events for the page "NexSong"


print("Extracting the Hour, Day, Week, Month, Year, Day of the Week each separately from the timestamp column from staging events table")

time_table_insert = ("""

INSERT INTO time
    (   start_time
        ,hour
        ,day
        ,week
        ,month
        ,year
        ,weekday
    )SELECT se2.ts
            ,EXTRACT(HOUR  FROM se2.ts)
            ,EXTRACT(DAY   FROM se2.ts)
            ,EXTRACT(WEEK  FROM se2.ts)
            ,EXTRACT(MONTH FROM se2.ts)
            ,EXTRACT(YEAR  FROM se2.ts)
            ,EXTRACT(DOW   FROM se2.ts)
    FROM(   SELECT DISTINCT se.ts 
            FROM        staging_events se
            WHERE   se.page     =   'NextSong'
        )   se2

""")

## 1. Create Table Queries
## 2. Drop Table Queries
## 3. Copy Table Queries

print("Running final table queries")

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


print("Exiting the sql queries module")
