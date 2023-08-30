**Udacity Data Engineer Nanodegree — Cloud Data Warehouse**

My name is Aseem Narula, I am currently working as a Data Engineer at NatWest Group. I have undertaken the Data Engineer Nanodegree. In this module, I will be talking about the Cloud Data Warehouse with Amazon Redshift.

**Introduction**

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a data engineer, my task is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.


**Project Datasets**


There are two datasets that reside in S3. Here are the S3 links for each:

**Song data:** 

s3://udacity-dend/song_data

**Log data**: 

s3://udacity-dend/log_data

**Log data json path**: 

s3://udacity-dend/log_json_path.json

**Song Dataset**

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song’s track ID. For example, here are file paths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json

song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.


{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist
longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

**Log Dataset**

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you’ll be working with are partitioned by year and month. For example, here are file paths to two files in this dataset.

log_data/2018/11/2018-11-12-events.json

log_data/2018/11/2018-11-13-events.json

And below is an example of what the data in a log file, 2018–11–12-events.json, looks like.


**Star Schema for SongPlay Analysis of Sparkify**

In the next step, I have designed the Star Schema for the Sparikfy for Song play data, where in we have the centralized fact table surrounded by the 4 dimension tables.

**Fact Table**

a) **songplays** — records in event data associated with song plays i.e. records with page NextSong

**column list** — songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

a) **users** — users in the app

**column list** — user_id, first_name, last_name, gender, level

b) **songs** — songs in music database

**column list** — song_id, title, artist_id, year, duration

c) **artists** — artists in music database

**column list** — artist_id, name, location, lattitude, longitude

d) **time** — timestamps of records in songplays broken down into specific units

**column list** — start_time, hour, day, week, month, year, weekday


**Project Steps**

Below are steps you can follow to complete each component of this project.

**Create Table Schemas**

1. Design schemas for your fact and dimension tables
2. Write a SQL CREATE statement for each of these tables in sql_queries.py
3. Complete the logic in create_tables.py to connect to the database and create these tables
4. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
5. Launch a redshift cluster and create an IAM role that has read access to S3.
6. Add redshift database and IAM role info to dwh.cfg.
7. Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.



**Build ETL Pipeline**
1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
3. Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
4. Delete your redshift cluster when finished.


**Note** — Since the volume of the dataset is high in the songs data set, I have filtered the dataset when being pulled from the filtered by use of the sub-folder inside the S3 bucket.

e.g. SONG_DATA=’s3://udacity-dend/song_data/A/A/B’

**GitHub Link** — aseemnarula1/Project_Data_Warehouse_Redshift: Udacity Project Data Warehouse (github.com)

**Medium Blog Link** - https://aseemnarula.medium.com/udacity-data-engineer-nanodegree-cloud-data-warehouse-ee2738c40745
 
**Reference Links** —

a) CREATE TABLE — Amazon Redshift

b) S3 Bucket Filtering — https://docs.aws.amazon.com/macie/latest/user/monitoring-s3-inventory-filter.html

c) S3 Bucket Content — https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html

d) Epoch Datetime — https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift

e) Epoch Datetime — https://saturncloud.io/blog/how-to-convert-epoch-to-datetime-in-amazon-redshift-a-stepbystep-guide/

f) COPY from S3 bucket format— https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html


**Acknowledgement**


All the datasets of Sparkify used in this Data Engineer Project are provided through Udacity and are used for my project with Udacity Data Engineer Nanodegree and reference links are also provided where the docs are referred.







