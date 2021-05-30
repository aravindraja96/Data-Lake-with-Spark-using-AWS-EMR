### Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As a data engineer, ETL pipeline was built to extracts the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.This will allow  analytics team to continue finding insights.

### Project Description
This project can load data from S3, process the data into analytics tables using Spark, and load them back into S3. The code was deployed on a cluster using **AWS EMR 3 node m3.xlarge instance**.

### Tech Stack 
**Spark** 2.4.0 on **Hadoop** 2.8.5 <br />
**Hive** 2.3.6<br />
**YARN** with **Ganglia** 3.7.2<br />
**Zeppelin** 0.8.0<br />

### Project Datasets
The datasets that reside in S3 Bucket<br />
**Song data**: s3://udacity-dend/song_data<br />
**Log data**: s3://udacity-dend/log_data

### Files Description
**etl.py** - Reads data from S3, processes that data using Spark, and writes them back to S3 bucket as a parquet file<br />
**dl.cfg** - Configuration file for AWS Credentials

### Schema
##### Fact Table
**songplays**- records in log data associated with song plays <br />
**columns**:
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

##### Dimension Tables
**users** - users in the app <br />
**columns**:
user_id, first_name, last_name, gender, level

**songs** - songs in music database <br />
**columns**:
song_id, title, artist_id, year, duration 

**artists** - artists in music database <br />
**columns**:
artist_id, name, location, latitude, longitude 

**time** - timestamps of records in songplays broken down into specific units <br />
**columns**:
start_time, hour, day, week, month, year, weekday