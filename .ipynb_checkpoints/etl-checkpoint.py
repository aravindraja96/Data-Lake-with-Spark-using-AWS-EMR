import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types 

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
""" This function creates spark session """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
"""This Function Loads Songs Data from S3 Bucket , Processes Data and Writes Songs and Artist Data as Parquet File to S3 Bucket"""
    # get filepath to song data file
    song_data = input_data+ 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songdata")

    # extract columns to create songs table
    songs_table = spark.sql(
             """SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
                FROM   songdata """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data+"songs_table/")

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT artist_id        AS artist_id,
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS latitude,
                artist_longitude AS longitude
    FROM   songdata
    """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artists_table/")


def process_log_data(spark, input_data, output_data):
"""This Function Loads Log Data from S3 Bucket , Processes Data and Writes Users,Time and SongPlays Data as Parquet File to S3 Bucket"""
    # get filepath to log data file
    log_data =input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')
    df.createOrReplaceTempView("events")

    # extract columns for users table    
    user_table =spark.sql("""SELECT DISTINCT userid,
                firstname,
                lastname,
                gender,
                level
FROM   events
WHERE  userid IS NOT NULL """) 
    
    # write users table to parquet files
    user_table.write.mode("overwrite").parquet(output_data+"user_table/")

    @udf(returnType= types.StringType())
    def convertDatetime(ts):
        return datetime.fromtimestamp(ts / 1000.0)\
                       .strftime('%Y-%m-%d %H:%M:%S')
    df_time = df.withColumn("datetime", \
                        convertDatetime("ts"))
    df_time.createOrReplaceTempView("timetemp")
    # extract columns to create time table
    time_table = spark.sql("""
                           SELECT  DISTINCT datetime AS start_time,
                                             hour(datetime) AS hour,
                                             day(datetime)  AS day,
                                             weekofyear(datetime) AS week,
                                             month(datetime) AS month,
                                             year(datetime) AS year,
                                             dayofweek(datetime) AS weekday
                            FROM timetemp
                            """)
    time_table.printSchema()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite")\
    .partitionBy("year", "month")\
    .parquet(output_data+"time_table/")

    # read in song data to use for songplays table
    song_df = input_data+ 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_df)
    song_df.createOrReplaceTempView("songstemp")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =spark.sql("""SELECT monotonically_increasing_id() as songplay_id,
                                        timetemp.datetime AS start_time,
                                        month(timetemp.datetime) as month,
                                        year(timetemp.datetime) as year,
                                        timetemp.userId as user_id,
                                        timetemp.level as level,
                                        songstemp.song_id as song_id,
                                        songstemp.artist_id as artist_id,
                                        timetemp.sessionId as session_id,
                                        timetemp.location as location,
                                        timetemp.userAgent as user_agent
                                FROM timetemp 
                                JOIN songstemp  
                                on timetemp.artist = songstemp.artist_name and timetemp.song = songstemp.title""") 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite")\
    .partitionBy("year", "month")\
    .parquet(output_data+"songplays_table/")


def main():
""" This is main function """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mybucketusingaws/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
