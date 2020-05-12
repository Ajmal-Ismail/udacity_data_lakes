import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# Parse project configurations
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('CREDENTIALS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('CREDENTIALS', 'AWS_SECRET_ACCESS_KEY')

LOG_DATA_PATH=config.get('PATHS', 'LOG_DATA_PATH')
SONG_DATA_PATH=config.get('PATHS', 'SONG_DATA_PATH')
OUTPUT_PATH=config.get('PATHS', 'OUTPUT_PATH')


def create_spark_session():
    """
    This functions is responsible for creating spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function is responsible for parsing songs input data and create two dimension tables song and artist.
    """
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create song data view to query from
    df.createOrReplaceTempView("tbl_song_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISTINCT 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration 
        FROM tbl_song_data
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = OUTPUT_PATH + "/song/song.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT 
            artist_id, 
            artist_name AS name, 
            artist_latitude AS latitude, 
            artist_longitude AS longitude 
        FROM tbl_song_data
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(path = OUTPUT_PATH + "/artist/artist.parquet", mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function is responsible for parsing logs input data and create remaining dimension table user and time.
    It also creats our fact table songplays by joining logs and songs data.
    """
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # create log data view to query from
    df.createOrReplaceTempView("tbl_log_data")
    
    # filter by actions for song plays
    df = spark.sql("""
        SELECT 
            *, 
            CAST(ts / 1000 AS TIMESTAMP) AS timestamp 
        FROM tbl_log_data 
        WHERE page = 'NextSong'
    """)
    
    # create filtered data view
    df.createOrReplaceTempView("tbl_log_data_filtered")

    # extract columns for users table    
    user_table = spark.sql("""
        SELECT DISTINCT 
            userId AS user_id, 
            firstName AS first_name, 
            lastName AS last_name, 
            gender, 
            level 
        FROM tbl_log_data_filtered
    """)
    
    # write users table to parquet files
    user_table.write.parquet(path = OUTPUT_PATH + "/user/user.parquet", mode = "overwrite")

    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINCT 
            timestamp AS start_time, 
            HOUR(timestamp) AS hour, 
            DAY(timestamp) AS day, 
            WEEKOFYEAR(timestamp) AS week, 
            MONTH(timestamp) AS month, 
            YEAR(timestamp) AS year, 
            WEEKDAY(timestamp) AS weekday 
        FROM tbl_log_data_filtered
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = OUTPUT_PATH + "/time/time.parquet", mode = "overwrite")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT 
            tldf.timestamp AS start_time,
            tldf.userId AS user_id,
            tldf.level,
            tsd.song_id,
            tsd.artist_id,
            tldf.sessionId AS session_id,
            tldf.location,
            tldf.userAgent AS user_agent,
            YEAR(tldf.timestamp) AS year,
            MONTH(tldf.timestamp) AS month
        FROM tbl_log_data_filtered tldf
            JOIN tbl_song_data tsd
                ON tldf.song = tsd.title AND tldf.artist = tsd.artist_name
        WHERE tldf.page = 'NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path = OUTPUT_PATH + "/songplays/songplays.parquet", mode = "overwrite")


def main():
    spark = create_spark_session()
    
    process_song_data(spark, SONG_DATA_PATH, OUTPUT_PATH)    
    process_log_data(spark, LOG_DATA_PATH, OUTPUT_PATH)


if __name__ == "__main__":
    main()
