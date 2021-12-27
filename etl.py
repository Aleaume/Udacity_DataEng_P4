import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from pyspark.sql import types as t
from pyspark.sql import functions as F



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['Udacity_KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Udacity_KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():

    """
    Description: This function is responsible for initiating the Spark session with aws haddop package.

    Arguments:

    Returns:
        spark: the initiated spark session.
    """
        
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    Description: This function is responsible for reading the json song files in the input s3 bucket,
                and creating song & artist parquet files from it in the output s3 bucket.
    
    Arguments:
        spark: the running spark session.
        input_data: s3 input file path.
        output_data: s3 output file path
        
    Returns:
        None
    """
    
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    song_data_schema = StructType([
        StructField("artist_id",StringType(),False),\
        StructField("artist_latitude",StringType(),True),\
        StructField("artist_longitude",StringType(), True),\
        StructField("artist_location", StringType(), True),\
        StructField("artist_name",StringType(), False),\
        StructField("song_id",StringType(),False),\
        StructField("title",StringType(), False),\
        StructField("duration",DoubleType(), False),\
        StructField("year",IntegerType(), False),\
    
    ])

    df = spark.read.json(song_data, song_data_schema)

    # extract columns to create songs table
    songs_table = df.select(["song_id","title","artist_id","year","duration"]).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.option("header",True) \
           .partitionBy("year","artist_id") \
           .mode("overwrite") \
           .parquet(output_data+"songs/")

    # extract columns to create artists table
    artists_table = df.select(df.artist_id, \
                              col("artist_name").alias("name"),\
                              col("artist_location").alias("location"), \
                              col("artist_latitude").alias("latitude"), \
                              col("artist_longitude").alias("longitude")) \
                      .distinct()

    
    # write artists table to parquet files
    artists_table.write.save(output_data+"artists/", format="parquet", header=True)


def process_log_data(spark, input_data, output_data):
    
    """
    Description: This function is responsible for reading the json log files in the input s3 bucket,
                and creating users, time & songplays parquet files from it in the output s3 bucket.
    
    Arguments:
        spark: the running spark session.
        input_data: s3 input file path.
        output_data: s3 output file path
        
    Returns:
        None
    """
    
    # get filepath to log data file
    log_data = input_data+"log_data/*/*/*.json"

    # read log data file
    log_data_schema = StructType([
        StructField("artist",StringType(),False),\
        StructField("auth",StringType(),True),\
        StructField("firstName",StringType(), True),\
        StructField("gender", StringType(), True),\
        StructField("itemInSession",LongType(), False),\
        StructField("lastName",StringType(),True),\
        StructField("length",DoubleType(), False),\
        StructField("level",StringType(), True),\
        StructField("location",StringType(), True),\
        StructField("method",StringType(), True),\
        StructField("page",StringType(), False), \
        StructField("registration",DoubleType(), True), \
        StructField("sessionId",LongType(), True), \
        StructField("song",StringType(), False), \
        StructField("status",LongType(), True), \
        StructField("ts",LongType(), False), \
        StructField("userAgent",StringType(), True), \
        StructField("userId",StringType(), True), \

    ])

    df = spark.read.json(log_data, log_data_schema)
    
    # filter by actions for song plays
    df = df.select("*").where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),\
                            col("firstName").alias("first_name"), \
                            col("lastName").alias("last_name"), \
                            df.gender, df.level) \
                    .distinct()

    
    # write users table to parquet files
    users_table.write.save(output_data+"users/", format="parquet", header=True)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), t.TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), t.DateType())
    df = df.withColumn("date_time", to_date(df.start_time))
    
    # extract columns to create time table
    df = df.withColumn("hour", hour(col("start_time"))) \
    .withColumn("day", dayofmonth(col("start_time"))) \
    .withColumn("week", weekofyear(col("start_time"))) \
    .withColumn("month", month(col("start_time"))) \
    .withColumn("year", year(col("start_time"))) \
    .withColumn("weekday", date_format(col("start_time"),"EEEE"))
    
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.option("header",True) \
           .partitionBy("year","month") \
           .mode("overwrite") \
           .parquet(output_data+"time/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.length == song_df.duration), "inner") \
                    .join(time_table,(df.start_time == time_table.start_time), "inner") \
                    .select(df.start_time, col("userId").alias("user_id"), df.level, \
                            song_df.song_id, song_df.artist_id, col("sessionId").alias("session_id"), \
                            df.location, col("userAgent").alias("user_agent"), \
                            time_table.year, time_table.month) \
                    .withColumn("songplay_id",F.monotonically_increasing_id()) \
                    .distinct()

    songplays_table = songplays_table.select("songplay_id","user_id","level","song_id","artist_id","session_id","location","user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.option("header",True) \
           .partitionBy("year","month") \
           .mode("overwrite") \
           .parquet(output_data+"songplays/")

def main():
    
    """
    Description: This main function is responsible for calling all functions previously defined in order.
                    This also where we can define the the input & output s3 locations.
    
    Arguments:
        None
        
    Returns:
        None
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacityexercisealeaume/P4/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
