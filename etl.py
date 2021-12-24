import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t
from pyspark.sql import functions as F



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['Udacity_KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Udacity_KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id","title","artist_id","year","duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.option("header",True) \
           .partitionBy("artist_id","year") \
           .mode("overwrite") \
           .parquet(output_data+"songs/")

    # extract columns to create artists table
    artists_table = df.select(df.artist_id, \
                              col("artist_name").alias("name"),\
                              col("artist_location").alias("location"), \
                              col("atist_latitude").alias("latitude"), \
                              col("artist_longitude").alias("longitude"))

    
    # write artists table to parquet files
    artists_table.write.save(output_data+"artists/", format="parquet", header=True)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+"log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.select("*").where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),\
                            col("firstName").alias("first_name"), \
                            col("lastName").alias("last_name"), \
                            df.gender, df.level)

    
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
    time_table.write.save(output_data+"time/", format="parquet", header=True)

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.length == song_df.duration), "inner") \
                    .select(df.start_time, col("userId").alias("user_id"), df.level, \
                            song_df.song_id, song_df.artist_id, col("sessionId").alias("session_id"), \
                            df.location, col("userAgent").alias("user_agent")) \
                    .withColumn("songplay_id",F.monotonically_increasing_id())

    songplays_table = songplays_table.select("songplay_id","user_id","level","song_id","artist_id","session_id","location","user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.save(output+"songplays/", format="parquet", header=True)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacityexercisealeaume/P4/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
