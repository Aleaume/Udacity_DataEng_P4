# Udacity_DataEng_P4
Udacity Course, Data Engineering Nanodegree, 4th Project, Data Lake

## Requirements
The following modules / component need to be installed:
- S3 bucket
- User with full access to S3 Bucket
- Workspace with Hadoop cluser and Spark intalled.

## Overview
In this project the goal is to build an ETL pipeline to extract data from an S3 bucket, process it using Spark and then load it back to an S3 bucket in the form of parquet files properly partitioned as a set of dimensional tables. 


### Architecture

The project is composed of different cloud components and a few scripts working together as described in this diagram:

![image](https://user-images.githubusercontent.com/32632731/147497747-34817834-1e45-4649-b227-09b1c0f03293.png)


#### The S3 Buckets

An AWS Bucket made publicly available, own & managed by udacity.
s3://udacity-dend/
It contains the different dataset needed for this project, that will be picked up by the etl.py scripts and copied over into the Redshift instance.
For more details on the dataset see the section "Dataset" = > https://github.com/Aleaume/Udacity_DataEng_P3#dataset 

The second S3 bucket is a bucket I created for the purpose of this project in my AWS instance

#### Jupyter Notebook

Optional for the project, the test.ipynb notebook helps testing the different transformation and extraction steps before putting them all together in the etl script.

#### Python script

Main script of the project, it is the central piece that instantiate the Spark session, reads songs & logs json files, transform and writes back into parquet files.

#### Config file

Simple config file where the key and secret key of the AWS user with full access to the S3 Bucket.

### Dataset

#### Song Data

- The song dataset is coming from the Million Song Dataset (https://labrosa.ee.columbia.edu/millionsong/). 
Each file contains metadat about 1 song and is in json format. 
Folder structure goes as follow: song_data/[A-Z]/[A-Z]/[A-Z]/name.json 

Here is an example of the file structure:

```json

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, 
"artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", 
"title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

```

- The Song data S3 bucket endpoint is : s3://udacity-dend/song_data and the json path: s3://udacity-dend/song_data/<A-Z>/<A-Z>/<A-Z>/<filename>.json

#### Log Data

- The second dataset is generated from an event simulator (https://github.com/Interana/eventsim) based on songs in the previous dataset. Also in json it containes the logs of activity of the music streaming app.
Folder structure goes as follow : log_data/[year]/[month]/[year]-[month]-[day]-events.json
The file structure itself is similar to this:

![image](https://user-images.githubusercontent.com/32632731/141263859-72aa801e-bad3-4a23-86e4-7898c3cca585.png)

- The Song data S3 bucket endpoint is : s3://udacity-dend/log_data and the json path: s3://udacity-dend/log_data/<YYYY>/<MM>/<filename>.json

## Files Extraction & Parquet files Creation

The structure of the 2 data types is similar to the one used in another project (https://github.com/Aleaume/Udacity_DataEng_P3):
  
![image](https://user-images.githubusercontent.com/32632731/147498691-be68ad70-51b7-41af-b156-92d874e91cff.png)
  
Based on this, 2 Schema are defined when reading the data:
  
For the Songs data:
 
```python
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
  
```
  
For the Logs data:
 
```python
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
  
```

Once read we start data wrangling when needed and create Dataframes for each dimension tables:

### songs
   
No specific transformation needed. After read, we simply partition by year then artist_id while writing.
  
```python
  
songs_table = df.select(["song_id","title","artist_id","year","duration"]).distinct()

songs_table.write.option("header",True) \
           .partitionBy("year","artist_id") \
           .mode("overwrite") \
           .parquet(output_data+"songs/")

```
  

### artists
No specific transformation needed.
 
```python
 artists_table = df.select(df.artist_id, \
                              col("artist_name").alias("name"),\
                              col("artist_location").alias("location"), \
                              col("artist_latitude").alias("latitude"), \
                              col("artist_longitude").alias("longitude")) \
                      .distinct()

artists_table.write.save(output_data+"artists/", format="parquet", header=True) 
  
```
  
### users
  
  
  No transformation needed, after we make sure we filter on page = "NextSong" we can write back.
  
```python
  df = df.select("*").where(df.page == "NextSong")
  
  users_table = df.select(col("userId").alias("user_id"),\
                            col("firstName").alias("first_name"), \
                            col("lastName").alias("last_name"), \
                            df.gender, df.level) \
                    .distinct()
  users_table.write.save(output_data+"users/", format="parquet", header=True)
```
### time

For creating the time file, we use 2 lambda functions in order to a create a timestamp and a datetime column.
Also, we create additional columns out of this created timestamp as required.
  
Once all created and in order, we write the time parquet files, partitioned by year then month.
 
 ```python
 get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), t.TimestampType())
 df = df.withColumn("start_time", get_timestamp(df.ts))
 
 get_datetime = udf(lambda x: from_unixtime(x), t.DateType())
 df = df.withColumn("date_time", to_date(df.start_time))
 
 df = df.withColumn("hour", hour(col("start_time"))) \
    .withColumn("day", dayofmonth(col("start_time"))) \
    .withColumn("week", weekofyear(col("start_time"))) \
    .withColumn("month", month(col("start_time"))) \
    .withColumn("year", year(col("start_time"))) \
    .withColumn("weekday", date_format(col("start_time"),"EEEE"))
 
time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
  
time_table.write.option("header",True) \
           .partitionBy("year","month") \
           .mode("overwrite") \
           .parquet(output_data+"time/")
 
```

### songplays
  
 For songplays, we need 2 other Dataframes, time & songs. We then do 2 INNER joins in order to select all needed values.
 For the songplay_id we use the function monotonically_increasing_id() to generate a unique id.
  
 Once all created and in order, we write the time parquet files, partitioned by year then month. 
  
 ```python
 songplays_table = df.join(song_df, (df.song == song_df.title) & (df.length == song_df.duration), "inner") \
                    .join(time_table,(df.start_time == time_table.start_time), "inner") \
                    .select(df.start_time, col("userId").alias("user_id"), df.level, \
                            song_df.song_id, song_df.artist_id, col("sessionId").alias("session_id"), \
                            df.location, col("userAgent").alias("user_agent"), \
                            time_table.year, time_table.month) \
                    .withColumn("songplay_id",F.monotonically_increasing_id()) \
                    .distinct()

songplays_table.write.option("header",True) \
           .partitionBy("year","month") \
           .mode("overwrite") \
           .parquet(output_data+"songplays/")
  
```

## Handling Data types & duplicates
  
- Data types as mentioned previously are handled thanks to defined schema when reading the data.

- For handling duplicates we make sure to use the .distinct() method when selecting data, making sure that duplicates are deleted before selection.

  ![image](https://user-images.githubusercontent.com/32632731/147499850-ba6cd853-c7bc-41f3-9879-34631a4b3f2c.png)


## Execution
 
 Once the spark activities all done, we can then browse through the output S3 bucket and see all the folder stucture created with parquet files in each of them:
  
  ![image](https://user-images.githubusercontent.com/32632731/147499675-98633e23-3dc3-48c4-9da4-71c5890dd4ea.png)

![image](https://user-images.githubusercontent.com/32632731/147499704-2186e99c-4928-495c-9bb4-ed3d60f7723a.png)

  ![image](https://user-images.githubusercontent.com/32632731/147499719-02939fa5-8c8c-4536-9d56-7ace949a00c3.png)

  
## Improvement suggestions / Additional work
  
 A lot of improvement could be done to this project, here is a list of ideas, directions that could be taken:
  
 - Test the newly created structure with queries such as fetching the top 10 artists or most active users. For this we can make use even of the notebook "test" or create a specifc Notebook to log in all queries for the future and help the analysts.
  
 - Performances could be improved. Until now I haven't been able to run the script completely on the whole dataset due to the tiemout of the udacity workspace (song_data = input_data+"song_data/*/*/*/*.json")
  
 - Create a specifc EMR in AWS with an attached Notebook to run the script but also to be able to monitor performance and the sequencing of the different Stages.
