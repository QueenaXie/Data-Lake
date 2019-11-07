import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.drop_duplicates().write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'),'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.drop_duplicates().partitionBy('artist_id').parquet(os.path.join(out_data, 'song'),'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/2018/11/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df['ts', 'userId', 'level','song', 'artist', 'sessionId', 'location', 'userAgent']

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    
    # write users table to parquet files
    users_table.drop_duplicates().write.partitionBy('userId').parquet(os.path.join(out_data, 'log'),'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x))
    df = df.withColumn('timestamp', get_ts(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : str(datetime.fromtimestamp(int(x)/1000.0)))
    df = df.withColumn('start_time', get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.select('timestamp',
                           hour('datatime').alias('hour'),
                           day('datatime').alias('day'),
                           week('datatime').alias('week'),
                           month('datatime').alias('month'),
                           year('datatime').alias('year'),
                           date_format('datetime’, 'F').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.drop_duplicates().write.partitionBy('timestamp').parquet(os.path.join(out_data, 'log'),'overwrite')

    # read in song data to use for songplays table
    song_df = df['title', 'artist_id', 'artist_name']

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (ldf. artist == song_df. artist_id) & (df.artist == song_df. artist_name) ,"left_outer")
        .select(
              df.timestamp,
              col('userId').alias('user_id'),
              col('level').alias('level'),
              col('song_id').alias('song_id'),
              col('artist_id').alias('artist_id'),
              col('session_id').alias('session_id'),
              col('location').alias('location'),
              col('user_agent').alias('user_agent')
        )
   df = df[df.page == 'NextSong']

    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.partitionBy('year','month').parquet(os.path.join(out_data, 'log'),'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://queena-bucket-dend/"
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
