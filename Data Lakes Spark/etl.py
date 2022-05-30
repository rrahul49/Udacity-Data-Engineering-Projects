import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

#Credentials removed from dl.config prior to submission

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3 and processes it using spark by extracting the songs and artist tables
        and then loads them back to S3 in separate folders
            
    """
    # get filepath to song data file
    
    #local workspace filepath for unzipped files
    #song_data = input_data + 'song-data/song_data/*/*/*/*.json'
    
    #s3 file path
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    #song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
  

    songs_table = spark.sql("""
                            SELECT s.song_id, 
                            s.title,
                            s.artist_id,
                            s.year,
                            s.duration
                            FROM song_data_table s
                            WHERE song_id IS NOT NULL
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')
    
    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT a.artist_id, 
                                a.artist_name,
                                a.artist_location,
                                a.artist_latitude,
                                a.artist_longitude
                                FROM song_data_table a
                                WHERE a.artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 and processes it by extracting the songs and artist tables
        and then again loads it back to S3. Also, the output of songs_table from song_data function is joined with log_data 
        to build songplays_table which is then written back to s3
            
    """
    # get filepath to log data file
    
    #local workspace filepath for unzipped files
    #log_path = input_data + 'log-data/*.json'
    
    #s3 filepath
    log_path = input_data + 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # created log view to write SQL Queries
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT u.userId as user_id,
                            u.firstName as first_name,
                            u.lastName as last_name,
                            u.gender as gender,
                            u.level as level
                            FROM log_data_table u
                            WHERE u.userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                            t.start_time_sub as start_time,
                            hour(t.start_time_sub) as hour,
                            dayofmonth(t.start_time_sub) as day,
                            weekofyear(t.start_time_sub) as week,
                            month(t.start_time_sub) as month,
                            year(t.start_time_sub) as year,
                            dayofweek(t.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(tst.ts/1000) as start_time_sub
                            FROM log_data_table tst
                            WHERE tst.ts IS NOT NULL
                            ) t
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(lt.ts/1000) as start_time,
                                month(to_timestamp(lt.ts/1000)) as month,
                                year(to_timestamp(lt.ts/1000)) as year,
                                lt.userId as user_id,
                                lt.level as level,
                                st.song_id as song_id,
                                st.artist_id as artist_id,
                                lt.sessionId as session_id,
                                lt.location as location,
                                lt.userAgent as user_agent

                                FROM log_data_table lt
                                JOIN song_data_table st on lt.artist = st.artist_name and lt.song = st.title
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')
    

def main():
    spark = create_spark_session()
    
    #s3 filepaths below
    
    input_data = "s3a://udacity-dend/"
    #output_data = "s3a://udacity-dend/dloutput/"
    
    #Writing to public s3 bucket on personal aws account due to s3 access error on udacity-dend (exhausted udacity aws credits)
    output_data = "s3a://rahulravi-test/dloutput/"
    
    #workspace locations below using files unzipped from ./data
    
    #input_data = "./data/"
    #output_data = "./dloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
