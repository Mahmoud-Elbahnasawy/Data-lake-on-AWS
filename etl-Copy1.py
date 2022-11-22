import configparser
from datetime import datetime
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, DateType, LongType, StructField, TimestampType



config = configparser.ConfigParser()
#config.read('dl.cfg')
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "song-data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json( input_data + song_data)

    # extract columns to create songs table
    # first define needed columns
    song_table_column_list = ["song_id","title","artist_id","year","duration"]
    
    # selecting the needed columns from the song_data
    songs_table = df.select(*song_table_column_list).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table

    
    
    
    
    # extract columns to create artists table
    # define needed columns list
    artist_table_column_list = ["artist_id","artist_name","artist_location","artist_longitude","artist_latitude"]
    
    # selecting the needed columns from the song_data
    artists_table = df.select(*artist_table_column_list).distinct()
    
    # write artists table to parquet files
    artists_table
    
    #because i need the song table in the process_log_data function so i will
    #make a return variable for this function to use it for calling the song_table file
    return song_table 


def process_log_data(spark, input_data, output_data , song_table):
    
    # get filepath to log data file
    log_data = "log-data/2018/11/*.json"    
    

    # read log data file
    df = spark.read.json( input_data + log_data )
    
    
    def type_adjuster(df):
        """THIS FUNCTION CHANGES DATA TYPES OF LOG DATA TO ITS PROPER TYPES
        INPUT : THE DATA FRAME THAT WE WANT TO CHANGE ITS COULMN
        OUTPUT : DATA FRAME WITH ITS TYPES ADJUSTED"""
        df = df.withColumn("itemInSession_c", df["itemInSession"].cast(IntegerType())).drop("itemInSession")
        df = df.withColumn("sessionId_c", df["sessionId"].cast(IntegerType())).drop("sessionId")
        df = df.withColumn("status_c", df["status"].cast(IntegerType())).drop("status")
        df = df.withColumn("as_date_C", to_utc_timestamp(from_unixtime(col("ts")/1000,'yyyy-MM-dd HH:mm:ss'),'EST')).drop("ts")
        df = df.withColumn("userId_C", df["userId"].cast(IntegerType())).drop("userId")
        return df
    
    # Calling the type_adjsuter function so that it modifies data types of log_data
    df = type_adjuster(df)
    
    
    
    # Note that i did filter song with page == "NextSong" as i am only getting users even if they did not listen to any music
    # extract columns for users table    
    #specify_needed_columns_list
    user_table_column_list = ["userId_C","firstName","lastName","gender","level"]
    
    user_table = df.select(*user_table_column_list).distinct()
    
    # write users table to parquet files
    user_table
    
    
    
    df = df.filter(df.page == 'NextSong')
    

    # create timestamp column from original timestamp column
    # here i add new columns for each field needed in the time table
    df = df.withColumn("star_time", date_format(log_data.as_date_C,"HH:mm:ss")).\
    withColumn('hour',date_format(log_data.as_date_C ,"h")).\
    withColumn('day',date_format(log_data.as_date_C ,"d")).\
    withColumn('week',weekofyear(log_data.as_date_C)).\
    withColumn('month',month(log_data.as_date_C)).\
    withColumn('year',year(log_data.as_date_C)).\
    withColumn('week_day',dayofweek(log_data.as_date_C)).\
    select("star_time","hour","day","week","month","year","week_day").\
    dropDuplicates()
    
    """
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    """
    
    # extract columns to create time table
    time_table = df
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song-data/*/*/*/*.json" )
    
    
    df.show(5)
    
    
    
    
    
    
    # selecting needed columns
    song_df = song_df.select("")

    # extract columns from joined song and log datasets to create songplays table 
    #songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    
    output_data = ""
    
    song_table = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data ,song_table)


if __name__ == "__main__":
    main()
