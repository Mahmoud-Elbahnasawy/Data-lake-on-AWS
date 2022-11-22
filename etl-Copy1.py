import configparser
from datetime import datetime
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, unix_timestamp, to_utc_timestamp, from_unixtime, to_date, dayofweek
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, DateType, LongType, StructField, TimestampType



config = configparser.ConfigParser()
#config.read('dl.cfg')
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    THIS FUNCTION CREATES A SPARK SESSION
    INPUT : NONE
    OUTPUT : SPARK SESSION
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    """spark = SparkSession \
        .builder \
        .getOrCreate()"""
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    THIS FUNCTION READS SONG DATA AND CREATES THE SONG AND ARTIST TABLES ALSO SAVES THE OUTPUT TABLE TO AN S3 BUCKET AS A PARQUET FILE
    INPUT : 
    SPARK : WHICH IS A SPARK SESSION
    INPUT_DATA : WHICH IS THE PATH OR URL FOR THE SOURCE DATA DIRECTORY 
    OUTPUT_DATA : TO INDICATES WHERE THE FINAL OUTPUT WILL BE SAVE"""
    
    # get filepath to song data file
    song_data = "song-data/A/A/A/*.json"
    # simple Note :  i used this path to process only simple amount of data as the are very big and take lots of time
    
    
    
    
    # if you are running on workspace or locally you have to uncomment this 
    #song_data = "song-data/song_data/*/*/*/*.json"
    # read song data file
    path = input_data + song_data
    
    df = spark.read.json(path)

    # extract columns to create songs table
    # first define needed columns
    song_table_column_list = ["song_id","title","artist_id","year","duration"]
    
    # selecting the needed columns from the song_data
    songs_table = df.select(*song_table_column_list).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    output_data = "s3://datalakeprojectegfwddataengineeringudacitymahmoud/songs/"
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data)

    
    
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #just for showing and i will erase it as soon as possible
    print(50*"+" , "here is the songs_table", 50*"+")
    songs_table.limit(1).show(1)
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    
    # extract columns to create artists table
    # define needed columns list
    artist_table_column_list = ["artist_id","artist_name","artist_location","artist_longitude","artist_latitude"]
    
    # selecting the needed columns from the song_data
    artists_table = df.select(*artist_table_column_list).distinct()
    
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #just for showing and i will erase it as soon as possible
    print(50*"+" , "here is the artists_table", 50*"+")
    artists_table.limit(1).show(1)
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    # write artists table to parquet files
    
    output_data = "s3://datalakeprojectegfwddataengineeringudacitymahmoud/artists/"
    artists_table.write.mode("overwrite").parquet(output_data)
    
    #because i need the song table in the process_log_data function so i will
    #make a return variable for this function to use it for calling the song_table file
    #return songs_table 


def process_log_data(spark, input_data, output_data):
    
    """
    THIS FUNCTION READS LOG DATA AND CREATES THE USER, TIME, SONGPLAY TABLES BY JOINING BETWEEN SONG_DATA AND LOG_DATA 
    ALSO SAVES THE OUTPUT TABLE TO AN S3 BUCKET AS A PARQUET FILE
    
    INPUT : 
    SPARK : WHICH IS A SPARK SESSION
    INPUT_DATA : WHICH IS THE PATH OR URL FOR THE SOURCE DATA DIRECTORY 
    OUTPUT_DATA : TO INDICATES WHERE THE FINAL OUTPUT WILL BE SAVE"""
    
    # get filepath to log data file
    # in case of cloud use this variable
    log_data = "log-data/2018/11/*.json"
    
    
    #in case of workspace or local mode use this variable
    #log_data = "log-data/*.json"
    
    path = input_data + log_data

    # read log data file
    df = spark.read.json(path)
    
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
    
    
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #just for showing and i will erase it as soon as possible
    print(50*"+" , "here is the user_table", 50*"+")
    user_table.limit(1).show(1)
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    # write users table to parquet files
    output_data = "s3://datalakeprojectegfwddataengineeringudacitymahmoud/users/"
    user_table.write.mode("overwrite").parquet(output_data)
    
    # filtering by page name == NextSong to view only the case in which users are listening to music
    df = df.filter(df.page == 'NextSong')

    # create timestamp column from original timestamp column
    # here i add new columns for each field needed in the time table
    time_table = df.withColumn("star_time", date_format(df.as_date_C,"HH:mm:ss")).\
    withColumn('hour',date_format(df.as_date_C ,"h")).\
    withColumn('day',date_format(df.as_date_C ,"d")).\
    withColumn('week',weekofyear(df.as_date_C)).\
    withColumn('month',month(df.as_date_C)).\
    withColumn('year',year(df.as_date_C)).\
    withColumn('week_day',dayofweek(df.as_date_C)).\
    select("star_time","hour","day","week","month","year","week_day").\
    dropDuplicates()
    
    """
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    """
    
    # extract columns to create time table
    #time_table = df
    
    
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #just for showing and i will erase it as soon as possible
    print(50*"+" , "here is the time_table", 50*"+")
    time_table.limit(1).show(1)
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    # write time table to parquet files partitioned by year and month
    output_data = "s3://datalakeprojectegfwddataengineeringudacitymahmoud/time/"
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data)
    
    
    # here i will start the join process to generate the songplay table

    # read in song data to use for songplays table
    song_data = "song-data/song_data/*/*/*/*.json"
    path = input_data + song_data
    song_df = spark.read.json( path )
    
    #renaming the df to become log_df
    log_df = df
    
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #just for showing and i will erase it as soon as possible
    print(50*"+" , "here is the log_df", 50*"+")
    log_df.limit(1).show(1)
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #just for showing and i will erase it as soon as possible
    print(50*"+" , "here is the song_df", 50*"+")
    song_df.limit(1).show(1)
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    
    # To generate the songplay table we have to join the song_df with the log_df
    joined_df = log_df.join(song_df, log_df.artist == song_df.artist_name, 'inner')
    
    # selecting columns from the joined dataframe
    song_play_column_list = ["songplay_id","as_date_C","userId_C","level","song_id","artist_id","sessionId_c","location","userAgent"]
    
    #adding  a column for the songplay id column using a built in function (monotonically_increasing_id)
    joined_df = joined_df.withColumn("songplay_id",(monotonically_increasing_id()+1))
    
    #appying the select statement to pick up only considered columns
    joined_df = joined_df.select(*song_play_column_list)
    
    #extracting the time from the timestamp column (as_data_C) and dropping the old column as_data_C
    joined_df = joined_df.withColumn("star_time", date_format(joined_df.as_date_C,"HH:mm:ss")).drop("as_date_C")

    
    # renaming columns
    joined_df = joined_df.withColumnRenamed("userId_C","user_id").\
    withColumnRenamed("sessionId_c","session_id")
    
    # rearranging column positions
    song_play_column_list = ["songplay_id","star_time","user_id","level","song_id","artist_id","session_id","location","userAgent"]
    
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    #just for showing and i will erase it as soon as possible
    print(50*"+" , "here is the songplays_table", 50*"+")
    joined_df = joined_df.select(*song_play_column_list)
    joined_df.limit(1).show(1)
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    # selecting needed columns
    #song_df = song_df.select("")

    # although it is not needed to add month and year in the songplay but theses two columns are required for partitioning when saving to parquet
    songplays_table = joined_df.withColumn("year",year(joined_df.star_time)).withColumn("month", month(joined_df.star_time))
    

    # write songplays table to parquet files partitioned by year and month
    output_data = "s3://datalakeprojectegfwddataengineeringudacitymahmoud/songplay/"
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data)
    


def main():
    spark = create_spark_session()
    
    #in case of cloud use this variable
    input_data = "s3a://udacity-dend/"
    #output_data = ""
    
    
    # in case of workspace or local mode use this variable
    #input_data = "data/"
    #output_data = "output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    print("Ali")


if __name__ == "__main__":
    main()
