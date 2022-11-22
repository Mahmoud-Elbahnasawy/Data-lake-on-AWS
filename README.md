Sparkify data lake on AWS

Summary of the project
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into a new S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

Project architecture
1 - Source S3 bucket (from which we will extract our data)
2 - Pyspark to handle a huge amount of data set on an EMR cluster and wrote on an EMR notebook on AWS
3 - Destination S3 bucket (to which we will write our tables as parquet partition files)


Schema discussion

1 - Source S3 :
    In this bucket we had two directories :
        a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
2 - dimensional modeling
    As the final purpose of this process is to Analyze data 
    I choose to make a star schema model as its ease of use and optimization
    so i designed four dimension tables (user , artist , time , songs) and one facttable that is songplay
    
3 - I had to make some changes to data types as automaic schema detection (inferSchema) might not be properly working
4 - I had to save each table as a parquet file as its the one of the best forms to save big data 
5 - I had to create an s3 bucket to save data into
6 - Each table had its own folder and some was partition by some its columns to avoid data skewness
7 - We can detect data skewness by counting number of distinct values



# How to run Python script

The file is named etl-Copy1.py
we have to case to run this file
First : locally or on Workspace
In this you have to adjsut the directory as the file are not on the same path when compared to the files on S3
all you have to is go the main function and uncomment the directory for the local mode , you also have to change that directory by uncommenting it on Prcocess_log_data and process_song_data_functions
Second : On AWS
The etl-Copy1.py is adjusted for this case (you don't have to do any this just run)
