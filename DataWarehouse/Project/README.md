<h2>Introduction</h2>

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL Pipeline that extracts their data from S3, stages it in Redshift and then transforms the data into a set of dimensional and fact Tables for the analytics Team to continue finding Insights to what songs their users are listening to.

<h2>Project Description</h2>

Application of Data warehouse concepts and AWS to build an ETL Pipeline for a database hosted on Redshift. Data needs to be loaded from S3 into staging tables on Redshift and then SQL Statements that create fact and dimension tables from these staging tables to create analytics are executed.

<h2>Project Datasets</h2>

Song Data Path     -->     s3://udacity-dend/song_data

Log Data Path      -->     s3://udacity-dend/log_data

Log Data JSON Path -->     s3://udacity-dend/log_json_path.json

<b>Song Dataset</b>

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
For example:

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

<b>Log Dataset</b>

The second dataset consists of log files in JSON format. The log files in the dataset with are partitioned by year and month.
For example:

log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

<h2>Schema for Song Play Analysis</h2>

A Star Schema would be required for optimized queries on song play queries

<b>Fact Table</b>

<b>songplays</b> - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

<b>Dimension Tables</b>

<b>users</b> - users in the app
user_id, first_name, last_name, gender, level

<b>songs</b> - songs in music database
song_id, title, artist_id, year, duration

<b>artists</b> - artists in music database
artist_id, name, location, lattitude, longitude

<b>time</b> - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

<h2>Project Template</h2>

Project Template includes four files:

<b>1. create_table.py</b> is where you'll create your fact and dimension tables for the star schema in Redshift.

<b>2. etl.py</b> is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

<b>3. sql_queries.py</b> is where you'll define you SQL statements, which will be imported into the two other files above.

<b>4. README.md</b> is where you'll provide discussion on your process and decisions for this ETL pipeline.

<b>Create Table Schema</b>

1. Write a SQL CREATE statement for each of these tables in sql_queries.py
2. Complete the logic in create_tables.py to connect to the database and create these tables
3. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
4. Launch a redshift cluster and create an IAM role that has read access to S3.
5. Add redshift database and IAM role info to dwh.cfg.
6. Test by running create_tables.py and checking the table schemas in your redshift database.

<b>Build ETL Pipeline</b>

1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
3. Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
4. Delete your redshift cluster when finished.

<h2>Final Instructions</h2>

1. Import all the necessary libraries
2. Write the configuration of AWS Cluster, store the important parameter in some other file
3. Configuration of boto3 which is an AWS SDK for Python
4. Using the bucket, can check whether files log files and song data files are present
5. Create an IAM User Role, Assign appropriate permissions and create the Redshift Cluster
6. Get the Value of Endpoint and Role for put into main configuration file
7. Authorize Security Access Group to Default TCP/IP Address
8. Launch database connectivity configuration
9. Go to Terminal write the command "python create_tables.py" and then "etl.py"
10. Should take around 4-10 minutes in total
11. Then you go back to jupyter notebook to test everything is working fine
12. I counted all the records in my tables
13. Now can delete the cluster, roles and assigned permission
