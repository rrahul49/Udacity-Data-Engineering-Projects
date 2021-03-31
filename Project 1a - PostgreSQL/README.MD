<h2>Introduction:</h2>

A startup called <b>Sparkify</b> wants to analyze the data they have been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

The aim is to create a Postgres Database Schema and ETL pipeline to optimize queries for song play analysis.

<h2>Project Description:</h2>

In this project, the objective is to perform data modeling with Postgres and build an ETL pipeline using Python. Fact and Dimension tables have been defined for a Star Schema for a particluar analytic focus. An ETL pipeline has been built to transfer data from local json files into Postgres tables.

<h2>Schema:</h2>
<b>Fact Table</b>

<b> songplays </b> records in log data associated with song plays

<b>Dimension Tables</b>

<b> users </b> in the app,<b> songs </b> in music database,<b> artists </b> in music database and <b> time: </b> timestamps of records in songplays broken down into specific units

<h2>Project Design:</h2>

<b>Database:</b> Optimized for analysis with few tables and querying additional specific information.

<b>ETL Design:</b> Read json files and parse accordingly to store the into relevant tables with proper formatting

<b>DB Scripts</b>

Execute "python create_tables.py" in terminal. Use test.ipynb to confirm if the tables have been created succesfully and restart kernel to reset db connection.

<b>Jupyter Notebook</b>

etl.ipynb, a Jupyter notebook is given to develop ETL processes for each table. The code in the notebook is then used accordingly in etl.py which is executed on the terminal just as create_tables.py (using "python etl.py"). We can verify our final output using the notebook test.ipynb to confirm if the data has been succesfully loaded.

<h2>Project Artifacts:</h2>

<b>test.ipynb </b>displays the first few rows of each table to verify table creation/data load status. Restart kernel to reset database connection

<b>create_tables.py </b>drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.

<b>etl.ipynb </b>reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.

<b>etl.ipynb </b>reads and processes files from song_data and log_data and loads them into the tables.

<b>sql_queries.py </b>contains all the sql queries and is imported into the files above

<h2> Project Execution Steps:</h2>

<b>Create Tables</b>
   Run create_tables.py to create your database and tables.
   Run test.ipynb to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.

<b>ETL Pipeline</b>

Execute etl.py to process the entire dataset. Run test.ipynb to confirm your records were successfully inserted into each table.
