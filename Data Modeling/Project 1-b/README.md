<h1>Project 1-b: Data Modeling with Cassandra</h1>

<h2>Introduction:</h2>
    
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. The objectiver is to is to create a database using Apache Cassandra for this analysis.

<h2>Project Overview:</h2>

In this project, we apply Data Modeling with Apache Cassandra and complete an ETL pipeline using Python. We are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

<h2>Datasets:</h2>

<b>event_data :</b>directory of CSV files partitioned by date. Below are examples of filepaths in the dataset:
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv

<h2>Project Template:</h2>

The project template includes one Jupyter Notebook file, in which:
•	you will process the event_datafile_new.csv dataset to create a denormalized dataset
•	you will model the data tables keeping in mind the queries you need to run
•	you have been provided queries that you will need to model your data tables for
•	you will load the data into tables you create in Apache Cassandra and run your queries

<h2>Project Steps:</h2>

Below are steps you can follow to complete each component of this project.

<b>Modelling your NoSQL Database or Apache Cassandra Database:</b>
    
1.	Design tables to answer the queries outlined in the project template
2.	Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
3.	Develop your CREATE statement for each of the tables to address each question
4.	Load the data with INSERT statement for each of the tables
5.	Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. We recommend you also include DROP TABLE statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
6.	Test by running the proper select statements with the correct WHERE clause

<b>Build ETL Pipeline:</b>
1.	Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
2.	Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT three statements to load processed records into relevant tables in your data model
3.	Test by running three SELECT statements after running the queries on your database
4.	Finally, drop the tables and shutdown the cluster

<h2>Project Artifacts:</h2>

<b>Project_1B.ipynb:</b> Jupyter notebook file to importing the files, generate a new csv file and loading all csv files into one. We then use the generated csv dataset to load data into the database and verify the results by executing specific queries against the Apache Cassandra database.

<b>Event_datafile_new.csv:</b> New dataset that is the combination of all the files in the folder event_data

<b>Event_Data Folder:</b> Source data folder containing all input files


