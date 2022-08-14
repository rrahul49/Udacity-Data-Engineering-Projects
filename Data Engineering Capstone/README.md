# Data Engineering Capstone Project

## Scope
The purpose of this project is to develop an ETL pipeline using Airflow, constructing a data warehouse through Redshift databases and S3 data storage as well as defining efficient data models e.g. star schema. In this project we will use US immigration data, focusing on the type of visas being issued and the profiles associated. The scope of this project is limited to the data sources listed below with data being aggregated across numerous dimensions such as visatype, gender, port_of_entry, nationality and month.

## Data Description & Sources
- I94 Immigration Data: This data comes from the US National Tourism and Trade Office found [here](https://travel.trade.gov/research/reports/i94/historical/2016.html). Each report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).
- World Temperature Data: This dataset came from Kaggle found [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
- U.S. City Demographic Data: This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. Dataset comes from OpenSoft found [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
- Airport Code Table: This is a simple table of airport codes and corresponding cities. The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the ICAO airport code which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia). It comes from [here](https://datahub.io/core/airport-codes#data).

Below is an ERD diagram of the immigration fact table and various dimension tables after extracting and creating the tables in Redshift
<img src="Schema.PNG"/>

## Data Storage
Data was stored in S3 buckets in a collection of CSV and PARQUET files. The immigration dataset extends to several million rows and thus this dataset was converted to PARQUET files to allow for easy data manipulation and processing.<br><br>

## ETL Pipeline
Defining the data model and creating the star schema involves various steps. Using Airflow we extract files from S3 buckets, transform the data and then writing CSV and PARQUET files to Redshift as highlighted below in the ETL Dag graph and Tree views. These steps include:
- Extracting data from SAS Documents and writing as CSV files to S3 immigration bucket
- Extracting remaining CSV and PARQUET files from S3 immigration bucket
- Writing CSV and PARQUET files from S3 to Redshift
- Performing data quality checks on the newly created tables
<img src="DAG_Graphview.png"/>
<img src="DAG_Treeview.PNG"/>
