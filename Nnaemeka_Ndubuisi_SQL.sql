-- Databricks notebook source
-- MAGIC %md
-- MAGIC # TASK 1
-- MAGIC
-- MAGIC ##### Using Spark SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Loading the Dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC import csv
-- MAGIC from io import StringIO
-- MAGIC import pandas as pd
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Defining the Clinicaltrial schema
-- MAGIC Clinical_Schema = StructType([
-- MAGIC     StructField("Id", StringType(), True),
-- MAGIC     StructField("Study Title", StringType(), True),
-- MAGIC     StructField("Acronym", StringType(), True),
-- MAGIC     StructField("Status", StringType(), True),
-- MAGIC     StructField("Conditions", StringType(), True),
-- MAGIC     StructField("Interventions", StringType(), True),
-- MAGIC     StructField("Sponsor", StringType(), True),
-- MAGIC     StructField("Collaborators", StringType(), True),
-- MAGIC     StructField("Enrollment", StringType(), True),
-- MAGIC     StructField("Funder Type", StringType(), True),
-- MAGIC     StructField("Type", StringType(), True),
-- MAGIC     StructField("Study Design", StringType(), True),
-- MAGIC     StructField("Start", StringType(), True),
-- MAGIC     StructField("Completion", StringType(), True)])
-- MAGIC
-- MAGIC
-- MAGIC # Dataframe
-- MAGIC Clinical_rdd = sc.textFile("/FileStore/tables/clinicaltrial_2023.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Defining the Pharma schema
-- MAGIC Pharma_Schema = StructType([
-- MAGIC     StructField("Company", StringType(), True),
-- MAGIC     StructField("Parent_Company", StringType(), True),
-- MAGIC     StructField("Penalty_Amount", StringType(), True), 
-- MAGIC     StructField("Subtraction_From_Penalty", StringType(), True),
-- MAGIC     StructField("Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting", StringType(), True),
-- MAGIC     StructField("Penalty_Year", StringType(), True),
-- MAGIC     StructField("Penalty_Date", StringType(), True),
-- MAGIC     StructField("Offense_Group", StringType(), True),
-- MAGIC     StructField("Primary_Offense", StringType(), True),
-- MAGIC     StructField("Secondary_Offense", StringType(), True),
-- MAGIC     StructField("Description", StringType(), True),
-- MAGIC     StructField("Level_of_Government", StringType(), True),
-- MAGIC     StructField("Action_Type", StringType(), True),
-- MAGIC     StructField("Agency", StringType(), True),
-- MAGIC     StructField("Civil/Criminal", StringType(), True),
-- MAGIC     StructField("Prosecution_Agreement", StringType(), True),
-- MAGIC     StructField("Court", StringType(), True),
-- MAGIC     StructField("Case_ID", StringType(), True),
-- MAGIC     StructField("Private_Litigation_Case_Title", StringType(), True),
-- MAGIC     StructField("Lawsuit_Resolution", StringType(), True),
-- MAGIC     StructField("Facility_State", StringType(), True),
-- MAGIC     StructField("City", StringType(), True),
-- MAGIC     StructField("Address", StringType(), True),
-- MAGIC     StructField("Zip", StringType(), True),
-- MAGIC     StructField("NAICS_Code", StringType(), True),
-- MAGIC     StructField("NAICS_Translation", StringType(), True),
-- MAGIC     StructField("HQ_Country_of_Parent", StringType(), True),
-- MAGIC     StructField("HQ_State_of_Parent", StringType(), True),
-- MAGIC     StructField("Ownership_Structure", StringType(), True),
-- MAGIC     StructField("Parent_Company_Stock_Ticker", StringType(), True),
-- MAGIC     StructField("Major_Industry_of_Parent", StringType(), True),
-- MAGIC     StructField("Specific_Industry_of_Parent", StringType(), True),
-- MAGIC     StructField("Info_Source", StringType(), True),
-- MAGIC     StructField("Notes", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Dataframe
-- MAGIC Pharma_rdd = sc.textFile("/FileStore/tables/pharma.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cleaning the data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Cleaning Clinicaltrial_2023
-- MAGIC # Removing commas, double quotes and splitting the data
-- MAGIC Cleaned_Clinical_rdd = Clinical_rdd.map(lambda line: line.replace(',,', ',').replace('"', ''))\
-- MAGIC                                    .map(lambda x: x.split('\t'))\
-- MAGIC                                    .map(lambda row: row[:-1] + [row[-1].split(',')[0]] if row[-1] else row)  
-- MAGIC
-- MAGIC # Replace missing or empty values with 'NA', ensuring the row length matches the schema
-- MAGIC Cleaned_Clinical_rdd = Cleaned_Clinical_rdd.map(lambda row: ['NA' if not value else value for value in row])\
-- MAGIC                                            .map(lambda row: row if len(row) == len(Clinical_Schema.fields) else row + ['NA'] * (len(Clinical_Schema.fields) - len(row)))
-- MAGIC
-- MAGIC
-- MAGIC # Retrieving and Filtering out the header
-- MAGIC header_Clinical_rdd = Cleaned_Clinical_rdd.first() 
-- MAGIC Cleaned_Clinical_rdd = Cleaned_Clinical_rdd.filter(lambda row: row != header_Clinical_rdd)  
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC Cleaned_Clinical_df = spark.createDataFrame(Cleaned_Clinical_rdd, Clinical_Schema)  
-- MAGIC
-- MAGIC # View results
-- MAGIC Cleaned_Clinical_df.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Cleaning Pharma
-- MAGIC # Removing all double quotation marks from each line and handling parsing
-- MAGIC Cleaned_Pharma_rdd = Pharma_rdd.map(lambda line: next(csv.reader(StringIO(line), delimiter=',')))
-- MAGIC
-- MAGIC # Replace missing values with 'NA' for each row
-- MAGIC Cleaned_Pharma_rdd = Cleaned_Pharma_rdd.map(lambda row: ['NA' if not value else value for value in row])
-- MAGIC
-- MAGIC # Ensuring each row has exactly the number of fields as in Pharma_Schema
-- MAGIC Cleaned_Pharma_rdd = Cleaned_Pharma_rdd.map(lambda row: row if len(row) == len(Pharma_Schema.fields) else row + ['NA'] * (len(Pharma_Schema.fields) - len(row)))
-- MAGIC
-- MAGIC # Retrieving and Filtering out the header row
-- MAGIC header_Pharma_rdd = Cleaned_Pharma_rdd.first()
-- MAGIC
-- MAGIC Cleaned_Pharma_rdd = Cleaned_Pharma_rdd.filter(lambda row: row != header_Pharma_rdd)
-- MAGIC
-- MAGIC # Since there's no mention of a header to be removed for Pharma_rdd, we proceed directly to DataFrame creation
-- MAGIC Cleaned_Pharma_df = spark.createDataFrame(Cleaned_Pharma_rdd, Pharma_Schema)
-- MAGIC
-- MAGIC # Show the result to verify
-- MAGIC Cleaned_Pharma_df.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create a temporary view for Clinical
-- MAGIC Cleaned_Clinical_df.createOrReplaceTempView("Clinical_sql")
-- MAGIC
-- MAGIC # Create a temporary view for Pharma
-- MAGIC Cleaned_Pharma_df.createOrReplaceTempView("Pharma_sql")

-- COMMAND ----------

-- Viewing the first 10 rows
SELECT * 
FROM Clinical_sql LIMIT 10

-- COMMAND ----------

-- Viewing the first 10 rows
SELECT * 
FROM Pharma_sql LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Questions
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No. 1

-- COMMAND ----------

SELECT COUNT(DISTINCT `Id`) AS distinct_studies 
FROM Clinical_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No. 2

-- COMMAND ----------

SELECT `Type`, COUNT(*) AS frequency
FROM Clinical_sql
GROUP BY `Type`
ORDER BY frequency DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No. 3

-- COMMAND ----------

SELECT `Conditions`, COUNT(*) AS frequency
FROM Clinical_sql GROUP BY `Conditions`
ORDER BY frequency DESC LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No. 4

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW top_10_non_pharma_sponsors AS
SELECT
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS Rank,
    c.Sponsor,
    COUNT(*) AS trials_count
FROM Clinical_sql c
LEFT ANTI JOIN Pharma_sql p ON c.Sponsor = p.Parent_Company
GROUP BY c.Sponsor
ORDER BY trials_count DESC
LIMIT 10;

SELECT * FROM global_temp.top_10_non_pharma_sponsors;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Spark session
-- MAGIC query_results = spark.sql("SELECT * FROM global_temp.top_10_non_pharma_sponsors")
-- MAGIC
-- MAGIC # Convert the Spark DataFrame to a Pandas DataFrame
-- MAGIC pandas_df = query_results.toPandas()
-- MAGIC
-- MAGIC # Sort the Pandas DataFrame by 'trials_count' in descending order
-- MAGIC sorted_df = pandas_df.sort_values(by='trials_count', ascending=False)
-- MAGIC
-- MAGIC # Setting up the visualization using Seaborn
-- MAGIC plt.figure(figsize=(8, 6))
-- MAGIC sns.barplot(x='trials_count', y='Sponsor', data=sorted_df, palette='plasma')
-- MAGIC plt.title('Top 10 Non-Pharma Sponsors by Number of Trials')
-- MAGIC plt.xlabel('Number of Trials')
-- MAGIC plt.ylabel('Sponsor')
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No. 5

-- COMMAND ----------

SELECT SUBSTRING(Completion, 6, 2) AS Month, COUNT(*) AS Completed_Studies
FROM Clinical_sql
WHERE Status = 'COMPLETED' AND SUBSTRING(Completion, 1, 4) = '2023'
GROUP BY Month
ORDER BY Month;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Saving the query result as in df
-- MAGIC no5_result_df = spark.sql("""
-- MAGIC     SELECT SUBSTRING(Completion, 6, 2) AS Month, COUNT(*) AS Completed_Studies
-- MAGIC     FROM Clinical_sql
-- MAGIC     WHERE Status = 'COMPLETED' AND SUBSTRING(Completion, 1, 4) = '2023'
-- MAGIC     GROUP BY Month
-- MAGIC     ORDER BY Month
-- MAGIC """)
-- MAGIC
-- MAGIC # Save result as CSV
-- MAGIC no5_result_df.write.csv("/FileStore/tables/completed_studies_2023_result.csv", header=True)

-- COMMAND ----------

CREATE TABLE completed_studies_2023_results
USING csv
OPTIONS (path "dbfs:/FileStore/tables/completed_studies_2023_result.csv", header "true",
inferSchema "true")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- remember to do BI visualization.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### No. 6 (c)

-- COMMAND ----------

-- top 5 pharmaceutical companies by total penalties paid

SELECT Parent_Company, HQ_Country_of_Parent,
    CONCAT('$', FORMAT_NUMBER(Total_Penalties, 2)) AS Sum_Total_Penalties
FROM (
    SELECT Parent_Company, HQ_Country_of_Parent,
        SUM(CAST(REPLACE(SUBSTRING(Penalty_Amount, 2), ',', '') AS DECIMAL(18,2))) AS Total_Penalties
    FROM Pharma_sql
    GROUP BY Parent_Company, HQ_Country_of_Parent
    ORDER BY Total_Penalties DESC
    LIMIT 10
) AS Sorted_Companies;

-- COMMAND ----------


