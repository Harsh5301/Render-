# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

file_path = "dbfs:/FileStore/tables/synthetic_covid_data_realistic.csv"
spark_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Convert Spark DataFrame to pandas DataFrame
covid_data = spark_df.toPandas() 
 
# Convert Date column to datetime
covid_data['Date'] = pd.to_datetime(covid_data['Date'])
 
# Set the style for seaborn
sns.set(style="whitegrid")
 
# Plot cases over time
plt.figure(figsize=(14, 8))
sns.lineplot(data=covid_data, x='Date', y='Cases', hue='Country')
plt.title('COVID-19 Cases Over Time by Country')
plt.ylabel('Total Cases')
plt.xlabel('Date')
plt.xticks(rotation=45)
plt.legend(title='Country')
plt.tight_layout()
plt.savefig('/dbfs/FileStore/covid_cases_over_time_python.png')
plt.show()
 
# Plot deaths over time
plt.figure(figsize=(14, 8))
sns.lineplot(data=covid_data, x='Date', y='Deaths', hue='Country')
plt.title('COVID-19 Deaths Over Time by Country')
plt.ylabel('Total Deaths')
plt.xlabel('Date')
plt.xticks(rotation=45)
plt.legend(title='Country')
plt.tight_layout()
plt.savefig('/dbfs/FileStore/covid_deaths_over_time_python.png')
plt.show()
