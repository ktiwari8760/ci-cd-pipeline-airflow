
# Create a Spark session
# Define the path where the files are located
# Read CSV files
# Filter the records which got completed
# Write back to GCS with the date naming convention

from pyspark.sql import SparkSession
from datetime import datetime , timedelta
import argparse
import json

with open('config.json' , 'r') as file:
    config = json.load(file)

directory = config['directory']

def main(date):
    spark = SparkSession.builder.appName("Read the file according to date").getOrCreate()
    inputpath = f"{directory}/input_files/orders_{date}.csv"
    outputpath = f"{directory}/output_files/orders_{date}.csv"
    orders_data = spark.read.format("csv").option("inferSchema" , "true").option("header" , "true").load(inputpath)
    filtered_data = orders_data.filter(orders_data.order_status == "Completed")
    filtered_data.write.format("csv").mode("overwrite").option("header" , "true").save(outputpath)
    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process date argument')
    parser.add_argument('--date', type=str, required=True, help='Date in yyyymmdd format')
    args = parser.parse_args()
    
    main(args.date)
