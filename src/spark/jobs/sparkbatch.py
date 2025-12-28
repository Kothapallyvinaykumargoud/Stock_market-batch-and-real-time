import os
import sys 
import traceback
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Access_key="minioadmin"
# Secert_key="minioadmin"
minio_bucket="stock-market-bucket1"
# Minio_host= "http://minio:9000"

def sparksession():
    print("initializing spark session with s3 configuration ")
    spark=(SparkSession.builder
       .appName("stock batch data")
       .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
       .getOrCreate())

    confg=spark.sparkContext._jsc.hadoopConfiguration()

    confg.set("fs.s3a.access.key","minioadmin")
    confg.set("fs.s3a.secret.key","minioadmin")
    confg.set("fs.s3a.endpoint","http://localhost:9000")
    confg.set("fs.s3a.path.style.access", "true")
    confg.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    confg.set("fs.s3a.connection.ssl.enabled", "false")
    confg.set("fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")
    print("created connection successfully with s3")

    return spark

def read_data_from_s3(spark,date):
    print("reading data from s3")

    if date is None:
        processed_date=datetime.now()
    else:
        processed_date=datetime.strptime(date,"%y-%m-%d")

    year=processed_date.year
    month=processed_date.month
    day=processed_date.day

    print(processed_date)

    path=f"s3a://{minio_bucket}/raw/historical/year={2025}/month={12:02d}/day={16:02d}/"

    try:
        df=spark.read.option("header","True").option("infraSchema","true").csv(path)
        print("Sample data:")
        df.show(5, truncate=False)
        df.printSchema()
        return df
    except Exception as e:
        print(f"error reading data from {path} {e}")    

def proccessed_stock_data(df):
    print("started procsessing the s3 data")

    if df is None or df.count()==0:
        print("there is no data to processes")
        return None

    try:

        record_count=df.count()

        print(f"proccessed {record_count} records ")

        window=Window.partitionBy('symbol','date')

        df=df.withColumn('daily_open',F.first("open").over(window))
        df=df.withColumn('daily_high',F.max("high").over(window))
        df=df.withColumn('daily_low',F.min("low").over(window))
        df=df.withColumn('daily_volume',F.sum("volume").over(window))
        df=df.withColumn('daily_close',F.first("close").over(window))

        df = df.withColumn("daily_change", (F.col("daily_close") - F.col("daily_open"))/F.col("daily_open") * 100)

        print("data processed")

        df.select("symbol","date", "daily_open", "daily_high", "daily_low", "daily_volume", "daily_close", "daily_change").show(5)

        return df
    except Exception as e:
        print(f"failed to processes data {e}")
        return None


def write_to_s3(df,date=None):

    if df is None:
        print("no process data written to s3")

    if date is None:
        processed_date=datetime.now().strftime("%y-%-m-%d")

    output_path = f"s3a://{minio_bucket}/processed/historical/date={processed_date}"
    print(f"writing data to s3 path {output_path}")

    try:
        df=df.write.partitionBy('symbol').mode('overwrite').parquet(output_path)
        print(f"written data successfully to s3")
        return df
    except Exception as e:
        print(f"failed to write data to {output_path}")
        return None



def main():

    """Main Function to process historical data"""
    print("\n=============================================")
    print("STARTING STOCK MARKET BATCH PROCESSOR")
    print("=============================================\n")
    
    date=None
    spark=sparksession()

    try:
        df=read_data_from_s3(spark,date)
        
        if df is not None:
            processed_data=proccessed_stock_data(df)

        if processed_data is not None:
            write_to_s3(processed_data)
            print("wrote data to s3 successfully")
        else:
            ("error processing the data")

    except Exception as e:
        print(f"Error occurred: {str(e)}")

    finally:
        print("\nStopping Spark Session")
        if spark is not None:

            spark.stop()
            print("\n=============================================")
            print("BATCH PROCESSING COMPLETE")
            print("=============================================\n")            

        
if __name__ == "__main__":
    main()