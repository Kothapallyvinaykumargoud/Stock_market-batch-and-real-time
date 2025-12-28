import json 
import logging
import os
from datetime import datetime

import pandas as pd 
import numpy as np

from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger=logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVER="localhost:9092"
KAFKA_TOPIC_BATCH="stock-market-batch"
group_id="stock-market-consumer-group"

#mini config

# Access_key="minioadmin"
# Secert_key="minioadmin"
# minio_bucket="stock-market-bucket1"
# Minio_host= "localhost:9000"

s3_ACCESS_KEY=os.getenv("s3_ACCESS_KEY")
s3_Secert_key=os.getenv("s3_SECRET_KEY")
s3_BUCKET=os.getenv("s3_BUCKET")
Region= "us-east-1"
S3_ENDPOINT = "s3.amazonaws.com" # Standard AWS S3 endpoint

# def create_minio_client():
#     return Minio(
#         Minio_host,
#         access_key=Access_key,
#         secret_key=Secert_key,
#         secure=False,

#     )
def create_s3_client():
    return Minio(
        S3_ENDPOINT,
        access_key=s3_ACCESS_KEY, # Matches os.getenv("s3_ACCESS_KEY")
        secret_key=s3_Secert_key,
        secure=True,              # Required for AWS
        region=Region             # MUST use 'region=' keyword
    )

# def  ensure_bucket_exists(minio_client,bucket_name):
#     try:
#         if not minio_client.bucket_exists(bucket_name):
#             minio_client.make_bucket(bucket_name)
#             logger.info(f"Created bucket {bucket_name}")
#         else:
#             logger.info(f"bucket already exists {bucket_name}")
#     except S3Error as e:
#         logger.error(f"Error creating bucket{bucket_name}:{e}")
#         raise    

def  ensure_bucket_exists(s3_client,bucket_name):
    try:
        if not s3_client.bucket_exists(bucket_name):
            s3_client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
        else:
            logger.info(f"bucket already exists {bucket_name}")
    except S3Error as e:
        logger.error(f"Error creating bucket{bucket_name}:{e}")
        raise   

def main():
    # minio_client=create_minio_client()
    s3_client=create_s3_client()
    # ensure_bucket_exists(minio_client,minio_bucket)
    ensure_bucket_exists(s3_client,s3_BUCKET)
    
    conf={
        'bootstrap.servers':KAFKA_BOOTSTRAP_SERVER,
        'group.id':group_id,
        'auto.offset.reset':'latest',
        'enable.auto.commit':False,
    }

    consumer=Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_BATCH])

    logger.info(f"starting consumer topic {KAFKA_TOPIC_BATCH}")

    batch_data = []
    batch_size = 100
   

    try:
        while True:
            msg=consumer.poll(timeout=180.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"consumer error :{msg.error()}")
                continue

            try:
                data=json.loads(msg.value().decode("utf-8"))
                batch_data.append(data)
                
                if len(batch_data) <= batch_size:

                     
                     first_record = batch_data[0]
                     symbol=data["symbol"]
                     date=data["batch_date"]
                     year, month, day =date.split("-")

                     df=pd.DataFrame(batch_data)

                     object_name = f"raw/historical/year={year}/month={month}/day={day}/{symbol}_{datetime.now().strftime('%H%M%S')}.csv"
                     parquet_file = f"/tmp/{symbol}.csv"
                     df.to_csv(parquet_file, index=False)

                     s3_client.fput_object(

                        s3_BUCKET,
                        object_name,
                        parquet_file,

                    )
                     logger.info(f"Wrote data for {symbol} to s3://{s3_BUCKET}/{object_name}")

                     os.remove(parquet_file)

                     consumer.commit()

            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    finally:
        consumer.close()  


if __name__=="__main__":
    main()  