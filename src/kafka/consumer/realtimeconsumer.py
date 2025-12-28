import json 
import logging
import os
from datetime import datetime
import time
from io import StringIO
import io

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

Kafka_bootstrap_server="localhost:29092"
kafka_real_topic=os.getenv("KAFKA_TOPIC_REALTIME")
kafka_group_id=os.getenv("KAFKA_GROUP_REAL_ID")

Access_key="minioadmin"
Secert_key="minioadmin"
minio_bucket="stock-market-bucket1"
Minio_host= "localhost:9000"


def create_minio_client():

    return Minio(
        Minio_host,
        access_key=Access_key,
        secret_key=Secert_key,
        secure=False
    )

def ensure_bucket_exists(minio_client,bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"successfully created bucket with name {bucket_name}")
        else:
            logger.info(f"bucket already exists {bucket_name}")
    except Exception as e:
        logger.error(f"failed to create bucket {bucket_name} {e}")
        raise

def main():
    minio_client=create_minio_client()

    ensure_bucket_exists(minio_client,minio_bucket)

    DEFAULT_BATCH_SIZE = 100
    flush_time = time.time()
    flush_interval = 60 #sec
    messages = []

    conf={
        "bootstrap.servers":Kafka_bootstrap_server,
        "group.id":kafka_group_id,
        'auto.offset.reset':'earliest',
        'enable.auto.commit':False,
    }

    consumer=Consumer(conf)

    consumer.subscribe([kafka_real_topic])

    try:
        while True:
            msg=consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                logger.error(f"message error occured for {msg.error}")
                continue

            try:
                key=msg.key().decode("utf-8") if msg.key() else None
                value=json.loads(msg.value().decode("utf-8"))

                messages.append(value)

                

                if len(messages) % 10 ==0:
                    logger.info(f"consumed {len(messages)} for the current batch")

                current_time=time.time()

                if ((len(messages)) > DEFAULT_BATCH_SIZE or (current_time-flush_time > flush_interval and len(messages)>0)):
                    
                    df=pd.DataFrame(messages)

                    now=datetime.now()
                    timestamp=now.strftime("%Y%m%d_%H%M%S")

                    object_name = (f"raw/realtime/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/stock_data_{timestamp}.csv")
                    
                    csv_buffer=StringIO()
                    df.to_csv(csv_buffer,index=False)
                    csv_data=csv_buffer.getvalue()
                    csv_bytes=csv_data.encode("utf-8")
                    stream_data=io.BytesIO(csv_bytes)
                    stream_data.seek(0)
                    data_length = len(csv_bytes)

                    try:
                        minio_client.put_object(
                            bucket_name=minio_bucket,
                            object_name=object_name,
                            data=stream_data,
                            length=data_length,
                        )

                        msg_count=len(messages)
                        logger.info(f"Wrote data for {msg_count} to s3://{minio_bucket}/{object_name}")
                    except S3Error as e:
                        logger.error(f"failed to put data into bucket {minio_bucket} {e}")
                        raise

                    messages = []
                    flush_time = time.time() 
            except Exception as e:
                logger.error(f"Error processing message: {e}")

                     
    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    finally:
        consumer.close()


if __name__=="__main__":
    main()






            

