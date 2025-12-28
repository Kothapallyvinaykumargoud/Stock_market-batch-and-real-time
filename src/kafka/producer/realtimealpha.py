import json
import os
import logging
import time
import requests
from datetime import datetime,timedelta
import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv
import random

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger=logging.getLogger(__name__)

Alpha_key=os.getenv("ALPHA_VANTAGE_API_KEY")
KAFKA_BOOTSTRAP_SERVER="localhost:29092"
KAFKA_TOPIC_STREAM=os.getenv("KAFKA_TOPIC_REALTIME")


# Define stocks with initial prices
STOCKS = {
    'AAPL': 180.0,   # Apple
    'MSFT': 350.0,   # Microsoft
    'GOOGL': 130.0,  # Alphabet (Google)
    'AMZN': 130.0,   # Amazon
    'META': 300.0,   # Meta (Facebook)
    'TSLA': 200.0,   # Tesla
    'NVDA': 400.0,   # NVIDIA
    'INTC': 35.0,    # Intel
}
class streamdatacollector:
        def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, topic=KAFKA_TOPIC_STREAM, interval=2):

            self.topic=topic
            self.logger=logger
            self.interval=interval

            self.current_stocks=STOCKS.copy()

            self.producer_cong={
                'bootstrap.servers':bootstrap_servers,
                'client.id':'continuous-stock-data-producer'
            }
            try:

                self.producer=Producer(self.producer_cong)
                self.logger.info(f"producer initialized sending to server {bootstrap_servers} and topic {topic}")
            except Exception as e:
                self.logger.error(f"failed to connect to server {bootstrap_servers}")
        def delivery_report(self, err, msg):
            if err is not None: 
                self.logger.error(f"Delivery failed for message: {msg}")
            else:
                self.logger.info(f"Message delivered successfully to topic {msg.topic} [{msg.partition()}]")

        def fetch_alpha_data(self,symbol):
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={Alpha_key}"
            try:

                response=requests.get(url)
                data=response.json()

                if "Global Quote" not in data:
                    self.logger.info(f"data is not fetched for {symbol} as global_quote missing")
                    return None

                quote=data["Global Quote"]

                stock_data = {
                'symbol': quote["01. symbol"],
                'price': float(quote["05. price"]),
                'change': float(quote["09. change"]),
                'percent_change': float(quote["10. change percent"].strip('%')),
                'volumes': int(quote["06. volume"]),
                "timestamp": datetime.now().isoformat()
                }

                return stock_data
            except Exception as e:
                self.logger.error(f"fail to fetch alpha data {e}")


        def produce_stock_data(self):
            self.logger.info(f"started fetching the data")

            try:
                while True:

                    successfull_symbols =0
                    failed_symbols=0

                    for symbol in self.current_stocks.keys():
                        try:


                            stock_data=self.fetch_alpha_data(symbol)

                            if stock_data:

                                message=json.dumps(stock_data)

                                self.producer.produce(
                                    self.topic,
                                    key=symbol,
                                    value=message,
                                    callback=self.delivery_report
                                )

                                # Trigger message delivery
                                self.producer.poll(0)

                                successfull_symbols +=1
                            else:
                                failed_symbols -=1
                        except Exception  as e:
                            self.logger.error(f"failed data for {symbol} {e}")
                            failed_symbols-=1
                    self.logger.info(f"Data Generation Summary: Successful: {successfull_symbols}, Failed: {failed_symbols}")

                    self.logger.info(f"Waiting {self.interval} seconds before next data generation...")
                    time.sleep(self.interval)
            except KeyboardInterrupt:
                self.logger.info("Producer stopped by user")
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")    

def main():
    logger.info("starting the process")

    try:
        producer=streamdatacollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            topic=KAFKA_TOPIC_STREAM,
            interval=60
        )

        producer.produce_stock_data()

    except Exception as e:
        logger.error(f"Fattal error:{e}")

if __name__=="__main__":
    main()            
