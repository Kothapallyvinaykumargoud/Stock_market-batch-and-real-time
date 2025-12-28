import json
import os
import logging
import time
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

        def generatestockdata(self,symbol):
            current_price=self.current_stocks[symbol]

            market_stock=random.uniform(-0.005,0.005)
            stock_change=random.uniform(-0.005,0.005)

            combined_data=market_stock+stock_change

            if random.random() < 0.005:
                 stock_factor += random.uniform(-0.02, 0.02)
        
            new_price = round(current_price * (1+combined_data), 2)

            self.current_stocks[symbol] = new_price

            price_change=round(new_price-current_price,2)
            percent_change = round((price_change / current_price) * 100, 2)

            volume = random.randint(1000,100000)

            stock_data={
                'symbol': symbol,
                'price': new_price,
                'change': price_change,
                'percent_change': percent_change,
                'volume': volume,
                "timestamp": datetime.now().isoformat()
            }

            return stock_data



        def produce_stock_data(self):
            self.logger.info(f"started fetching the data")

            try:
                while True:

                    successfull_symbols =0
                    failed_symbols=0

                    for symbol in self.current_stocks.keys():
                        try:


                            stock_data=self.generatestockdata(symbol)

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
            interval=2
        )

        producer.produce_stock_data()

    except Exception as e:
        logger.error(f"Fattal error:{e}")

if __name__=="__main__":
    main()            
