import json
import os
import logging
import time
from datetime import datetime,timedelta
import pandas as pd
import yfinance as yf
from confluent_kafka import Producer
from dotenv import load_dotenv

# load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s,[%(levelname)s],%(message)s',
)

logger=logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVER="localhost:29092"
KAFKA_TOPIC_BATCH="stock-market-batch1"

#Define stocks to collect for historical data
STOCKS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "TSLA",
    "NVDA",
    "INTC",
    "JPM",
    "V"
]

class HistoricalDataCollector:
   def  __init__(self,bootstrapserver=KAFKA_BOOTSTRAP_SERVER,topic=KAFKA_TOPIC_BATCH):

    self.logger=logger
    self.topic=topic

    self.producer={
        "bootstrap.servers": bootstrapserver,
        "client.id":"historical_data_collector",
    }

    try:
        self.producer=Producer(self.producer)
        self.logger.info(f"producer initialized . sending to {bootstrapserver}")
    except Exception as e:
        self.logger.error(f"failed to create kafka producer {e}")
        raise

    
   def fetch_historical_data(self, symbol:str,date_str="2025-12-22"):#period: str = "1y"
        try:
            start_date = datetime.strptime(date_str, "%Y-%m-%d")
            end_date = start_date + timedelta(days=1)
            self.logger.info(f"fetching historical data {symbol}")

            ticker=yf.Ticker(symbol)

            # df=ticker.history(period=period)

            df = ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d")
            )

            df.reset_index(inplace=True)
            
            df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            },inplace=True

            )

            df['date']=df['date'].dt.strftime("%Y-%m-%d")
            df['symbol']=symbol
            df = df[['date','symbol',"open","high","low","close","volume"]]

            self.logger.info(f"fetched the data for {len(df)} daysof data for {symbol}")
            return df

        except Exception as e:
            self.logger.error(f"failed to fetch the {symbol}  {e}")
            return None

   def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed for message: {msg}")
        else:
            self.logger.info(f"Message delivered successfully to topic {msg.topic} [{msg.partition()}]")


   def producer_to_kafka(self, df: pd.DataFrame, symbol= str):

    df["ingestion_time2"] = datetime.utcnow().isoformat()

    batch_id=datetime.now().strftime("%Y%m%d%H%M%S")
    df['batch_id']=batch_id
    df['batch_date']=datetime.now().strftime("%Y-%m-%d")
    
    successful_records=0
    failed_records=0

    records=df.to_dict(orient='records')
    
    for record in records:
        try:
            data=json.dumps(record)

            self.producer.produce(
                topic=self.topic,
                value=data,
                key=symbol,
                callback=self.delivery_report,
            )

            successful_records +=1
        
        except Exception as e:
            self.logger.error(f"Failed to produce message for {symbol} {e}")
            failed_records +=1

        self.producer.flush()
        self.logger.info(f"Successfully produced {successful_records} records for {symbol} and failed {failed_records}")


   def collect_historical_data(self,date_str):

    symbols=STOCKS

    self.logger.info(f"Starting historical data collection for {len(symbols)} symbols")
    
    successfull_symbols=0
    failed_symbols=0

    for symbol in symbols:
        try:
            df=self.fetch_historical_data(symbol,date_str)

            if df is not None and not df.empty:
                self.producer_to_kafka(df,symbol)
                successfull_symbols +=1
            else:
                self.logger.warning(f"no data returned for {symbol}")
                failed_symbols +1

        except Exception as e:
            self.logger.error(f"error processing {symbol} :{e}")
            failed_symbols +=1

        time.sleep(1)

    self.logger.info(f"Historical data colelction completed. Successful: {successfull_symbols}, Failed: {failed_symbols}")



        

        


def main():
    try:
        logger.info(f"starting historical stock collector")

        collector=HistoricalDataCollector(
            bootstrapserver=KAFKA_BOOTSTRAP_SERVER,
            topic=KAFKA_TOPIC_BATCH
        )
        #collect historical data
        collector.collect_historical_data(date_str="2025-12-22")#(period="1y")#(date_str="2025-12-16")

    except Exception as e:
        logger.error(f"Fatal error : {e}")


if __name__=="__main__":
    main()
          