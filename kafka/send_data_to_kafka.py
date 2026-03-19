import requests
import kafka
import logging
import json
from datetime import datetime
from pathlib import Path


# create logger instance and configure it to write logs to a folder named "logs" in the current directory. The log files should be named with the current date and time (e.g., "app_2024-06-01_12-00-00.log"). 

log_dir = Path("C:\\Users\\Shivani-Parate\\Desktop\\Crypto-pipeline-foundations\\kafka\\logs")
log_dir.mkdir(exist_ok= True)

timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
log_file = log_dir/f"app_{timestamp}.log"


logging.basicConfig(
    level=logging.INFO,  # Set the minimum logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define the log message format
    handlers =[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# send this data to kafka

try:
    my_producer = kafka.KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode(
            "utf-8"
        ),  # to serialize the data
    )
    logger.info("connected to kafka")
    print("connected to kafka")

except:

    logger.info("couldn't connect to kafka")
    print("not connected to kafka")

# create a new producer


def send_data_kafka(user_msg, my_producer):

    if my_producer == None:
        return "Producer not initialized"

    try:
        my_producer.send(
            "my_topic",
            user_msg,
        )

        my_producer.flush()
        logger.info("data sent to kafka")

        return "Success!!"

    except:
        logger.info("could not send data to kafka")


# fetch data from this sample api
response = requests.get(
    "https://api.open-meteo.com/v1/forecast",
    params={"latitude": 51.5, "longitude": -0.11, "current": "temperature_2m"},
)

if response.status_code == 200:
    user_msg = response.json()
    send_data_kafka(user_msg, my_producer)
