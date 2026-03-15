import requests
import kafka
import logging
import json


# create logger instance
logging.basicConfig(
    filename="app.log",
    level=logging.DEBUG,  # Set the minimum logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Define the log message format
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
