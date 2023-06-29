import pika
import sys
import pymongo
from datetime import datetime


def get_rabbitmq_connection():
    credentials = pika.PlainCredentials("admin", "admin")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="broker",
            credentials=credentials,
        )
    )

    return connection


def get_mongodb_connection():
    client = pymongo.MongoClient(
        "mongodb://root:admin@mongodb:27017",
        replicaSet="replicaset",
    )
    return client


if __name__ == "__main__":
    try:
        rabbitmq_connection = get_rabbitmq_connection()
        channel = rabbitmq_connection.channel()
        channel.queue_declare(queue="position")
        print("RabbitMQ connection established")

        mongodb_connection = get_mongodb_connection()
        simulator_db = mongodb_connection["simulator"]
        positions_collection = simulator_db["positions"]
        print("MongoDB connection established")

        def callback(_ch, _method, _properties, body):
            message = body.decode("utf-8")

            highway, plate, timestamp, lane, dist, in_lane = message.split(",")
            positions_collection.insert_one(
                {
                    "highway": highway,
                    "plate": plate,
                    "in_lane": in_lane,
                    "unix_time": timestamp,
                    "lane": int(lane),
                    "dist": int(dist)
                }
            )

        channel.basic_consume(
            queue="position",
            on_message_callback=callback,
            auto_ack=True,
        )

        print("Waiting for messages.")
        channel.start_consuming()
    except:
        sys.exit(1)
