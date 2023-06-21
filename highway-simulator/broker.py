import pika as pk
from time import sleep


def get_rabbitmq_connection():
    while True:
        try:
            credentials = pk.PlainCredentials("admin", "admin")
            connection = pk.BlockingConnection(
                pk.ConnectionParameters(
                    host="broker",
                    credentials=credentials,
                )
            )
            break
        except pk.exceptions.AMQPConnectionError as e:
            print(e)
            print("Retrying in 1 second...")
            sleep(1)

    return connection


def get_position_channel():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="position")

    return channel
