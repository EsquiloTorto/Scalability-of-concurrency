import pika as pk


def get_rabbitmq_connection():
    credentials = pk.PlainCredentials("admin", "admin")
    connection = pk.BlockingConnection(
        pk.ConnectionParameters(
            host="broker",
            credentials=credentials,
        )
    )

    return connection


def get_position_channel():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="position")

    return channel
