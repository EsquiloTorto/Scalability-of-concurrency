#/bin/sh

rabbitmq-server -detached
rabbitmqctl wait --pid 1 --timeout 60 && python /app/consumer.py
