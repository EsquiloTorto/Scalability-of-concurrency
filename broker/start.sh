#/bin/sh

set -e

rabbitmq-server -detached
rabbitmqctl wait --pid 1 && python /app/consumer.py
