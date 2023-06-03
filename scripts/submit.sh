#!/bin/bash

JOB_NAME=$1

if [ -z "$JOB_NAME" ]; then
  echo "Usage: ./submit.sh <job_name>"
  exit 1
fi

if [ ! -f "jobs/$JOB_NAME.py" ]; then
  echo "Job file jobs/$JOB_NAME.py does not exist"
  exit 1
fi

docker ps | grep spark-master >/dev/null

if [ $? -ne 0 ]; then
  echo "Spark container is not running"
  exit 1
fi

docker exec \
  -it esc-spark-master-1 /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/$JOB_NAME.py
