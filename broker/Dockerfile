FROM rabbitmq:management-alpine

WORKDIR /app

RUN apk update && apk add --no-cache python3 py3-pip

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY . /app

CMD ["./start.sh"]
