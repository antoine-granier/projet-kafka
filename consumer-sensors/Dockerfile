FROM openjdk:8-jdk-slim

RUN apt-get update && \
  apt-get install -y python3 python3-pip procps && \
  apt-get clean

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./consumer-sensors .

CMD ["python3", "consumer.py"]
