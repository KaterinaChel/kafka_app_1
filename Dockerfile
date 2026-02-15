FROM python:3.9-slim
# Установка системных зависимостей для сборки confluent-kafka
RUN apt-get update && apt-get install -y gcc librdkafka-dev python3-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Установка с обходом проверки SSL сертификатов
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org \
    confluent-kafka
COPY main.py consumers.py producer_wrapper.py message.py ./