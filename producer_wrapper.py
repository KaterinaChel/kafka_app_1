from confluent_kafka import Producer
from confluent_kafka.serialization import IntegerSerializer
from message import serde

import time
int_deserializer = IntegerSerializer()

class KafkaProducerWrapper:
    def __init__(self, conf, topic):
        self.topic = topic
        self.producer = Producer(conf)

    def delivery_report(self,err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Отправка сообщения
# В цикле отправки
    def send_messages(self, start=0, end=500, interval=1):
        for i in range(start, end):
            self.producer.produce(
                topic=self.topic,
                key=serde.serialize_int(i + 5), # Красивая упаковка в байты
                value=serde.serialize_int(i),
                callback=self.delivery_report
            )
            time.sleep(interval)
            if i % 100 == 0:
                self.producer.poll(0)  # Обработка колбэков
        self.producer.flush()