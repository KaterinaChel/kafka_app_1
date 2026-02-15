from confluent_kafka import Consumer
from confluent_kafka.serialization import IntegerDeserializer
from message import serde
#from log_config import ErrorLogger

int_deserializer = IntegerDeserializer()

class SingleMessageConsumer:
    def __init__(self, conf, topic):
        self.consumer = Consumer(conf)
        self.consumer.subscribe([topic])

    def run(self):
        try:
            while True:
                messages = self.consumer.consume(num_messages=1, timeout=1.0)

                if messages is None:
                    continue
                for msg in messages:
                    if msg.error(): # Теперь вызываем error() у конкретного сообщения
                        print(f"Ошибка SingleMessageConsumer: {msg.error()}")
                        continue

                    key = serde.deserialize_int(msg.key())
                    value = serde.deserialize_int(msg.value())
                    print(
                        f"Получено сообщение SingleMessageConsumer: {key=}, {value=}, "
                        f"partition={msg.partition()}, offset={msg.offset()}"
                    )
        finally:
            self.consumer.close()

class BatchMessageConsumer:
    def __init__(self, conf, topic, batch_size=10):
        self.consumer = Consumer(conf)
        self.consumer.subscribe([topic])
        self.batch_size = batch_size
        self.buffer = []  # Наш накопитель

    def run(self):
        try:
            while True:
                # Пытаемся забрать порцию (может вернуть от 0 до batch_size)
                messages = self.consumer.consume(num_messages=self.batch_size, timeout=1.0)

                if not messages:
                    continue

                # Добавляем только валидные сообщения в буфер
                for msg in messages:
                    if msg.error():
                        print(f"Ошибка в сообщении BatchMessageConsumer: {msg.error()}")
                        continue
                    self.buffer.append(msg)

                # ПРОВЕРКА: Накопилось ли минимум 10?
                if len(self.buffer) >= self.batch_size:
                    print(f"--- Начинаю обработку пачки из BatchMessageConsumer {len(self.buffer)} сообщений ---")
                    
                    for msg in self.buffer:
                        key = serde.deserialize_int(msg.key())
                        value = serde.deserialize_int(msg.value())
                        print(f"Обработано BatchMessageConsumer: {key=}, {value=}")

                    # Фиксируем прогресс в Kafka
                    self.consumer.commit(asynchronous=False)
                    
                    # Очищаем буфер для следующей пачки
                    self.buffer.clear()
                else:
                    print(f"В буфере BatchMessageConsumer {len(self.buffer)} сообщений, ждем еще...")

        except Exception as e:
            print(f"Ошибка BatchMessageConsumer: {e}")
        finally:
            self.consumer.close()