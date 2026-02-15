import os
import time
from consumers import SingleMessageConsumer, BatchMessageConsumer
from producer_wrapper import KafkaProducerWrapper
import threading

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
consumer_group_single = os.getenv("CONSUMER_GROUP_SINGLE", "consumer_group_01")
consumer_group_batch = os.getenv("CONSUMER_GROUP_BATCH", "consumer_group_02")

# Конфигурация SingleMessageConsumer
conf_single = {
    "bootstrap.servers": bootstrap_servers, 
    "group.id": consumer_group_single, #id группы консьюмеров
    "auto.offset.reset": "earliest", # если сброс офсета начинается вычитка с самого раннего сообщения в топике
    "enable.auto.commit": True, #авто комит сообщений
    "fetch.min.bytes": 1, #минимальный размер накопленных сообщений в топике для забора
    "fetch.wait.max.ms": 1000, #максимальное время (в миллисекундах), которое брокер будет ждать накопления данных
 #   "value.deserializer": IntegerDeserializer() #десериализатор
}

# Конфигурация BatchMessageConsumer
conf_batch = {
    "bootstrap.servers": bootstrap_servers,
    "group.id": consumer_group_batch,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "fetch.wait.max.ms": 1000,
   # "value.deserializer": IntegerDeserializer()
}

producer_conf = {
        "bootstrap.servers": bootstrap_servers,  # адрес сервера
        "acks": "1",  # Гарантия хотя бы один раз сообщение доставлено
        "retries": 3, #количество повторений отправки при сбое
    #    "value.serializer": IntegerSerializer() #сериализатор
    }

if __name__ == "__main__":
    print("Ожидаю запуск Kafka (60 сек)...")
    time.sleep(60)

    # 1. Запускаем Single Consumer в фоновом потоке
    print("Запуск Single Consumer...")
    c1 = SingleMessageConsumer(conf_single, topic="topic_app")
    thread1 = threading.Thread(target=c1.run, daemon=True)
    thread1.start()

    # 2. Запускаем Batch Consumer во втором фоновом потоке
    print("Запуск Batch Consumer...")
    c2 = BatchMessageConsumer(conf_batch, topic="topic_app", batch_size=10)
    thread2 = threading.Thread(target=c2.run, daemon=True)
    thread2.start()

    # 3. Продюсер запускается в основном потоке и шлет сообщения
    print("Запуск Продюсера...")
    producer = KafkaProducerWrapper(producer_conf, topic="topic_app")
    # Он будет работать тут, пока не отправит все 500 сообщений
    producer.send_messages(start=0, end=500, interval=1)

    print("Продюсер закончил работу. Консьюмеры продолжают слушать...")
    
    # Чтобы скрипт не завершился сразу после продюсера, 
    # подождем завершения потоков (хотя они бесконечные)
    thread1.join()
    thread2.join()