from confluent_kafka.serialization import IntegerDeserializer, IntegerSerializer

class KafkaSerde:
    def __init__(self):
        # Создаем экземпляры один раз при инициализации класса
        self._int_deserializer = IntegerDeserializer()
        self._int_serializer = IntegerSerializer()

    def deserialize_int(self, data):
        """Превращает байты из Kafka в число Python"""
        if data is None:
            return None
        return self._int_deserializer(data)

    def serialize_int(self, value):
        """Превращает число Python в байты для Kafka"""
        if value is None:
            return None
        return self._int_serializer(value)

# Создаем один экземпляр для переиспользования (синглтон)
serde = KafkaSerde()
        
