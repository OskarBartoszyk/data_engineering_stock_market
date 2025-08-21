from kafka import KafkaProducer
import time

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),  # Dodaj wersję API
        retries=5,
        request_timeout_ms=30000,
        value_serializer=lambda v: str(v).encode('utf-8')
    )
    print("Połączenie z Kafka udane!")
    
    # Wyślij prostą wiadomość
    producer.send('test-topic', 'Hello Kafka!')
    producer.flush()
    print("Wiadomość wysłana!")
    
except Exception as e:
    print(f"Błąd: {e}")