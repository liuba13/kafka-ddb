from kafka import KafkaConsumer
import json
import time

print("Створюємо Consumer...")
consumer = KafkaConsumer(
    'clean-power-data',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    group_id=f'test-group-{int(time.time())}'  # Унікальна група
)

print("Чекаю повідомлення... (10 секунд)")

import signal
def timeout_handler(signum, frame):
    raise TimeoutError("Timeout!")

signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(10)  # 10 секунд timeout

try:
    for message in consumer:
        print(f"📨 Отримано: {message.value}")
        print(f"   Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
        break  # Виходимо після першого повідомлення
except TimeoutError:
    print("⏰ Timeout - повідомлення не надійшли")

consumer.close()
print("Тест Consumer завершено")
