from kafka import KafkaConsumer
import json
import time

print("Створюємо стійкий Consumer...")

# Створюємо consumer без JSON deserializer
consumer = KafkaConsumer(
    'clean-power-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id=f'test-robust-group-{int(time.time())}',  # Унікальна група
    enable_auto_commit=True
)

print("Чекаю повідомлення... (15 секунд)")

def safe_json_parse(raw_message):
    """Безпечний парсер JSON з fallback на текст"""
    try:
        text_message = raw_message.decode('utf-8')
        # Спробувати як JSON
        return json.loads(text_message), 'json'
    except (json.JSONDecodeError, UnicodeDecodeError):
        try:
            # Якщо не JSON, повернути як текст
            return raw_message.decode('utf-8'), 'text'
        except UnicodeDecodeError:
            return str(raw_message), 'bytes'

import signal

def timeout_handler(signum, frame):
    raise TimeoutError("Timeout!")

signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(15)  # 15 секунд timeout

message_count = 0
try:
    for message in consumer:
        parsed_value, data_type = safe_json_parse(message.value)
        
        print(f"📨 Повідомлення #{message_count + 1}:")
        print(f"   Тип даних: {data_type}")
        print(f"   Значення: {parsed_value}")
        print(f"   Topic: {message.topic}")
        print(f"   Partition: {message.partition}")
        print(f"   Offset: {message.offset}")
        print("-" * 50)
        
        message_count += 1
        
        # Зупинитись після 5 повідомлень для демо
        if message_count >= 5:
            break
            
except TimeoutError:
    print("⏰ Timeout - повідомлення не надійшли")
except KeyboardInterrupt:
    print("🛑 Зупинено користувачем")

consumer.close()
print(f"Тест завершено. Опрацьовано {message_count} повідомлень.")