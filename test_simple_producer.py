from kafka import KafkaProducer
import json
import time

print("Створюємо Producer...")
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(10):
    message = {"test": f"Message {i+1}", "timestamp": time.time()}
    print(f"Відправляю повідомлення {i+1}: {message}")
    
    try:
        future = producer.send('clean-power-data', message)
        result = future.get(timeout=10)  # Чекаємо результат
        print(f"✅ УСПІХ! Topic: {result.topic}, Partition: {result.partition}, Offset: {result.offset}")
    except Exception as e:
        print(f"❌ ПОМИЛКА: {e}")
    
    time.sleep(2)

producer.close()
print("Тест завершено")