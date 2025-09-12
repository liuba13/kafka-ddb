# kafka-ddb

Крок 1: Підготовка робочого середовища

1.1 Перевірка системних вимог
Мінімальні вимоги для Kafka 3.7.1:
·	RAM: 4GB (рекомендовано 8GB)
·	CPU: 2 cores (рекомендовано 4 cores)
·	Диск: 10GB вільного місця (SSD рекомендовано)
·	Java: 8+ (підтримується до Java 21)

1.2 Встановлення Java
macOS:
bash
# Через Homebrew
brew install openjdk@11

# Додаємо в PATH
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Перевіряємо
java -version

Windows:
1.	Завантажте OpenJDK з https://adoptium.net/
2.	Встановіть як звичайну програму
3.	Додайте JAVA_HOME до змінних середовища:
o	JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.x-hotspot\
o	Додайте %JAVA_HOME%\bin до PATH

Крок 2: Завантаження та встановлення Kafka

2.1 Завантаження Kafka 3.7.1

Linux/macOS:
bash
# Завантажуємо архів Kafka 3.7.1 (найновіша стабільна версія)
wget https://downloads.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz

# Завантажуємо архів Kafka 3.7.1 (якщо версія перенесена в архів Apache) !!!
wget https://archive.apache.org/dist/kafka/3.7.1/kafka_2.13-3.7.1.tgz 

# Розпаковуємо
tar -xzf kafka_2.13-3.7.1.tgz

# Перейменовуємо для зручності
mv kafka_2.13-3.7.1 kafka

# Переходимо в директорію Kafka
cd kafka

Windows:
cmd
REM Завантажте Kafka 3.7.1 з https://downloads.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz
REM Розпакуйте в C:\kafka або в робочу директорію
cd C:\kafka

2.2 Огляд структури Kafka 3.7.1
bash
# Дивимося що у нас є
ls -la

Основні директорії:
·	bin/ - виконувані файли та скрипти
·	config/ - файли конфігурації
·	libs/ - Java бібліотеки
·	logs/ – лог-файли роботи Kafka та ZooKeeper (server.log, zookeeper.log)
·	data/ – директорія для збереження повідомлень (шлях задається у server.properties)

Важливі файли:
·	bin/kafka-server-start.sh – запуск Kafka брокера
·	bin/kafka-server-stop.sh – зупинка Kafka брокера 
·	bin/zookeeper-server-start.sh – запуск ZooKeeper (до Kafka 3.8)
·	bin/zookeeper-server-stop.sh – зупинка ZooKeeper
·	bin/kafka-topics.sh – створення, видалення та перегляд топіків
·	bin/kafka-console-producer.sh – відправка повідомлень у топік із терміналу
·	bin/kafka-console-consumer.sh – читання повідомлень із топіка
·	bin/kafka-consumer-groups.sh – робота з консюмерськими групами (офсети, lag, reset)
·	bin/kafka-configs.sh – управління конфігураціями топіків, користувачів, брокерів
·	config/server.properties – основна конфігурація Kafka брокера
·	config/zookeeper.properties – конфігурація ZooKeeper
·	config/producer.properties – стандартні налаштування продюсера
·	config/consumer.properties – стандартні налаштування консюмера
·	config/kraft/ – конфігурації для роботи Kafka у режимі KRaft (без ZooKeeper)

2.3 Налаштування конфігурації
Налаштування Kafka config/server.properties
Налаштування Zookeeper config/zookeeper.properties

Крок 3: Запуск Kafka 

3.1 Запуск Zookeeper
Linux/macOS:
bash
# В першому терміналі запускаємо Zookeeper
./bin/zookeeper-server-start.sh config/zookeeper.properties

Windows:
cmd
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

Залиште цей термінал відкритим!


3.2 Запуск Kafka брокера
У новому терміналі:
# Linux/macOS
./bin/kafka-server-start.sh config/server.properties

# Windows
bin\windows\kafka-server-start.bat config\server.properties

Залиште і цей термінал відкритим!

3.3 Перевірка запуску
У третьому терміналі:
# Перевіряємо чи працює Kafka
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Linux альтернатива для перевірки портів
netstat -tln | grep -E "(2181|9092)"

# macOS альтернатива
lsof -i :2181
lsof -i :9092

# Перевірка версії та API
./bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092


Крок 4: Робота з Topic 
4.1 Створення Topic 
bash
# Створюємо topic для даних електростанцій
./bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic NEW-station-data \
  --partitions 3 \
  --replication-factor 1

4.2 Операції з Topics в Kafka 
bash
# Список всіх topics
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Детальна інформація про topic 
./bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic NEW-station-data

# Конфігурація topic
./bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name power-station-data --describe

# Створення додаткових topics для проекту
./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic power-telemetry --partitions 3 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic grid-alerts --partitions 2 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic energy-consumption --partitions 4 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic weather-data --partitions 2 --replication-factor 1

4.3 Перевірка версії та можливостей Kafka 3.7.1
bash
# Перевірка версії Kafka
./bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Інформація про брокер
./bin/kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe

# Тест продуктивності продюсера
./bin/kafka-producer-perf-test.sh \
  --topic NEW-station-data \
  --num-records 10000 \
  --record-size 1024 \
  --throughput 1000 \
  --producer-props bootstrap.servers=localhost:9092 compression.type=gzip


Крок 5: Відправка даних (Producer) 
5.1 Тестуємо консольний Producer
bash
# У четвертому терміналі запускаємо консольний producer
./bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic NEW-station-data
Що ви побачите:
>
Тепер введіть тестові дані (кожен рядок = окреме повідомлення):
Київська ТЕС: 1200 МВт
Дніпровська ГЕС: 800 МВт  
Сонячна ферма: 150 МВт

Для виходу: Ctrl+C


5.2 Створення Python Producer для Kafka 
Встановлення Python бібліотеки для Kafka:
bash
# Повертаємося в основну директорію
cd ../

# Встановлюємо оновлену бібліотеку для Kafka 
pip3 install kafka-python==2.0.2

# Створюємо requirements.txt з усіма залежностями
cat > requirements.txt << EOF
kafka-python==2.0.2
confluent-kafka==2.3.0
avro==1.11.3
requests==2.31.0
python-dotenv==1.0.0
matplotlib==3.8.2
pandas==2.1.4
numpy==1.26.2
EOF

# Встановлюємо всі залежності
pip3 install -r requirements.txt
Створимо файл scripts/simple_producer.py:


5.4 Запуск Python Producer
bash
# Запускаємо наш producer
python scripts/simple_producer.py

Залиште його працювати і відкрийте новий термінал!


Крок 6: Отримання даних (Consumer)
6.1 Тестуємо консольний Consumer
У п'ятому терміналі:
bash
# Запускаємо консольний consumer
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic NEW-station-data \
  --from-beginning

6.2 Створення Python Consumer для Kafka 
Створимо файл scripts/simple_consumer.py:

6.3 Запуск Python Consumer
У шостому терміналі:
bash
# Запускаємо наш consumer
python scripts/simple_consumer.py


Крок 7: Моніторинг та адміністрування Kafka 

7.1 Перевірка статусу системи
У сьомому терміналі:
bash
# Статистика по topics 
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Детальна інформація про всі topics
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

# Інформація про consumer groups
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Детальна інформація про нашу группу
./bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group energy-monitor-1757662385

7.2 Перевірка продуктивності Kafka 
bash
# Тест продуктивності producer 
./bin/kafka-producer-perf-test.sh \
  --topic power-station-data \
  --num-records 10000 \
  --record-size 1024 \
  --throughput 1000 \
  --producer-props bootstrap.servers=localhost:9092 compression.type=gzip

# Тест продуктивності consumer
./bin/kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 \
  --topic power-station-data \
  --messages 10000

7.3 Логи та налагодження 
bash
# Перегляд логів Kafka
tail -f logs/server.log

# Перегляд конфігурації брокера
./bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-name 0 --describe

# Перегляд конфігурації topic
./bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name power-station-data --describe

# Перегляд метаданих
./bin/kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe

Крок 8: Правильна зупинка системи
8.1 Graceful shutdown Kafka 3.7.1

Зупиняємо Python програми:
bash
# У терміналах з Producer та Consumer
Ctrl+C

2. Зупиняємо Kafka 3.7.1:
bash
# У терміналі з Kafka або через скрипт
cd kafka-energy-lab/kafka
./bin/kafka-server-stop.sh

3. Зупиняємо Zookeeper:
bash
./bin/zookeeper-server-stop.sh

8.2 Очистка даних Kafka 3.7.1 (опціонально)
bash
# Видалення логів Kafka 3.7.1 (повна очистка)
rm -rf ../kafka-logs/*

# Видалення даних Zookeeper
rm -rf ../data/zookeeper/*

# Видалення завантаженого Kafka 3.7.1
rm -rf kafka/
rm kafka_2.13-3.7.1.tgz

# Якщо робили backup старої версії
rm -rf kafka_old/

8.3 Перевірка очистки
bash
# Переконуємося що процеси зупинені
ps aux | grep -E "(kafka|zookeeper)"

# Переконуємося що порти вільні
netstat -tln | grep -E "(2181|9092)"
