#!/usr/bin/env python3
"""
Розширений Consumer для енергетичних даних з Kafka + Cassandra
Читає дані з Kafka та зберігає в Cassandra для довгострокового аналізу
"""

from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import json
import time
from datetime import datetime, timedelta
import uuid
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnergyDataProcessor:
    """Клас для обробки енергетичних даних та запису в Cassandra"""
    
    def __init__(self):
        self.kafka_consumer = None
        self.cassandra_session = None
        self.cluster = None
        self.message_count = 0
        
        # Prepared statements для швидкості
        self.insert_power_metrics = None
        self.insert_power_by_region = None
        self.insert_power_by_type = None
        self.insert_hourly_aggregate = None
        self.insert_alert = None
    
    def setup_kafka_consumer(self):
        """Налаштування Kafka consumer"""
        try:
            self.kafka_consumer = KafkaConsumer(
                'NEW-station-data',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='latest',
                group_id=f'energy-cassandra-{int(time.time())}',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                max_poll_records=100
            )
            logger.info("✅ Kafka consumer налаштовано")
            return True
        except Exception as e:
            logger.error(f"❌ Помилка налаштування Kafka: {e}")
            return False
    
    def setup_cassandra_connection(self):
        """Налаштування підключення до Cassandra"""
        try:
            self.cluster = Cluster(['127.0.0.1'], port=9042)
            self.cassandra_session = self.cluster.connect('energy_kafka_db')
            
            # Prepared statements для продуктивності
            self.prepare_statements()
            
            logger.info("✅ Cassandra підключення налаштовано")
            return True
        except Exception as e:
            logger.error(f"❌ Помилка підключення до Cassandra: {e}")
            return False
    
    def prepare_statements(self):
        """Підготовка SQL statements для швидшого виконання"""
        
        # Основна таблиця метрик
        self.insert_power_metrics = self.cassandra_session.prepare("""
            INSERT INTO power_metrics (
                plant_id, timestamp, station_name, station_type, 
                power_output_mw, voltage_kv, frequency_hz, 
                efficiency_percent, kafka_version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Таблиця по регіонах
        self.insert_power_by_region = self.cassandra_session.prepare("""
            INSERT INTO power_by_region (
                region, timestamp, plant_id, station_name, power_output_mw
            ) VALUES (?, ?, ?, ?, ?)
        """)
        
        # Таблиця по типах станцій
        self.insert_power_by_type = self.cassandra_session.prepare("""
            INSERT INTO power_by_type (
                station_type, timestamp, plant_id, power_output_mw, efficiency_percent
            ) VALUES (?, ?, ?, ?, ?)
        """)
        
        # Таблиця алертів
        self.insert_alert = self.cassandra_session.prepare("""
            INSERT INTO power_alerts (
                plant_id, timestamp, alert_type, severity, 
                message, value, threshold, acknowledged
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        logger.info("✅ Prepared statements готові")
    
    def generate_plant_id(self, station_name):
        """Генерація стабільного plant_id на основі назви станції"""
        # Створюємо стабільний UUID на основі назви
        namespace = uuid.UUID('12345678-1234-5678-1234-567812345678')
        return str(uuid.uuid5(namespace, station_name))
    
    def extract_region(self, station_name):
        """Витягання регіону з назви станції"""
        station_lower = station_name.lower()
        if 'київ' in station_lower:
            return 'Київ'
        elif 'львів' in station_lower:
            return 'Львів'
        elif 'дніпр' in station_lower:
            return 'Дніпро'
        elif 'запоріж' in station_lower:
            return 'Запоріжжя'
        elif 'нікопол' in station_lower:
            return 'Дніпропетровська область'
        else:
            return 'Інший регіон'
    
    def check_for_alerts(self, data, plant_id):
        """Перевірка даних на наявність алертів"""
        alerts = []
        now = datetime.now()
        
        # Перевірка напруги
        voltage = data.get('voltage_kv', 220)
        if voltage < 218 or voltage > 222:
            severity = 'critical' if (voltage < 215 or voltage > 225) else 'high'
            alerts.append({
                'plant_id': plant_id,
                'timestamp': now,
                'alert_type': 'voltage',
                'severity': severity,
                'message': f'Напруга поза межами: {voltage} кВ',
                'value': voltage,
                'threshold': 220.0,
                'acknowledged': False
            })
        
        # Перевірка частоти
        frequency = data.get('frequency_hz', 50)
        if frequency < 49.8 or frequency > 50.2:
            severity = 'critical'
            alerts.append({
                'plant_id': plant_id,
                'timestamp': now,
                'alert_type': 'frequency',
                'severity': severity,
                'message': f'Критична частота: {frequency} Гц',
                'value': frequency,
                'threshold': 50.0,
                'acknowledged': False
            })
        
        # Перевірка ефективності
        efficiency = data.get('efficiency_percent', 85)
        if efficiency < 70:
            severity = 'medium' if efficiency > 60 else 'high'
            alerts.append({
                'plant_id': plant_id,
                'timestamp': now,
                'alert_type': 'efficiency',
                'severity': severity,
                'message': f'Низька ефективність: {efficiency}%',
                'value': efficiency,
                'threshold': 80.0,
                'acknowledged': False
            })
        
        # Перевірка потужності
        power = data.get('power_output_mw', 0)
        if power < 10:  # Критично низька потужність
            alerts.append({
                'plant_id': plant_id,
                'timestamp': now,
                'alert_type': 'power',
                'severity': 'high',
                'message': f'Критично низька потужність: {power} МВт',
                'value': power,
                'threshold': 100.0,
                'acknowledged': False
            })
        
        return alerts
    
    def save_to_cassandra(self, data):
        """Збереження даних в Cassandra"""
        try:
            # Генеруємо plant_id та витягуємо дані
            station_name = data.get('station_name', 'Unknown Station')
            plant_id = self.generate_plant_id(station_name)
            region = self.extract_region(station_name)
            
            # Парсимо timestamp
            timestamp_str = data.get('timestamp')
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except:
                    timestamp = datetime.now()
            else:
                timestamp = datetime.now()
            
            created_at = datetime.now()
            
            # 1. Зберігаємо в основну таблицю метрик
            self.cassandra_session.execute(self.insert_power_metrics, [
                plant_id,
                timestamp,
                station_name,
                data.get('station_type', 'unknown'),
                data.get('power_output_mw', 0.0),
                data.get('voltage_kv', 220.0),
                data.get('frequency_hz', 50.0),
                data.get('efficiency_percent', 85.0),
                data.get('kafka_version', 'unknown'),
                created_at
            ])
            
            # 2. Зберігаємо в таблицю по регіонах
            self.cassandra_session.execute(self.insert_power_by_region, [
                region,
                timestamp,
                plant_id,
                station_name,
                data.get('power_output_mw', 0.0)
            ])
            
            # 3. Зберігаємо в таблицю по типах
            self.cassandra_session.execute(self.insert_power_by_type, [
                data.get('station_type', 'unknown'),
                timestamp,
                plant_id,
                data.get('power_output_mw', 0.0),
                data.get('efficiency_percent', 85.0)
            ])
            
            # 4. Перевіряємо та зберігаємо алерти
            alerts = self.check_for_alerts(data, plant_id)
            for alert in alerts:
                self.cassandra_session.execute(self.insert_alert, [
                    alert['plant_id'],
                    alert['timestamp'],
                    alert['alert_type'],
                    alert['severity'],
                    alert['message'],
                    alert['value'],
                    alert['threshold'],
                    alert['acknowledged']
                ])
            
            return True, alerts
            
        except Exception as e:
            logger.error(f" Помилка запису в Cassandra: {e}")
            return False, []
    
    def process_message(self, message):
        """Обробка повідомлення з Kafka"""
        try:
            self.message_count += 1
            data = message.value
            
            logger.info(f" [{self.message_count}] Обробляємо: {data.get('station_name')}")
            
            # Зберігаємо в Cassandra
            success, alerts = self.save_to_cassandra(data)
            
            if success:
                logger.info(f" Збережено в Cassandra")
                
                # Виводимо алерти якщо є
                if alerts:
                    logger.warning(f" Знайдено {len(alerts)} алертів:")
                    for alert in alerts:
                        logger.warning(f"   {alert['severity'].upper()}: {alert['message']}")
            else:
                logger.error(" Помилка запису в Cassandra")
            
            return success
            
        except Exception as e:
            logger.error(f" Помилка обробки повідомлення: {e}")
            return False
    
    def run(self):
        """Основний цикл обробки"""
        logger.info(" Запуск енергетичного моніторингу з Cassandra...")
        
        # Налаштування підключень
        if not self.setup_kafka_consumer():
            return
        
        if not self.setup_cassandra_connection():
            return
        
        logger.info(" Очікуємо дані від електростанцій...")
        logger.info(" Натисніть Ctrl+C для зупинки\n")
        
        try:
            for message in self.kafka_consumer:
                success = self.process_message(message)
                
                if not success:
                    logger.warning(" Повідомлення не було оброблено")
                
        except KeyboardInterrupt:
            logger.info(f"\n Зупинено. Оброблено {self.message_count} повідомлень")
        
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Очистка ресурсів"""
        try:
            if self.kafka_consumer:
                self.kafka_consumer.close()
                logger.info("🔌 Kafka з'єднання закрито")
            
            if self.cluster:
                self.cluster.shutdown()
                logger.info("🔌 Cassandra з'єднання закрито")
                
        except Exception as e:
            logger.error(f"Помилка при закритті: {e}")

def main():
    """Запуск додатку"""
    processor = EnergyDataProcessor()
    processor.run()

if __name__ == "__main__":
    main()