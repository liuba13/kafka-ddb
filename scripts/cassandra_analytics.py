#!/usr/bin/env python3
"""
Аналітика енергетичних даних 
"""

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from datetime import datetime
import logging

# Вимикаємо Cassandra warnings
logging.getLogger('cassandra').setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CleanAnalytics:
    """Клас аналітики"""
    
    def __init__(self):
        # Підключення з правильними налаштуваннями
        self.cluster = Cluster(
            ['127.0.0.1'],
            port=9042,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
            protocol_version=5,  # Фіксована версія
            connect_timeout=10,
            control_connection_timeout=10
        )
        self.session = self.cluster.connect('energy_kafka_db')
        print("✅ Підключено до energy_kafka_db")
    
    def show_summary(self):
        """Показати загальну інформацію"""
        print("\n" + "="*60)
        print("📊 ЗВІТ ПО ЕНЕРГЕТИЧНІЙ СИСТЕМІ")
        print("="*60)
        
        # 1. Загальна кількість записів (попереджуємо про aggregation warning)
        try:
            result = self.session.execute("SELECT COUNT(*) as total FROM power_metrics")
            total = result.one().total
            print(f"📋 Всього записів: {total:,}")
        except Exception as e:
            print(f"❌ Помилка підрахунку: {e}")
        
        # 2. Показати перші 10 записів
        print(f"\n📋 ОСТАННІ ЗАПИСИ:")
        try:
            rows = self.session.execute("SELECT plant_id, timestamp, station_name, station_type, power_output_mw FROM power_metrics LIMIT 10")
            
            count = 0
            for row in rows:
                count += 1
                plant_id_short = row.plant_id[:8] if row.plant_id else 'N/A'
                timestamp_str = row.timestamp.strftime('%m-%d %H:%M') if row.timestamp else 'N/A'
                station_name = row.station_name or 'Unknown'
                station_type = row.station_type or 'unknown'
                power = row.power_output_mw or 0
                
                print(f"  {count:2d}. {station_name} ({station_type}) - {power:.1f} МВт - {timestamp_str}")
            
            if count == 0:
                print("  ⚠️ Записів не знайдено")
                
        except Exception as e:
            print(f"❌ Помилка отримання записів: {e}")
        
        # 3. Унікальні станції (уникаємо DISTINCT щоб не було warnings)
        print(f"\n🏭 СТАНЦІЇ В СИСТЕМІ:")
        try:
            rows = self.session.execute("SELECT plant_id, station_name, station_type FROM power_metrics LIMIT 100")
            
            stations = {}
            for row in rows:
                plant_id = row.plant_id
                if plant_id not in stations:
                    stations[plant_id] = {
                        'name': row.station_name or 'Unknown',
                        'type': row.station_type or 'unknown',
                        'count': 0
                    }
                stations[plant_id]['count'] += 1
            
            for i, (plant_id, info) in enumerate(stations.items(), 1):
                plant_id_short = plant_id[:8] if plant_id else 'N/A'
                print(f"  {i}. {info['name']} ({info['type'].upper()}) - {info['count']} записів - ID: {plant_id_short}...")
            
        except Exception as e:
            print(f"❌ Помилка аналізу станцій: {e}")
        
        # 4. Статистика по типах
        print(f"\n📊 СТАТИСТИКА ПО ТИПАХ:")
        try:
            rows = self.session.execute("SELECT station_type, power_output_mw FROM power_metrics LIMIT 200")
            
            types_stats = {}
            for row in rows:
                station_type = row.station_type or 'unknown'
                power = row.power_output_mw or 0
                
                if station_type not in types_stats:
                    types_stats[station_type] = {'count': 0, 'total_power': 0, 'max_power': 0}
                
                types_stats[station_type]['count'] += 1
                types_stats[station_type]['total_power'] += power
                types_stats[station_type]['max_power'] = max(types_stats[station_type]['max_power'], power)
            
            for station_type, stats in types_stats.items():
                avg_power = stats['total_power'] / stats['count'] if stats['count'] > 0 else 0
                print(f"  {station_type.upper()}: {stats['count']} записів, сер: {avg_power:.1f} МВт, макс: {stats['max_power']:.1f} МВт")
            
        except Exception as e:
            print(f"❌ Помилка статистики: {e}")
    
    def show_alerts_summary(self):
        """Показати алерти якщо є"""
        print(f"\n🚨 АЛЕРТИ В СИСТЕМІ:")
        try:
            # Спочатку отримуємо зразок алертів для підрахунку
            alert_rows = list(self.session.execute("SELECT plant_id FROM power_alerts LIMIT 500"))
            alerts_count = len(alert_rows)
            
            print(f"📊 Приблизно алертів: {alerts_count}+ (в зразку)")
            
            if alerts_count > 0:
                # Показуємо останні алерти
                recent_alerts = self.session.execute("SELECT plant_id, timestamp, alert_type, severity, message FROM power_alerts LIMIT 10")
                
                print(f"📋 Останні алерти:")
                for i, alert in enumerate(recent_alerts, 1):
                    plant_id_short = alert.plant_id[:8] if alert.plant_id else 'N/A'
                    timestamp_str = alert.timestamp.strftime('%m-%d %H:%M') if alert.timestamp else 'N/A'
                    severity = alert.severity or 'unknown'
                    alert_type = alert.alert_type or 'unknown'
                    message = (alert.message or 'No message')[:50] + ('...' if len(alert.message or '') > 50 else '')
                    
                    print(f"  {i}. [{severity.upper()}] {alert_type} - {message} ({plant_id_short}..., {timestamp_str})")
            else:
                print("✅ Алертів немає - система працює нормально")
                
        except Exception as e:
            print(f"ℹ️ Таблиця алертів недоступна: {e}")
    
    def show_detailed_station_analysis(self, limit=3):
        """Детальний аналіз по станціях"""
        print(f"\n🔍 ДЕТАЛЬНИЙ АНАЛІЗ ПО СТАНЦІЯХ:")
        try:
            # Отримуємо кілька різних plant_id для аналізу
            plant_ids_rows = list(self.session.execute("SELECT DISTINCT plant_id FROM power_metrics LIMIT 50"))
            plant_ids = [row.plant_id for row in plant_ids_rows][:limit]
            
            for i, plant_id in enumerate(plant_ids, 1):
                print(f"\n  {i}. Станція ID: {plant_id[:8]}...")
                
                # Отримуємо останні записи для цієї станції
                station_query = f"SELECT timestamp, station_name, power_output_mw, voltage_kv, frequency_hz FROM power_metrics WHERE plant_id = '{plant_id}' ORDER BY timestamp DESC LIMIT 5"
                station_rows = self.session.execute(station_query)
                
                records = list(station_rows)
                if records:
                    station_name = records[0].station_name or 'Unknown'
                    print(f"     Назва: {station_name}")
                    print(f"     Останні показники:")
                    
                    for j, record in enumerate(records, 1):
                        timestamp_str = record.timestamp.strftime('%m-%d %H:%M:%S') if record.timestamp else 'N/A'
                        power = record.power_output_mw or 0
                        voltage = record.voltage_kv or 0
                        frequency = record.frequency_hz or 0
                        
                        # Визначаємо статус показників
                        power_status = "🟢" if power > 100 else "🟡" if power > 10 else "🔴"
                        voltage_status = "🟢" if 218 <= voltage <= 222 else "🟠"
                        freq_status = "🟢" if 49.8 <= frequency <= 50.2 else "🔴"
                        
                        print(f"       {j}. {power:.1f} МВт {power_status}, {voltage:.1f} кВ {voltage_status}, {frequency:.2f} Гц {freq_status} ({timestamp_str})")
                else:
                    print(f"     Записів не знайдено")
        
        except Exception as e:
            print(f"❌ Помилка детального аналізу: {e}")
    
    def cleanup(self):
        """Закриття з'єднання"""
        if self.cluster:
            self.cluster.shutdown()

def main():
    """Головна функція"""
    print("АНАЛІТИКА ЕНЕРГЕТИЧНИХ ДАНИХ")
    print("Keyspace: energy_kafka_db ")
    print("="*50)
    
    analytics = CleanAnalytics()
    
    try:
        # Основний звіт
        analytics.show_summary()
        
        # Алерти
        analytics.show_alerts_summary()
        
        # Детальний аналіз станцій
        analytics.show_detailed_station_analysis()
        
    except KeyboardInterrupt:
        print("\n👋 До побачення!")
    except Exception as e:
        logger.error(f"❌ Критична помилка: {e}")
    finally:
        analytics.cleanup()

if __name__ == "__main__":
    main()