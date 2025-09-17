#!/usr/bin/env python3
"""
Діагностика проблем з Cassandra
"""

import socket
import subprocess
import sys
import time

def check_port(host, port):
    """Перевірка доступності порту"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False

def check_cassandra_process():
    """Перевірка чи запущений процес Cassandra"""
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        return 'cassandra' in result.stdout.lower()
    except:
        return False

def try_simple_connection():
    """Спроба простого підключення до Cassandra"""
    try:
        from cassandra.cluster import Cluster
        from cassandra.policies import DCAwareRoundRobinPolicy
        
        # Мінімальні налаштування
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect()
        
        # Перевірка версії
        row = session.execute("SELECT release_version FROM system.local").one()
        version = row.release_version
        
        cluster.shutdown()
        return True, version
    except Exception as e:
        return False, str(e)

def main():
    print("🔍 ДІАГНОСТИКА CASSANDRA")
    print("=" * 30)
    
    # Крок 1: Перевірка порту
    print("1. Перевірка порту 9042...")
    if check_port('127.0.0.1', 9042):
        print("   ✅ Порт 9042 відкритий")
    else:
        print("   ❌ Порт 9042 недоступний")
        print("   💡 Cassandra ймовірно не запущена")
    
    # Крок 2: Перевірка процесу
    print("\n2. Перевірка процесу Cassandra...")
    if check_cassandra_process():
        print("   ✅ Процес Cassandra знайдено")
    else:
        print("   ❌ Процес Cassandra не знайдено")
    
    # Крок 3: Спроба підключення
    print("\n3. Тест підключення...")
    success, result = try_simple_connection()
    if success:
        print(f"   ✅ Підключення успішне (версія: {result})")
    else:
        print(f"   ❌ Помилка підключення: {result}")
    
    # Рекомендації
    print("\n💡 РЕКОМЕНДАЦІЇ:")
    
    if not check_port('127.0.0.1', 9042):
        print("🚀 Запустіть Cassandra:")
        print("   macOS: brew services start cassandra")
        print("   Linux: sudo systemctl start cassandra")
        print("   Вручну: cassandra -f")
        print("\n⏳ Зачекайте 30-60 секунд після запуску")
        
    elif not success:
        print("🔧 Проблеми з підключенням:")
        print("   • Перезапустіть Cassandra")
        print("   • Перевірте логи: tail -f /usr/local/var/log/cassandra/system.log")
        print("   • Спробуйте: cqlsh localhost 9042")
    
    else:
        print("✅ Cassandra працює нормально!")

if __name__ == "__main__":
    main()