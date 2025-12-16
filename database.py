import sqlite3
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import pandas as pd

class GHUDatabase:
    """Простая база данных для службы заказчика ГЖУ на SQLite"""
    
    def __init__(self, db_path: str = "ghu_database.db"):
        self.db_path = db_path
        self.conn = None
        self._create_tables()
    
    def connect(self):
        """Подключение к базе данных"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Для доступа к колонкам по имени
    
    def close(self):
        """Закрытие соединения"""
        if self.conn:
            self.conn.close()
    
    def _create_tables(self):
        """Создание всех таблиц"""
        self.connect()
        
        # Таблица домов
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS buildings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            floors INTEGER,
            year_built INTEGER,
            total_apartments INTEGER DEFAULT 0
        )
        """)
        
        # Таблица квартир
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS apartments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            building_id INTEGER NOT NULL,
            number TEXT NOT NULL,
            area REAL NOT NULL,
            rooms INTEGER,
            privatized BOOLEAN DEFAULT 0,
            has_cold_water BOOLEAN DEFAULT 0,
            has_hot_water BOOLEAN DEFAULT 0,
            has_elevator BOOLEAN DEFAULT 0,
            FOREIGN KEY (building_id) REFERENCES buildings(id) ON DELETE CASCADE
        )
        """)
        
        # Таблица жильцов
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS residents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apartment_id INTEGER NOT NULL,
            full_name TEXT NOT NULL,
            birth_date TEXT NOT NULL,
            passport TEXT,
            is_owner BOOLEAN DEFAULT 0,
            registration_date TEXT DEFAULT CURRENT_DATE,
            FOREIGN KEY (apartment_id) REFERENCES apartments(id) ON DELETE CASCADE
        )
        """)
        
        # Таблица платежей
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apartment_id INTEGER NOT NULL,
            period TEXT NOT NULL,
            amount REAL NOT NULL,
            is_paid BOOLEAN DEFAULT 0,
            payment_date TEXT,
            service_type TEXT DEFAULT 'квартплата',
            FOREIGN KEY (apartment_id) REFERENCES apartments(id) ON DELETE CASCADE
        )
        """)
        
        # Создаем индексы для ускорения запросов
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_apartments_building ON apartments(building_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_residents_apartment ON residents(apartment_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_period ON payments(period)")
        
        self.conn.commit()
        print("Таблицы созданы успешно!")
    
    def add_building(self, address: str, floors: int = None, year_built: int = None) -> int:
        """Добавить дом"""
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO buildings (address, floors, year_built) VALUES (?, ?, ?)",
            (address, floors, year_built)
        )
        self.conn.commit()
        return cursor.lastrowid
    
    def add_apartment(self, building_id: int, number: str, area: float, 
                     rooms: int = None, privatized: bool = False) -> int:
        """Добавить квартиру"""
        self.connect()
        cursor = self.conn.cursor()
        
        cursor.execute(
            """INSERT INTO apartments 
            (building_id, number, area, rooms, privatized) 
            VALUES (?, ?, ?, ?, ?)""",
            (building_id, number, area, rooms, 1 if privatized else 0)
        )
        
        # Увеличиваем счетчик квартир в доме
        cursor.execute(
            "UPDATE buildings SET total_apartments = total_apartments + 1 WHERE id = ?",
            (building_id,)
        )
        
        self.conn.commit()
        return cursor.lastrowid
    
    def add_resident(self, apartment_id: int, full_name: str, birth_date: str,
                    passport: str = None, is_owner: bool = False) -> int:
        """Добавить жильца"""
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO residents 
            (apartment_id, full_name, birth_date, passport, is_owner) 
            VALUES (?, ?, ?, ?, ?)""",
            (apartment_id, full_name, birth_date, passport, 1 if is_owner else 0)
        )
        self.conn.commit()
        return cursor.lastrowid
    
    def add_payment(self, apartment_id: int, period: str, amount: float,
                   is_paid: bool = False, payment_date: str = None) -> int:
        """Добавить платеж"""
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO payments 
            (apartment_id, period, amount, is_paid, payment_date) 
            VALUES (?, ?, ?, ?, ?)""",
            (apartment_id, period, amount, 1 if is_paid else 0, payment_date)
        )
        self.conn.commit()
        return cursor.lastrowid
    
    def get_all_buildings(self) -> List[Dict]:
        """Получить все дома"""
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM buildings ORDER BY address")
        return [dict(row) for row in cursor.fetchall()]
    
    def get_apartments_in_building(self, building_id: int) -> List[Dict]:
        """Получить все квартиры в доме"""
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM apartments WHERE building_id = ? ORDER BY number",
            (building_id,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_residents_in_apartment(self, apartment_id: int) -> List[Dict]:
        """Получить всех жильцов в квартире"""
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM residents WHERE apartment_id = ? ORDER BY full_name",
            (apartment_id,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_payments_for_apartment(self, apartment_id: int) -> List[Dict]:
        """Получить все платежи по квартире"""
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM payments WHERE apartment_id = ? ORDER BY period DESC",
            (apartment_id,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def mark_payment_as_paid(self, payment_id: int, payment_date: str = None):
        """Отметить платеж как оплаченный"""
        self.connect()
        if not payment_date:
            payment_date = datetime.now().strftime("%Y-%m-%d")
        
        self.conn.execute(
            "UPDATE payments SET is_paid = 1, payment_date = ? WHERE id = ?",
            (payment_date, payment_id)
        )
        self.conn.commit()
    
    def delete_building(self, building_id: int):
        """Удалить дом (автоматически удалятся все связанные квартиры, жильцы и платежи)"""
        self.connect()
        self.conn.execute("DELETE FROM buildings WHERE id = ?", (building_id,))
        self.conn.commit()
    
    def add_sample_data(self):
        """Добавить примеры данных для тестирования"""
        print("Добавление тестовых данных...")
        
        # Добавляем дома
        building1_id = self.add_building("ул. Ленина, 10", 5, 1985)
        building2_id = self.add_building("ул. Советская, 25", 9, 1990)
        
        # Добавляем квартиры
        apartment1_id = self.add_apartment(building1_id, "25", 55.5, 2, True)
        apartment2_id = self.add_apartment(building1_id, "26", 42.0, 1, False)
        apartment3_id = self.add_apartment(building2_id, "101", 75.0, 3, True)
        
        # Добавляем жильцов
        self.add_resident(apartment1_id, "Иванов Иван Иванович", "1980-05-15", 
                         "1234 567890", True)
        self.add_resident(apartment1_id, "Иванова Мария Петровна", "1985-07-22",
                         "1234 567891", False)
        self.add_resident(apartment2_id, "Петров Петр Петрович", "1975-11-30",
                         "4321 123456", True)
        self.add_resident(apartment3_id, "Сидорова Анна Васильевна", "1950-12-05",
                         "5678 901234", True)
        
        # Добавляем платежи
        self.add_payment(apartment1_id, "2024-01-01", 1500.00, True, "2024-01-15")
        self.add_payment(apartment1_id, "2024-02-01", 1500.00, False)
        self.add_payment(apartment2_id, "2024-01-01", 1200.00, True, "2024-01-10")
        self.add_payment(apartment3_id, "2024-01-01", 1800.00, True, "2024-01-20")
        
        print("Тестовые данные добавлены успешно!")