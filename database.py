import sqlite3
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import pandas as pd

class GHUDatabase:
    """База данных для службы заказчика ГЖУ"""
    
    def __init__(self, db_path: str = "ghu_database.db"):
        self.db_path = db_path
        self.conn = None
        self._create_tables()
        self._insert_sample_data()
    
    def connect(self):
        """Подключение к базе данных"""
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def close(self):
        """Закрытие соединения"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def _create_tables(self):
        """Создание всех таблиц"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Таблица районов
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS districts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            manager TEXT,
            phone TEXT
        )
        """)
        
        # Таблица домов
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS buildings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id INTEGER NOT NULL,
            address TEXT NOT NULL,
            year_built INTEGER,
            floors INTEGER,
            total_apartments INTEGER DEFAULT 0,
            FOREIGN KEY (district_id) REFERENCES districts(id) ON DELETE CASCADE
        )
        """)
        
        # Таблица квартир
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS apartments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            building_id INTEGER NOT NULL,
            number TEXT NOT NULL,
            area REAL NOT NULL CHECK(area > 0),
            rooms INTEGER,
            privatized BOOLEAN DEFAULT 0,
            has_water BOOLEAN DEFAULT 1,
            has_heating BOOLEAN DEFAULT 1,
            has_electricity BOOLEAN DEFAULT 1,
            FOREIGN KEY (building_id) REFERENCES buildings(id) ON DELETE CASCADE
        )
        """)
        
        # Таблица жильцов
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS residents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apartment_id INTEGER NOT NULL,
            full_name TEXT NOT NULL,
            birth_date TEXT NOT NULL,
            passport TEXT,
            is_owner BOOLEAN DEFAULT 0,
            phone TEXT,
            registration_date TEXT DEFAULT CURRENT_DATE,
            FOREIGN KEY (apartment_id) REFERENCES apartments(id) ON DELETE CASCADE
        )
        """)
        
        # Таблица услуг
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS services (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            price_per_m2 REAL NOT NULL CHECK(price_per_m2 > 0),
            description TEXT
        )
        """)
        
        # Таблица платежей (отношение 1:М к квартирам)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            apartment_id INTEGER NOT NULL,
            service_id INTEGER NOT NULL,
            period TEXT NOT NULL,
            amount REAL NOT NULL CHECK(amount >= 0),
            is_paid BOOLEAN DEFAULT 0,
            payment_date TEXT,
            FOREIGN KEY (apartment_id) REFERENCES apartments(id) ON DELETE CASCADE,
            FOREIGN KEY (service_id) REFERENCES services(id)
        )
        """)
        
        # Индексы для ускорения запросов
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_buildings_district ON buildings(district_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_apartments_building ON apartments(building_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_residents_apartment ON residents(apartment_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_apartment ON payments(apartment_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_period ON payments(period)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_payments_paid ON payments(is_paid)")
        
        conn.commit()
    
    def _insert_sample_data(self):
        """Вставка тестовых данных"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Проверяем, есть ли уже данные
        cursor.execute("SELECT COUNT(*) FROM districts")
        if cursor.fetchone()[0] > 0:
            return
        
        # Добавляем районы
        districts = [
            ("Центральный район", "Иванов И.И.", "+7-111-222-3333"),
            ("Северный район", "Петров П.П.", "+7-111-222-4444"),
            ("Южный район", "Сидоров С.С.", "+7-111-222-5555")
        ]
        
        for name, manager, phone in districts:
            cursor.execute(
                "INSERT INTO districts (name, manager, phone) VALUES (?, ?, ?)",
                (name, manager, phone)
            )
        
        # Добавляем услуги
        services = [
            ("Холодное водоснабжение", 25.50, "Водоснабжение холодной водой"),
            ("Горячее водоснабжение", 45.30, "Водоснабжение горячей водой"),
            ("Отопление", 35.20, "Отопление помещений"),
            ("Электроснабжение", 4.80, "Электроэнергия"),
            ("Вывоз ТБО", 8.90, "Вывоз твердых бытовых отходов")
        ]
        
        for name, price, desc in services:
            cursor.execute(
                "INSERT INTO services (name, price_per_m2, description) VALUES (?, ?, ?)",
                (name, price, desc)
            )
        
        conn.commit()
    
    # === CRUD операции для всех таблиц ===
    
    def get_all(self, table_name: str) -> List[Dict]:
        """Получить все записи из таблицы"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        return [dict(row) for row in cursor.fetchall()]
    
    def get_by_id(self, table_name: str, record_id: int) -> Optional[Dict]:
        """Получить запись по ID"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE id = ?", (record_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def insert(self, table_name: str, data: Dict) -> int:
        """Вставить новую запись"""
        conn = self.connect()
        cursor = conn.cursor()
        
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        values = tuple(data.values())
        
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, values)
        conn.commit()
        
        return cursor.lastrowid
    
    def update(self, table_name: str, record_id: int, data: Dict) -> bool:
        """Обновить запись"""
        conn = self.connect()
        cursor = conn.cursor()
        
        set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
        values = tuple(data.values()) + (record_id,)
        
        query = f"UPDATE {table_name} SET {set_clause} WHERE id = ?"
        cursor.execute(query, values)
        conn.commit()
        
        return cursor.rowcount > 0
    
    def delete(self, table_name: str, record_id: int) -> bool:
        """Удалить запись"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute(f"DELETE FROM {table_name} WHERE id = ?", (record_id,))
        conn.commit()
        
        return cursor.rowcount > 0
    
    def search(self, table_name: str, field: str, value: str) -> List[Dict]:
        """Поиск записей по полю"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Для числовых полей
        if field in ['id', 'year_built', 'floors', 'area', 'rooms', 'price_per_m2', 'amount']:
            try:
                num_value = float(value) if '.' in value else int(value)
                cursor.execute(f"SELECT * FROM {table_name} WHERE {field} = ?", (num_value,))
            except:
                cursor.execute(f"SELECT * FROM {table_name} WHERE {field} LIKE ?", (f'%{value}%',))
        else:
            cursor.execute(f"SELECT * FROM {table_name} WHERE {field} LIKE ?", (f'%{value}%',))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def filter_records(self, table_name: str, conditions: Dict) -> List[Dict]:
        """Фильтрация записей по нескольким полям"""
        conn = self.connect()
        cursor = conn.cursor()
        
        if not conditions:
            return self.get_all(table_name)
        
        where_clauses = []
        values = []
        
        for field, value in conditions.items():
            if value not in ['', None]:
                if isinstance(value, (int, float)):
                    where_clauses.append(f"{field} = ?")
                    values.append(value)
                else:
                    where_clauses.append(f"{field} LIKE ?")
                    values.append(f'%{value}%')
        
        if not where_clauses:
            return self.get_all(table_name)
        
        query = f"SELECT * FROM {table_name} WHERE {' AND '.join(where_clauses)}"
        cursor.execute(query, values)
        
        return [dict(row) for row in cursor.fetchall()]
    
    def sort_records(self, table_name: str, field: str, ascending: bool = True) -> List[Dict]:
        """Сортировка записей по полю"""
        conn = self.connect()
        cursor = conn.cursor()
        
        order = "ASC" if ascending else "DESC"
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY {field} {order}")
        
        return [dict(row) for row in cursor.fetchall()]
    
    # === Специфичные методы для отношений ===
    
    def get_buildings_by_district(self, district_id: int) -> List[Dict]:
        """Получить все дома в районе"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT b.*, d.name as district_name 
            FROM buildings b 
            JOIN districts d ON b.district_id = d.id 
            WHERE b.district_id = ?
            ORDER BY b.address
        """, (district_id,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_apartments_by_building(self, building_id: int) -> List[Dict]:
        """Получить все квартиры в доме"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.*, b.address as building_address 
            FROM apartments a 
            JOIN buildings b ON a.building_id = b.id 
            WHERE a.building_id = ?
            ORDER BY a.number
        """, (building_id,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_residents_by_apartment(self, apartment_id: int) -> List[Dict]:
        """Получить всех жильцов в квартире"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT r.*, a.number as apartment_number, b.address as building_address
            FROM residents r 
            JOIN apartments a ON r.apartment_id = a.id 
            JOIN buildings b ON a.building_id = b.id 
            WHERE r.apartment_id = ?
            ORDER BY r.is_owner DESC, r.full_name
        """, (apartment_id,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_payments_by_apartment(self, apartment_id: int) -> List[Dict]:
        """Получить все платежи по квартире"""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.*, s.name as service_name, a.number as apartment_number
            FROM payments p 
            JOIN services s ON p.service_id = s.id 
            JOIN apartments a ON p.apartment_id = a.id 
            WHERE p.apartment_id = ?
            ORDER BY p.period DESC
        """, (apartment_id,))
        return [dict(row) for row in cursor.fetchall()]
    
    def add_apartment_with_residents(self, building_id: int, apartment_data: Dict, residents_data: List[Dict]) -> int:
        """Добавить квартиру с жильцами (форма 1:М)"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Добавляем квартиру
            cursor.execute("""
                INSERT INTO apartments (building_id, number, area, rooms, privatized, has_water, has_heating, has_electricity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                building_id,
                apartment_data['number'],
                apartment_data['area'],
                apartment_data.get('rooms'),
                1 if apartment_data.get('privatized') else 0,
                1 if apartment_data.get('has_water', True) else 0,
                1 if apartment_data.get('has_heating', True) else 0,
                1 if apartment_data.get('has_electricity', True) else 0
            ))
            
            apartment_id = cursor.lastrowid
            
            # Обновляем счетчик квартир в доме
            cursor.execute("UPDATE buildings SET total_apartments = total_apartments + 1 WHERE id = ?", (building_id,))
            
            # Добавляем жильцов
            for resident in residents_data:
                cursor.execute("""
                    INSERT INTO residents (apartment_id, full_name, birth_date, passport, is_owner, phone)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    apartment_id,
                    resident['full_name'],
                    resident['birth_date'],
                    resident.get('passport'),
                    1 if resident.get('is_owner') else 0,
                    resident.get('phone')
                ))
            
            conn.commit()
            return apartment_id
            
        except Exception as e:
            conn.rollback()
            raise e
    
    def calculate_payment(self, apartment_id: int, service_id: int, period: str) -> float:
        """Рассчитать сумму платежа"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Получаем площадь квартиры и цену услуги
        cursor.execute("""
            SELECT a.area, s.price_per_m2 
            FROM apartments a, services s 
            WHERE a.id = ? AND s.id = ?
        """, (apartment_id, service_id))
        
        result = cursor.fetchone()
        if result:
            area, price = result
            return area * price
        return 0.0
