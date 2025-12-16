from datetime import datetime, date
from typing import List, Dict, Any
import pandas as pd
from database import GHUDatabase

class GHUReports:
    """Класс для генерации отчетов"""
    
    def __init__(self, db: GHUDatabase):
        self.db = db
    
    def generate_buildings_report(self) -> pd.DataFrame:
        """Отчет по всем домам"""
        buildings = self.db.get_all_buildings()
        
        data = []
        for building in buildings:
            apartments = self.db.get_apartments_in_building(building['id'])
            total_area = sum(apt['area'] for apt in apartments)
            
            data.append({
                'Адрес': building['address'],
                'Этажей': building['floors'] or '-',
                'Год постройки': building['year_built'] or '-',
                'Квартир': building['total_apartments'],
                'Общая площадь': round(total_area, 2)
            })
        
        return pd.DataFrame(data)
    
    def generate_payments_report(self, year: int = None, month: int = None) -> pd.DataFrame:
        """Отчет по платежам"""
        self.db.connect()
        cursor = self.db.conn.cursor()
        
        query = """
        SELECT 
            b.address as дом,
            a.number as квартира,
            r.full_name as жилец,
            p.period as период,
            p.amount as сумма,
            CASE WHEN p.is_paid THEN 'Оплачено' ELSE 'Не оплачено' END as статус,
            p.payment_date as дата_оплаты
        FROM payments p
        JOIN apartments a ON p.apartment_id = a.id
        JOIN buildings b ON a.building_id = b.id
        JOIN residents r ON a.id = r.apartment_id AND r.is_owner = 1
        """
        
        params = []
        if year and month:
            query += " WHERE strftime('%Y', p.period) = ? AND strftime('%m', p.period) = ?"
            params = [str(year), f"{month:02d}"]
        
        query += " ORDER BY p.period DESC, b.address, a.number"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        data = []
        for row in rows:
            data.append({
                'Дом': row['дом'],
                'Квартира': row['квартира'],
                'Жилец': row['жилец'],
                'Период': row['период'],
                'Сумма': row['сумма'],
                'Статус': row['статус'],
                'Дата оплаты': row['дата_оплаты'] or '-'
            })
        
        return pd.DataFrame(data)
    
    def generate_electoral_register(self) -> pd.DataFrame:
        """Список жильцов для избирательного участка"""
        self.db.connect()
        cursor = self.db.conn.cursor()
        
        cursor.execute("""
        SELECT 
            b.address as адрес,
            a.number as квартира,
            r.full_name as фио,
            r.birth_date as дата_рождения,
            date('now') - r.birth_date as возраст,
            r.registration_date as дата_регистрации
        FROM residents r
        JOIN apartments a ON r.apartment_id = a.id
        JOIN buildings b ON a.building_id = b.id
        WHERE 
            date('now', '-18 years') >= r.birth_date
            AND r.is_owner = 1
        ORDER BY b.address, a.number, r.full_name
        """)
        
        rows = cursor.fetchall()
        
        data = []
        for row in rows:
            # Вычисляем возраст
            birth_date = datetime.strptime(row['дата_рождения'], '%Y-%m-%d').date()
            today = date.today()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            
            data.append({
                'Адрес': row['адрес'],
                'Квартира': row['квартира'],
                'ФИО': row['фио'],
                'Дата рождения': row['дата_рождения'],
                'Возраст': age,
                'Дата регистрации': row['дата_регистрации']
            })
        
        return pd.DataFrame(data)
    
    def generate_debts_report(self) -> pd.DataFrame:
        """Отчет по задолженностям"""
        self.db.connect()
        cursor = self.db.conn.cursor()
        
        cursor.execute("""
        SELECT 
            b.address as адрес,
            a.number as квартира,
            r.full_name as должник,
            COUNT(p.id) as месяцев_задолженности,
            SUM(p.amount) as общая_задолженность,
            MAX(p.period) as последний_период
        FROM payments p
        JOIN apartments a ON p.apartment_id = a.id
        JOIN buildings b ON a.building_id = b.id
        JOIN residents r ON a.id = r.apartment_id AND r.is_owner = 1
        WHERE p.is_paid = 0
        GROUP BY b.address, a.number, r.full_name
        HAVING SUM(p.amount) > 0
        ORDER BY общая_задолженность DESC
        """)
        
        rows = cursor.fetchall()
        
        data = []
        for row in rows:
            data.append({
                'Адрес': row['адрес'],
                'Квартира': row['квартира'],
                'Должник': row['должник'],
                'Месяцев задолженности': row['месяцев_задолженности'],
                'Общая задолженность': row['общая_задолженность'],
                'Последний период': row['последний_период']
            })
        
        return pd.DataFrame(data)
    
    def generate_apartment_details(self, apartment_id: int) -> Dict:
        """Подробная информация по квартире"""
        self.db.connect()
        cursor = self.db.conn.cursor()
        
        # Информация о квартире
        cursor.execute("""
        SELECT 
            a.*,
            b.address as building_address,
            b.floors,
            b.year_built
        FROM apartments a
        JOIN buildings b ON a.building_id = b.id
        WHERE a.id = ?
        """, (apartment_id,))
        
        apartment = dict(cursor.fetchone())
        
        # Жильцы
        residents = self.db.get_residents_in_apartment(apartment_id)
        
        # Платежи
        payments = self.db.get_payments_for_apartment(apartment_id)
        
        return {
            'Квартира': {
                'Адрес': f"{apartment['building_address']}, кв. {apartment['number']}",
                'Площадь': apartment['area'],
                'Комнат': apartment['rooms'] or '-',
                'Приватизирована': 'Да' if apartment['privatized'] else 'Нет'
            },
            'Жильцы': residents,
            'Платежи': payments,
            'Статистика': {
                'Всего платежей': len(payments),
                'Оплачено': sum(1 for p in payments if p['is_paid']),
                'Не оплачено': sum(1 for p in payments if not p['is_paid']),
                'Общая сумма': sum(p['amount'] for p in payments)
            }
        }