import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Any
from database import GHUDatabase

class GHUReports:
    """Класс для генерации отчетов"""
    
    def __init__(self, db: GHUDatabase):
        self.db = db
    
    def generate_payments_report(self, filters: Dict = None, sort_by: str = "period", ascending: bool = True):
        """
        Отчет 1: Платежи по услугам
        """
        conn = self.db.connect()
        
        # Базовый запрос
        query = """
        SELECT 
            p.period as Период,
            b.address as Адрес_дома,
            a.number as Квартира,
            r.full_name as Жилец,
            s.name as Услуга,
            a.area as Площадь,
            s.price as Тариф,
            p.amount as Сумма,
            CASE WHEN p.is_paid THEN 'Оплачено' ELSE 'Не оплачено' END as Статус,
            p.payment_date as Дата_оплаты
        FROM payments p
        JOIN apartments a ON p.apartment_id = a.id
        JOIN buildings b ON a.building_id = b.id
        JOIN residents r ON a.id = r.apartment_id AND r.is_owner = 1
        JOIN services s ON p.service_id = s.id
        WHERE 1=1
        """
        
        params = []
        
        if filters:
            if 'period' in filters and filters['period']:
                query += " AND p.period LIKE ?"
                params.append(f'%{filters["period"]}%')
            
            if 'address' in filters and filters['address']:
                query += " AND b.address LIKE ?"
                params.append(f'%{filters["address"]}%')
            
            if 'status' in filters and filters['status']:
                if filters['status'] == 'paid':
                    query += " AND p.is_paid = 1"
                elif filters['status'] == 'unpaid':
                    query += " AND p.is_paid = 0"
            
            if 'min_amount' in filters and filters['min_amount']:
                try:
                    query += " AND p.amount >= ?"
                    params.append(float(filters['min_amount']))
                except:
                    pass
        
        # Сортировка
        sort_mapping = {
            'period': 'p.period',
            'address': 'b.address',
            'amount': 'p.amount',
            'status': 'p.is_paid'
        }
        
        sort_field = sort_mapping.get(sort_by, 'p.period')
        order = "ASC" if ascending else "DESC"
        query += f" ORDER BY {sort_field} {order}"
        
        # Выполняем запрос
        try:
            df = pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            print(f"Ошибка SQL: {e}")
            return pd.DataFrame(), pd.DataFrame(), {}
        
        if not df.empty:
            # Вычисляем дополнительные поля
            df['Сумма_с_НДС'] = df['Сумма'] * 1.2
            df['Площадь_на_человека'] = df.apply(
                lambda row: f"{row['Площадь']:.1f} м²", axis=1
            )
            
            # Группировка по адресу
            grouped = df.groupby('Адрес_дома').agg({
                'Сумма': 'sum',
                'Квартира': 'count',
                'Площадь': 'sum'
            }).round(2)
            
            grouped = grouped.rename(columns={
                'Сумма': 'Итого_по_дому',
                'Квартира': 'Кол-во_квартир',
                'Площадь': 'Общая_площадь'
            })
            
            # Итоги
            totals = {
                'Всего_сумма': df['Сумма'].sum(),
                'Средний_чек': df['Сумма'].mean(),
                'Кол-во_платежей': len(df),
                'Процент_оплаты': (df['Статус'] == 'Оплачено').mean() * 100
            }
            
            return df, grouped, totals
        
        return pd.DataFrame(), pd.DataFrame(), {}
    
    def generate_debts_report(self, filters: Dict = None, sort_by: str = "amount", ascending: bool = False):
        """
        Отчет 2: Задолженности по квартирам
        """
        conn = self.db.connect()
        
        query = """
        SELECT 
            b.address as Адрес,
            a.number as Квартира,
            r.full_name as Должник,
            COUNT(p.id) as Месяцев_задолженности,
            SUM(p.amount) as Общая_задолженность,
            MAX(p.period) as Последний_период,
            a.area as Площадь,
            r.phone as Телефон
        FROM payments p
        JOIN apartments a ON p.apartment_id = a.id
        JOIN buildings b ON a.building_id = b.id
        JOIN residents r ON a.id = r.apartment_id AND r.is_owner = 1
        WHERE p.is_paid = 0
        GROUP BY a.id, r.full_name
        HAVING SUM(p.amount) > 0
        """
        
        params = []
        
        if filters:
            if 'address' in filters and filters['address']:
                query += " AND b.address LIKE ?"
                params.append(f'%{filters["address"]}%')
            
            if 'min_debt' in filters and filters['min_debt']:
                try:
                    query += " HAVING SUM(p.amount) >= ?"
                    params.append(float(filters['min_debt']))
                except:
                    pass
        
        # Сортировка
        sort_mapping = {
            'amount': 'SUM(p.amount)',
            'period': 'MAX(p.period)',
            'address': 'b.address',
            'months': 'COUNT(p.id)'
        }
        
        sort_field = sort_mapping.get(sort_by, 'SUM(p.amount)')
        order = "DESC" if not ascending else "ASC"
        query += f" ORDER BY {sort_field} {order}"
        
        try:
            df = pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            print(f"Ошибка SQL: {e}")
            return pd.DataFrame(), pd.DataFrame(), {}
        
        if not df.empty:
            # Вычисляемые поля
            df['Долг_за_м2'] = df['Общая_задолженность'] / df['Площадь']
            df['Стаж_задолженности'] = df['Месяцев_задолженности'].apply(
                lambda x: f"{x} мес." if x < 12 else f"{x//12} г. {x%12} мес."
            )
            
            # Группировка по адресу
            grouped = df.groupby('Адрес').agg({
                'Общая_задолженность': 'sum',
                'Квартира': 'count',
                'Месяцев_задолженности': 'mean'
            }).round(2)
            
            grouped = grouped.rename(columns={
                'Общая_задолженность': 'Сумма_долга_по_дому',
                'Квартира': 'Кол-во_должников',
                'Месяцев_задолженности': 'Средний_стаж_долга'
            })
            
            # Итоги
            totals = {
                'Общий_долг': df['Общая_задолженность'].sum(),
                'Средний_долг': df['Общая_задолженность'].mean(),
                'Всего_должников': len(df),
                'Самый_большой_долг': df['Общая_задолженность'].max()
            }
            
            return df, grouped, totals
        
        return pd.DataFrame(), pd.DataFrame(), {}
    
    def generate_electoral_register(self, filters: Dict = None, sort_by: str = "birth_date", ascending: bool = True):
        """
        Отчет 3: Избирательные списки
        """
        conn = self.db.connect()
        
        query = """
        SELECT 
            b.address as Адрес,
            a.number as Квартира,
            r.full_name as ФИО,
            r.birth_date as Дата_рождения,
            (strftime('%Y', 'now') - strftime('%Y', r.birth_date)) as Возраст,
            r.passport as Паспорт,
            r.registration_date as Дата_регистрации
        FROM residents r
        JOIN apartments a ON r.apartment_id = a.id
        JOIN buildings b ON a.building_id = b.id
        WHERE (strftime('%Y', 'now') - strftime('%Y', r.birth_date)) >= 18
        """
        
        params = []
        
        if filters:
            if 'address' in filters and filters['address']:
                query += " AND b.address LIKE ?"
                params.append(f'%{filters["address"]}%')
            
            if 'min_age' in filters and filters['min_age']:
                try:
                    min_age = int(filters['min_age'])
                    query += " AND (strftime('%Y', 'now') - strftime('%Y', r.birth_date)) >= ?"
                    params.append(min_age)
                except:
                    pass
            
            if 'max_age' in filters and filters['max_age']:
                try:
                    max_age = int(filters['max_age'])
                    query += " AND (strftime('%Y', 'now') - strftime('%Y', r.birth_date)) <= ?"
                    params.append(max_age)
                except:
                    pass
        
        # Сортировка
        sort_mapping = {
            'birth_date': 'r.birth_date',
            'age': 'strftime("%Y", "now") - strftime("%Y", r.birth_date)',
            'address': 'b.address'
        }
        
        sort_field = sort_mapping.get(sort_by, 'r.birth_date')
        order = "ASC" if ascending else "DESC"
        query += f" ORDER BY {sort_field} {order}"
        
        try:
            df = pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            print(f"Ошибка SQL: {e}")
            return pd.DataFrame(), pd.DataFrame(), {}
        
        if not df.empty:
            # Вычисляем точный возраст
            def calculate_age(birth_date_str):
                try:
                    birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d').date()
                    today = date.today()
                    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
                except:
                    return 0
            
            df['Возраст_лет'] = df['Дата_рождения'].apply(calculate_age)
            
            # Возрастные группы
            def get_age_group(age):
                if age < 30:
                    return "18-29 лет"
                elif age < 45:
                    return "30-44 года"
                elif age < 60:
                    return "45-59 лет"
                else:
                    return "60+ лет"
            
            df['Возрастная_группа'] = df['Возраст_лет'].apply(get_age_group)
            
            # Группировка
            grouped = df.groupby(['Адрес', 'Возрастная_группа']).agg({
                'ФИО': 'count',
                'Возраст_лет': 'mean'
            }).round(1)
            
            grouped = grouped.rename(columns={
                'ФИО': 'Количество',
                'Возраст_лет': 'Средний_возраст'
            })
            
            # Итоги
            totals = {
                'Всего_избирателей': len(df),
                'Средний_возраст': df['Возраст_лет'].mean(),
                'Самый_старший': df['Возраст_лет'].max(),
                'Самый_молодой': df['Возраст_лет'].min()
            }
            
            return df, grouped, totals
        
        return pd.DataFrame(), pd.DataFrame(), {}
