from database import GHUDatabase
from reports import GHUReports
import pandas as pd

def print_title(title):
    print("\n" + "="*50)
    print(f"  {title}")
    print("="*50)

def main():
    # Создаем базу данных
    db = GHUDatabase("ghu_simple.db")
    
    # Добавляем тестовые данные
    db.add_sample_data()
    
    # Создаем объект для отчетов
    reports = GHUReports(db)
    
    while True:
        print("\n" + "="*50)
        print("  СИСТЕМА УЧЕТА ЖИЛОГО ФОНДА ГЖУ")
        print("="*50)
        print("1. Показать все дома")
        print("2. Отчет по платежам")
        print("3. Избирательный список")
        print("4. Отчет по задолженностям")
        print("5. Добавить новый дом")
        print("6. Добавить квартиру")
        print("7. Отметить платеж как оплаченный")
        print("8. Подробности по квартире")
        print("0. Выход")
        
        choice = input("\nВыберите действие: ").strip()
        
        if choice == "1":
            print_title("ОТЧЕТ ПО ВСЕМ ДОМАМ")
            df = reports.generate_buildings_report()
            print(df.to_string(index=False))
            
        elif choice == "2":
            print_title("ОТЧЕТ ПО ПЛАТЕЖАМ")
            year = input("Год (оставьте пустым для всех): ")
            month = input("Месяц (оставьте пустым для всех): ")
            
            df = reports.generate_payments_report(
                int(year) if year else None,
                int(month) if month else None
            )
            print(df.to_string(index=False))
            
            # Сводка
            total = df['Сумма'].sum()
            paid = df[df['Статус'] == 'Оплачено']['Сумма'].sum()
            print(f"\nИтого: {total:.2f} руб. (Оплачено: {paid:.2f} руб.)")
            
        elif choice == "3":
            print_title("ИЗБИРАТЕЛЬНЫЙ СПИСОК")
            df = reports.generate_electoral_register()
            if not df.empty:
                print(df.to_string(index=False))
                print(f"\nВсего избирателей: {len(df)} чел.")
            else:
                print("Нет данных для отчета")
                
        elif choice == "4":
            print_title("ОТЧЕТ ПО ЗАДОЛЖЕННОСТЯМ")
            df = reports.generate_debts_report()
            if not df.empty:
                print(df.to_string(index=False))
                total_debt = df['Общая задолженность'].sum()
                print(f"\nОбщая задолженность: {total_debt:.2f} руб.")
            else:
                print("Задолженностей нет")
                
        elif choice == "5":
            print_title("ДОБАВЛЕНИЕ НОВОГО ДОМА")
            address = input("Адрес дома: ")
            floors = input("Количество этажей (опционально): ")
            year = input("Год постройки (опционально): ")
            
            building_id = db.add_building(
                address,
                int(floors) if floors else None,
                int(year) if year else None
            )
            print(f"Дом добавлен! ID: {building_id}")
            
        elif choice == "6":
            print_title("ДОБАВЛЕНИЕ КВАРТИРЫ")
            
            # Показываем список домов
            buildings = db.get_all_buildings()
            print("Список домов:")
            for b in buildings:
                print(f"{b['id']}. {b['address']}")
            
            building_id = int(input("\nID дома: "))
            number = input("Номер квартиры: ")
            area = float(input("Площадь (м²): "))
            rooms = input("Количество комнат (опционально): ")
            privatized = input("Приватизирована? (да/нет): ").lower() == 'да'
            
            apartment_id = db.add_apartment(
                building_id, number, area,
                int(rooms) if rooms else None,
                privatized
            )
            print(f"Квартира добавлена! ID: {apartment_id}")
            
        elif choice == "7":
            print_title("ОПЛАТА ПЛАТЕЖА")
            
            # Показываем неоплаченные платежи
            self.db.connect()
            cursor = self.db.conn.cursor()
            cursor.execute("""
            SELECT p.id, b.address, a.number, p.period, p.amount
            FROM payments p
            JOIN apartments a ON p.apartment_id = a.id
            JOIN buildings b ON a.building_id = b.id
            WHERE p.is_paid = 0
            ORDER BY p.period
            """)
            
            unpaid = cursor.fetchall()
            if unpaid:
                print("Неоплаченные платежи:")
                for p in unpaid:
                    print(f"{p['id']}. {p['address']}, кв. {p['number']} - {p['period']}: {p['amount']} руб.")
                
                payment_id = int(input("\nID платежа для оплаты: "))
                db.mark_payment_as_paid(payment_id)
                print("Платеж отмечен как оплаченный!")
            else:
                print("Неоплаченных платежей нет")
                
        elif choice == "8":
            print_title("ПОДРОБНОСТИ ПО КВАРТИРЕ")
            
            # Показываем список квартир
            self.db.connect()
            cursor = self.db.conn.cursor()
            cursor.execute("""
            SELECT a.id, b.address, a.number
            FROM apartments a
            JOIN buildings b ON a.building_id = b.id
            ORDER BY b.address, a.number
            """)
            
            apartments = cursor.fetchall()
            print("Список квартир:")
            for a in apartments:
                print(f"{a['id']}. {a['address']}, кв. {a['number']}")
            
            apartment_id = int(input("\nID квартиры: "))
            details = reports.generate_apartment_details(apartment_id)
            
            print(f"\nАдрес: {details['Квартира']['Адрес']}")
            print(f"Площадь: {details['Квартира']['Площадь']} м²")
            print(f"Комнат: {details['Квартира']['Комнат']}")
            print(f"Приватизирована: {details['Квартира']['Приватизирована']}")
            
            print("\nЖильцы:")
            for resident in details['Жильцы']:
                owner = " (владелец)" if resident['is_owner'] else ""
                print(f"  - {resident['full_name']}{owner}")
            
            print("\nПоследние платежи:")
            for payment in details['Платежи'][:5]:  # Показываем последние 5
                status = "✓" if payment['is_paid'] else "✗"
                print(f"  {status} {payment['period']}: {payment['amount']} руб.")
            
        elif choice == "0":
            print("Выход из программы.")
            db.close()
            break
            
        else:
            print("Неверный выбор. Попробуйте снова.")

if __name__ == "__main__":
    main()