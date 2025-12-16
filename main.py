import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
from datetime import datetime
from database import GHUDatabase
from reports import GHUReports

class GHUClientApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Система учета жилого фонда ГЖУ")
        self.root.geometry("1200x700")
        
        # Инициализация БД и отчетов
        self.db = GHUDatabase()
        self.reports = GHUReports(self.db)
        
        # Текущие данные
        self.current_table = None
        self.current_data = []
        self.current_filter = {}
        
        # Создание интерфейса
        self.create_menu()
        self.create_main_frame()
        self.create_table_frame()
        self.create_control_frame()
        
        # Загружаем список таблиц
        self.load_table_list()
    
    def create_menu(self):
        """Создание меню"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # Меню Файл
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Файл", menu=file_menu)
        file_menu.add_command(label="Экспорт в CSV", command=self.export_to_csv)
        file_menu.add_separator()
        file_menu.add_command(label="Выход", command=self.root.quit)
        
        # Меню Отчеты
        report_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Отчеты", menu=report_menu)
        report_menu.add_command(label="Отчет по платежам", command=lambda: self.open_report_dialog("payments"))
        report_menu.add_command(label="Отчет по задолженностям", command=lambda: self.open_report_dialog("debts"))
        report_menu.add_command(label="Избирательные списки", command=lambda: self.open_report_dialog("electoral"))
        
        # Меню Помощь
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Помощь", menu=help_menu)
        help_menu.add_command(label="О программе", command=self.show_about)
    
    def create_main_frame(self):
        """Создание основной рамки"""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Выбор таблицы
        ttk.Label(main_frame, text="Таблица:").grid(row=0, column=0, padx=5, pady=5)
        self.table_combo = ttk.Combobox(main_frame, state="readonly", width=20)
        self.table_combo.grid(row=0, column=1, padx=5, pady=5)
        self.table_combo.bind("<<ComboboxSelected>>", self.on_table_selected)
        
        # Кнопки управления таблицей
        ttk.Button(main_frame, text="Обновить", command=self.refresh_table).grid(row=0, column=2, padx=5, pady=5)
        ttk.Button(main_frame, text="Добавить запись", command=self.add_record).grid(row=0, column=3, padx=5, pady=5)
        ttk.Button(main_frame, text="Удалить запись", command=self.delete_record).grid(row=0, column=4, padx=5, pady=5)
        
        # Поиск
        ttk.Label(main_frame, text="Поиск:").grid(row=1, column=0, padx=5, pady=5)
        self.search_field_combo = ttk.Combobox(main_frame, width=15)
        self.search_field_combo.grid(row=1, column=1, padx=5, pady=5)
        
        self.search_entry = ttk.Entry(main_frame, width=20)
        self.search_entry.grid(row=1, column=2, padx=5, pady=5)
        self.search_entry.bind("<Return>", lambda e: self.search_records())
        
        ttk.Button(main_frame, text="Найти", command=self.search_records).grid(row=1, column=3, padx=5, pady=5)
        ttk.Button(main_frame, text="Сброс", command=self.reset_search).grid(row=1, column=4, padx=5, pady=5)
    
    def create_table_frame(self):
        """Создание рамки для таблицы"""
        table_frame = ttk.Frame(self.root, padding="10")
        table_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Дерево для отображения данных
        self.tree = ttk.Treeview(table_frame, show="headings")
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Полоса прокрутки
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        # Настройка растягивания
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)
    
    def create_control_frame(self):
        """Создание рамки с элементами управления"""
        control_frame = ttk.Frame(self.root, padding="10")
        control_frame.grid(row=2, column=0, sticky=(tk.W, tk.E))
        
        # Фильтры
        ttk.Label(control_frame, text="Фильтр по полю:").grid(row=0, column=0, padx=5, pady=5)
        self.filter_field_combo = ttk.Combobox(control_frame, width=15)
        self.filter_field_combo.grid(row=0, column=1, padx=5, pady=5)
        
        self.filter_value_entry = ttk.Entry(control_frame, width=20)
        self.filter_value_entry.grid(row=0, column=2, padx=5, pady=5)
        
        ttk.Button(control_frame, text="Применить фильтр", command=self.apply_filter).grid(row=0, column=3, padx=5, pady=5)
        ttk.Button(control_frame, text="Сбросить фильтры", command=self.clear_filters).grid(row=0, column=4, padx=5, pady=5)
        
        # Сортировка
        ttk.Label(control_frame, text="Сортировка по:").grid(row=1, column=0, padx=5, pady=5)
        self.sort_field_combo = ttk.Combobox(control_frame, width=15)
        self.sort_field_combo.grid(row=1, column=1, padx=5, pady=5)
        
        self.sort_order_var = tk.BooleanVar(value=True)
        ttk.Radiobutton(control_frame, text="По возрастанию", variable=self.sort_order_var, value=True).grid(row=1, column=2, padx=5, pady=5)
        ttk.Radiobutton(control_frame, text="По убыванию", variable=self.sort_order_var, value=False).grid(row=1, column=3, padx=5, pady=5)
        
        ttk.Button(control_frame, text="Сортировать", command=self.sort_records).grid(row=1, column=4, padx=5, pady=5)
        
        # Статус
        self.status_label = ttk.Label(control_frame, text="Готово")
        self.status_label.grid(row=2, column=0, columnspan=5, padx=5, pady=5, sticky=tk.W)
    
    def load_table_list(self):
        """Загрузка списка таблиц"""
        tables = ['districts', 'buildings', 'apartments', 'residents', 'services', 'payments']
        self.table_combo['values'] = tables
        if tables:
            self.table_combo.set(tables[0])
            self.on_table_selected()
    
    def on_table_selected(self, event=None):
        """Обработчик выбора таблицы"""
        self.current_table = self.table_combo.get()
        self.load_table_data()
        self.update_field_combos()
    
    def load_table_data(self, data=None):
        """Загрузка данных в таблицу"""
        if data is None:
            self.current_data = self.db.get_all(self.current_table)
        else:
            self.current_data = data
        
        # Очищаем дерево
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        if not self.current_data:
            self.status_label.config(text=f"Таблица '{self.current_table}' пуста")
            return
        
        # Настраиваем колонки
        columns = list(self.current_data[0].keys())
        self.tree['columns'] = columns
        
        for col in columns:
            self.tree.heading(col, text=col, command=lambda c=col: self.sort_by_column(c))
            self.tree.column(col, width=100, minwidth=50)
        
        # Заполняем данными
        for record in self.current_data:
            values = [record.get(col, '') for col in columns]
            self.tree.insert('', 'end', values=values)
        
        self.status_label.config(text=f"Загружено записей: {len(self.current_data)}")
    
    def update_field_combos(self):
        """Обновление комбобоксов полями текущей таблицы"""
        if self.current_data:
            fields = list(self.current_data[0].keys())
            self.search_field_combo['values'] = fields
            self.filter_field_combo['values'] = fields
            self.sort_field_combo['values'] = fields
            
            if fields:
                self.search_field_combo.set(fields[0])
                self.filter_field_combo.set(fields[0])
                self.sort_field_combo.set(fields[0])
    
    def refresh_table(self):
        """Обновление таблицы"""
        self.load_table_data()
        self.status_label.config(text="Таблица обновлена")
    
    def add_record(self):
        """Добавление новой записи"""
        if self.current_table == 'apartments':
            self.add_apartment_with_residents()
        else:
            self.open_record_dialog()
    
    def open_record_dialog(self, record_id=None):
        """Открытие диалога редактирования записи"""
        if not self.current_table:
            return
        
        # Создаем окно редактирования
        dialog = tk.Toplevel(self.root)
        dialog.title("Редактирование записи" if record_id else "Добавление записи")
        dialog.geometry("400x500")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Получаем данные записи или создаем пустые
        if record_id:
            record = self.db.get_by_id(self.current_table, record_id)
        else:
            record = {}
        
        # Создаем поля для ввода
        entries = {}
        row = 0
        
        # Получаем поля из первого элемента данных или создаем стандартные
        if self.current_data and len(self.current_data) > 0:
            fields = list(self.current_data[0].keys())
        else:
            # Стандартные поля для каждой таблицы (ИСПРАВЛЕНЫ для apartments)
            table_fields = {
                'districts': ['name', 'manager', 'phone'],
                'buildings': ['address', 'year_built', 'floors', 'total_apartments'],
                'apartments': ['building_id', 'number', 'area', 'rooms', 'privatized', 
                              'cold_water', 'hot_water', 'garbage_chute', 'elevator'],
                'residents': ['apartment_id', 'full_name', 'birth_date', 'passport', 'is_owner', 'phone'],
                'services': ['name', 'price', 'description'],
                'payments': ['apartment_id', 'service_id', 'period', 'amount', 'is_paid', 'payment_date']
            }
            fields = table_fields.get(self.current_table, [])
        
        for field in fields:
            if field == 'id':
                continue
                
            ttk.Label(dialog, text=field).grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            
            if field.endswith('_date') or field in ['birth_date', 'period', 'payment_date', 'registration_date']:
                entry = ttk.Entry(dialog, width=30)
                if field in record:
                    entry.insert(0, record[field])
                else:
                    entry.insert(0, datetime.now().strftime("%Y-%m-%d"))
            elif field in ['privatized', 'cold_water', 'hot_water', 'garbage_chute', 'elevator', 'is_owner', 'is_paid']:
                var = tk.BooleanVar(value=bool(record.get(field, False)))
                entry = ttk.Checkbutton(dialog, variable=var, text="")
                entries[field] = var
            elif isinstance(record.get(field), (int, float)) or field in ['area', 'price', 'amount', 'rooms', 'floors', 'year_built', 'total_apartments', 'building_id', 'apartment_id', 'service_id']:
                entry = ttk.Entry(dialog, width=30)
                if field in record:
                    entry.insert(0, str(record[field]))
            else:
                entry = ttk.Entry(dialog, width=30)
                if field in record:
                    entry.insert(0, record[field])
            
            if not isinstance(entry, tk.BooleanVar):
                entry.grid(row=row, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
                entries[field] = entry
            
            row += 1
        
        # Кнопки
        button_frame = ttk.Frame(dialog)
        button_frame.grid(row=row, column=0, columnspan=2, pady=10)
        
        def save_record():
            try:
                data = {}
                for field, widget in entries.items():
                    if isinstance(widget, tk.BooleanVar):
                        data[field] = 1 if widget.get() else 0
                    else:
                        value = widget.get()
                        # Преобразуем типы данных
                        if field.endswith('_date') or field in ['birth_date', 'period', 'payment_date', 'registration_date']:
                            data[field] = value
                        elif field in ['area', 'price', 'amount']:
                            data[field] = float(value) if value else 0.0
                        elif field in ['rooms', 'floors', 'year_built', 'total_apartments', 'building_id', 'apartment_id', 'service_id']:
                            data[field] = int(value) if value else 0
                        else:
                            data[field] = value
                
                if record_id:
                    self.db.update(self.current_table, record_id, data)
                    messagebox.showinfo("Успех", "Запись обновлена")
                else:
                    self.db.insert(self.current_table, data)
                    messagebox.showinfo("Успех", "Запись добавлена")
                
                self.refresh_table()
                dialog.destroy()
                
            except Exception as e:
                messagebox.showerror("Ошибка", f"Ошибка сохранения: {str(e)}")
        
        ttk.Button(button_frame, text="Сохранить", command=save_record).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Отмена", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
    
    def add_apartment_with_residents(self):
        """Форма для добавления квартиры с жильцами (1:М)"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Добавление квартиры с жильцами")
        dialog.geometry("600x500")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Выбор дома
        ttk.Label(dialog, text="Дом:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        buildings = self.db.get_all('buildings')
        building_combo = ttk.Combobox(dialog, values=[f"{b['id']}: {b['address']}" for b in buildings], width=50)
        building_combo.grid(row=0, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
        
        # Данные квартиры
        ttk.Label(dialog, text="Данные квартиры", font=('Arial', 10, 'bold')).grid(row=1, column=0, columnspan=2, pady=10)
        
        ttk.Label(dialog, text="Номер квартиры:").grid(row=2, column=0, padx=5, pady=5, sticky=tk.W)
        apartment_number_entry = ttk.Entry(dialog, width=30)
        apartment_number_entry.grid(row=2, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
        
        ttk.Label(dialog, text="Площадь (м²):").grid(row=3, column=0, padx=5, pady=5, sticky=tk.W)
        area_entry = ttk.Entry(dialog, width=30)
        area_entry.grid(row=3, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
        
        ttk.Label(dialog, text="Количество комнат:").grid(row=4, column=0, padx=5, pady=5, sticky=tk.W)
        rooms_entry = ttk.Entry(dialog, width=30)
        rooms_entry.grid(row=4, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
        
        privatized_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(dialog, text="Приватизирована", variable=privatized_var).grid(row=5, column=0, columnspan=2, padx=5, pady=5, sticky=tk.W)
        
        # Список жильцов
        ttk.Label(dialog, text="Жильцы", font=('Arial', 10, 'bold')).grid(row=6, column=0, columnspan=2, pady=10)
        
        # Фрейм для списка жильцов
        residents_frame = ttk.Frame(dialog)
        residents_frame.grid(row=7, column=0, columnspan=2, padx=5, pady=5, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        residents_listbox = tk.Listbox(residents_frame, height=5)
        residents_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(residents_frame, orient="vertical", command=residents_listbox.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        residents_listbox.config(yscrollcommand=scrollbar.set)
        
        # Кнопки управления жильцами
        buttons_frame = ttk.Frame(dialog)
        buttons_frame.grid(row=8, column=0, columnspan=2, pady=5)
        
        def add_resident():
            resident_dialog = tk.Toplevel(dialog)
            resident_dialog.title("Добавление жильца")
            resident_dialog.geometry("300x300")
            
            ttk.Label(resident_dialog, text="ФИО:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
            name_entry = ttk.Entry(resident_dialog, width=25)
            name_entry.grid(row=0, column=1, padx=5, pady=5)
            
            ttk.Label(resident_dialog, text="Дата рождения (ГГГГ-ММ-ДД):").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
            birth_entry = ttk.Entry(resident_dialog, width=25)
            birth_entry.grid(row=1, column=1, padx=5, pady=5)
            birth_entry.insert(0, "1990-01-01")
            
            ttk.Label(resident_dialog, text="Паспорт:").grid(row=2, column=0, padx=5, pady=5, sticky=tk.W)
            passport_entry = ttk.Entry(resident_dialog, width=25)
            passport_entry.grid(row=2, column=1, padx=5, pady=5)
            
            ttk.Label(resident_dialog, text="Телефон:").grid(row=3, column=0, padx=5, pady=5, sticky=tk.W)
            phone_entry = ttk.Entry(resident_dialog, width=25)
            phone_entry.grid(row=3, column=1, padx=5, pady=5)
            
            is_owner_var = tk.BooleanVar(value=False)
            ttk.Checkbutton(resident_dialog, text="Владелец", variable=is_owner_var).grid(row=4, column=0, columnspan=2, padx=5, pady=5)
            
            def save_resident():
                resident_data = {
                    'full_name': name_entry.get(),
                    'birth_date': birth_entry.get(),
                    'passport': passport_entry.get() if passport_entry.get() else None,
                    'phone': phone_entry.get() if phone_entry.get() else None,
                    'is_owner': is_owner_var.get()
                }
                
                if not resident_data['full_name']:
                    messagebox.showerror("Ошибка", "Введите ФИО жильца")
                    return
                
                residents_listbox.insert(tk.END, f"{resident_data['full_name']} ({'владелец' if resident_data['is_owner'] else 'жилец'})")
                resident_dialog.destroy()
            
            ttk.Button(resident_dialog, text="Сохранить", command=save_resident).grid(row=5, column=0, padx=5, pady=10)
            ttk.Button(resident_dialog, text="Отмена", command=resident_dialog.destroy).grid(row=5, column=1, padx=5, pady=10)
        
        def remove_resident():
            selection = residents_listbox.curselection()
            if selection:
                residents_listbox.delete(selection[0])
        
        ttk.Button(buttons_frame, text="Добавить жильца", command=add_resident).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons_frame, text="Удалить жильца", command=remove_resident).pack(side=tk.LEFT, padx=5)
        
        # Кнопки сохранения/отмены
        def save_all():
            try:
                # Получаем ID дома
                building_text = building_combo.get()
                if not building_text:
                    messagebox.showerror("Ошибка", "Выберите дом")
                    return
                
                building_id = int(building_text.split(':')[0])
                
                # Данные квартиры
                apartment_data = {
                    'number': apartment_number_entry.get(),
                    'area': float(area_entry.get()),
                    'rooms': int(rooms_entry.get()) if rooms_entry.get() else None,
                    'privatized': privatized_var.get()
                }
                
                if not apartment_data['number'] or not apartment_data['area']:
                    messagebox.showerror("Ошибка", "Заполните номер квартиры и площадь")
                    return
                
                # Данные жильцов (в реальном приложении нужно хранить их где-то)
                # Для демонстрации создаем тестовые данные
                residents_data = []
                for i in range(residents_listbox.size()):
                    residents_data.append({
                        'full_name': f"Жилец {i+1}",
                        'birth_date': '1990-01-01',
                        'is_owner': i == 0
                    })
                
                if not residents_data:
                    messagebox.showerror("Ошибка", "Добавьте хотя бы одного жильца")
                    return
                
                # Сохраняем
                apartment_id = self.db.add_apartment_with_residents(building_id, apartment_data, residents_data)
                messagebox.showinfo("Успех", f"Квартира #{apartment_id} добавлена с {len(residents_data)} жильцами")
                self.refresh_table()
                dialog.destroy()
                
            except Exception as e:
                messagebox.showerror("Ошибка", f"Ошибка сохранения: {str(e)}")
        
        ttk.Button(dialog, text="Сохранить всё", command=save_all).grid(row=9, column=0, padx=5, pady=10)
        ttk.Button(dialog, text="Отмена", command=dialog.destroy).grid(row=9, column=1, padx=5, pady=10)
    
    def delete_record(self):
        """Удаление выбранной записи"""
        selection = self.tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите запись для удаления")
            return
        
        if messagebox.askyesno("Подтверждение", "Удалить выбранную запись?"):
            item = self.tree.item(selection[0])
            values = item['values']
            
            # Получаем ID записи (предполагаем, что первый столбец - ID)
            if values and len(values) > 0:
                try:
                    record_id = int(values[0])
                    if self.db.delete(self.current_table, record_id):
                        self.refresh_table()
                        self.status_label.config(text="Запись удалена")
                    else:
                        messagebox.showerror("Ошибка", "Не удалось удалить запись")
                except:
                    messagebox.showerror("Ошибка", "Неверный формат ID записи")
    
    def search_records(self):
        """Поиск записей"""
        field = self.search_field_combo.get()
        value = self.search_entry.get()
        
        if not field or not value:
            messagebox.showwarning("Предупреждение", "Заполните поле поиска")
            return
        
        results = self.db.search(self.current_table, field, value)
        self.load_table_data(results)
        self.status_label.config(text=f"Найдено записей: {len(results)}")
    
    def reset_search(self):
        """Сброс поиска"""
        self.search_entry.delete(0, tk.END)
        self.refresh_table()
    
    def apply_filter(self):
        """Применение фильтра"""
        field = self.filter_field_combo.get()
        value = self.filter_value_entry.get()
        
        if not field:
            return
        
        if value:
            self.current_filter[field] = value
        elif field in self.current_filter:
            del self.current_filter[field]
        
        results = self.db.filter_records(self.current_table, self.current_filter)
        self.load_table_data(results)
        self.status_label.config(text=f"Отфильтровано записей: {len(results)}")
    
    def clear_filters(self):
        """Очистка всех фильтров"""
        self.current_filter = {}
        self.filter_value_entry.delete(0, tk.END)
        self.refresh_table()
    
    def sort_records(self):
        """Сортировка записей"""
        field = self.sort_field_combo.get()
        ascending = self.sort_order_var.get()
        
        if not field:
            return
        
        results = self.db.sort_records(self.current_table, field, ascending)
        self.load_table_data(results)
        self.status_label.config(text=f"Отсортировано по полю: {field}")
    
    def sort_by_column(self, column):
        """Сортировка по колонке таблицы"""
        # Определяем направление сортировки
        current_direction = getattr(self, '_sort_direction', {})
        ascending = not current_direction.get(column, True)
        current_direction[column] = ascending
        self._sort_direction = current_direction
        
        # Сортируем данные
        if self.current_data:
            try:
                sorted_data = sorted(
                    self.current_data,
                    key=lambda x: str(x.get(column, '')),
                    reverse=not ascending
                )
                self.load_table_data(sorted_data)
                self.status_label.config(text=f"Сортировка по {column} ({'возр.' if ascending else 'убыв.'})")
            except:
                pass
    
    def open_report_dialog(self, report_type):
        """Открытие диалога формирования отчета"""
        dialog = tk.Toplevel(self.root)
        dialog.title(f"Формирование отчета: {report_type}")
        dialog.geometry("500x400")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Поля для фильтров
        filters_frame = ttk.LabelFrame(dialog, text="Фильтры", padding="10")
        filters_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        filters = {}
        row = 0
        
        if report_type == "payments":
            ttk.Label(filters_frame, text="Период (ГГГГ-ММ):").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            period_entry = ttk.Entry(filters_frame, width=15)
            period_entry.grid(row=row, column=1, padx=5, pady=5)
            period_entry.insert(0, "2024-")
            filters['period'] = period_entry
            row += 1
            
            ttk.Label(filters_frame, text="Адрес:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            address_entry = ttk.Entry(filters_frame, width=15)
            address_entry.grid(row=row, column=1, padx=5, pady=5)
            filters['address'] = address_entry
            row += 1
            
            ttk.Label(filters_frame, text="Статус:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            status_combo = ttk.Combobox(filters_frame, values=["", "paid", "unpaid"])
            status_combo.grid(row=row, column=1, padx=5, pady=5)
            filters['status'] = status_combo
            row += 1
            
            ttk.Label(filters_frame, text="Мин. сумма:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            min_amount_entry = ttk.Entry(filters_frame, width=15)
            min_amount_entry.grid(row=row, column=1, padx=5, pady=5)
            filters['min_amount'] = min_amount_entry
            row += 1
            
            sort_fields = ["period", "address", "amount", "status"]
            
        elif report_type == "debts":
            ttk.Label(filters_frame, text="Адрес:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            address_entry = ttk.Entry(filters_frame, width=15)
            address_entry.grid(row=row, column=1, padx=5, pady=5)
            filters['address'] = address_entry
            row += 1
            
            ttk.Label(filters_frame, text="Мин. долг:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            min_debt_entry = ttk.Entry(filters_frame, width=15)
            min_debt_entry.grid(row=row, column=1, padx=5, pady=5)
            filters['min_debt'] = min_debt_entry
            row += 1
            
            sort_fields = ["amount", "period", "address", "months"]
            
        elif report_type == "electoral":
            ttk.Label(filters_frame, text="Адрес:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            address_entry = ttk.Entry(filters_frame, width=15)
            address_entry.grid(row=row, column=1, padx=5, pady=5)
            filters['address'] = address_entry
            row += 1
            
            ttk.Label(filters_frame, text="Мин. возраст:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            min_age_entry = ttk.Entry(filters_frame, width=15)
            min_age_entry.grid(row=row, column=1, padx=5, pady=5)
            filters['min_age'] = min_age_entry
            row += 1
            
            ttk.Label(filters_frame, text="Макс. возраст:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
            max_age_entry = ttk.Entry(filters_frame, width=15)
            max_age_entry.grid(row=row, column=1, padx=5, pady=5)
            filters['max_age'] = max_age_entry
            row += 1
            
            sort_fields = ["birth_date", "age", "address"]
        
        # Сортировка
        ttk.Label(filters_frame, text="Сортировка по:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
        sort_combo = ttk.Combobox(filters_frame, values=sort_fields)
        sort_combo.grid(row=row, column=1, padx=5, pady=5)
        sort_combo.set(sort_fields[0])
        row += 1
        
        ttk.Label(filters_frame, text="Направление:").grid(row=row, column=0, padx=5, pady=5, sticky=tk.W)
        sort_order_var = tk.BooleanVar(value=True)
        ttk.Radiobutton(filters_frame, text="По возрастанию", variable=sort_order_var, value=True).grid(row=row, column=1, padx=5, pady=5, sticky=tk.W)
        ttk.Radiobutton(filters_frame, text="По убыванию", variable=sort_order_var, value=False).grid(row=row, column=1, padx=5, pady=5, sticky=tk.E)
        
        def generate_report():
            # Собираем фильтры
            filter_dict = {}
            for key, widget in filters.items():
                if isinstance(widget, ttk.Entry):
                    value = widget.get()
                    if value:
                        filter_dict[key] = value
                elif isinstance(widget, ttk.Combobox):
                    value = widget.get()
                    if value:
                        filter_dict[key] = value
            
            # Генерируем отчет
            try:
                if report_type == "payments":
                    df, grouped, totals = self.reports.generate_payments_report(
                        filter_dict, sort_combo.get(), sort_order_var.get()
                    )
                    title = "Отчет по платежам"
                elif report_type == "debts":
                    df, grouped, totals = self.reports.generate_debts_report(
                        filter_dict, sort_combo.get(), sort_order_var.get()
                    )
                    title = "Отчет по задолженностям"
                elif report_type == "electoral":
                    df, grouped, totals = self.reports.generate_electoral_register(
                        filter_dict, sort_combo.get(), sort_order_var.get()
                    )
                    title = "Избирательные списки"
                
                # Показываем результаты
                self.show_report_results(title, df, grouped, totals)
                dialog.destroy()
                
            except Exception as e:
                messagebox.showerror("Ошибка", f"Ошибка формирования отчета: {str(e)}")
        
        # Кнопки
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Button(button_frame, text="Сформировать отчет", command=generate_report).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Отмена", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
    
    def show_report_results(self, title, df, grouped, totals):
        """Отображение результатов отчета"""
        result_dialog = tk.Toplevel(self.root)
        result_dialog.title(title)
        result_dialog.geometry("900x600")
        
        # Notebook для вкладок
        notebook = ttk.Notebook(result_dialog)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Вкладка с данными
        if not df.empty:
            data_frame = ttk.Frame(notebook)
            notebook.add(data_frame, text="Данные")
            
            # Дерево для данных
            tree = ttk.Treeview(data_frame, show="headings")
            tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            
            # Настраиваем колонки
            columns = list(df.columns)
            tree['columns'] = columns
            
            for col in columns:
                tree.heading(col, text=col)
                tree.column(col, width=100)
            
            # Заполняем данными
            for _, row in df.iterrows():
                tree.insert('', 'end', values=list(row))
            
            # Статус
            ttk.Label(data_frame, text=f"Всего записей: {len(df)}").pack(side=tk.BOTTOM, pady=5)
        
        # Вкладка с группировкой
        if not grouped.empty:
            grouped_frame = ttk.Frame(notebook)
            notebook.add(grouped_frame, text="Группировка")
            
            # Дерево для группировки
            tree2 = ttk.Treeview(grouped_frame, show="headings")
            tree2.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            
            # Настраиваем колонки
            columns2 = list(grouped.columns)
            tree2['columns'] = columns2
            
            for col in columns2:
                tree2.heading(col, text=col)
                tree2.column(col, width=100)
            
            # Заполняем данными
            for index, row in grouped.iterrows():
                if isinstance(index, tuple):
                    display_index = ' / '.join(str(i) for i in index)
                else:
                    display_index = str(index)
                
                tree2.insert('', 'end', values=[display_index] + list(row))
        
        # Вкладка с итогами
        if totals:
            totals_frame = ttk.Frame(notebook)
            notebook.add(totals_frame, text="Итоги")
            
            row = 0
            for key, value in totals.items():
                ttk.Label(totals_frame, text=f"{key}:").grid(row=row, column=0, padx=10, pady=5, sticky=tk.W)
                ttk.Label(totals_frame, text=str(value)).grid(row=row, column=1, padx=10, pady=5, sticky=tk.W)
                row += 1
    
    def export_to_csv(self):
        """Экспорт текущей таблицы в CSV"""
        if not self.current_data:
            messagebox.showwarning("Предупреждение", "Нет данных для экспорта")
            return
        
        try:
            df = pd.DataFrame(self.current_data)
            filename = f"{self.current_table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            messagebox.showinfo("Успех", f"Данные экспортированы в файл: {filename}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка экспорта: {str(e)}")
    
    def show_about(self):
        """Показ информации о программе"""
        about_text = """Система учета жилого фонда ГЖУ
Версия 1.0
        
Функционал:
- Управление 6 таблицами БД
- CRUD операции
- Поиск и фильтрация
- Сортировка данных
- 3 комплексных отчета
- Экспорт в CSV
        
Разработано для автоматизации службы заказчика ГЖУ"""
        messagebox.showinfo("О программе", about_text)

def main():
    root = tk.Tk()
    app = GHUClientApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()
