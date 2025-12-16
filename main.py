import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
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
        ttk.Radiobutton(control_frame, text="По
