import os
import shutil

def clean_start():
    """Очистка и перезапуск приложения"""
    
    # Удаляем файлы базы данных
    db_files = ['ghu_database.db', 'ghu_database.db-journal']
    for db_file in db_files:
        if os.path.exists(db_file):
            os.remove(db_file)
            print(f"Удален файл: {db_file}")
    
    # Удаляем CSV файлы (если есть)
    for file in os.listdir('.'):
        if file.endswith('.csv'):
            os.remove(file)
            print(f"Удален файл: {file}")
    
    print("\nЗапуск приложения...")
    print("=" * 50)
    
    # Импортируем и запускаем main
    from main import main
    main()

if __name__ == "__main__":
    clean_start()
