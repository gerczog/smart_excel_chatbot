#!/usr/bin/env python3
"""
Запуск веб-приложения Smart Excel Chatbot
"""

import os
import sys
import argparse
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

def check_requirements(excel_file="data/детальні_продажі_по_регіонах.xlsx"):
    """Проверяет наличие необходимых файлов и настроек
    
    Args:
        excel_file: Путь к Excel файлу (по умолчанию: data/детальні_продажі_по_регіонах.xlsx)
    """
    
    # Проверяем API ключ
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Ошибка: OPENAI_API_KEY не установлен")
        print("💡 Создайте файл .env и добавьте:")
        print("   OPENAI_API_KEY=your_api_key_here")
        return False
    
    # Проверяем наличие Excel файла
    if not os.path.exists(excel_file):
        print(f"❌ Ошибка: Файл {excel_file} не найден")
        print("💡 Убедитесь, что указанный файл существует")
        return False
    
    # Проверяем зависимости
    try:
        import flask
        import pandas
        import openpyxl
        import langchain
        import openai
        print("✅ Все зависимости установлены")
    except ImportError as e:
        print(f"❌ Ошибка: Не установлена зависимость - {e}")
        print("💡 Установите зависимости: pip install -r requirements.txt")
        return False
    
    return True

def main():
    """Основная функция запуска"""
    
    print("🤖 Smart Excel Chatbot")
    print("=" * 40)
    
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description='Запуск Smart Excel Chatbot')
    parser.add_argument('--file', '-f', 
                        help='Путь к Excel файлу (по умолчанию: data/детальні_продажі_по_регіонах.xlsx)',
                        default="data/детальні_продажі_по_регіонах.xlsx")
    parser.add_argument('--no-change-file', action='store_true',
                        help='Отключить функцию изменения файла')
    parser.add_argument('--no-new-chat', action='store_true',
                        help='Отключить функцию нового чата')
    args = parser.parse_args()
    
    # Выводим информацию о выбранном файле
    print(f"📊 Выбранный файл: {args.file}")
    
    # Проверяем требования
    if not check_requirements(args.file):
        sys.exit(1)
    
    print("🚀 Запуск веб-приложения...")
    print("📱 Откройте браузер и перейдите по адресу: http://localhost:5003")
    print("⏹️  Для остановки нажмите Ctrl+C")
    print()
    
    try:
        # Импортируем и запускаем приложение
        from app.main import app
        
        # Устанавливаем путь к файлу как глобальную переменную для Flask
        os.environ['EXCEL_FILE_PATH'] = args.file
        
        # Устанавливаем начальные значения для функций изменения файла и нового чата
        # По умолчанию обе функции включены
        os.environ['ALLOW_CHANGE_FILE'] = 'True'
        os.environ['ALLOW_NEW_CHAT'] = 'True'
        
        # Проверяем аргументы командной строки
        if args.no_change_file:
            os.environ['ALLOW_CHANGE_FILE'] = 'False'
        if args.no_new_chat:
            os.environ['ALLOW_NEW_CHAT'] = 'False'
        
        # Выводим информацию о включенных функциях
        change_file_enabled = os.environ.get('ALLOW_CHANGE_FILE', 'True').lower() == 'true'
        new_chat_enabled = os.environ.get('ALLOW_NEW_CHAT', 'True').lower() == 'true'
        
        print("✅ Функция изменения файла включена" if change_file_enabled else "❌ Функция изменения файла отключена")
        print("✅ Функция нового чата включена" if new_chat_enabled else "❌ Функция нового чата отключена")
        print("💡 Вы можете управлять этими функциями через веб-интерфейс")
        
        app.run(debug=True, host='0.0.0.0', port=5003)
        
    except KeyboardInterrupt:
        print("\n👋 Приложение остановлено")
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")

if __name__ == "__main__":
    main() 