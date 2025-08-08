# 📊 Data Analysis Tools

This repository contains multiple tools for data analysis and exploration:

## 🤖 Smart Excel Chatbot

Умный чат-бот для анализа Excel файлов с автоматическим определением листов и пониманием контекста.

## 🌐 GitHub Data Explorer

A universal agent for working with data in tables from GitHub repositories and local files. This tool provides a beautiful, dynamic interface in English.

### ✨ Features

- **📊 GitHub Integration**: Load data directly from GitHub repositories (CSV, Excel, JSON)
- **📁 Local File Support**: Work with local data files in various formats
- **💬 Natural Language Queries**: Ask questions about your data in plain English
- **🔄 Data Comparison**: Compare datasets and analyze differences
- **📈 Visualizations**: Automatic visualization of query results
- **📱 Responsive UI**: Beautiful, modern interface that works on all devices

### 🚀 Quick Start

```bash
# Run the GitHub Data Explorer
python -m github_io.run
```

Open your browser and navigate to: http://localhost:5000

For more details, see the [GitHub Data Explorer README](github_io/README.md).

## ✨ Особенности

- **🤖 Умный анализ**: Автоматически определяет структуру Excel файлов
- **🌍 Многоязычность**: Понимает запросы на русском и украинском языках
- **🔍 Контекстное понимание**: Анализирует данные с учетом контекста запроса
- **📊 Автоматическое сравнение**: Сравнивает данные между регионами
- **💬 Интуитивный интерфейс**: Современный веб-интерфейс в стиле чата
- **📱 Адаптивный дизайн**: Работает на всех устройствах
- **📁 Загрузка файлов**: Возможность загружать новые Excel файлы через веб-интерфейс
- **🔄 Новый чат**: Функция начала нового чата с очисткой истории
- **⚙️ Управление функциями**: Включение/отключение функций через веб-интерфейс

## 🚀 Быстрый старт

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Настройка API ключа

Создайте файл `.env` в корневой папке проекта:

```env
OPENAI_API_KEY=your_openai_api_key_here
```

### 3. Запуск веб-приложения

```bash
# Базовый запуск (все функции включены)
python run.py

# Запуск с отключенными функциями
python run.py --no-change-file --no-new-chat

# Запуск с другим файлом
python run.py --file path/to/your/file.xlsx
```

Откройте браузер и перейдите по адресу: http://localhost:5003

#### Параметры запуска:
- `--file, -f`: Путь к Excel файлу (по умолчанию: data/детальні_продажі_по_регіонах.xlsx)
- `--no-change-file`: Отключить функцию загрузки новых файлов
- `--no-new-chat`: Отключить функцию начала нового чата

## 📁 Project Structure

```
test_demo/
├── app/                        # Smart Excel Chatbot application
│   ├── main.py                 # Flask web application
│   └── utils/
│       ├── smart_excel_chatbot.py  # Smart chatbot implementation
│       └── excel_agent.py          # Base agent (legacy)
├── github_io/                  # GitHub Data Explorer application
│   ├── static/                 # Static assets
│   │   ├── css/                # CSS styles
│   │   └── js/                 # JavaScript files
│   ├── templates/              # HTML templates
│   ├── utils/                  # Utility modules
│   │   └── github_data_agent.py  # GitHub data agent implementation
│   ├── app.py                  # Flask application
│   ├── run.py                  # Run script
│   └── README.md               # Detailed documentation
├── data/
│   └── детальні_продажі_по_регіонах.xlsx  # Excel data file
├── templates/
│   └── index.html              # Smart Excel Chatbot web interface
├── example_usage.py            # Usage examples
├── run.py                      # Smart Excel Chatbot run script
└── requirements.txt            # Dependencies
```

## 💡 Примеры использования

### Через веб-интерфейс

1. Откройте http://localhost:5000
2. Дождитесь загрузки файла
3. Задавайте вопросы в чате:

```
"Яка середня ціна за Сентябрь в Киеве?"
"Порівняй ціни за Сентябрь по всіх регіонах"
"Які найпопулярніші товари?"
"Покажи статистику продажів по Львову"
"Який регіон має найвищі ціни?"
```

### Через Python API

```python
from app.core.smart_excel_chatbot import create_smart_excel_chatbot

# Создаем чат-бота
chatbot = create_smart_excel_chatbot("data/детальні_продажі_по_регіонах.xlsx")

# Задаем вопросы
response = chatbot.chat("Яка середня ціна за Сентябрь в Киеве?")
print(response)

# Получаем информацию о файле
info = chatbot.get_file_info()
print(f"Доступные регионы: {info['sheets']}")
```

## 🔧 Технические особенности

### Умный анализ данных

- **Автоматическое определение листов**: Чат-бот анализирует структуру Excel файла
- **Нормализация названий**: Понимает русские и украинские названия регионов
- **Контекстное понимание**: Определяет релевантные листы на основе запроса
- **Структурный анализ**: Анализирует типы данных и колонки

### Обработка запросов

```python
# Чат-бот автоматически:
# 1. Определяет релевантные листы
# 2. Нормализует названия регионов
# 3. Создает контекстный промпт
# 4. Анализирует данные
# 5. Предоставляет структурированный ответ
```

### Поддерживаемые форматы

- **Excel файлы**: .xlsx, .xls
- **Структура данных**: Многостраничные файлы с региональными данными
- **Языки запросов**: Украинский, русский
- **Типы анализа**: Статистика, сравнения, тренды

## 🎯 Возможности

### 📊 Анализ данных
- Средние значения по регионам
- Сравнение показателей
- Тренды по времени
- Популярные товары/услуги

### 🔍 Поиск и фильтрация
- Поиск по конкретным регионам
- Фильтрация по периодам
- Сравнение между регионами
- Ранжирование показателей

### 📈 Визуализация
- Структурированные ответы
- Сравнительные таблицы
- Статистические сводки
- Детальные отчеты

## 🛠️ Разработка

### Добавление новых функций

```python
class SmartExcelChatbot:
    def custom_analysis(self, query: str) -> str:
        """Добавьте свою функцию анализа"""
        # Ваша логика здесь
        pass
```

### Расширение поддержки языков

```python
def _normalize_region_name(self, region_name: str) -> str:
    # Добавьте новые соответствия
    region_mapping = {
        "киев": "Київ",
        "новый_регион": "Новий регіон",
        # ...
    }
```

## 📋 Требования

- Python 3.8+
- OpenAI API ключ
- Excel файл с данными

## 🔒 Безопасность

- API ключи хранятся в .env файле
- Валидация входных данных
- Обработка ошибок
- Безопасная работа с файлами

## 🤝 Поддержка

Если у вас возникли вопросы или проблемы:

1. Проверьте наличие всех зависимостей
2. Убедитесь в правильности API ключа
3. Проверьте формат Excel файла
4. Обратитесь к примерам использования

## 📄 Лицензия

MIT License - используйте свободно для своих проектов! 