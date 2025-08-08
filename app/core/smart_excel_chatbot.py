import os
import pandas as pd
import re
from typing import Dict, List, Optional, Tuple, Any
from functools import lru_cache
from dotenv import load_dotenv
from langchain.agents.agent_types import AgentType
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from langchain.prompts import ChatPromptTemplate

# Load environment variables
load_dotenv()


class SmartExcelChatbot:
    """
    Умный чат-бот для работы с Excel файлами
    Автоматически определяет листы и понимает контекст
    """

    def __init__(self, file_path: str):
        """
        Инициализация чат-бота

        Args:
            file_path: Путь к Excel файлу
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")

        self.file_path = file_path
        self.excel_file = pd.ExcelFile(file_path)
        self.sheet_names = self.excel_file.sheet_names
        self.dataframes = {}
        self.agents = {}
        self.combined_df = None  # Объединенный DataFrame для сравнения регионов
        self.combined_agent = None  # Агент для работы с объединенными данными
        self.context = {}

        # Проверяем API ключ
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY не установлен. Установите его в .env файле.")

        # Инициализируем LLM
        self.llm = ChatOpenAI(temperature=0, model="gpt-4o-mini")

        # Загружаем данные для каждого листа
        self._load_all_sheets()
        self._create_combined_dataframe()
        self._analyze_structure()

    def _load_all_sheets(self):
        """Загружает все листы из Excel файла"""
        for sheet_name in self.sheet_names:
            try:
                df = pd.read_excel(self.file_path, sheet_name=sheet_name, parse_dates=True, date_format='%Y-%m-%d')
                if not df.empty:
                    self.dataframes[sheet_name] = df
                    # Создаем агента для каждого листа
                    self.agents[sheet_name] = create_pandas_dataframe_agent(
                        self.llm,
                        df,
                        verbose=False,
                        agent_type=AgentType.OPENAI_FUNCTIONS,
                        allow_dangerous_code=True
                    )
            except Exception as e:
                print(f"Ошибка загрузки листа {sheet_name}: {e}")

    def _create_combined_dataframe(self):
        """Создает объединенный DataFrame для сравнения регионов"""
        combined_data = []

        for sheet_name, df in self.dataframes.items():
            # Добавляем колонку с названием региона
            df_copy = df.copy()
            df_copy['Регіон'] = sheet_name

            # Добавляем данные в общий список
            combined_data.append(df_copy)

        if combined_data:
            # Объединяем все DataFrame
            self.combined_df = pd.concat(combined_data, ignore_index=True)

            # Создаем агента для работы с объединенными данными
            self.combined_agent = create_pandas_dataframe_agent(
                self.llm,
                self.combined_df,
                verbose=False,
                agent_type=AgentType.OPENAI_FUNCTIONS,
                allow_dangerous_code=True
            )

            print(
                f"✅ Создан объединенный DataFrame с {len(self.combined_df)} записями из {len(self.dataframes)} регионов")
        else:
            print("⚠️ Не удалось создать объединенный DataFrame - нет данных")

    def _analyze_column_semantics(self, column_name: str, sample_data: list, data_type: str) -> dict:
        """
        Анализирует семантику названия колонки и её содержимое для лучшего понимания LLM
        
        Args:
            column_name: Название колонки
            sample_data: Примеры данных из колонки
            data_type: Тип данных колонки
            
        Returns:
            Словарь с семантической информацией о колонке
        """
        try:
            # Создаем промпт для анализа колонки
            prompt = ChatPromptTemplate.from_messages([
                SystemMessage(content="""
                Ты эксперт по анализу данных. Проанализируй название колонки и её содержимое, чтобы определить:
                1. Что означает эта колонка (на украинском языке)
                2. Какие операции можно с ней выполнять
                3. Как она связана с бизнес-процессами
                4. Возможные единицы измерения (если применимо)
                
                Отвечай ТОЛЬКО в формате JSON:
                {
                  "meaning": "краткое описание того, что означает колонка",
                  "operations": ["список возможных операций"],
                  "business_context": "бизнес-контекст использования",
                  "units": "единицы измерения (если есть)",
                  "category": "категория данных (финансы, даты, товары, клиенты и т.д.)"
                }
                """),
                HumanMessage(content=f"""
                Название колонки: "{column_name}"
                Тип данных: {data_type}
                Примеры значений: {sample_data[:5]}
                """)
            ])
            
            response = self.llm.invoke(prompt)
            
            # Попытка распарсить JSON ответ
            try:
                import json
                semantic_info = json.loads(response.content.strip())
                return semantic_info
            except:
                # Если не удалось распарсить JSON, создаем базовую информацию
                return {
                    "meaning": f"Колонка '{column_name}' типу {data_type}",
                    "operations": ["базові операції"],
                    "business_context": "загальний контекст",
                    "units": "не визначено",
                    "category": "загальні дані"
                }
                
        except Exception as e:
            # В случае ошибки возвращаем минимальную информацию
            return {
                "meaning": f"Колонка '{column_name}'",
                "operations": ["базові операції"],
                "business_context": "загальний контекст",
                "units": "",
                "category": "дані"
            }

    def _analyze_structure(self):
        """Анализирует структуру данных и создает детальный контекст"""
        print("🔍 Начинаем анализ структуры данных...")

        self.context = {
            "file_path": self.file_path,
            "total_sheets": len(self.sheet_names),
            "sheets_info": {},
            "global_analysis": {},
            "column_semantics": {}  # Добавляем семантический анализ колонок
        }

        all_columns = set()
        all_data_types = {}
        total_rows = 0
        file_size = os.path.getsize(self.file_path) / (1024 * 1024)  # Размер файла в МБ

        print(f"📊 Файл: {os.path.basename(self.file_path)}")
        print(f"📏 Размер: {file_size:.2f} МБ")
        print(f"📋 Количество листов: {len(self.sheet_names)}")
        print(f"🏙️ Листы: {', '.join(self.sheet_names)}")

        for sheet_name in self.sheet_names:
            if sheet_name in self.dataframes:
                df = self.dataframes[sheet_name]
                total_rows += len(df)

                print(f"\n📊 Анализ листа '{sheet_name}':")
                print(f"  - Строк: {len(df)}")
                print(f"  - Колонок: {len(df.columns)}")
                print(f"  - Колонки: {list(df.columns)}")

                # Детальный анализ каждого листа
                sheet_info = {
                    "columns": df.columns.tolist(),
                    "rows": len(df),
                    "data_types": {str(col): str(dtype) for col, dtype in df.dtypes.items()},
                    "sample_data": [],  # Будем заполнять безопасно
                    "column_analysis": {},
                    "unique_values": {},
                    "date_columns": [],
                    "numeric_columns": [],
                    "text_columns": [],
                    "missing_values": {},
                    "column_formats": {},
                    "data_quality": {}
                }

                # Безопасно заполняем sample_data
                for idx, row in df.head(3).iterrows():
                    row_data = {}
                    for col in df.columns:
                        value = row[col]
                        if isinstance(value, bool):
                            row_data[col] = str(value)
                        elif pd.isna(value):
                            row_data[col] = None
                        else:
                            row_data[col] = value
                    sheet_info["sample_data"].append(row_data)

                # Анализ каждой колонки
                for col in df.columns:
                    col_str = str(col)
                    all_columns.add(col_str)

                    # Анализ пропущенных значений
                    missing_count = df[col].isnull().sum()
                    missing_percent = (missing_count / len(df)) * 100
                    sheet_info["missing_values"][col_str] = {
                        "count": int(missing_count),
                        "percent": float(missing_percent)
                    }

                    # Определяем тип колонки
                    col_type = "text"  # по умолчанию
                    
                    # Сначала проверяем числовые типы
                    if pd.api.types.is_numeric_dtype(df[col]):
                        sheet_info["numeric_columns"].append(col_str)
                        col_type = "numeric"
                    # Затем проверяем datetime типы
                    elif pd.api.types.is_datetime64_any_dtype(df[col]):
                        sheet_info["date_columns"].append(col_str)
                        col_type = "datetime"
                    else:
                        # Для текстовых колонок пытаемся определить, содержат ли они даты
                        try:
                            # Проверяем только если это не числовая колонка
                            sample_data = df[col].dropna().head(10)
                            if len(sample_data) > 0:
                                # Проверяем, содержат ли значения паттерны дат
                                sample_str = str(sample_data.iloc[0])
                                if (len(sample_str) >= 8 and 
                                    ('-' in sample_str or '/' in sample_str) and
                                    any(char.isdigit() for char in sample_str)):
                                    
                                    test_conversion = pd.to_datetime(sample_data, errors='coerce')
                                    # Если больше 80% значений успешно преобразованы - это дата
                                    if test_conversion.notna().sum() / len(test_conversion) > 0.8:
                                        sheet_info["date_columns"].append(col_str)
                                        col_type = "datetime"
                                    else:
                                        sheet_info["text_columns"].append(col_str)
                                        col_type = "text"
                                else:
                                    sheet_info["text_columns"].append(col_str)
                                    col_type = "text"
                            else:
                                sheet_info["text_columns"].append(col_str)
                                col_type = "text"
                        except:
                            sheet_info["text_columns"].append(col_str)
                            col_type = "text"

                    # Анализ уникальных значений
                    unique_count = df[col].nunique()
                    total_count = len(df)
                    sheet_info["unique_values"][col_str] = {
                        "unique_count": unique_count,
                        "total_count": total_count,
                        "unique_ratio": unique_count / total_count if total_count > 0 else 0
                    }

                    # Анализ формата данных
                    sheet_info["column_formats"][col_str] = {
                        "dtype": str(df[col].dtype),
                        "type_category": col_type,
                        "has_missing": str(missing_count > 0),  # Convert boolean to string
                        "missing_percent": float(missing_percent)
                    }

                    # Семантический анализ колонки для лучшего понимания LLM
                    sample_values_for_analysis = df[col].dropna().head(5).tolist()
                    # Безопасно конвертируем значения для анализа
                    safe_sample_values = []
                    for val in sample_values_for_analysis:
                        if isinstance(val, bool):
                            safe_sample_values.append(str(val))
                        elif pd.isna(val):
                            safe_sample_values.append("NaN")
                        else:
                            safe_sample_values.append(str(val))
                    
                    # Выполняем семантический анализ только для важных колонок (избегаем лишних вызовов)
                    if len(safe_sample_values) > 0 and col_str not in ['index', 'id', 'ID']:
                        try:
                            semantic_info = self._analyze_column_semantics(col_str, safe_sample_values, col_type)
                            self.context["column_semantics"][f"{sheet_name}_{col_str}"] = semantic_info
                            print(f"    🧠 {col_str}: {semantic_info.get('meaning', 'аналіз не вдався')}")
                        except Exception as e:
                            print(f"    ⚠️ Семантический анализ {col_str} не удался: {e}")
                            # Добавляем базовую семантику в случае ошибки
                            self.context["column_semantics"][f"{sheet_name}_{col_str}"] = {
                                "meaning": f"Колонка {col_str}",
                                "operations": ["базові операції"],
                                "business_context": "загальний контекст",
                                "units": "",
                                "category": col_type
                            }

                    # Для текстовых колонок - примеры значений
                    if col_str in sheet_info["text_columns"] and unique_count <= 20:
                        # Безопасно обрабатываем sample_values
                        sample_values = df[col].dropna().unique()[:10]
                        safe_sample_values = []
                        for val in sample_values:
                            if isinstance(val, bool):
                                safe_sample_values.append(str(val))
                            else:
                                safe_sample_values.append(val)

                        # Безопасно обрабатываем most_common
                        most_common_dict = df[col].value_counts().head(5).to_dict()
                        safe_most_common = {}
                        for key, value in most_common_dict.items():
                            if isinstance(key, bool):
                                safe_most_common[str(key)] = int(value)
                            else:
                                safe_most_common[key] = int(value)

                        sheet_info["column_analysis"][col_str] = {
                            "sample_values": safe_sample_values,
                            "most_common": safe_most_common,
                            "avg_length": float(df[col].astype(str).str.len().mean()) if not df[col].empty else 0.0
                        }

                    # Для числовых колонок - статистика
                    elif col_str in sheet_info["numeric_columns"]:
                        sheet_info["column_analysis"][col_str] = {
                            "min": float(df[col].min()) if not df[col].empty else None,
                            "max": float(df[col].max()) if not df[col].empty else None,
                            "mean": float(df[col].mean()) if not df[col].empty else None,
                            "median": float(df[col].median()) if not df[col].empty else None,
                            "std": float(df[col].std()) if not df[col].empty else None,
                            "zeros": int((df[col] == 0).sum()),
                            "negative": int((df[col] < 0).sum()) if not df[col].empty else 0
                        }

                    # Для дат - дополнительный анализ
                    elif col_str in sheet_info["date_columns"]:
                        # Преобразуем колонку в datetime если она еще не datetime
                        date_series = df[col]
                        if not pd.api.types.is_datetime64_any_dtype(date_series):
                            date_series = pd.to_datetime(date_series, errors='coerce')
                        
                        # Безопасно обрабатываем weekdays_distribution
                        weekdays_dict = {}
                        if not date_series.empty:
                            valid_dates = date_series.dropna()
                            if not valid_dates.empty:
                                weekdays_series = valid_dates.dt.day_name().value_counts()
                                for day, count in weekdays_series.items():
                                    weekdays_dict[str(day)] = int(count)

                        sheet_info["column_analysis"][col_str] = {
                            "min_date": str(date_series.min()) if not date_series.empty and date_series.notna().any() else None,
                            "max_date": str(date_series.max()) if not date_series.empty and date_series.notna().any() else None,
                            "date_range_days": int((date_series.max() - date_series.min()).days) if not date_series.empty and date_series.notna().any() else 0,
                            "weekdays_distribution": weekdays_dict
                        }

                    # Выводим информацию о колонке
                    print(
                        f"    📋 {col_str}: {col_type} ({df[col].dtype}) - {unique_count} уникальных, {missing_count} пропущенных ({missing_percent:.1f}%)")

                # Анализ временных рядов
                if sheet_info["date_columns"]:
                    sheet_info["date_range"] = {}
                    sheet_info["date_analysis"] = {}
                    for date_col in sheet_info["date_columns"]:
                        date_series = pd.to_datetime(df[date_col], errors='coerce')
                        valid_dates = date_series.dropna()
                        if not valid_dates.empty:
                            # Базовая информация о диапазоне дат
                            sheet_info["date_range"][date_col] = {
                                "start": valid_dates.min().strftime('%Y-%m-%d'),
                                "end": valid_dates.max().strftime('%Y-%m-%d'),
                                "total_days": (valid_dates.max() - valid_dates.min()).days
                            }

                            # Детальный анализ по месяцам и годам
                            # Безопасно обрабатываем records_per_month
                            records_per_month_dict = {}
                            records_per_month_series = valid_dates.dt.strftime('%Y-%m').value_counts()
                            for month_year, count in records_per_month_series.items():
                                records_per_month_dict[str(month_year)] = int(count)

                            # Безопасно обрабатываем records_per_year
                            records_per_year_dict = {}
                            records_per_year_series = valid_dates.dt.year.value_counts()
                            for year, count in records_per_year_series.items():
                                records_per_year_dict[int(year)] = int(count)

                            sheet_info["date_analysis"][date_col] = {
                                "years": sorted(valid_dates.dt.year.unique().tolist()),
                                "months": sorted(valid_dates.dt.month.unique().tolist()),
                                "month_names": sorted(valid_dates.dt.strftime('%B').unique().tolist()),
                                "month_year_combinations": sorted(valid_dates.dt.strftime('%Y-%m').unique().tolist()),
                                "records_per_month": records_per_month_dict,
                                "records_per_year": records_per_year_dict
                            }

                            print(
                                f"    📅 {date_col}: {valid_dates.min().strftime('%Y-%m-%d')} до {valid_dates.max().strftime('%Y-%m-%d')} ({len(valid_dates)} записей)")

                # Анализ качества данных
                total_missing = sum(info["count"] for info in sheet_info["missing_values"].values())
                data_quality_score = ((len(df) * len(df.columns)) - total_missing) / (len(df) * len(df.columns)) * 100

                sheet_info["data_quality"] = {
                    "total_missing_values": total_missing,
                    "completeness_score": float(data_quality_score),
                    "columns_with_missing": [col for col, info in sheet_info["missing_values"].items() if
                                             info["count"] > 0],
                    "high_quality_columns": [col for col, info in sheet_info["missing_values"].items() if
                                             info["percent"] < 5],
                    "problematic_columns": [col for col, info in sheet_info["missing_values"].items() if
                                            info["percent"] > 20]
                }

                print(f"    📊 Качество данных: {data_quality_score:.1f}% полноты")
                if sheet_info["data_quality"]["problematic_columns"]:
                    print(f"    ⚠️ Проблемные колонки: {', '.join(sheet_info['data_quality']['problematic_columns'])}")

                self.context["sheets_info"][sheet_name] = sheet_info

                # Обновляем глобальную статистику
                for col in df.columns:
                    col_str = str(col)
                    if col_str not in all_data_types:
                        all_data_types[col_str] = set()
                    all_data_types[col_str].add(str(df[col].dtype))

        # Глобальный анализ
        self.context["global_analysis"] = {
            "total_rows": total_rows,
            "all_columns": list(all_columns),
            "common_columns": [col for col in all_columns if len([s for s in self.sheet_names if
                                                                  col in self.context["sheets_info"].get(s, {}).get(
                                                                      "columns", [])]) > 1],
            "column_frequency": {col: len(
                [s for s in self.sheet_names if col in self.context["sheets_info"].get(s, {}).get("columns", [])]) for
                                 col in all_columns},
            "data_types_summary": {col: list(types) for col, types in all_data_types.items()},
            "file_size_mb": float(file_size),
            "total_sheets": len(self.sheet_names),
            "sheets_with_data": len([s for s in self.sheet_names if s in self.dataframes]),
            "average_rows_per_sheet": total_rows / len([s for s in self.sheet_names if s in self.dataframes]) if len(
                [s for s in self.sheet_names if s in self.dataframes]) > 0 else 0,
            "total_columns": len(all_columns),
            "date_columns_count": len([col for col in all_columns if any(
                col in self.context["sheets_info"].get(s, {}).get("date_columns", []) for s in self.sheet_names)]),
            "numeric_columns_count": len([col for col in all_columns if any(
                col in self.context["sheets_info"].get(s, {}).get("numeric_columns", []) for s in self.sheet_names)]),
            "text_columns_count": len([col for col in all_columns if any(
                col in self.context["sheets_info"].get(s, {}).get("text_columns", []) for s in self.sheet_names)])
        }

        # Выводим итоговую сводку
        print(f"\n📊 ИТОГОВАЯ СВОДКА:")
        print(f"  📁 Файл: {os.path.basename(self.file_path)} ({file_size:.2f} МБ)")
        print(f"  📋 Листов: {len(self.sheet_names)}")
        print(f"  📊 Всего строк: {total_rows:,}")
        print(f"  📋 Всего колонок: {len(all_columns)}")
        print(f"  📅 Колонок с датами: {self.context['global_analysis']['date_columns_count']}")
        print(f"  🔢 Числовых колонок: {self.context['global_analysis']['numeric_columns_count']}")
        print(f"  📝 Текстовых колонок: {self.context['global_analysis']['text_columns_count']}")
        print(f"  🔗 Общих колонок: {len(self.context['global_analysis']['common_columns'])}")

        if self.context['global_analysis']['common_columns']:
            print(f"  🔗 Общие колонки: {', '.join(self.context['global_analysis']['common_columns'])}")

        print("✅ Анализ структуры данных завершен!")

    def _create_enhanced_context(self) -> str:
        """
        Создает расширенный контекст для понимания структуры данных
        """
        context = f"""
        📊 ДЕТАЛЬНЫЙ АНАЛИЗ EXCEL ФАЙЛА

        📁 ФАЙЛ:
        - Путь: {self.file_path}
        - Размер: {self.context['global_analysis']['file_size_mb']:.2f} МБ
        - Листов: {self.context['total_sheets']}
        - Листов с данными: {self.context['global_analysis']['sheets_with_data']}

        📊 ОБЩАЯ СТАТИСТИКА:
        - Всего строк: {self.context['global_analysis']['total_rows']:,}
        - Всего колонок: {self.context['global_analysis']['total_columns']}
        - Среднее строк на лист: {self.context['global_analysis']['average_rows_per_sheet']:.0f}

        📋 ТИПЫ ДАННЫХ:
        - Колонок с датами: {self.context['global_analysis']['date_columns_count']}
        - Числовых колонок: {self.context['global_analysis']['numeric_columns_count']}
        - Текстовых колонок: {self.context['global_analysis']['text_columns_count']}

        📋 СПИСОК ЛИСТОВ:
        {', '.join(self.sheet_names)}

        🔍 ГЛОБАЛЬНЫЙ АНАЛИЗ:
        - Всего уникальных колонок: {len(self.context['global_analysis']['all_columns'])}
        - Общие колонки (есть в нескольких листах): {', '.join(self.context['global_analysis']['common_columns'])}

        📈 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ ПО ЛИСТАМ:
        """

        for sheet_name, info in self.context["sheets_info"].items():
            context += f"""
        🏙️ ЛИСТ: {sheet_name}
        - Строк: {info['rows']:,}
        - Колонок: {len(info['columns'])}
        - Качество данных: {info['data_quality']['completeness_score']:.1f}% полноты

        📊 КОЛОНКИ:
        {', '.join(info['columns'])}

        🧠 СЕМАНТИЧЕСКИЙ АНАЛИЗ КОЛОНОК:"""
            
            # Добавляем семантическую информацию для каждой колонки
            for col in info['columns']:
                semantic_key = f"{sheet_name}_{col}"
                if semantic_key in self.context.get("column_semantics", {}):
                    sem_info = self.context["column_semantics"][semantic_key]
                    context += f"""
        📋 {col}:
          • Значення: {sem_info.get('meaning', 'не визначено')}
          • Категорія: {sem_info.get('category', 'загальні дані')}
          • Бізнес-контекст: {sem_info.get('business_context', 'не визначено')}
          • Можливі операції: {', '.join(sem_info.get('operations', ['базові']))}"""
                    if sem_info.get('units'):
                        context += f"""
          • Одиниці виміру: {sem_info.get('units')}"""

            context += f"""

        🔢 ТИПЫ ДАННЫХ И ФОРМАТЫ:
        """
            for col, dtype in info['data_types'].items():
                missing_info = info['missing_values'].get(col, {})
                missing_text = f" ({missing_info.get('count', 0)} пропущенных, {missing_info.get('percent', 0):.1f}%)" if missing_info.get(
                    'count', 0) > 0 else ""
                context += f"  - {col}: {dtype}{missing_text}\n"

            if info['numeric_columns']:
                context += f"\n📈 ЧИСЛОВЫЕ КОЛОНКИ: {', '.join(info['numeric_columns'])}\n"
                for col in info['numeric_columns']:
                    if col in info['column_analysis']:
                        stats = info['column_analysis'][col]
                        context += f"  - {col}: min={stats['min']}, max={stats['max']}, среднее={stats['mean']:.2f}, std={stats['std']:.2f}\n"
                        if stats['zeros'] > 0:
                            context += f"    (нулевых значений: {stats['zeros']})\n"
                        if stats['negative'] > 0:
                            context += f"    (отрицательных значений: {stats['negative']})\n"

            if info['date_columns']:
                context += f"\n📅 ДАТЫ: {', '.join(info['date_columns'])}\n"
                for col in info['date_columns']:
                    if col in info.get('date_range', {}):
                        date_info = info['date_range'][col]
                        context += f"  - {col}: {date_info['start']} до {date_info['end']} ({date_info['total_days']} дней)\n"

                        # Добавляем детальную информацию о месяцах и годах
                        if col in info.get('date_analysis', {}):
                            date_analysis = info['date_analysis'][col]
                            context += f"    📊 Анализ по периодам:\n"
                            context += f"    - Годы: {', '.join(map(str, date_analysis['years']))}\n"
                            context += f"    - Месяцы: {', '.join(date_analysis['month_names'])}\n"
                            context += f"    - Записи по месяцам: {date_analysis['records_per_month']}\n"
                            context += f"    - Записи по годам: {date_analysis['records_per_year']}\n"

            if info['text_columns']:
                context += f"\n📝 ТЕКСТОВЫЕ КОЛОНКИ: {', '.join(info['text_columns'])}\n"
                for col in info['text_columns']:
                    if col in info['column_analysis']:
                        analysis = info['column_analysis'][col]
                        if 'sample_values' in analysis:
                            context += f"  - {col}: примеры значений: {', '.join(map(str, analysis['sample_values'][:5]))}\n"
                            if 'avg_length' in analysis:
                                context += f"    (средняя длина: {analysis['avg_length']:.1f} символов)\n"

            # Добавляем информацию о качестве данных
            if info['data_quality']['problematic_columns']:
                context += f"\n⚠️ ПРОБЛЕМНЫЕ КОЛОНКИ (много пропущенных значений):\n"
                for col in info['data_quality']['problematic_columns']:
                    missing_info = info['missing_values'][col]
                    context += f"  - {col}: {missing_info['count']} пропущенных ({missing_info['percent']:.1f}%)\n"

            if info['data_quality']['high_quality_columns']:
                context += f"\n✅ ВЫСОКОКАЧЕСТВЕННЫЕ КОЛОНКИ (мало пропущенных значений):\n"
                context += f"  {', '.join(info['data_quality']['high_quality_columns'])}\n"

        context += f"""

        💡 РЕКОМЕНДАЦИИ ДЛЯ АНАЛИЗА:
        1. Используй числовые колонки для расчетов и статистики
        2. Используй даты для временного анализа
        3. Используй текстовые колонки для группировки и фильтрации
        4. Обращай внимание на общие колонки между листами для сравнения
        5. Учитывай уникальность значений в колонках
        6. 🧠 ИСПОЛЬЗУЙ СЕМАНТИЧЕСКУЮ ИНФОРМАЦИЮ: обращай внимание на значення, бізнес-контекст и можливі операції для каждой колонки
        7. 🎯 УЧИТЫВАЙ ЕДИНИЦЫ ИЗМЕРЕНИЯ: если указаны единицы измерения, включай их в ответы для точности
        8. 📊 ВЫБИРАЙ ПОДХОДЯЩИЕ ОПЕРАЦИИ: используй рекомендованные операции для каждой колонки согласно их семантике
        
        🚀 УЛУЧШЕННОЕ ПОНИМАНИЕ ДАННЫХ:
        Теперь у тебя есть детальная семантическая информация о каждой колонке, что позволяет:
        - Лучше понимать бизнес-логику данных
        - Выбирать более подходящие методы анализа
        - Давать более точные и содержательные ответы
        - Учитывать контекст использования данных
        
        Давай краткий ответ по запросу пользователя, основываясь на этой информации.
        Отвечай на украинском языке.
        """

        return context

    def _normalize_region_name(self, region_name: str) -> str:
        """
        Нормализует название региона для поиска соответствий с использованием LLM
        """
        # Если регион пустой или None, возвращаем как есть
        if not region_name:
            return region_name

        try:
            # Создаем промпт для LLM
            prompt = ChatPromptTemplate.from_messages([
                SystemMessage(content="""
                Ты - эксперт по географии Украины. Твоя задача - нормализовать названия регионов Украины.
                Если пользователь вводит название региона на русском языке, переведи его на украинский.
                Если название уже на украинском, оставь его как есть.
                Если это не название региона Украины, верни исходное значение.

                Примеры:
                "киев" -> "Київ"
                "львов" -> "Львів"
                "харьков" -> "Харків"
                "одесса" -> "Одеса"
                "днепр" -> "Дніпро"
                "запорожье" -> "Запоріжжя"
                "винница" -> "Вінниця"
                "полтава" -> "Полтава"
                "Київ" -> "Київ"
                "Львів" -> "Львів"

                Верни только нормализованное название региона без дополнительных комментариев.
                """),
                HumanMessage(content=f"Нормализуй название региона: {region_name}")
            ])

            # Получаем ответ от LLM
            response = self.llm.invoke(prompt)
            normalized_region = response.content.strip()

            # Если ответ пустой или слишком длинный, возвращаем исходное значение
            if not normalized_region or len(normalized_region) > 50:
                return region_name

            return normalized_region

        except Exception as e:
            # В случае ошибки, используем запасной вариант с словарем
            region_mapping = {
                "киев": "Київ",
                "львов": "Львів",
                "харьков": "Харків",
                "одесса": "Одеса",
                "днепр": "Дніпро",
                "запорожье": "Запоріжжя",
                "винница": "Вінниця",
                "полтава": "Полтава"
            }

            normalized = region_name.lower().strip()
            return region_mapping.get(normalized, region_name)

    def _normalize_date_reference(self, text: str) -> str:
        """
        Нормализует упоминания дат и месяцев в запросе пользователя

        Args:
            text: Текст запроса пользователя

        Returns:
            Текст с нормализованными названиями месяцев
        """
        if not text:
            return text

        try:
            # Создаем промпт для LLM
            prompt = ChatPromptTemplate.from_messages([
                SystemMessage(content="""
                Ты - эксперт по обработке дат. Твоя задача - нормализовать названия месяцев в тексте.
                Если в тексте есть названия месяцев на русском языке, замени их на украинский эквивалент.
                Если название уже на украинском, оставь его как есть.
                Обрабатывай только названия месяцев, остальной текст оставь без изменений.

                Примеры:
                "январь" -> "січень"
                "февраль" -> "лютий"
                "март" -> "березень"
                "апрель" -> "квітень"
                "май" -> "травень"
                "июнь" -> "червень"
                "июль" -> "липень"
                "август" -> "серпень"
                "сентябрь" -> "вересень"
                "октябрь" -> "жовтень"
                "ноябрь" -> "листопад"
                "декабрь" -> "грудень"

                Верни весь текст с замененными названиями месяцев.
                """),
                HumanMessage(content=f"Нормализуй названия месяцев в тексте: {text}")
            ])

            # Получаем ответ от LLM
            response = self.llm.invoke(prompt)
            normalized_text = response.content.strip()

            # Если ответ пустой или слишком длинный, возвращаем исходное значение
            if not normalized_text or len(normalized_text) > len(text) * 2:
                # Используем запасной вариант
                return self._normalize_date_reference_fallback(text)

            return normalized_text

        except Exception as e:
            # В случае ошибки, используем запасной вариант
            return self._normalize_date_reference_fallback(text)

    def _normalize_date_reference_fallback(self, text: str) -> str:
        """
        Запасной метод для нормализации названий месяцев с использованием словаря
        """
        month_mapping = {
            "январь": "січень",
            "января": "січня",
            "январе": "січні",
            "февраль": "лютий",
            "февраля": "лютого",
            "феврале": "лютому",
            "март": "березень",
            "марта": "березня",
            "марте": "березні",
            "апрель": "квітень",
            "апреля": "квітня",
            "апреле": "квітні",
            "май": "травень",
            "мая": "травня",
            "мае": "травні",
            "июнь": "червень",
            "июня": "червня",
            "июне": "червні",
            "июль": "липень",
            "июля": "липня",
            "июле": "липні",
            "август": "серпень",
            "августа": "серпня",
            "августе": "серпні",
            "сентябрь": "вересень",
            "сентября": "вересня",
            "сентябре": "вересні",
            "октябрь": "жовтень",
            "октября": "жовтня",
            "октябре": "жовтні",
            "ноябрь": "листопад",
            "ноября": "листопада",
            "ноябре": "листопаді",
            "декабрь": "грудень",
            "декабря": "грудня",
            "декабре": "грудні"
        }

        # Создаем регулярное выражение для поиска всех месяцев
        pattern = '|'.join(month_mapping.keys())

        # Функция для замены найденных месяцев
        def replace_month(match):
            found = match.group(0).lower()
            return month_mapping.get(found, found)

        # Используем регулярное выражение для замены
        result = re.sub(f'({pattern})', replace_month, text, flags=re.IGNORECASE)

        return result

    def _extract_month_from_query(self, query: str) -> Optional[str]:
        """
        Извлекает месяц из запроса и возвращает его номер (01-12)
        """
        month_mapping = {
            "січень": "01", "январь": "01", "января": "01",
            "лютий": "02", "февраль": "02", "февраля": "02",
            "березень": "03", "март": "03", "марта": "03",
            "квітень": "04", "апрель": "04", "апреля": "04",
            "травень": "05", "май": "05", "мая": "05",
            "червень": "06", "июнь": "06", "июня": "06",
            "липень": "07", "июль": "07", "июля": "07",
            "серпень": "08", "август": "08", "августа": "08",
            "вересень": "09", "сентябрь": "09", "сентября": "09",
            "жовтень": "10", "октябрь": "10", "октября": "10",
            "листопад": "11", "ноябрь": "11", "ноября": "11",
            "грудень": "12", "декабрь": "12", "декабря": "12"
        }

        query_lower = query.lower()
        for month_name, month_num in month_mapping.items():
            if month_name in query_lower:
                return month_num
        return None

    def _create_date_filtered_prompt(self, query: str, relevant_sheets: List[str]) -> str:
        """
        Создает специальный промпт для запросов с фильтрацией по датам
        """
        month_num = self._extract_month_from_query(query)

        if not month_num:
            return ""

        # Получаем информацию о доступных данных по месяцам
        date_info = ""
        sample_data_info = ""
        first_date_col = None  # Сохраняем первую найденную колонку с датами

        for sheet_name in relevant_sheets:
            if sheet_name in self.context["sheets_info"]:
                info = self.context["sheets_info"][sheet_name]
                if "date_analysis" in info:
                    for date_col, analysis in info["date_analysis"].items():
                        # Сохраняем первую найденную колонку с датами
                        if first_date_col is None:
                            first_date_col = date_col
                            
                        # Проверяем, есть ли данные за указанный месяц
                        month_year_combinations = analysis.get("month_year_combinations", [])
                        records_per_month = analysis.get("records_per_month", {})

                        # Ищем записи за указанный месяц
                        month_records = {}
                        for month_year, count in records_per_month.items():
                            if month_year.endswith(f"-{month_num}"):
                                month_records[month_year] = count

                        # Добавляем примеры данных
                        if sheet_name in self.dataframes:
                            df = self.dataframes[sheet_name]
                            if date_col in df.columns:
                                sample_dates = df[date_col].dropna().head(5).tolist()

                                sample_data_info += f"""
        📋 ПРИМЕРЫ ДАННЫХ - {sheet_name}:
        - Колонка даты: '{date_col}'
        - Примеры дат: {sample_dates}
        - Тип данных: {df[date_col].dtype}
        """

                        if month_records:
                            date_info += f"""
        📅 {sheet_name} - {date_col}:
        - ✅ Найдены записи за месяц {month_num}: {month_records}
        - Всего записей за этот месяц: {sum(month_records.values())}
        """
                        else:
                            date_info += f"""
        ⚠️ {sheet_name} - {date_col}:
        - ❌ НЕТ записей за месяц {month_num}
        - Доступные месяцы: {', '.join(month_year_combinations)}
        - Доступные записи по месяцам: {records_per_month}
        """

        if date_info and first_date_col:
            return f"""
        📅 ФИЛЬТРАЦИЯ ПО ДАТАМ:
        Запрос касается месяца: {month_num}

        {sample_data_info}

        {date_info}

        💡 ИНСТРУКЦИИ ДЛЯ ФИЛЬТРАЦИИ:
        1. ВСЕГДА сначала проверяй наличие данных за указанный месяц
        2. Если есть записи за указанный месяц - используй фильтр по дате
        3. Если записей нет - сообщи об этом и предложи альтернативы
        4. Используй pandas для фильтрации: df[df['{first_date_col}'].dt.month == {month_num}]
        5. Или фильтруй по строке: df[df['{first_date_col}'].str.contains('2024-{month_num}')]
        6. Для проверки доступных месяцев используй: df['{first_date_col}'].dt.strftime('%Y-%m').value_counts()
        7. Всегда проверяй наличие данных перед анализом
        8. Если данных нет, предложи доступные месяцы для анализа
        """

        return ""

    def _create_data_examples_prompt(self, relevant_sheets: List[str]) -> str:
        """
        Создает промпт с примерами данных для лучшего понимания структуры
        """
        examples = ""

        # Сначала добавляем примеры из объединенного DataFrame, если он существует
        if self.combined_df is not None and not self.combined_df.empty:
            # Создаем безопасное представление данных из объединенного DataFrame
            combined_sample_data = []
            for idx, row in self.combined_df.head(3).iterrows():
                row_data = {}
                for col in self.combined_df.columns:
                    value = row[col]
                    # Преобразуем булевы значения в строки
                    if isinstance(value, bool):
                        row_data[col] = str(value)
                    elif pd.isna(value):
                        row_data[col] = "NaN"
                    else:
                        row_data[col] = str(value)
                combined_sample_data.append(row_data)

            examples += f"""
        📊 ПРИМЕРЫ ОБЪЕДИНЕННЫХ ДАННЫХ:
        - Размер данных: {len(self.combined_df)} строк, {len(self.combined_df.columns)} колонок
        - Колонки: {list(self.combined_df.columns)}
        - ВАЖНО: Колонка 'Регіон' содержит название региона и используется для группировки
        - Первые 3 записи:
        """

            # Добавляем примеры данных в безопасном формате
            for i, row_data in enumerate(combined_sample_data, 1):
                examples += f"        Запись {i}:\n"
                for col, value in row_data.items():
                    examples += f"          {col}: {value}\n"

            # Добавляем информацию о типах данных
            combined_dtypes_info = self.combined_df.dtypes.to_dict()
            examples += f"        - Типы данных:\n"
            for col, dtype in combined_dtypes_info.items():
                examples += f"          {col}: {dtype}\n"

            # Добавляем примеры уникальных значений колонки 'Регіон'
            if 'Регіон' in self.combined_df.columns:
                region_values = self.combined_df['Регіон'].unique().tolist()
                examples += f"        - Уникальные значения колонки 'Регіон': {region_values}\n"
                examples += f"        - Пример группировки: df.groupby('Регіон')['колонка'].mean()\n"

        # Затем добавляем примеры из отдельных листов
        for sheet_name in relevant_sheets:
            if sheet_name in self.dataframes:
                df = self.dataframes[sheet_name]

                # Создаем безопасное представление данных
                sample_data = []
                for idx, row in df.head(3).iterrows():
                    row_data = {}
                    for col in df.columns:
                        value = row[col]
                        # Преобразуем булевы значения в строки
                        if isinstance(value, bool):
                            row_data[col] = str(value)
                        elif pd.isna(value):
                            row_data[col] = "NaN"
                        else:
                            row_data[col] = str(value)
                    sample_data.append(row_data)

                examples += f"""
        📊 ПРИМЕРЫ ДАННЫХ - {sheet_name}:
        - Размер данных: {len(df)} строк, {len(df.columns)} колонок
        - Колонки: {list(df.columns)}
        - Первые 3 записи:
        """

                # Добавляем примеры данных в безопасном формате
                for i, row_data in enumerate(sample_data, 1):
                    examples += f"        Запись {i}:\n"
                    for col, value in row_data.items():
                        examples += f"          {col}: {value}\n"

                # Добавляем информацию о типах данных
                dtypes_info = df.dtypes.to_dict()
                examples += f"        - Типы данных:\n"
                for col, dtype in dtypes_info.items():
                    examples += f"          {col}: {dtype}\n"

                # Добавляем информацию о датах
                date_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
                if date_columns:
                    examples += f"        - Колонки с датами: {date_columns}\n"
                    for date_col in date_columns:
                        sample_dates = df[date_col].dropna().head(5).astype(str).tolist()
                        examples += f"          {date_col}: {sample_dates}\n"

        return examples

    def _determine_query_type(self, query: str) -> str:
        """
        Определяет тип запроса для выбора стратегии обработки

        Args:
            query: Нормализованный запрос

        Returns:
            Тип запроса: "single_region", "comparison", "general"
        """
        query_lower = query.lower()

        # Ключевые слова для сравнения регионов
        comparison_keywords = [
            "найвищі", "найнижчі", "найкращі", "найгірші", "порівняй", "сравни",
            "який", "яка", "які", "де", "в якому", "в якій", "найбільше", "найменше",
            "топ", "рейтинг", "ранжируй", "сортуй", "від найвищого", "від найнижчого"
        ]

        # Проверяем, содержит ли запрос ключевые слова для сравнения
        for keyword in comparison_keywords:
            if keyword in query_lower:
                return "comparison"

        # Если запрос содержит "все" или "всі" - это общий запрос
        if "всі" in query_lower or "все" in query_lower or "кожен" in query_lower:
            return "general"

        # По умолчанию считаем запросом для сравнения, если он не содержит конкретных регионов
        return "comparison"

    def _find_relevant_sheets(self, query: str) -> List[str]:
        """
        Находит релевантные листы на основе запроса
        Оптимизированная логика для точного определения городов
        """
        query_lower = query.lower()
        found_regions = []

        # Словарь для поиска городов в запросе
        city_keywords = {
            'киев': ['киев', 'києв', 'киеве', 'києві'],
            'львов': ['львов', 'львів', 'львове', 'львові'],
            'харьков': ['харьков', 'харків', 'харькове', 'харкові'],
            'одесса': ['одесса', 'одеса', 'одессе', 'одесі'],
            'днепр': ['днепр', 'дніпро', 'днепре', 'дніпрі'],
            'запорожье': ['запорожье', 'запоріжжя', 'запорожье', 'запоріжжі'],
            'винница': ['винница', 'вінниця', 'виннице', 'вінниці'],
            'полтава': ['полтава', 'полтаве', 'полтаві']
        }

        # Ищем упоминания городов в запросе
        for city_name, keywords in city_keywords.items():
            for keyword in keywords:
                if keyword in query_lower:
                    found_regions.append(city_name)
                    break

        # Если найдены конкретные города, ищем соответствующие листы
        if found_regions:
            relevant_sheets = []
            for region in found_regions:
                # Нормализуем название региона
                normalized_region = self._normalize_region_name(region)

                # Ищем точное соответствие
                if normalized_region in self.sheet_names:
                    relevant_sheets.append(normalized_region)
                else:
                    # Ищем частичное соответствие
                    for sheet_name in self.sheet_names:
                        if normalized_region.lower() in sheet_name.lower() or sheet_name.lower() in normalized_region.lower():
                            relevant_sheets.append(sheet_name)
                            break

            # Убираем дубликаты и возвращаем найденные листы
            return list(set(relevant_sheets))

        # Если города не найдены, возвращаем все листы
        return self.sheet_names

    def _create_system_prompt(self, query: str, relevant_sheets: List[str]) -> str:
        """
        Создает системный промпт для контекстного понимания с расширенным анализом
        """
        # Получаем расширенный контекст
        enhanced_context = self._create_enhanced_context()

        # Создаем специальный промпт для фильтрации по датам
        date_filter_prompt = self._create_date_filtered_prompt(query, relevant_sheets)

        # Создаем промпт с примерами данных
        data_examples = self._create_data_examples_prompt(relevant_sheets)

        # Создаем детальную информацию о релевантных листах
        relevant_info = ""
        for sheet_name in relevant_sheets:
            if sheet_name in self.context["sheets_info"]:
                info = self.context["sheets_info"][sheet_name]
                relevant_info += f"""
        🎯 РЕЛЕВАНТНЫЙ ЛИСТ: {sheet_name}
        - Строк: {info['rows']}
        - Колонок: {len(info['columns'])}
        - Числовые колонки: {', '.join(info['numeric_columns'])}
        - Дата колонки: {', '.join(info['date_columns'])}
        - Текстовые колонки: {', '.join(info['text_columns'])}
        """

                # Добавляем статистику по числовым колонкам
                if info['numeric_columns']:
                    relevant_info += "📊 СТАТИСТИКА ПО ЧИСЛОВЫМ КОЛОНКАМ:\n"
                    for col in info['numeric_columns']:
                        if col in info['column_analysis']:
                            stats = info['column_analysis'][col]
                            relevant_info += f"  - {col}: min={stats['min']}, max={stats['max']}, среднее={stats['mean']:.2f}\n"

        prompt = f"""
        🤖 Ты - эксперт по анализу Excel данных с глубоким пониманием структуры данных.

        {enhanced_context}

        🎯 АНАЛИЗ ЗАПРОСА:
        Запрос пользователя: "{query}"
        Релевантные листы: {', '.join(relevant_sheets)}
        Количество анализируемых регионов: {len(relevant_sheets)}

        {relevant_info}

        {data_examples}

        {date_filter_prompt}

        🧠 СЕМАНТИЧЕСКОЕ ПОНИМАНИЕ КОЛОНОК:
        Используй предоставленную семантическую информацию о колонках для лучшего понимания:
        - Значення: что означает каждая колонка в бизнес-контексте
        - Категорія: к какой категории данных относится (финансы, товары, клиенты и т.д.)
        - Бізнес-контекст: как колонка используется в бизнес-процессах  
        - Можливі операції: какие аналитические операции подходят для этой колонки
        - Одиниці виміру: единицы измерения для точных ответов
        
        📋 ИНСТРУКЦИИ ДЛЯ АНАЛИЗА:
        1. 🧠 ИСПОЛЬЗУЙ СЕМАНТИКУ: всегда учитывай семантическое значение колонок при анализе
        2. Используй детальную информацию о структуре данных для точного ответа
        3. Если запрос касается конкретного региона, фокусируйся на соответствующем листе
        4. Если запрос общий, сравнивай данные между листами
        5. Используй числовые колонки для расчетов (суммы, средние, проценты)
        6. Используй даты для временного анализа (тренды, сезонность)
        7. Используй текстовые колонки для группировки и категоризации
        8. Учитывай статистику по колонкам (min, max, среднее) для контекста
        9. 🎯 ВКЛЮЧАЙ ЕДИНИЦЫ ИЗМЕРЕНИЯ: всегда указывай единицы измерения в ответах
        10. 📊 ВЫБИРАЙ ПОДХОДЯЩИЕ ОПЕРАЦИИ: используй рекомендованные операции для каждой колонки
        11. Отвечай на украинском языке
        12. Предоставляй конкретные цифры и выводы
        13. Если нужно, делай сравнения между регионами

        💡 СТРАТЕГИЯ ОТВЕТА:
        - Сначала определи тип запроса (статистика, сравнение, тренд, поиск)
        - Выбери подходящие колонки для анализа
        - Используй соответствующие агрегации (sum, mean, count, groupby)
        - Предоставь интерпретацию результатов
        - Если анализируешь один регион, дай детальный ответ
        - Если анализируешь несколько регионов, сравнивай их
        - Если анализируешь все регионы, дай общую картину
        - ВАЖНО: Если запрос касается конкретного месяца, всегда проверяй наличие данных за этот месяц

        🔄 РАБОТА С ОБЪЕДИНЕННЫМИ ДАННЫМИ:
        - Если данные содержат колонку 'Регіон', используй её для группировки
        - Для сравнения регионов используй: df.groupby('Регіон')['колонка'].agg(['mean', 'max', 'min'])
        - Для ранжирования используй: df.groupby('Регіон')['колонка'].mean().sort_values(ascending=False)
        - Всегда группируй по региону при сравнении данных между регионами

        📅 РАБОТА С ДАТАМИ:
        - ВСЕГДА сначала проверяй наличие данных за указанный период
        - Используй df['колонка_даты'].dt.month для фильтрации по месяцам
        - Используй df['колонка_даты'].dt.year для фильтрации по годам
        - Для проверки доступных дат используй: df['колонка_даты'].dt.strftime('%Y-%m').value_counts()
        - Если данных за указанный период нет, сообщи об этом и предложи альтернативы
        - Всегда показывай примеры доступных дат в ответе
        - КРИТИЧНО: При фильтрации по месяцам ВСЕГДА сначала создавай отфильтрованный DataFrame:
          filtered_df = df[df['колонка_даты'].dt.month == номер_месяца]
        - Затем проверяй размер: if len(filtered_df) > 0:
        - И только потом выполняй расчеты с filtered_df, НЕ с month_filtered_df или другими неопределенными переменными
        """

        return prompt

    def chat(self, query: str) -> str:
        """
        Основной метод для общения с чат-ботом
        Оптимизированная логика для эффективной обработки запросов

        Args:
            query: Вопрос пользователя

        Returns:
            Ответ чат-бота
        """
        try:
            # Нормализуем названия месяцев в запросе
            normalized_query = self._normalize_date_reference(query)

            # Определяем тип запроса
            query_type = self._determine_query_type(normalized_query)

            # Находим релевантные листы
            relevant_sheets = self._find_relevant_sheets(normalized_query)

            print(f"🔍 Найдены релевантные листы: {relevant_sheets}")
            print(f"📋 Тип запроса: {query_type}")

            # Создаем системный промпт
            system_prompt = self._create_system_prompt(normalized_query, relevant_sheets)

            # Сценарий 1: Один конкретный регион
            if len(relevant_sheets) == 1:
                sheet_name = relevant_sheets[0]
                if sheet_name in self.agents:
                    print(f"🎯 Анализируем только регион: {sheet_name}")
                    # Проверяем, содержит ли запрос упоминания месяцев
                    month_in_query = self._extract_month_from_query(normalized_query)
                    month_instructions = ""
                    if month_in_query:
                        month_instructions = f"""

КРИТИЧНЫЕ ИНСТРУКЦИИ ДЛЯ ФИЛЬТРАЦИИ ПО МЕСЯЦАМ:
1. ВСЕГДА сначала создавай отфильтрованный DataFrame: filtered_df = df[df['колонка_даты'].dt.month == {month_in_query}]
2. Затем проверяй размер: if len(filtered_df) > 0:
3. Выполняй расчеты только с filtered_df, НЕ используй переменные типа month_filtered_df
4. Пример расчета средней цены: filtered_df['Ціна (₴)'].mean()
5. Если данных нет, сообщи об отсутствии данных за указанный месяц
"""
                    
                    enhanced_query = f"Контекст: {system_prompt}\n\nЗапрос: {normalized_query}{month_instructions}"
                    result = self.agents[sheet_name].invoke(enhanced_query)
                    return result['output']

            # Сценарий 2: Сравнение регионов (используем объединенный DataFrame)
            elif query_type == "comparison" and self.combined_agent:
                print(f"🔄 Используем объединенный DataFrame для сравнения регионов")

                # Фильтруем данные только по релевантным регионам
                if len(relevant_sheets) < len(self.sheet_names):
                    filtered_df = self.combined_df[self.combined_df['Регіон'].isin(relevant_sheets)]
                    if not filtered_df.empty:
                        # Создаем временного агента для отфильтрованных данных
                        temp_agent = create_pandas_dataframe_agent(
                            self.llm,
                            filtered_df,
                            verbose=False,
                            agent_type=AgentType.OPENAI_FUNCTIONS,
                            allow_dangerous_code=True
                        )
                        enhanced_query = f"""Контекст: {system_prompt}

Запрос: {normalized_query}

ВАЖНО: Данные содержат колонку 'Регіон' для группировки по регионам. 
Используй эту колонку для группировки и сравнения данных между регионами.
Пример: df.groupby('Регіон')['Ціна (₴)'].mean() - для сравнения средних цен по регионам.
Доступные регионы: {', '.join(filtered_df['Регіон'].unique())}"""
                        result = temp_agent.invoke(enhanced_query)
                        return result['output']

                # Если не удалось отфильтровать, используем полный объединенный DataFrame
                enhanced_query = f"""Контекст: {system_prompt}

Запрос: {normalized_query}

ВАЖНО: Данные содержат колонку 'Регіон' для группировки по регионам. 
Используй эту колонку для группировки и сравнения данных между регионами.
Пример: df.groupby('Регіон')['Ціна (₴)'].mean() - для сравнения средних цен по регионам.
Доступные регионы: {', '.join(self.combined_df['Регіон'].unique())}"""
                result = self.combined_agent.invoke(enhanced_query)
                return result['output']

            # Сценарий 3: Несколько конкретных регионов (индивидуальный анализ)
            elif len(relevant_sheets) < len(self.sheet_names):
                print(f"🎯 Анализируем конкретные регионы: {relevant_sheets}")
                responses = {}
                for sheet_name in relevant_sheets:
                    if sheet_name in self.agents:
                        enhanced_query = f"Контекст: {system_prompt}\n\nЗапрос: {normalized_query}"
                        result = self.agents[sheet_name].invoke(enhanced_query)
                        responses[sheet_name] = result['output']

                # Объединяем ответы по конкретным регионам
                if len(responses) == 1:
                    return list(responses.values())[0]
                else:
                    combined_response = f"📊 Анализ по запрошенным регионам ({len(responses)} регионов):\n\n"
                    for region, response in responses.items():
                        combined_response += f"🏙️ {region}:\n{response}\n\n"
                    return combined_response

            # Сценарий 4: Общий запрос по всем регионам (используем объединенный DataFrame)
            else:
                print(f"🌍 Анализируем все регионы через объединенный DataFrame")
                if self.combined_agent:
                    enhanced_query = f"""Контекст: {system_prompt}

Запрос: {normalized_query}

ВАЖНО: Данные содержат колонку 'Регіон' для группировки по регионам. 
Используй эту колонку для группировки и сравнения данных между регионами.
Пример: df.groupby('Регіон')['Ціна (₴)'].mean() - для сравнения средних цен по регионам.
Доступные регионы: {', '.join(self.combined_df['Регіон'].unique())}"""
                    result = self.combined_agent.invoke(enhanced_query)
                    return result['output']
                else:
                    # Fallback к старому методу
                    print(f"🌍 Fallback: анализируем все регионы по отдельности")
                    responses = {}
                    for sheet_name in relevant_sheets:
                        if sheet_name in self.agents:
                            enhanced_query = f"Контекст: {system_prompt}\n\nЗапрос: {normalized_query}"
                            result = self.agents[sheet_name].invoke(enhanced_query)
                            responses[sheet_name] = result['output']

                    combined_response = f"🌍 Анализ по всем регионам ({len(responses)} регионов):\n\n"
                    for region, response in responses.items():
                        combined_response += f"🏙️ {region}:\n{response}\n\n"
                    return combined_response

        except Exception as e:
            return f"❌ Ошибка при обработке запроса: {str(e)}"

    def generate_file_summary(self) -> str:
        """
        Генерирует краткое описание файла с помощью LLM для пользователя
        """
        try:
            # Проверяем, есть ли данные в файле
            total_rows = self.context['global_analysis']['total_rows']
            if total_rows == 0:
                return "📄 Файл пустой - данные не найдены."
            
            # Создаем краткий контекст для LLM
            brief_context = f"""
            Файл: {os.path.basename(self.file_path)}
            Листов: {len(self.sheet_names)}
            Названия листов: {', '.join(self.sheet_names)}
            Всего строк данных: {total_rows:,}
            Размер файла: {self.context['global_analysis']['file_size_mb']:.2f} МБ
            
            Типы колонок:
            - Числовых: {self.context['global_analysis']['numeric_columns_count']}
            - С датами: {self.context['global_analysis']['date_columns_count']}  
            - Текстовых: {self.context['global_analysis']['text_columns_count']}
            
            Общие колонки между листами: {', '.join(self.context['global_analysis']['common_columns']) if self.context['global_analysis']['common_columns'] else 'Нет'}
            """
            
            # Добавляем примеры данных из первого листа
            if self.sheet_names and self.sheet_names[0] in self.context["sheets_info"]:
                first_sheet = self.context["sheets_info"][self.sheet_names[0]]
                brief_context += f"\n\nПример колонок из листа '{self.sheet_names[0]}': {', '.join(first_sheet['columns'][:5])}"
                
                # Добавляем информацию о датах если есть
                if first_sheet['date_columns']:
                    for col in first_sheet['date_columns']:
                        if col in first_sheet.get('date_analysis', {}):
                            date_analysis = first_sheet['date_analysis'][col]
                            brief_context += f"\nПериод данных: {', '.join(date_analysis['month_names'])}"
                            break
            
            # Создаем промпт для LLM
            summary_prompt = f"""
            Создай краткое и понятное описание Excel файла для пользователя на украинском языке.
            Описание должно быть дружелюбным и информативным, объясняющим что содержит файл.
            
            Информация о файле:
            {brief_context}
            
            Начни описание с: "📊 Чудово! Стандартний файл завантажено."
            
            Затем кратко опиши:
            - Что содержит файл (по регионам, продажам и т.д. на основе названий листов)
            - Сколько данных (строк) 
            - Какой период охвачен (если есть даты)
            - Какие основные показатели можно анализировать
            
            Ответ должен быть не более 3-4 предложений, позитивным и полезным.
            """
            
            # Используем существующий LLM для генерации summary
            llm = ChatOpenAI(temperature=0.1, model="gpt-3.5-turbo")
            response = llm.invoke(summary_prompt)
            
            return response.content.strip()
            
        except Exception as e:
            # В случае ошибки возвращаем базовое описание
            if self.context['global_analysis']['total_rows'] == 0:
                return "📄 Файл пустой - данные не найдены."
            else:
                return f"📊 Чудово! Стандартний файл завантажено. Файл містить {len(self.sheet_names)} листів з даними по регіонах ({self.context['global_analysis']['total_rows']:,} записів). Тепер ви можете задавати запитання про дані."

    def get_file_info(self) -> Dict[str, Any]:
        """
        Возвращает информацию о файле
        """
        return {
            "file_path": self.file_path,
            "total_sheets": len(self.sheet_names),
            "sheets": self.sheet_names,
            "structure": self.context["sheets_info"]
        }

    def get_detailed_analysis(self) -> str:
        """
        Возвращает детальный анализ структуры данных в читаемом формате
        """
        return self._create_enhanced_context()

    def get_sheet_summary(self, sheet_name: str = None) -> Dict[str, Any]:
        """
        Возвращает краткую сводку по листу или всем листам

        Args:
            sheet_name: Название конкретного листа (если None, возвращает по всем)
        """
        if sheet_name:
            if sheet_name in self.context["sheets_info"]:
                info = self.context["sheets_info"][sheet_name]
                return {
                    "sheet_name": sheet_name,
                    "rows": info["rows"],
                    "columns": info["columns"],
                    "numeric_columns": info["numeric_columns"],
                    "date_columns": info["date_columns"],
                    "text_columns": info["text_columns"],
                    "data_types": info["data_types"]
                }
            else:
                return {"error": f"Лист '{sheet_name}' не найден"}
        else:
            summary = {}
            for name, info in self.context["sheets_info"].items():
                summary[name] = {
                    "rows": info["rows"],
                    "columns_count": len(info["columns"]),
                    "numeric_columns": info["numeric_columns"],
                    "date_columns": info["date_columns"],
                    "text_columns": info["text_columns"]
                }
            return summary

    def get_available_regions(self) -> List[str]:
        """Возвращает список доступных регионов"""
        return self.sheet_names

    def get_available_dates(self) -> Dict[str, Any]:
        """Возвращает информацию о доступных датах по регионам"""
        date_info = {}

        for sheet_name in self.sheet_names:
            if sheet_name in self.context["sheets_info"]:
                info = self.context["sheets_info"][sheet_name]
                if "date_analysis" in info:
                    date_info[sheet_name] = {}
                    for date_col, analysis in info["date_analysis"].items():
                        date_info[sheet_name][date_col] = {
                            "years": analysis.get("years", []),
                            "months": analysis.get("months", []),
                            "month_names": analysis.get("month_names", []),
                            "month_year_combinations": analysis.get("month_year_combinations", []),
                            "records_per_month": analysis.get("records_per_month", {}),
                            "records_per_year": analysis.get("records_per_year", {})
                        }

        return date_info

    def get_date_summary(self) -> str:
        """Возвращает сводку по датам в читаемом формате"""
        date_info = self.get_available_dates()

        if not date_info:
            return "📅 В данных нет колонок с датами"

        summary = "📅 СВОДКА ПО ДАТАМ:\n\n"

        for region, date_cols in date_info.items():
            summary += f"🏙️ {region}:\n"
            for date_col, info in date_cols.items():
                summary += f"  📊 {date_col}:\n"
                summary += f"    - Годы: {', '.join(map(str, info['years']))}\n"
                summary += f"    - Месяцы: {', '.join(info['month_names'])}\n"
                summary += f"    - Записи по месяцам: {info['records_per_month']}\n"
                summary += f"    - Записи по годам: {info['records_per_year']}\n"
            summary += "\n"

        return summary

    def query_specific_region(self, region: str, query: str) -> str:
        """
        Запрос к конкретному региону

        Args:
            region: Название региона
            query: Вопрос

        Returns:
            Ответ
        """
        normalized_region = self._normalize_region_name(region)
        normalized_query = self._normalize_date_reference(query)

        # Ищем точное соответствие
        if normalized_region in self.sheet_names:
            return self.chat(f"Для региона {normalized_region}: {normalized_query}")

        # Ищем частичное соответствие
        for sheet_name in self.sheet_names:
            if normalized_region.lower() in sheet_name.lower():
                return self.chat(f"Для региона {sheet_name}: {normalized_query}")

        return f"Регион '{region}' не найден. Доступные регионы: {', '.join(self.sheet_names)}"


def create_smart_excel_chatbot(file_path: str) -> SmartExcelChatbot:
    """
    Создает экземпляр умного чат-бота для работы с Excel файлом

    Args:
        file_path: Путь к Excel файлу

    Returns:
        Экземпляр SmartExcelChatbot
    """
    return SmartExcelChatbot(file_path)


# Пример использования
if __name__ == "__main__":
    # Путь к файлу с данными
    file_path = "../../data/детальні_продажі_по_регіонах.xlsx"

    try:
        # Создаем чат-бота
        chatbot = create_smart_excel_chatbot(file_path)

        # Получаем информацию о файле
        info = chatbot.get_file_info()
        print(f"📊 Файл: {info['file_path']}")
        print(f"📋 Листов: {info['total_sheets']}")
        print(f"🏙️ Регионы: {', '.join(info['sheets'])}")

        # Показываем детальный анализ структуры
        print("\n=== ДЕТАЛЬНЫЙ АНАЛИЗ СТРУКТУРЫ ===")
        detailed_analysis = chatbot.get_detailed_analysis()
        print(detailed_analysis)

        # Показываем краткую сводку
        print("\n=== КРАТКАЯ СВОДКА ===")
        summary = chatbot.get_sheet_summary()
        for sheet_name, sheet_info in summary.items():
            print(f"📋 {sheet_name}: {sheet_info['rows']} строк, {sheet_info['columns_count']} колонок")
            print(f"   Числовые: {len(sheet_info['numeric_columns'])}")
            print(f"   Даты: {len(sheet_info['date_columns'])}")
            print(f"   Текстовые: {len(sheet_info['text_columns'])}")

        # Примеры запросов
        print("\n=== Примеры запросов ===")

        # Запрос 1: По конкретному региону (с русским названием)
        print("\n1. Запрос по Киеву:")
        response = chatbot.chat("Яка середня ціна за Сентябрь в Киеве?")
        print(response)

        # Запрос 2: Общий запрос
        print("\n2. Общий запрос:")
        response = chatbot.chat("Порівняй ціни за Сентябрь по всіх регіонах")
        print(response)

        # Запрос 3: Анализ данных
        print("\n3. Анализ данных:")
        response = chatbot.chat("Які найпопулярніші товари?")
        print(response)

        # Запрос 4: Статистический анализ
        print("\n4. Статистический анализ:")
        response = chatbot.chat("Покажи статистику по всіх числових колонках")
        print(response)

    except Exception as e:
        print(f"Ошибка: {e}") 