"""
Data processing module for Excel files
"""
import os
import pandas as pd
from typing import Dict, List
from langchain.agents.agent_types import AgentType
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent


class ExcelDataProcessor:
    """
    Класс для загрузки и обработки данных из Excel файлов
    """
    
    def __init__(self, file_path: str):
        """
        Инициализация процессора данных
        
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
        self.combined_df = None
        self.combined_agent = None
    
    def load_all_sheets(self, llm):
        """
        Загружает все листы из Excel файла
        
        Args:
            llm: Language model для создания агентов
        """
        for sheet_name in self.sheet_names:
            try:
                df = pd.read_excel(
                    self.file_path, 
                    sheet_name=sheet_name, 
                    parse_dates=True, 
                    date_format='%Y-%m-%d'
                )
                if not df.empty:
                    self.dataframes[sheet_name] = df
                    # Создаем агента для каждого листа
                    self.agents[sheet_name] = create_pandas_dataframe_agent(
                        llm,
                        df,
                        verbose=False,
                        agent_type=AgentType.OPENAI_FUNCTIONS,
                        allow_dangerous_code=True
                    )
            except Exception as e:
                print(f"Ошибка загрузки листа {sheet_name}: {e}")
    
    def create_combined_dataframe(self, llm):
        """
        Создает объединенный DataFrame для сравнения регионов
        
        Args:
            llm: Language model для создания агента
        """
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
                llm,
                self.combined_df,
                verbose=False,
                agent_type=AgentType.OPENAI_FUNCTIONS,
                allow_dangerous_code=True
            )

            print(
                f"✅ Создан объединенный DataFrame с {len(self.combined_df)} записями из {len(self.dataframes)} регионов"
            )
        else:
            print("⚠️ Не удалось создать объединенный DataFrame - нет данных")
    
    def get_available_regions(self) -> List[str]:
        """Возвращает список доступных регионов"""
        return list(self.dataframes.keys())
    
    def get_available_dates(self) -> List[str]:
        """Возвращает список доступных дат из всех листов"""
        all_dates = set()
        
        for df in self.dataframes.values():
            # Ищем столбцы с датами
            date_columns = df.select_dtypes(include=['datetime64']).columns
            for col in date_columns:
                dates = df[col].dropna().dt.strftime('%Y-%m-%d').unique()
                all_dates.update(dates)
        
        return sorted(list(all_dates))