"""
Refactored Smart Excel Chatbot with modular architecture
"""
import os
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

from .data_processor import ExcelDataProcessor
from .text_normalizer import TextNormalizer
from .data_analyzer import DataAnalyzer
from .prompt_generator import PromptGenerator
from .query_handler import QueryHandler

# Load environment variables
load_dotenv()


class SmartExcelChatbot:
    """
    Умный чат-бот для работы с Excel файлами
    Рефакторенная версия с модульной архитектурой
    """

    def __init__(self, file_path: str):
        """
        Инициализация чат-бота
        
        Args:
            file_path: Путь к Excel файлу
        """
        # Проверяем API ключ
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY не установлен. Установите его в .env файле.")

        # Инициализируем LLM
        self.llm = ChatOpenAI(temperature=0, model="gpt-4o-mini")
        
        # Инициализируем компоненты
        self.data_processor = ExcelDataProcessor(file_path)
        self.data_analyzer = None
        self.prompt_generator = None
        self.query_handler = None
        
        # Загружаем и анализируем данные
        self._initialize_components()
    
    def _initialize_components(self):
        """Инициализирует все компоненты системы"""
        # Загружаем данные
        self.data_processor.load_all_sheets(self.llm)
        self.data_processor.create_combined_dataframe(self.llm)
        
        # Создаем анализатор данных
        self.data_analyzer = DataAnalyzer(self.data_processor.dataframes)
        context = self.data_analyzer.create_enhanced_context()
        
        # Создаем генератор промптов
        self.prompt_generator = PromptGenerator(context, self.data_processor.dataframes)
        
        # Создаем обработчик запросов
        self.query_handler = QueryHandler(
            llm=self.llm,
            agents=self.data_processor.agents,
            combined_agent=self.data_processor.combined_agent,
            dataframes=self.data_processor.dataframes,
            prompt_generator=self.prompt_generator
        )
    
    def chat(self, query: str) -> str:
        """
        Основной метод для обработки пользовательских запросов
        
        Args:
            query: Пользовательский запрос
            
        Returns:
            Ответ на запрос
        """
        return self.query_handler.chat(query)
    
    def generate_file_summary(self) -> str:
        """
        Генерирует сводку по файлу
        
        Returns:
            Сводная информация о файле
        """
        return self.query_handler.generate_file_summary()
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        Возвращает информацию о файле
        
        Returns:
            Словарь с информацией о файле
        """
        return self.data_analyzer.get_file_info()
    
    def get_detailed_analysis(self) -> Dict[str, Any]:
        """
        Возвращает детальный анализ структуры данных
        
        Returns:
            Подробный анализ данных
        """
        return self.data_analyzer.context
    
    def get_sheet_summary(self, sheet_name: str = None) -> Dict[str, Any]:
        """
        Возвращает сводку по листу/листам
        
        Args:
            sheet_name: Название листа (опционально)
            
        Returns:
            Сводная информация
        """
        return self.data_analyzer.get_sheet_summary(sheet_name)
    
    def get_available_regions(self) -> List[str]:
        """
        Возвращает список доступных регионов
        
        Returns:
            Список названий регионов
        """
        return self.data_processor.get_available_regions()
    
    def get_available_dates(self) -> List[str]:
        """
        Возвращает список доступных дат
        
        Returns:
            Список дат
        """
        return self.data_processor.get_available_dates()
    
    def get_date_summary(self) -> Dict[str, List[str]]:
        """
        Возвращает сводку по датам
        
        Returns:
            Сводка дат по листам
        """
        return self.data_analyzer.get_date_summary()
    
    def query_specific_region(self, region: str, query: str) -> str:
        """
        Выполняет запрос по конкретному региону
        
        Args:
            region: Название региона
            query: Пользовательский запрос
            
        Returns:
            Ответ по данным региона
        """
        return self.query_handler.query_specific_region(region, query)
    
    def get_query_suggestions(self, query: str) -> List[str]:
        """
        Получает предложения запросов
        
        Args:
            query: Текущий запрос пользователя
            
        Returns:
            Список предложений
        """
        return self.query_handler.get_query_suggestions(query)
    
    # Методы для обратной совместимости с оригинальным API
    def get_context(self) -> Dict[str, Any]:
        """Возвращает контекст для обратной совместимости"""
        return self.data_analyzer.context if self.data_analyzer else {}
    
    @property
    def dataframes(self) -> Dict[str, Any]:
        """Возвращает DataFrame'ы для обратной совместимости"""
        return self.data_processor.dataframes
    
    @property
    def combined_df(self):
        """Возвращает объединенный DataFrame для обратной совместимости"""
        return self.data_processor.combined_df
    
    @property
    def sheet_names(self) -> List[str]:
        """Возвращает названия листов для обратной совместимости"""
        return self.data_processor.sheet_names


def create_smart_excel_chatbot(file_path: str) -> SmartExcelChatbot:
    """
    Фабричная функция для создания экземпляра чат-бота
    
    Args:
        file_path: Путь к Excel файлу
        
    Returns:
        Экземпляр SmartExcelChatbot
    """
    return SmartExcelChatbot(file_path)