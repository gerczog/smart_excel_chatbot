"""
Рефакторений Smart Excel Chatbot з модульною архітектурою
"""
import os
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

from .data_processor import ExcelDataProcessor
from .data_analyzer import DataAnalyzer
from .prompt_generator import PromptGenerator
from .query_handler import QueryHandler

# Load environment variables
load_dotenv()


class SmartExcelChatbot:
    """
    Розумний чат-бот для роботи з Excel файлами
    Рефакторена версія з модульною архітектурою
    """

    def __init__(self, file_path: str):
        """
        Ініціалізація чат-бота
        
        Args:
            file_path: Шлях до Excel файлу
        """
        # Перевіряємо API ключ
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY не встановлено. Встановіть його у .env файлі.")

        # Ініціалізуємо LLM
        self.llm = ChatOpenAI(temperature=0, model="gpt-4o-mini")
        
        # Ініціалізуємо компоненти
        self.data_processor = ExcelDataProcessor(file_path)
        self.data_analyzer = None
        self.prompt_generator = None
        self.query_handler = None
        
        # Завантажуємо та аналізуємо дані
        self._initialize_components()
    
    def _initialize_components(self):
        """Ініціалізує всі компоненти системи"""
        # Завантажуємо дані
        self.data_processor.load_all_sheets(self.llm)
        self.data_processor.create_combined_dataframe(self.llm)
        
        # Створюємо аналізатор даних
        self.data_analyzer = DataAnalyzer(self.data_processor.dataframes)
        context = self.data_analyzer.create_enhanced_context()
        
        # Створюємо генератор промптів
        self.prompt_generator = PromptGenerator(context, self.data_processor.dataframes)
        
        # Створюємо обробник запитів
        self.query_handler = QueryHandler(
            llm=self.llm,
            agents=self.data_processor.agents,
            combined_agent=self.data_processor.combined_agent,
            dataframes=self.data_processor.dataframes,
            prompt_generator=self.prompt_generator
        )
    
    def chat(self, query: str) -> str:
        """
        Основний метод для обробки користувацьких запитів
        
        Args:
            query: Користувацький запит
            
        Returns:
            Відповідь на запит
        """
        return self.query_handler.chat(query)
    
    def generate_file_summary(self) -> str:
        """
        Генерує зведення по файлу
        
        Returns:
            Зведена інформація про файл
        """
        return self.query_handler.generate_file_summary()
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        Повертає інформацію про файл
        
        Returns:
            Словник з інформацією про файл
        """
        return self.data_analyzer.get_file_info()
    
    def get_detailed_analysis(self) -> Dict[str, Any]:
        """
        Повертає детальний аналіз структури даних
        
        Returns:
            Докладний аналіз даних
        """
        return self.data_analyzer.context
    
    def get_sheet_summary(self, sheet_name: str = None) -> Dict[str, Any]:
        """
        Повертає зведення по аркушу/аркушах
        
        Args:
            sheet_name: Назва аркуша (опціонально)
            
        Returns:
            Зведена інформація
        """
        return self.data_analyzer.get_sheet_summary(sheet_name)
    
    def get_available_regions(self) -> List[str]:
        """
        Повертає список доступних регіонів
        
        Returns:
            Список назв регіонів
        """
        return self.data_processor.get_available_regions()
    
    def get_available_dates(self) -> List[str]:
        """
        Повертає список доступних дат
        
        Returns:
            Список дат
        """
        return self.data_processor.get_available_dates()
    
    def get_date_summary(self) -> Dict[str, List[str]]:
        """
        Повертає зведення по датах
        
        Returns:
            Зведення дат по аркушах
        """
        return self.data_analyzer.get_date_summary()
    
    def query_specific_region(self, region: str, query: str) -> str:
        """
        Виконує запит по конкретному регіону
        
        Args:
            region: Назва регіону
            query: Користувацький запит
            
        Returns:
            Відповідь по даних регіону
        """
        return self.query_handler.query_specific_region(region, query)
    
    def get_query_suggestions(self, query: str) -> List[str]:
        """
        Отримує пропозиції запитів
        
        Args:
            query: Поточний запит користувача
            
        Returns:
            Список пропозицій
        """
        return self.query_handler.get_query_suggestions(query)
    
    # Методи для зворотної сумісності з оригінальним API
    def get_context(self) -> Dict[str, Any]:
        """Повертає контекст для зворотної сумісності"""
        return self.data_analyzer.context if self.data_analyzer else {}
    
    @property
    def dataframes(self) -> Dict[str, Any]:
        """Повертає DataFrame'и для зворотної сумісності"""
        return self.data_processor.dataframes
    
    @property
    def combined_df(self):
        """Повертає об'єднаний DataFrame для зворотної сумісності"""
        return self.data_processor.combined_df
    
    @property
    def sheet_names(self) -> List[str]:
        """Повертає назви аркушів для зворотної сумісності"""
        return self.data_processor.sheet_names


def create_smart_excel_chatbot(file_path: str) -> SmartExcelChatbot:
    """
    Фабрична функція для створення екземпляра чат-бота
    
    Args:
        file_path: Шлях до Excel файлу
        
    Returns:
        Екземпляр SmartExcelChatbot
    """
    return SmartExcelChatbot(file_path)