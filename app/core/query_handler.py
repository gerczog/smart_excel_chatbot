"""
Query handling module for managing AI agent interactions
"""
from typing import Dict, List, Optional, Any, Tuple
from langchain.schema import HumanMessage, SystemMessage
from .text_normalizer import TextNormalizer
from .prompt_generator import PromptGenerator


class QueryHandler:
    """
    Класс для обработки пользовательских запросов и взаимодействия с AI агентами
    """
    
    def __init__(self, llm, agents: Dict[str, Any], combined_agent, 
                 dataframes: Dict[str, Any], prompt_generator: PromptGenerator):
        """
        Инициализация обработчика запросов
        
        Args:
            llm: Language model
            agents: Словарь агентов для каждого листа
            combined_agent: Агент для объединенных данных
            dataframes: Словарь с DataFrame'ами
            prompt_generator: Генератор промптов
        """
        self.llm = llm
        self.agents = agents
        self.combined_agent = combined_agent
        self.dataframes = dataframes
        self.prompt_generator = prompt_generator
    
    def chat(self, query: str) -> str:
        """
        Обрабатывает пользовательский запрос и возвращает ответ
        
        Args:
            query: Пользовательский запрос
            
        Returns:
            Ответ на запрос
        """
        try:
            # Нормализуем запрос
            normalized_query = self._normalize_query(query)
            
            # Определяем релевантные листы
            relevant_sheets = self.prompt_generator.find_relevant_sheets(normalized_query)
            
            # Определяем тип запроса
            query_type = self.prompt_generator.determine_query_type(normalized_query)
            
            # Выбираем подходящего агента и создаем промпт
            if self._needs_combined_agent(query_type, relevant_sheets):
                agent = self.combined_agent
                system_prompt = self.prompt_generator.create_system_prompt(normalized_query, relevant_sheets)
            else:
                agent = self._select_best_agent(relevant_sheets)
                system_prompt = self.prompt_generator.create_system_prompt(normalized_query, relevant_sheets)
            
            # Создаем сообщения для AI
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=normalized_query)
            ]
            
            # Получаем ответ от агента
            if agent:
                response = agent.invoke(normalized_query)
                return self._format_response(response, query_type)
            else:
                return "Не удалось найти подходящий агент для обработки запроса."
                
        except Exception as e:
            return f"Произошла ошибка при обработке запроса: {str(e)}"
    
    def query_specific_region(self, region: str, query: str) -> str:
        """
        Выполняет запрос по конкретному региону
        
        Args:
            region: Название региона
            query: Пользовательский запрос
            
        Returns:
            Ответ по данным конкретного региона
        """
        try:
            # Нормализуем название региона
            normalized_region = TextNormalizer.normalize_region_name(region)
            
            # Ищем соответствующий агент
            agent = None
            actual_region_name = None
            
            for sheet_name in self.agents.keys():
                if TextNormalizer.normalize_region_name(sheet_name).lower() == normalized_region.lower():
                    agent = self.agents[sheet_name]
                    actual_region_name = sheet_name
                    break
            
            if not agent:
                return f"Данные для региона '{region}' не найдены."
            
            # Создаем специфичный промпт для региона
            region_prompt = self.prompt_generator.create_region_specific_prompt(actual_region_name, query)
            
            # Создаем сообщения
            messages = [
                SystemMessage(content=region_prompt),
                HumanMessage(content=query)
            ]
            
            # Получаем ответ
            response = agent.invoke(query)
            return self._format_response(response, 'specific')
            
        except Exception as e:
            return f"Ошибка при обработке запроса по региону '{region}': {str(e)}"
    
    def generate_file_summary(self) -> str:
        """
        Генерирует сводку по всему файлу
        
        Returns:
            Сводная информация о файле
        """
        try:
            # Создаем промпт для сводки
            summary_prompt = self.prompt_generator.generate_file_summary()
            
            # Используем LLM для генерации сводки
            messages = [
                SystemMessage(content="Ты - эксперт по анализу данных. Создай информативную сводку."),
                HumanMessage(content=summary_prompt)
            ]
            
            response = self.llm.invoke(messages)
            return response.content if hasattr(response, 'content') else str(response)
            
        except Exception as e:
            return f"Ошибка при создании сводки: {str(e)}"
    
    def _normalize_query(self, query: str) -> str:
        """
        Нормализует пользовательский запрос
        
        Args:
            query: Исходный запрос
            
        Returns:
            Нормализованный запрос
        """
        # Нормализуем даты в запросе
        normalized = TextNormalizer.normalize_date_reference(query)
        normalized = TextNormalizer.normalize_date_reference_fallback(normalized)
        
        return normalized.strip()
    
    def _needs_combined_agent(self, query_type: str, relevant_sheets: List[str]) -> bool:
        """
        Определяет, нужно ли использовать объединенного агента
        
        Args:
            query_type: Тип запроса
            relevant_sheets: Список релевантных листов
            
        Returns:
            True, если нужен объединенный агент
        """
        # Используем объединенный агент для сравнения между регионами
        if query_type == 'comparison' and len(relevant_sheets) > 1:
            return True
        
        # Или если запрос касается всех регионов
        if len(relevant_sheets) == len(self.dataframes):
            return True
        
        # Или если запрос требует агрегации по всем данным
        if query_type == 'summary' and len(relevant_sheets) > 1:
            return True
        
        return False
    
    def _select_best_agent(self, relevant_sheets: List[str]):
        """
        Выбирает наиболее подходящего агента
        
        Args:
            relevant_sheets: Список релевантных листов
            
        Returns:
            Выбранный агент или None
        """
        if not relevant_sheets:
            return self.combined_agent
        
        # Если релевантен только один лист, используем его агента
        if len(relevant_sheets) == 1:
            sheet_name = relevant_sheets[0]
            return self.agents.get(sheet_name, self.combined_agent)
        
        # Если несколько листов, используем объединенный агент
        return self.combined_agent
    
    def _format_response(self, response: str, query_type: str) -> str:
        """
        Форматирует ответ в зависимости от типа запроса
        
        Args:
            response: Исходный ответ от агента
            query_type: Тип запроса
            
        Returns:
            Отформатированный ответ
        """
        if not response:
            return "Не удалось получить ответ на запрос."
        
        # Извлекаем текст ответа из различных форматов
        if isinstance(response, dict):
            # Если response - словарь, извлекаем текст
            if 'output' in response:
                formatted_response = str(response['output']).strip()
            elif 'content' in response:
                formatted_response = str(response['content']).strip()
            else:
                formatted_response = str(response).strip()
        else:
            # Если response - строка
            formatted_response = str(response).strip()
        
        # Добавляем контекстные подсказки в зависимости от типа запроса
        if query_type == 'comparison':
            if "сравнени" not in formatted_response.lower():
                formatted_response += "\n\n💡 Порада: Для більш детального порівняння ви можете задати уточнюючі питання."
        
        elif query_type == 'temporal':
            if "период" not in formatted_response.lower() and "время" not in formatted_response.lower():
                formatted_response += "\n\n📅 Порада: Можна запросити аналіз за іншими часовими періодами"
        
        elif query_type == 'summary':
            if not any(word in formatted_response.lower() for word in ['итого', 'всего', 'сумма']):
                formatted_response += "\n\n📊 Порада: Можна запросити більш детальну статистику за окремими показниками."
        
        return formatted_response
    
    def get_query_suggestions(self, query: str) -> List[str]:
        """
        Предлагает похожие запросы на основе введенного
        
        Args:
            query: Пользовательский запрос
            
        Returns:
            Список предложений
        """
        suggestions = []
        query_lower = query.lower()
        
        # Базовые предложения на основе ключевых слов
        if any(word in query_lower for word in ['продаж', 'sales']):
            suggestions.extend([
                "Покажи топ-3 региона по продажам",
                "Сравни продажи за последний месяц",
                "Какова динамика продаж?"
            ])
        
        if any(word in query_lower for word in ['регион', 'область']):
            suggestions.extend([
                "Какой регион показывает лучшие результаты?",
                "Сравни все регионы по основным показателям",
                "Покажи статистику по каждому региону"
            ])
        
        if any(word in query_lower for word in ['месяц', 'дата', 'период']):
            suggestions.extend([
                "Покажи тренды за несколько месяцев",
                "Какой месяц был наиболее успешным?",
                "Сравни показатели по месяцам"
            ])
        
        # Если нет специфичных предложений, добавляем общие
        if not suggestions:
            suggestions = [
                "Покажи общую статистику по файлу",
                "Какие данные доступны для анализа?",
                "Сравни основные показатели между регионами"
            ]
        
        return suggestions[:5]  # Возвращаем максимум 5 предложений