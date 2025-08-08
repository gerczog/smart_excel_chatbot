"""
Модуль генерації промптів для створення AI промптів на основі контексту даних
"""
from typing import Dict, List, Optional, Any
from .text_normalizer import TextNormalizer


class PromptGenerator:
    """
    Клас для генерації промптів для AI на основі контексту даних
    """
    
    def __init__(self, context: Dict[str, Any], dataframes: Dict[str, Any]):
        """
        Ініціалізація генератора промптів
        
        Args:
            context: Контекст даних від DataAnalyzer
            dataframes: Словник з DataFrame'ами
        """
        self.context = context
        self.dataframes = dataframes
    
    def determine_query_type(self, query: str) -> str:
        """
        Визначає тип запиту користувача
        
        Args:
            query: Користувацький запит
            
        Returns:
            Тип запиту: 'comparison', 'temporal', 'summary', 'specific', 'general'
        """
        query_lower = query.lower()
        
        # Ключові слова для різних типів запитів
        comparison_keywords = ['порівн', 'між', 'проти', 'краще', 'гірше', 'різниц', 'відмінніст', 'сравн', 'между', 'против', 'лучше', 'хуже', 'разниц', 'отличие']
        temporal_keywords = ['місяць', 'рік', 'дата', 'період', 'динамік', 'тренд', 'зміни', 'месяц', 'год', 'дата', 'период', 'динамик', 'тренд', 'изменени']
        summary_keywords = ['загальн', 'всього', 'підсумок', 'сумарн', 'середн', 'максимум', 'мінімум', 'общий', 'всего', 'итого', 'суммарн', 'средн', 'максимум', 'минимум']
        specific_keywords = ['конкретн', 'саме', 'точно', 'тільки', 'виключно', 'именно', 'только', 'исключительно']
        
        if any(keyword in query_lower for keyword in comparison_keywords):
            return 'comparison'
        elif any(keyword in query_lower for keyword in temporal_keywords):
            return 'temporal'
        elif any(keyword in query_lower for keyword in summary_keywords):
            return 'summary'
        elif any(keyword in query_lower for keyword in specific_keywords):
            return 'specific'
        else:
            return 'general'
    
    def find_relevant_sheets(self, query: str) -> List[str]:
        """
        Визначає релевантні аркуші на основі запиту
        
        Args:
            query: Користувацький запит
            
        Returns:
            Список назв релевантних аркушів
        """
        relevant_sheets = []
        query_lower = query.lower()
        
        # Перевіряємо згадки регіонів у запиті
        for sheet_name in self.dataframes.keys():
            normalized_region = TextNormalizer.normalize_region_name(sheet_name)
            
            # Перевіряємо пряме згадування
            if normalized_region.lower() in query_lower or sheet_name.lower() in query_lower:
                relevant_sheets.append(sheet_name)
                continue
            
            # Перевіряємо варіанти назв регіонів
            for standard_name, variants in TextNormalizer.REGION_MAPPINGS.items():
                if standard_name == normalized_region.lower():
                    if any(variant in query_lower for variant in variants):
                        relevant_sheets.append(sheet_name)
                        break
        
        # Якщо не знайдено конкретних регіонів, повертаємо всі
        if not relevant_sheets:
            relevant_sheets = list(self.dataframes.keys())
        
        return relevant_sheets
    
    def create_date_filtered_prompt(self, query: str, relevant_sheets: List[str]) -> str:
        """
        Створює промпт з врахуванням фільтрації за датами
        
        Args:
            query: Користувацький запит
            relevant_sheets: Список релевантних аркушів
            
        Returns:
            Промпт з інструкціями по роботі з датами
        """
        month = TextNormalizer.extract_month_from_query(query)
        
        prompt = f"""
        Користувач поставив питання: "{query}"
        
        Доступні дані в аркушах: {', '.join(relevant_sheets)}
        """
        
        if month:
            prompt += f"""
        
        ВАЖЛИВО: У запиті згадується місяць {month}. 
        При аналізі даних обов'язково фільтруй за місяцем {month}.
        Використовуй фільтрацію типу: df[df['дата'].dt.month == {int(month)}]
        або аналогічну для відповідних стовпців з датами.
        """
        
        prompt += """
        
        Інструкції:
        1. Визнач, які стовпці містять дати
        2. Якщо потрібна фільтрація за часом, застосуй відповідні фільтри
        3. Виконай аналіз даних згідно запиту
        4. Надай чіткий та структурований відповідь
        """
        
        return prompt
    
    def create_data_examples_prompt(self, relevant_sheets: List[str]) -> str:
        """
        Створює промпт з прикладами даних з релевантних аркушів
        
        Args:
            relevant_sheets: Список релевантних аркушів
            
        Returns:
            Промпт з прикладами структури даних
        """
        prompt = "Структура доступних даних:\n\n"
        
        for sheet_name in relevant_sheets[:3]:  # Обмежуємо до 3 аркушів для стислості
            if sheet_name in self.dataframes:
                df = self.dataframes[sheet_name]
                
                prompt += f"Аркуш '{sheet_name}':\n"
                prompt += f"- Розмір: {df.shape[0]} рядків, {df.shape[1]} стовпців\n"
                prompt += f"- Стовпці: {', '.join(df.columns.tolist()[:5])}\n"
                
                # Додаємо приклади даних
                if not df.empty:
                    sample_data = df.head(2).to_string(index=False, max_cols=5)
                    prompt += f"- Приклад даних:\n{sample_data}\n"
                
                prompt += "\n"
        
        return prompt
    
    def create_system_prompt(self, query: str, relevant_sheets: List[str]) -> str:
        """
        Створює системний промпт для AI агента
        
        Args:
            query: Користувацький запит
            relevant_sheets: Список релевантних аркушів
            
        Returns:
            Повний системний промпт
        """
        query_type = self.determine_query_type(query)
        
        base_prompt = """
        Ти - експерт з аналізу даних Excel. Твоя задача - допомогти користувачеві отримати інсайти з даних.
        
        ВАЖЛИВІ ПРАВИЛА:
        1. Завжди використовуй реальні дані з наданих DataFrame
        2. Надавай конкретні числа та факти
        3. Якщо потрібно порівняти регіони, використовуй об'єднаний DataFrame
        4. Форматуй відповіді чітко та структуровано
        5. Якщо даних недостатньо для відповіді, чесно про це скажи
        """
        
        # Додаємо специфічні інструкції залежно від типу запиту
        if query_type == 'comparison':
            base_prompt += """
        
        ТИП ЗАПИТУ: Порівняння
        - Використовуй об'єднаний DataFrame для порівняння між регіонами
        - Створюй чіткі порівняльні таблиці або графіки
        - Виділяй ключові відмінності та тренди
        """
        
        elif query_type == 'temporal':
            base_prompt += """
        
        ТИП ЗАПИТУ: Часовий аналіз
        - Звертай особливу увагу на стовпці з датами
        - Групуй дані за часовими періодами
        - Показуй динаміку змін
        """
        
        elif query_type == 'summary':
            base_prompt += """
        
        ТИП ЗАПИТУ: Зведена статистика
        - Розраховуй агреговані показники
        - Використовуй sum(), mean(), max(), min() де доречно
        - Надавай загальну картину даних
        """
        
        # Додаємо інформацію про доступні дані
        base_prompt += f"\n\nДоступні аркуші даних: {', '.join(relevant_sheets)}\n"
        
        # Додаємо приклади структури даних
        base_prompt += self.create_data_examples_prompt(relevant_sheets)
        
        # Додаємо фільтрацію за датами якщо потрібно
        date_prompt = self.create_date_filtered_prompt(query, relevant_sheets)
        base_prompt += f"\n{date_prompt}"
        
        return base_prompt
    
    def generate_file_summary(self) -> str:
        """
        Генерує промпт для створення зведення по файлу
        
        Returns:
            Промпт для генерації зведення
        """
        file_info = self.context.get('file_structure', {})
        summary = file_info.get('summary', {})
        
        prompt = f"""
        Створи коротке зведення по завантаженому Excel файлу на основі наступної інформації:
        
        Загальна інформація:
        - Кількість аркушів: {summary.get('total_sheets', 0)}
        - Загальна кількість рядків: {summary.get('total_rows', 0)}
        - Загальна кількість стовпців: {summary.get('total_columns', 0)}
        
        Типи даних:
        - Стовпці з датами: {len(summary.get('date_columns', []))}
        - Числові стовпці: {len(summary.get('numeric_columns', []))}
        - Текстові стовпці: {len(summary.get('text_columns', []))}
        
        Доступні операції:
        {chr(10).join(f"- {op}" for op in self.context.get('common_operations', []))}
        
        Підказки для запитів:
        {chr(10).join(f"- {hint}" for hint in self.context.get('query_hints', []))}
        
        Створи зрозуміле та інформативне зведення, яке допоможе користувачеві зрозуміти,
        які дані доступні та які питання можна ставити.
        """
        
        return prompt
    
    def create_region_specific_prompt(self, region: str, query: str) -> str:
        """
        Створює промпт для запиту по конкретному регіону
        
        Args:
            region: Назва регіону
            query: Користувацький запит
            
        Returns:
            Промпт для аналізу даних конкретного регіону
        """
        normalized_region = TextNormalizer.normalize_region_name(region)
        
        prompt = f"""
        Користувач поставив питання про регіон "{region}" (нормалізовано як "{normalized_region}"):
        "{query}"
        
        ВАЖЛИВО: Аналізуй ТІЛЬКИ дані по регіону "{region}".
        Використовуй відповідний DataFrame для цього регіону.
        
        Інструкції:
        1. Зосередься тільки на даних вказаного регіону
        2. Надай детальний аналіз по цьому регіону
        3. Якщо потрібне порівняння, порівняй із загальними показниками
        4. Згадай специфіку саме цього регіону
        
        Структура даних регіону "{region}":
        """
        
        if region in self.dataframes:
            df = self.dataframes[region]
            prompt += f"""
            - Розмір даних: {df.shape[0]} рядків, {df.shape[1]} стовпців
            - Доступні стовпці: {', '.join(df.columns.tolist())}
            """
            
            # Додаємо приклад даних
            if not df.empty:
                sample = df.head(3).to_string(index=False)
                prompt += f"\n- Приклад даних:\n{sample}"
        
        return prompt