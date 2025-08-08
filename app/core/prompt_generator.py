"""
Prompt generation module for creating AI prompts based on data context
"""
from typing import Dict, List, Optional, Any
from .text_normalizer import TextNormalizer


class PromptGenerator:
    """
    Класс для генерации промптов для AI на основе контекста данных
    """
    
    def __init__(self, context: Dict[str, Any], dataframes: Dict[str, Any]):
        """
        Инициализация генератора промптов
        
        Args:
            context: Контекст данных от DataAnalyzer
            dataframes: Словарь с DataFrame'ами
        """
        self.context = context
        self.dataframes = dataframes
    
    def determine_query_type(self, query: str) -> str:
        """
        Определяет тип запроса пользователя
        
        Args:
            query: Пользовательский запрос
            
        Returns:
            Тип запроса: 'comparison', 'temporal', 'summary', 'specific', 'general'
        """
        query_lower = query.lower()
        
        # Ключевые слова для разных типов запросов
        comparison_keywords = ['сравн', 'между', 'против', 'лучше', 'хуже', 'разниц', 'отличие']
        temporal_keywords = ['месяц', 'год', 'дата', 'период', 'динамик', 'тренд', 'изменени']
        summary_keywords = ['общий', 'всего', 'итого', 'суммарн', 'средн', 'максимум', 'минимум']
        specific_keywords = ['конкретн', 'именно', 'точно', 'только', 'исключительно']
        
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
        Определяет релевантные листы на основе запроса
        
        Args:
            query: Пользовательский запрос
            
        Returns:
            Список названий релевантных листов
        """
        relevant_sheets = []
        query_lower = query.lower()
        
        # Проверяем упоминания регионов в запросе
        for sheet_name in self.dataframes.keys():
            normalized_region = TextNormalizer.normalize_region_name(sheet_name)
            
            # Проверяем прямое упоминание
            if normalized_region.lower() in query_lower or sheet_name.lower() in query_lower:
                relevant_sheets.append(sheet_name)
                continue
            
            # Проверяем варианты названий регионов
            for standard_name, variants in TextNormalizer.REGION_MAPPINGS.items():
                if standard_name == normalized_region.lower():
                    if any(variant in query_lower for variant in variants):
                        relevant_sheets.append(sheet_name)
                        break
        
        # Если не найдено конкретных регионов, возвращаем все
        if not relevant_sheets:
            relevant_sheets = list(self.dataframes.keys())
        
        return relevant_sheets
    
    def create_date_filtered_prompt(self, query: str, relevant_sheets: List[str]) -> str:
        """
        Создает промпт с учетом фильтрации по датам
        
        Args:
            query: Пользовательский запрос
            relevant_sheets: Список релевантных листов
            
        Returns:
            Промпт с инструкциями по работе с датами
        """
        month = TextNormalizer.extract_month_from_query(query)
        
        prompt = f"""
        Пользователь задал вопрос: "{query}"
        
        Доступные данные в листах: {', '.join(relevant_sheets)}
        """
        
        if month:
            prompt += f"""
        
        ВАЖНО: В запросе упоминается месяц {month}. 
        При анализе данных обязательно фильтруй по месяцу {month}.
        Используй фильтрацию вида: df[df['дата'].dt.month == {int(month)}]
        или аналогичную для соответствующих столбцов с датами.
        """
        
        prompt += """
        
        Инструкции:
        1. Определи, какие столбцы содержат даты
        2. Если нужна фильтрация по времени, примени соответствующие фильтры
        3. Выполни анализ данных согласно запросу
        4. Предоставь четкий и структурированный ответ
        """
        
        return prompt
    
    def create_data_examples_prompt(self, relevant_sheets: List[str]) -> str:
        """
        Создает промпт с примерами данных из релевантных листов
        
        Args:
            relevant_sheets: Список релевантных листов
            
        Returns:
            Промпт с примерами структуры данных
        """
        prompt = "Структура доступных данных:\n\n"
        
        for sheet_name in relevant_sheets[:3]:  # Ограничиваем до 3 листов для краткости
            if sheet_name in self.dataframes:
                df = self.dataframes[sheet_name]
                
                prompt += f"Лист '{sheet_name}':\n"
                prompt += f"- Размер: {df.shape[0]} строк, {df.shape[1]} столбцов\n"
                prompt += f"- Столбцы: {', '.join(df.columns.tolist()[:5])}\n"
                
                # Добавляем примеры данных
                if not df.empty:
                    sample_data = df.head(2).to_string(index=False, max_cols=5)
                    prompt += f"- Пример данных:\n{sample_data}\n"
                
                prompt += "\n"
        
        return prompt
    
    def create_system_prompt(self, query: str, relevant_sheets: List[str]) -> str:
        """
        Создает системный промпт для AI агента
        
        Args:
            query: Пользовательский запрос
            relevant_sheets: Список релевантных листов
            
        Returns:
            Полный системный промпт
        """
        query_type = self.determine_query_type(query)
        
        base_prompt = """
        Ты - эксперт по анализу данных Excel. Твоя задача - помочь пользователю получить инсайты из данных.
        
        ВАЖНЫЕ ПРАВИЛА:
        1. Всегда используй реальные данные из предоставленных DataFrame
        2. Предоставляй конкретные числа и факты
        3. Если нужно сравнить регионы, используй объединенный DataFrame
        4. Форматируй ответы четко и структурированно
        5. Если данных недостаточно для ответа, честно об этом скажи
        """
        
        # Добавляем специфичные инструкции в зависимости от типа запроса
        if query_type == 'comparison':
            base_prompt += """
        
        ТИП ЗАПРОСА: Сравнение
        - Используй объединенный DataFrame для сравнения между регионами
        - Создавай четкие сравнительные таблицы или графики
        - Выделяй ключевые различия и тренды
        """
        
        elif query_type == 'temporal':
            base_prompt += """
        
        ТИП ЗАПРОСА: Временной анализ
        - Обращай особое внимание на столбцы с датами
        - Группируй данные по временным периодам
        - Показывай динамику изменений
        """
        
        elif query_type == 'summary':
            base_prompt += """
        
        ТИП ЗАПРОСА: Сводная статистика
        - Рассчитывай агрегированные показатели
        - Используй sum(), mean(), max(), min() где уместно
        - Предоставляй общую картину данных
        """
        
        # Добавляем информацию о доступных данных
        base_prompt += f"\n\nДоступные листы данных: {', '.join(relevant_sheets)}\n"
        
        # Добавляем примеры структуры данных
        base_prompt += self.create_data_examples_prompt(relevant_sheets)
        
        # Добавляем фильтрацию по датам если нужно
        date_prompt = self.create_date_filtered_prompt(query, relevant_sheets)
        base_prompt += f"\n{date_prompt}"
        
        return base_prompt
    
    def generate_file_summary(self) -> str:
        """
        Генерирует промпт для создания сводки по файлу
        
        Returns:
            Промпт для генерации сводки
        """
        file_info = self.context.get('file_structure', {})
        summary = file_info.get('summary', {})
        
        prompt = f"""
        Создай краткую сводку по загруженному Excel файлу на основе следующей информации:
        
        Общая информация:
        - Количество листов: {summary.get('total_sheets', 0)}
        - Общее количество строк: {summary.get('total_rows', 0)}
        - Общее количество столбцов: {summary.get('total_columns', 0)}
        
        Типы данных:
        - Столбцы с датами: {len(summary.get('date_columns', []))}
        - Числовые столбцы: {len(summary.get('numeric_columns', []))}
        - Текстовые столбцы: {len(summary.get('text_columns', []))}
        
        Доступные операции:
        {chr(10).join(f"- {op}" for op in self.context.get('common_operations', []))}
        
        Подсказки для запросов:
        {chr(10).join(f"- {hint}" for hint in self.context.get('query_hints', []))}
        
        Создай понятную и информативную сводку, которая поможет пользователю понять,
        какие данные доступны и какие вопросы можно задавать.
        """
        
        return prompt
    
    def create_region_specific_prompt(self, region: str, query: str) -> str:
        """
        Создает промпт для запроса по конкретному региону
        
        Args:
            region: Название региона
            query: Пользовательский запрос
            
        Returns:
            Промпт для анализа данных конкретного региона
        """
        normalized_region = TextNormalizer.normalize_region_name(region)
        
        prompt = f"""
        Пользователь задал вопрос о регионе "{region}" (нормализовано как "{normalized_region}"):
        "{query}"
        
        ВАЖНО: Анализируй ТОЛЬКО данные по региону "{region}".
        Используй соответствующий DataFrame для этого региона.
        
        Инструкции:
        1. Сосредоточься только на данных указанного региона
        2. Предоставь детальный анализ по этому региону
        3. Если нужно сравнение, сравни с общими показателями
        4. Упомяни специфику именно этого региона
        
        Структура данных региона "{region}":
        """
        
        if region in self.dataframes:
            df = self.dataframes[region]
            prompt += f"""
            - Размер данных: {df.shape[0]} строк, {df.shape[1]} столбцов
            - Доступные столбцы: {', '.join(df.columns.tolist())}
            """
            
            # Добавляем пример данных
            if not df.empty:
                sample = df.head(3).to_string(index=False)
                prompt += f"\n- Пример данных:\n{sample}"
        
        return prompt