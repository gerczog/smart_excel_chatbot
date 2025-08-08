"""
Data analysis module for Excel data structure and semantic analysis
"""
import pandas as pd
import re
from typing import Dict, List, Optional, Any, Tuple
from functools import lru_cache


class DataAnalyzer:
    """
    Класс для анализа структуры данных и создания контекста
    """
    
    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        """
        Инициализация анализатора
        
        Args:
            dataframes: Словарь с DataFrame'ами для каждого листа
        """
        self.dataframes = dataframes
        self.context = {}
    
    def analyze_column_semantics(self, column_name: str, sample_data: list, data_type: str) -> Dict[str, Any]:
        """
        Анализирует семантику столбца
        
        Args:
            column_name: Название столбца
            sample_data: Примеры данных из столбца
            data_type: Тип данных
            
        Returns:
            Словарь с результатами анализа
        """
        semantic_info = {
            'type': data_type,
            'semantic_category': 'unknown',
            'patterns': [],
            'description': ''
        }
        
        column_lower = column_name.lower()
        
        # Анализ названий столбцов на украинском/русском
        if any(word in column_lower for word in ['дата', 'date', 'день', 'день', 'месяц', 'год']):
            semantic_info['semantic_category'] = 'temporal'
            semantic_info['description'] = 'Содержит временные данные (даты, дни, месяцы)'
        
        elif any(word in column_lower for word in ['продаж', 'sales', 'выручка', 'доход', 'прибыль', 'revenue']):
            semantic_info['semantic_category'] = 'financial'
            semantic_info['description'] = 'Содержит финансовые данные (продажи, выручка)'
        
        elif any(word in column_lower for word in ['количество', 'кількість', 'count', 'qty', 'штук']):
            semantic_info['semantic_category'] = 'quantity'
            semantic_info['description'] = 'Содержит количественные данные'
        
        elif any(word in column_lower for word in ['регион', 'регіон', 'region', 'область', 'город', 'місто']):
            semantic_info['semantic_category'] = 'geographic'
            semantic_info['description'] = 'Содержит географические данные (регионы, города)'
        
        elif any(word in column_lower for word in ['товар', 'продукт', 'product', 'item', 'название', 'назва']):
            semantic_info['semantic_category'] = 'product'
            semantic_info['description'] = 'Содержит информацию о товарах/продуктах'
        
        elif any(word in column_lower for word in ['клиент', 'client', 'customer', 'покупатель']):
            semantic_info['semantic_category'] = 'customer'
            semantic_info['description'] = 'Содержит информацию о клиентах'
        
        # Анализ паттернов в данных
        if sample_data and len(sample_data) > 0:
            # Проверяем на даты
            date_patterns = [
                r'\d{2,4}[-/.]\d{1,2}[-/.]\d{1,4}',
                r'\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}',
            ]
            
            for pattern in date_patterns:
                if any(re.match(pattern, str(item)) for item in sample_data[:5] if pd.notna(item)):
                    semantic_info['patterns'].append('date_format')
                    if semantic_info['semantic_category'] == 'unknown':
                        semantic_info['semantic_category'] = 'temporal'
                    break
            
            # Проверяем на числовые данные с валютой
            currency_pattern = r'[\d\s,.]+(грн|UAH|₴|руб|RUB|₽|\$|USD|€|EUR)'
            if any(re.search(currency_pattern, str(item)) for item in sample_data[:5] if pd.notna(item)):
                semantic_info['patterns'].append('currency')
                semantic_info['semantic_category'] = 'financial'
        
        return semantic_info
    
    def analyze_structure(self) -> Dict[str, Any]:
        """
        Анализирует структуру всех данных
        
        Returns:
            Подробный анализ структуры данных
        """
        analysis = {
            'sheets': {},
            'summary': {
                'total_sheets': len(self.dataframes),
                'total_rows': 0,
                'total_columns': 0,
                'common_columns': [],
                'data_types_distribution': {},
                'date_columns': [],
                'numeric_columns': [],
                'text_columns': []
            }
        }
        
        all_columns = []
        
        for sheet_name, df in self.dataframes.items():
            if df.empty:
                continue
            
            # Базовая информация о листе
            sheet_info = {
                'shape': df.shape,
                'columns': list(df.columns),
                'dtypes': df.dtypes.to_dict(),
                'null_counts': df.isnull().sum().to_dict(),
                'sample_data': {},
                'column_analysis': {}
            }
            
            # Анализ каждого столбца
            for col in df.columns:
                # Получаем примеры данных (не null)
                sample_values = df[col].dropna().head(10).tolist()
                sheet_info['sample_data'][col] = sample_values
                
                # Анализируем семантику столбца
                data_type = str(df[col].dtype)
                semantic_analysis = self.analyze_column_semantics(col, sample_values, data_type)
                sheet_info['column_analysis'][col] = semantic_analysis
                
                # Собираем статистику для общего анализа
                if 'datetime' in data_type or semantic_analysis['semantic_category'] == 'temporal':
                    analysis['summary']['date_columns'].append(f"{sheet_name}.{col}")
                elif 'float' in data_type or 'int' in data_type:
                    analysis['summary']['numeric_columns'].append(f"{sheet_name}.{col}")
                else:
                    analysis['summary']['text_columns'].append(f"{sheet_name}.{col}")
            
            analysis['sheets'][sheet_name] = sheet_info
            analysis['summary']['total_rows'] += df.shape[0]
            analysis['summary']['total_columns'] += df.shape[1]
            all_columns.extend(df.columns)
        
        # Находим общие столбцы
        if len(self.dataframes) > 1:
            column_counts = {}
            for col in all_columns:
                column_counts[col] = column_counts.get(col, 0) + 1
            
            common_cols = [col for col, count in column_counts.items() 
                          if count >= len(self.dataframes) * 0.5]  # В хотя бы 50% листов
            analysis['summary']['common_columns'] = common_cols
        
        # Распределение типов данных
        type_counts = {}
        for sheet_info in analysis['sheets'].values():
            for dtype in sheet_info['dtypes'].values():
                dtype_str = str(dtype)
                type_counts[dtype_str] = type_counts.get(dtype_str, 0) + 1
        
        analysis['summary']['data_types_distribution'] = type_counts
        
        return analysis
    
    def create_enhanced_context(self) -> Dict[str, Any]:
        """
        Создает расширенный контекст на основе анализа структуры
        
        Returns:
            Расширенный контекст для работы с данными
        """
        structure_analysis = self.analyze_structure()
        
        context = {
            'file_structure': structure_analysis,
            'semantic_mapping': {},
            'query_hints': [],
            'common_operations': [],
            'data_relationships': []
        }
        
        # Создаем семантическую карту
        for sheet_name, sheet_info in structure_analysis['sheets'].items():
            context['semantic_mapping'][sheet_name] = {}
            
            for col, analysis in sheet_info['column_analysis'].items():
                context['semantic_mapping'][sheet_name][col] = {
                    'category': analysis['semantic_category'],
                    'description': analysis['description'],
                    'queryable': analysis['semantic_category'] != 'unknown'
                }
        
        # Генерируем подсказки для запросов
        date_cols = structure_analysis['summary']['date_columns']
        if date_cols:
            context['query_hints'].append(
                f"Доступны данные по датам в столбцах: {', '.join(date_cols[:3])}"
            )
        
        numeric_cols = structure_analysis['summary']['numeric_columns']
        if numeric_cols:
            context['query_hints'].append(
                f"Можно анализировать числовые показатели: {', '.join(numeric_cols[:3])}"
            )
        
        # Определяем возможные операции
        if len(self.dataframes) > 1:
            context['common_operations'].append("Сравнение данных между регионами")
            context['common_operations'].append("Агрегация данных по всем регионам")
        
        if date_cols:
            context['common_operations'].append("Анализ временных трендов")
            context['common_operations'].append("Группировка по периодам")
        
        if numeric_cols:
            context['common_operations'].append("Расчет сумм, средних, максимумов")
            context['common_operations'].append("Ранжирование по показателям")
        
        # Анализируем связи между данными
        common_cols = structure_analysis['summary']['common_columns']
        if common_cols:
            context['data_relationships'].append(
                f"Общие поля для связи данных: {', '.join(common_cols)}"
            )
        
        self.context = context
        return context
    
    def get_file_info(self) -> Dict[str, Any]:
        """Возвращает общую информацию о файле"""
        total_rows = sum(df.shape[0] for df in self.dataframes.values())
        total_cols = sum(df.shape[1] for df in self.dataframes.values())
        
        return {
            'total_sheets': len(self.dataframes),
            'sheet_names': list(self.dataframes.keys()),
            'total_rows': total_rows,
            'total_columns': total_cols,
            'file_size_estimate': f"{total_rows * total_cols * 8} bytes"  # Примерная оценка
        }
    
    def get_sheet_summary(self, sheet_name: str = None) -> Dict[str, Any]:
        """
        Возвращает сводку по конкретному листу или всем листам
        
        Args:
            sheet_name: Название листа (если None, то по всем листам)
            
        Returns:
            Сводная информация
        """
        if sheet_name and sheet_name in self.dataframes:
            df = self.dataframes[sheet_name]
            return {
                'sheet_name': sheet_name,
                'shape': df.shape,
                'columns': list(df.columns),
                'data_types': df.dtypes.to_dict(),
                'memory_usage': df.memory_usage(deep=True).sum(),
                'sample_data': df.head(3).to_dict('records') if not df.empty else []
            }
        else:
            # Сводка по всем листам
            summaries = {}
            for name, df in self.dataframes.items():
                summaries[name] = {
                    'rows': df.shape[0],
                    'columns': df.shape[1],
                    'column_names': list(df.columns)
                }
            return summaries
    
    def get_date_summary(self) -> Dict[str, List[str]]:
        """Возвращает сводку по датам в данных"""
        date_summary = {}
        
        for sheet_name, df in self.dataframes.items():
            dates = []
            
            # Ищем столбцы с датами
            date_columns = df.select_dtypes(include=['datetime64']).columns
            for col in date_columns:
                unique_dates = df[col].dropna().dt.strftime('%Y-%m-%d').unique()
                dates.extend(unique_dates)
            
            if dates:
                date_summary[sheet_name] = sorted(list(set(dates)))
        
        return date_summary