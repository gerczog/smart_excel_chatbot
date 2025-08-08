"""
Text normalization module for processing user queries and data
"""
import re
from typing import Optional


class TextNormalizer:
    """
    Класс для нормализации текстовых данных и пользовательских запросов
    """
    
    # Словарь для нормализации названий регионов
    REGION_MAPPINGS = {
        'київ': ['киев', 'київ', 'kyiv', 'kiev', 'столица', 'столиця'],
        'харків': ['харьков', 'харків', 'kharkiv', 'kharkov'],
        'одеса': ['одесса', 'одеса', 'odessa', 'odesa'],
        'дніпро': ['днепр', 'дніпро', 'днипро', 'dnipro', 'dnipropetrovsk'],
        'львів': ['львов', 'львів', 'lviv', 'lvov'],
        'запоріжжя': ['запорожье', 'запоріжжя', 'zaporizhzhia'],
        'кривий ріг': ['кривой рог', 'кривий ріг', 'kryvyi rih'],
        'миколаїв': ['николаев', 'миколаїв', 'mykolaiv'],
        'маріуполь': ['мариуполь', 'маріуполь', 'mariupol'],
        'луганськ': ['луганск', 'луганськ', 'luhansk'],
        'вінниця': ['винница', 'вінниця', 'vinnytsia'],
        'макіївка': ['макеевка', 'макіївка', 'makiivka'],
        'чернігів': ['чернигов', 'чернігів', 'chernihiv'],
        'полтава': ['полтава', 'poltava'],
        'житомир': ['житомир', 'zhytomyr'],
        'суми': ['сумы', 'суми', 'sumy'],
        'хмельницький': ['хмельницкий', 'хмельницький', 'khmelnytskyi'],
        'черкаси': ['черкассы', 'черкаси', 'cherkasy'],
        'чернівці': ['черновцы', 'чернівці', 'chernivtsi'],
        'івано-франківськ': ['ивано-франковск', 'івано-франківськ', 'ivano-frankivsk'],
        'тернопіль': ['тернополь', 'тернопіль', 'ternopil'],
        'луцьк': ['луцк', 'луцьк', 'lutsk'],
        'ужгород': ['ужгород', 'uzhhorod'],
        'рівне': ['ровно', 'рівне', 'rivne'],
        'кропивницький': ['кировоград', 'кропивницький', 'kropyvnytskyi']
    }

    # Словарь месяцев
    MONTHS_MAP = {
        'января': '01', 'январь': '01', 'jan': '01', 'january': '01',
        'февраля': '02', 'февраль': '02', 'feb': '02', 'february': '02',
        'марта': '03', 'март': '03', 'mar': '03', 'march': '03',
        'апреля': '04', 'апрель': '04', 'apr': '04', 'april': '04',
        'мая': '05', 'май': '05', 'may': '05',
        'июня': '06', 'июнь': '06', 'jun': '06', 'june': '06',
        'июля': '07', 'июль': '07', 'jul': '07', 'july': '07',
        'августа': '08', 'август': '08', 'aug': '08', 'august': '08',
        'сентября': '09', 'сентябрь': '09', 'sep': '09', 'september': '09',
        'октября': '10', 'октябрь': '10', 'oct': '10', 'october': '10',
        'ноября': '11', 'ноябрь': '11', 'nov': '11', 'november': '11',
        'декабря': '12', 'декабрь': '12', 'dec': '12', 'december': '12'
    }

    @classmethod
    def normalize_region_name(cls, region_name: str) -> str:
        """
        Нормализует название региона
        
        Args:
            region_name: Исходное название региона
            
        Returns:
            Нормализованное название региона
        """
        if not region_name:
            return region_name

        # Приводим к нижнему регистру и убираем лишние пробелы
        normalized = region_name.lower().strip()
        
        # Убираем префиксы вроде "область", "регион", "край"
        prefixes_to_remove = [
            'область', 'обл', 'регион', 'рег', 'край', 
            'область.', 'обл.', 'регион.', 'рег.', 'край.'
        ]
        for prefix in prefixes_to_remove:
            if normalized.endswith(f' {prefix}'):
                normalized = normalized[:-len(prefix)-1].strip()
            elif normalized.startswith(f'{prefix} '):
                normalized = normalized[len(prefix)+1:].strip()

        # Проверяем соответствие в словаре
        for standard_name, variants in cls.REGION_MAPPINGS.items():
            if normalized in variants or normalized == standard_name:
                return standard_name

        # Если точного соответствия нет, ищем частичные совпадения
        for standard_name, variants in cls.REGION_MAPPINGS.items():
            for variant in variants:
                if variant in normalized or normalized in variant:
                    return standard_name

        return normalized

    @classmethod
    def normalize_date_reference(cls, text: str) -> str:
        """
        Нормализует ссылки на даты в тексте
        
        Args:
            text: Исходный текст
            
        Returns:
            Текст с нормализованными датами
        """
        if not text:
            return text

        # Паттерн для поиска дат в формате "число месяц" или "месяц число"
        date_pattern = r'(\d{1,2})\s+(' + '|'.join(cls.MONTHS_MAP.keys()) + r')|(' + '|'.join(cls.MONTHS_MAP.keys()) + r')\s+(\d{1,2})'
        
        def replace_date(match):
            day, month1, month2, day2 = match.groups()
            if day and month1:
                month_num = cls.MONTHS_MAP.get(month1.lower(), month1)
                return f"{day:>02s}-{month_num}"
            elif month2 and day2:
                month_num = cls.MONTHS_MAP.get(month2.lower(), month2)
                return f"{day2:>02s}-{month_num}"
            return match.group()

        # Заменяем найденные даты
        normalized_text = re.sub(date_pattern, replace_date, text, flags=re.IGNORECASE)
        
        return normalized_text

    @classmethod
    def normalize_date_reference_fallback(cls, text: str) -> str:
        """
        Альтернативная нормализация дат с более широким поиском
        
        Args:
            text: Исходный текст
            
        Returns:
            Текст с нормализованными датами
        """
        if not text:
            return text

        def replace_month(match):
            month_name = match.group(1).lower()
            return cls.MONTHS_MAP.get(month_name, match.group(0))

        # Заменяем названия месяцев на номера
        month_pattern = r'\b(' + '|'.join(cls.MONTHS_MAP.keys()) + r')\b'
        return re.sub(month_pattern, replace_month, text, flags=re.IGNORECASE)

    @classmethod
    def extract_month_from_query(cls, query: str) -> Optional[str]:
        """
        Извлекает номер месяца из запроса
        
        Args:
            query: Пользовательский запрос
            
        Returns:
            Номер месяца в формате '01'-'12' или None
        """
        query_lower = query.lower()
        
        for month_name, month_num in cls.MONTHS_MAP.items():
            if month_name in query_lower:
                return month_num
                
        # Поиск цифрового представления месяца
        month_pattern = r'\b(0?[1-9]|1[0-2])\b'
        match = re.search(month_pattern, query)
        if match:
            month = int(match.group(1))
            return f"{month:02d}"
            
        return None