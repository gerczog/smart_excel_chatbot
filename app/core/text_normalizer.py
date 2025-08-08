"""
Модуль нормалізації тексту для обробки користувацьких запитів та даних
"""
import re
from typing import Optional


class TextNormalizer:
    """
    Клас для нормалізації текстових даних та користувацьких запитів
    """
    
    # Словник для нормалізації назв регіонів
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

    # Словник місяців
    MONTHS_MAP = {
        # Українські назви місяців
        'січня': '01', 'січень': '01', 
        'лютого': '02', 'лютий': '02',
        'березня': '03', 'березень': '03',
        'квітня': '04', 'квітень': '04',
        'травня': '05', 'травень': '05',
        'червня': '06', 'червень': '06',
        'липня': '07', 'липень': '07',
        'серпня': '08', 'серпень': '08',
        'вересня': '09', 'вересень': '09',
        'жовтня': '10', 'жовтень': '10',
        'листопада': '11', 'листопад': '11',
        'грудня': '12', 'грудень': '12',
        # Російські назви місяців (для сумісності)
        'января': '01', 'январь': '01', 
        'февраля': '02', 'февраль': '02', 
        'марта': '03', 'март': '03', 
        'апреля': '04', 'апрель': '04', 
        'мая': '05', 'май': '05', 
        'июня': '06', 'июнь': '06', 
        'июля': '07', 'июль': '07', 
        'августа': '08', 'август': '08', 
        'сентября': '09', 'сентябрь': '09', 
        'октября': '10', 'октябрь': '10', 
        'ноября': '11', 'ноябрь': '11', 
        'декабря': '12', 'декабрь': '12',
        # Англійські назви місяців
        'jan': '01', 'january': '01',
        'feb': '02', 'february': '02',
        'mar': '03', 'march': '03',
        'apr': '04', 'april': '04',
        'may': '05',
        'jun': '06', 'june': '06',
        'jul': '07', 'july': '07',
        'aug': '08', 'august': '08',
        'sep': '09', 'september': '09',
        'oct': '10', 'october': '10',
        'nov': '11', 'november': '11',
        'dec': '12', 'december': '12'
    }

    @classmethod
    def normalize_region_name(cls, region_name: str) -> str:
        """
        Нормалізує назву регіону
        
        Args:
            region_name: Вихідна назва регіону
            
        Returns:
            Нормалізована назва регіону
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