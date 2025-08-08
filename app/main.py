from flask import Flask, render_template, request, jsonify
import os
import uuid
from werkzeug.utils import secure_filename
from app.core.smart_excel_chatbot import create_smart_excel_chatbot
import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# set logging level to DEBUG
logger.setLevel(logging.DEBUG)

app = Flask(__name__, 
           template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'templates')))

# Создаем директорию для загруженных файлов, если она не существует
UPLOAD_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'uploads'))
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Глобальная переменная для чат-бота
chatbot = None

@app.route('/')
def index():
    """Главная страница"""
    # Передаем флаги доступности функций в шаблон
    allow_change_file = os.environ.get('ALLOW_CHANGE_FILE', 'False').lower() == 'true'
    allow_new_chat = os.environ.get('ALLOW_NEW_CHAT', 'False').lower() == 'true'
    
    # Получаем путь к файлу по умолчанию
    default_file = os.environ.get('EXCEL_FILE_PATH')
    
    return render_template('index.html', 
                          allow_change_file=allow_change_file,
                          allow_new_chat=allow_new_chat,
                          default_file=default_file)

@app.route('/init', methods=['POST'])
def init_chatbot():
    """Инициализация чат-бота"""
    global chatbot
    
    try:
        data = request.get_json()
        # Используем переменную окружения EXCEL_FILE_PATH, если она установлена
        default_file = os.environ.get('EXCEL_FILE_PATH', 'data/детальні_продажі_по_регіонах.xlsx')
        file_path = data.get('file_path', default_file)
        logger.debug(f"Инициализация чат-бота с файлом: {file_path}")
        
        # Создаем чат-бота
        chatbot = create_smart_excel_chatbot(file_path)
        
        # Получаем информацию о файле
        info = chatbot.get_file_info()
        
        # Генерируем краткое описание файла для пользователя
        file_summary = chatbot.generate_file_summary()
        
        return jsonify({
            'success': True,
            'message': 'Чат-бот успешно инициализирован',
            'file_info': info,
            'file_summary': file_summary
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка инициализации: {str(e)}'
        })

@app.route('/reinit', methods=['POST'])
def reinit_chatbot():
    """Реинициализация чат-бота с тем же файлом (начать новый чат)"""
    global chatbot
    
    # Проверяем, разрешена ли функция нового чата
    if os.environ.get('ALLOW_NEW_CHAT', 'False').lower() != 'true':
        return jsonify({
            'success': False,
            'message': 'Функция нового чата отключена. Запустите приложение с флагом --new-chat или -n'
        })
    
    try:
        if chatbot is None:
            return jsonify({
                'success': False,
                'message': 'Чат-бот не был инициализирован'
            })
            
        # Получаем текущий путь к файлу
        current_file_path = chatbot.file_path
        
        # Создаем чат-бота заново с тем же файлом
        chatbot = create_smart_excel_chatbot(current_file_path)
        
        # Получаем информацию о файле
        info = chatbot.get_file_info()
        
        # Генерируем краткое описание файла для пользователя
        file_summary = chatbot.generate_file_summary()
        
        return jsonify({
            'success': True,
            'message': 'Чат-бот успешно реинициализирован',
            'file_info': info,
            'file_summary': file_summary
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка реинициализации: {str(e)}'
        })

@app.route('/chat', methods=['POST'])
def chat():
    """Обработка чат-запросов"""
    global chatbot
    
    if chatbot is None:
        return jsonify({
            'success': False,
            'message': 'Чат-бот не инициализирован. Сначала загрузите файл.'
        })
    try:
        data = request.get_json()
        query = data.get('query', '')
        
        if not query:
            return jsonify({
                'success': False,
                'message': 'Запрос не может быть пустым'
            })
        
        # Получаем ответ от чат-бота
        response = chatbot.chat(query)
        
        return jsonify({
            'success': True,
            'response': response,
            'query': query
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка обработки запроса: {str(e)}'
        })

@app.route('/info')
def get_info():
    """Получение информации о файле"""
    global chatbot
    
    if chatbot is None:
        return jsonify({
            'success': False,
            'message': 'Чат-бот не инициализирован'
        })
    
    try:
        info = chatbot.get_file_info()
        return jsonify({
            'success': True,
            'info': info
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка получения информации: {str(e)}'
        })

@app.route('/upload', methods=['POST'])
def upload_file():
    """Загрузка нового Excel файла"""
    # Проверяем, разрешена ли функция изменения файла
    if os.environ.get('ALLOW_CHANGE_FILE', 'False').lower() != 'true':
        return jsonify({
            'success': False,
            'message': 'Функция изменения файла отключена. Запустите приложение с флагом --change-file или -c'
        })
    
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'message': 'Файл не знайдено в запиті'
        })
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({
            'success': False,
            'message': 'Файл не вибрано'
        })
    
    if not (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
        return jsonify({
            'success': False,
            'message': 'Підтримуються тільки файли Excel (.xlsx або .xls)'
        })
    
    try:
        # Создаем уникальное имя файла
        filename = secure_filename(file.filename)
        unique_filename = f"{uuid.uuid4()}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        
        # Сохраняем файл
        file.save(file_path)
        
        return jsonify({
            'success': True,
            'message': 'Файл успішно завантажено',
            'file_path': file_path
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Помилка завантаження файлу: {str(e)}'
        })

@app.route('/examples')
def get_examples():
    """Получение примеров запросов"""
    examples = [
        "Яка середня ціна за вересень у Києві?",
    "Порівняй ціни за вересень у всіх регіонах",
    "Які товари є найпопулярнішими?",
    "Покажи статистику продажів у Львові",
    "Який регіон має найвищі ціни?",
    "Скільки товарів продається в Одесі?",
    "Порівняй обсяги продажів між Києвом і Харковом",
    "У яких місяцях фіксуються найвищі продажі?"
    ]
    
    return jsonify({
        'success': True,
        'examples': examples
    })

@app.route('/toggle-change-file', methods=['POST'])
def toggle_change_file():
    """Включение/отключение функции изменения файла"""
    current_state = os.environ.get('ALLOW_CHANGE_FILE', 'False').lower() == 'true'
    new_state = not current_state
    os.environ['ALLOW_CHANGE_FILE'] = str(new_state).lower()
    
    return jsonify({
        'success': True,
        'enabled': new_state,
        'message': 'Функція зміни файлу включена' if new_state else 'Функція зміни файлу відключена'
    })

@app.route('/toggle-new-chat', methods=['POST'])
def toggle_new_chat():
    """Включение/отключение функции нового чата"""
    current_state = os.environ.get('ALLOW_NEW_CHAT', 'False').lower() == 'true'
    new_state = not current_state
    os.environ['ALLOW_NEW_CHAT'] = str(new_state).lower()
    
    return jsonify({
        'success': True,
        'enabled': new_state,
        'message': 'Функція нового чату включена' if new_state else 'Функція нового чату відключена'
    })

@app.route('/get-features-state')
def get_features_state():
    """Получение текущего состояния функций"""
    allow_change_file = os.environ.get('ALLOW_CHANGE_FILE', 'False').lower() == 'true'
    allow_new_chat = os.environ.get('ALLOW_NEW_CHAT', 'False').lower() == 'true'
    
    return jsonify({
        'success': True,
        'allow_change_file': allow_change_file,
        'allow_new_chat': allow_new_chat
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000, )