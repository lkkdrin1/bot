import json
import os
import uuid
import re
import shutil
import sys
import signal
import asyncio
import hashlib
import threading
import atexit
import gc
import socket
import time
from datetime import datetime, timedelta
from pathlib import Path
from collections import OrderedDict
from typing import Optional, Tuple, Dict, Any, List
from functools import wraps

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError, TimedOut, NetworkError, RetryAfter
from telegram.request import HTTPXRequest
import httpx

# Конфигурация
TOKEN = "8269137568:AAEgVAKaSOtV8du7YUOpQaPjCe3z1S5IuBo"
DATA_FILE = "bot_data.json"
PHOTO_PATH = "photo.jpg"
SUPPORT_USERNAME = "gift_helpers"

# Конфигурация видео для разных сообщений
VIDEO_FILES = {
    "start": "IMG_3668.mp4",
    "settings": "IMG_3668.mp4",
    "referral": "IMG_3668.mp4",
    "my_reqs": "IMG_3669.mp4",
    "create_deal": "IMG_3670.mp4",
    "default": "IMG_3668.mp4"
}

# Константы
MAX_VIDEO_SIZE = 50 * 1024 * 1024  # 50MB
MAX_CALLBACK_DATA = 64
MAX_DEALS_PER_USER = 10000
SHORT_CODE_TTL_HOURS = 24  # Время жизни коротких кодов
CLEANUP_INTERVAL = 100  # Очистка каждые 100 операций
MAX_RETRIES = 5  # Максимальное количество повторных попыток
RETRY_DELAY = 3  # Задержка между попытками в секундах
CALLBACK_ANSWER_DELAY = 0.5  # Задержка для callback ответа
MAX_CAPTION_LENGTH = 1024  # Максимальная длина caption для видео/фото
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20MB максимальный размер файла

DATA_LOCK = threading.Lock()
SHORT_CODES_LOCK = threading.Lock()
NETWORK_LOCK = threading.Lock()

# Глобальные переменные
data = {}
deal_short_codes: Dict[str, Tuple[str, str]] = {}  # short_code -> (deal_code, creator_id)
short_codes_created_at: Dict[str, datetime] = {}  # short_code -> creation time
operation_counter = 0  # Счетчик операций для периодической очистки
last_network_error_time = 0  # Время последней сетевой ошибки
keyboard_cache = {}  # Кэш для клавиатур
pending_payment = {}  # Ожидающие оплаты сделки {user_id: deal_code}

# Загрузка данных
def load_data():
    global data
    if not Path(DATA_FILE).exists():
        data = {}
        return data
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"⚠️ Ошибка загрузки данных: {e}. Создаем новый файл.")
        data = {}
        return data
    except Exception as e:
        print(f"❌ Неожиданная ошибка при загрузке данных: {e}")
        data = {}
        return data

def save_data():
    """Безопасное сохранение данных с созданием резервной копии"""
    global data
    if not isinstance(data, dict):
        print(f"❌ Ошибка: данные должны быть словарем, получен {type(data)}")
        return False
    
    with DATA_LOCK:
        temp_file = DATA_FILE + ".tmp"
        backup_file = DATA_FILE + ".backup"
        
        try:
            # Конвертируем datetime в строку для JSON
            def json_serializer(obj):
                if isinstance(obj, (datetime, timedelta)):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")
            
            # Сначала сохраняем во временный файл
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=json_serializer)
            
            # Проверяем, что временный файл создан и не пустой
            if not Path(temp_file).exists() or Path(temp_file).stat().st_size == 0:
                raise Exception("Временный файл не создан или пустой")
            
            # Создаем резервную копию существующего файла
            if Path(DATA_FILE).exists():
                shutil.copy2(DATA_FILE, backup_file)
            
            # Заменяем основной файл временным
            shutil.move(temp_file, DATA_FILE)
            print(f"✅ Данные сохранены в {DATA_FILE}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка при сохранении данных: {e}")
            # Удаляем временный файл в случае ошибки
            if Path(temp_file).exists():
                try:
                    os.remove(temp_file)
                except:
                    pass
            return False

def cleanup_old_data():
    """Очистка старых данных пользователей"""
    global data
    with DATA_LOCK:
        for user_id in list(data.keys()):
            if isinstance(data[user_id], dict):
                # Очищаем старые сделки (оставляем последние 100)
                if "deals" in data[user_id] and isinstance(data[user_id]["deals"], list):
                    if len(data[user_id]["deals"]) > 100:
                        data[user_id]["deals"] = data[user_id]["deals"][-100:]
                
                # Очищаем старые реферальные записи
                if "referrals" in data[user_id] and isinstance(data[user_id]["referrals"], list):
                    # Оставляем только уникальные рефералы
                    data[user_id]["referrals"] = list(set(data[user_id]["referrals"]))

def cleanup_old_short_codes():
    """Удаляет короткие коды старше заданного времени"""
    global deal_short_codes, short_codes_created_at
    with SHORT_CODES_LOCK:
        current_time = datetime.now()
        expired = []
        for code, created_at in short_codes_created_at.items():
            if (current_time - created_at) > timedelta(hours=SHORT_CODE_TTL_HOURS):
                expired.append(code)
        
        for code in expired:
            deal_short_codes.pop(code, None)
            short_codes_created_at.pop(code, None)
        
        if expired:
            print(f"🧹 Очищено {len(expired)} устаревших коротких кодов")

def increment_operation_counter():
    """Увеличивает счетчик операций и выполняет очистку при необходимости"""
    global operation_counter
    operation_counter += 1
    if operation_counter % CLEANUP_INTERVAL == 0:
        cleanup_old_short_codes()
        if operation_counter % (CLEANUP_INTERVAL * 10) == 0:
            cleanup_old_data()
            gc.collect()

def add_deal_short_code(deal_code: str, creator_id: str):
    """Добавляет короткий код для быстрого поиска сделки"""
    with SHORT_CODES_LOCK:
        short_code = get_short_code(deal_code)
        deal_short_codes[short_code] = (deal_code, creator_id)
        short_codes_created_at[short_code] = datetime.now()
        
        # Периодическая очистка
        if len(deal_short_codes) % CLEANUP_INTERVAL == 0:
            cleanup_old_short_codes()

def find_deal_by_short_code(short_code: str) -> Tuple[Optional[Dict], Optional[str]]:
    """Находит сделку по короткому коду"""
    with SHORT_CODES_LOCK:
        deal_info = deal_short_codes.get(short_code)
    
    if not deal_info:
        return None, None
    
    deal_code, creator_id = deal_info
    return find_deal_by_code(deal_code)

# Загружаем данные при старте
load_data()

def cleanup():
    """Очистка при выходе"""
    print("\n🔄 Сохранение данных перед выходом...")
    save_data()
    cleanup_old_data()
    print("✅ Завершение работы")

atexit.register(cleanup)

# Декоратор для повторных попыток при сетевых ошибках
def retry_on_network_error(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        global last_network_error_time
        for attempt in range(MAX_RETRIES):
            try:
                # Проверяем, не было ли недавно сетевой ошибки
                current_time = time.time()
                if current_time - last_network_error_time < 5:
                    await asyncio.sleep(2)
                
                return await func(*args, **kwargs)
            except (NetworkError, TimedOut, httpx.HTTPError, socket.gaierror) as e:
                last_network_error_time = time.time()
                print(f"⚠️ Сетевая ошибка (попытка {attempt + 1}/{MAX_RETRIES}): {e}")
                
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    print(f"⏳ Повтор через {wait_time}с...")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"❌ Превышено количество попыток: {e}")
                    raise
            except Exception as e:
                print(f"❌ Неожиданная ошибка: {e}")
                raise
    return wrapper

# Проверка существования видео
def check_video_exists(video_key):
    """Проверяет существование видео файла"""
    video_path = VIDEO_FILES.get(video_key, VIDEO_FILES["default"])
    try:
        if os.path.exists(video_path):
            size = os.path.getsize(video_path)
            if size > MAX_VIDEO_SIZE:
                print(f"⚠️ Видео {video_path} слишком большое ({size} байт)")
                return False
            return size > 0
    except:
        return False
    return False

async def get_video_input(video_key):
    """Асинхронно получает видео файл"""
    video_path = VIDEO_FILES.get(video_key, VIDEO_FILES["default"])
    file_obj = None
    try:
        if os.path.exists(video_path) and os.path.getsize(video_path) > 0:
            # Открываем файл без контекстного менеджера
            file_obj = open(video_path, 'rb')
            return InputFile(file_obj), file_obj
    except Exception as e:
        print(f"❌ Ошибка при открытии видео {video_path}: {e}")
        if file_obj:
            try:
                file_obj.close()
            except:
                pass
    return None, None

def get_user_completed_deals_count(user_id):
    """Возвращает количество завершенных сделок пользователя"""
    try:
        user_data = data.get(str(user_id), {})
        if not isinstance(user_data, dict):
            return 0
        deals = user_data.get("deals", [])
        if not isinstance(deals, list):
            return 0
        # Считаем только завершенные сделки (со статусом completed)
        completed = sum(1 for deal in deals if isinstance(deal, dict) and deal.get("status") == "completed")
        return completed
    except Exception as e:
        print(f"⚠️ Ошибка в get_user_completed_deals_count: {e}")
        return 0

def find_deal_by_code(deal_code):
    """Находит сделку по коду"""
    try:
        for uid, user_data in data.items():
            if isinstance(user_data, dict):
                deals = user_data.get("deals", [])
                if isinstance(deals, list):
                    for deal in deals:
                        if isinstance(deal, dict) and deal.get("id") == deal_code:
                            return deal, uid
    except Exception as e:
        print(f"⚠️ Ошибка в find_deal_by_code: {e}")
    return None, None

def generate_referral_link(bot_username, user_id):
    """Генерирует реферальную ссылку для пользователя"""
    return f"https://t.me/{bot_username}?start=ref_{user_id}"

def get_referral_stats(user_id):
    """Получает статистику реферальной системы для пользователя"""
    try:
        user_data = data.get(str(user_id), {})
        if not isinstance(user_data, dict):
            user_data = {}
        
        referrals = user_data.get("referrals", [])
        if not isinstance(referrals, list):
            referrals = []
        
        referral_balance_ton = float(user_data.get("referral_balance_ton", 0))
        referral_balance_usdt = float(user_data.get("referral_balance_usdt", 0))
        
        return {
            "count": len(referrals),
            "balance_ton": referral_balance_ton,
            "balance_usdt": referral_balance_usdt
        }
    except Exception as e:
        print(f"⚠️ Ошибка в get_referral_stats: {e}")
        return {"count": 0, "balance_ton": 0, "balance_usdt": 0}

def add_referral(user_id, referred_id):
    """Добавляет реферала пользователю"""
    try:
        user_id = str(user_id)
        referred_id = str(referred_id)
        
        if user_id not in data:
            data[user_id] = {"requisites": [], "deals": [], "referrals": []}
        elif not isinstance(data[user_id], dict):
            data[user_id] = {"requisites": [], "deals": [], "referrals": []}
        
        if "referrals" not in data[user_id] or not isinstance(data[user_id]["referrals"], list):
            data[user_id]["referrals"] = []
        
        # Проверяем, не был ли уже добавлен этот реферал
        if referred_id not in data[user_id]["referrals"]:
            data[user_id]["referrals"].append(referred_id)
            save_data()
            return True
    except Exception as e:
        print(f"⚠️ Ошибка в add_referral: {e}")
    return False

def get_short_code(long_code):
    """Создает короткий код для callback_data"""
    return hashlib.md5(long_code.encode()).hexdigest()[:8]

# Функция для форматирования текста с поддержкой HTML
def format_text(text: str, format_type: str = None) -> str:
    """Форматирует текст с поддержкой HTML тегов"""
    formats = {
        'bold': '<b>{}</b>',
        'italic': '<i>{}</i>',
        'underline': '<u>{}</u>',
        'strike': '<s>{}</s>',
        'code': '<code>{}</code>',
        'pre': '<pre>{}</pre>',
        'quote': '<blockquote>{}</blockquote>'
    }
    
    if format_type and format_type in formats:
        return formats[format_type].format(text)
    return text

# Функция для создания многострочного форматированного текста
def create_formatted_message(**kwargs) -> str:
    """Создает форматированное сообщение с различными стилями"""
    lines = []
    for key, value in kwargs.items():
        if key == 'bold':
            lines.append(format_text(value, 'bold'))
        elif key == 'italic':
            lines.append(format_text(value, 'italic'))
        elif key == 'code':
            lines.append(format_text(value, 'code'))
        elif key == 'pre':
            lines.append(format_text(value, 'pre'))
        elif key == 'quote':
            lines.append(format_text(value, 'quote'))
        elif key == 'text':
            lines.append(value)
        elif key == 'text2':
            lines.append(value)
        elif key == 'text3':
            lines.append(value)
        elif key == 'text4':
            lines.append(value)
        elif key == 'text5':
            lines.append(value)
        elif key == 'text6':
            lines.append(value)
        elif key == 'text7':
            lines.append(value)
        elif key == 'text8':
            lines.append(value)
        elif key == 'text9':
            lines.append(value)
        elif key == 'text10':
            lines.append(value)
        elif key == 'text11':
            lines.append(value)
        elif key == 'text12':
            lines.append(value)
        elif key == 'code2':
            lines.append(format_text(value, 'code'))
        elif key == 'code3':
            lines.append(format_text(value, 'code'))
        elif key == 'code4':
            lines.append(format_text(value, 'code'))
    return '\n'.join(lines)

# Функция для разделения длинного текста на части
def split_long_message(text: str, max_length: int = MAX_CAPTION_LENGTH) -> List[str]:
    """Разделяет длинное сообщение на части по max_length символов"""
    if len(text) <= max_length:
        return [text]
    
    parts = []
    current_part = ""
    
    # Разделяем по строкам
    lines = text.split('\n')
    
    for line in lines:
        # Если текущая часть + новая строка превышает лимит
        if len(current_part) + len(line) + 1 > max_length:
            if current_part:
                parts.append(current_part)
                current_part = line
            else:
                # Если одна строка слишком длинная, режем её
                while len(line) > max_length:
                    parts.append(line[:max_length])
                    line = line[max_length:]
                current_part = line
        else:
            if current_part:
                current_part += '\n' + line
            else:
                current_part = line
    
    if current_part:
        parts.append(current_part)
    
    return parts

# Клавиатуры
def start_keyboard():
    kb = [
        [InlineKeyboardButton("💻 | Мои реквизиты", callback_data="menu_my_reqs")],
        [InlineKeyboardButton("🔓 | Создать сделку", callback_data="menu_create_deal")],
        [
            InlineKeyboardButton("⛓ | Настройки", callback_data="menu_settings"),
            InlineKeyboardButton("🛒 | Саппорт", url=f"https://t.me/{SUPPORT_USERNAME}")
        ]
    ]
    return InlineKeyboardMarkup(kb)

def my_reqs_keyboard(user_id):
    try:
        user_data = data.get(str(user_id), {})
        if not isinstance(user_data, dict):
            user_data = {}
        user_reqs = user_data.get("requisites", [])
        if not isinstance(user_reqs, list):
            user_reqs = []
    except:
        user_reqs = []
        
    kb = []
    if user_reqs:
        kb.append([InlineKeyboardButton("👀 Посмотреть все реквизиты", callback_data="view_all_reqs")])
    kb.append([InlineKeyboardButton("🔑 | Добавить TON", callback_data="add_ton")])
    kb.append([InlineKeyboardButton("💳 | Добавить карту", callback_data="add_card")])
    kb.append([InlineKeyboardButton("📍 | Добавить username", callback_data="add_username")])
    kb.append([InlineKeyboardButton("◀️ В меню", callback_data="back_to_start")])
    return InlineKeyboardMarkup(kb)

def view_all_reqs_keyboard():
    """Клавиатура для просмотра всех реквизитов с кнопкой удаления"""
    kb = [
        [InlineKeyboardButton("❌ | Удалить реквизит", callback_data="delete_requisite")],
        [InlineKeyboardButton("◀️ В меню", callback_data="back_to_start")]
    ]
    return InlineKeyboardMarkup(kb)

def single_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ В меню", callback_data="back_to_start")]])

def create_deal_keyboard():
    kb = [
        [InlineKeyboardButton("🎁 | Подарки", callback_data="deal_gifts")],
        [InlineKeyboardButton("◀️ В меню", callback_data="back_to_start")],
    ]
    return InlineKeyboardMarkup(kb)

def gifts_currency_keyboard():
    kb = [
        [InlineKeyboardButton("🌏 | TON", callback_data="currency_ton")],
        [InlineKeyboardButton("💰 | Рубли", callback_data="currency_rub")],
        [InlineKeyboardButton("⭐️ | Звезды", callback_data="currency_stars")],
        [InlineKeyboardButton("◀️ В меню", callback_data="back_to_start")],
    ]
    return InlineKeyboardMarkup(kb)

def choose_requisite_keyboard(user_id):
    try:
        user_data = data.get(str(user_id), {})
        if not isinstance(user_data, dict):
            user_data = {}
        user_reqs = user_data.get("requisites", [])
        if not isinstance(user_reqs, list):
            user_reqs = []
    except:
        user_reqs = []
        
    kb = []
    for idx, r in enumerate(user_reqs):
        if isinstance(r, dict):
            label = f"{r.get('type', 'Unknown')} - {str(r.get('value', ''))[:20]}..."
            kb.append([InlineKeyboardButton(label, callback_data=f"choose_req_{idx}")])
    kb.append([InlineKeyboardButton("◀️ В меню", callback_data="back_to_start")])
    return InlineKeyboardMarkup(kb)

def back_to_menu_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Вернуться в меню", callback_data="back_to_start")]])

def deal_participant_keyboard():
    """Клавиатура для покупателя в сделке"""
    kb = [
        [
            InlineKeyboardButton("🌊 | Саппорт", url=f"https://t.me/{SUPPORT_USERNAME}"),
            InlineKeyboardButton("❌ | Выйти со сделки", callback_data="exit_deal")
        ]
    ]
    return InlineKeyboardMarkup(kb)

def seller_paid_keyboard(deal_code):
    """Клавиатура для продавца после оплаты"""
    short_code = get_short_code(deal_code)
    kb = [
        [InlineKeyboardButton("✅ Я передал подарок", callback_data=f"transferred_{short_code}")],
        [InlineKeyboardButton("🔥 | Саппорт", url=f"https://t.me/{SUPPORT_USERNAME}")]
    ]
    return InlineKeyboardMarkup(kb)

def buyer_received_keyboard(deal_code):
    """Клавиатура для покупателя после получения"""
    short_code = get_short_code(deal_code)
    kb = [
        [InlineKeyboardButton("Подтверждаю получение подарка", callback_data=f"confirm_receive_{short_code}")],
        [InlineKeyboardButton("🍷 | Саппорт", url=f"https://t.me/{SUPPORT_USERNAME}")]
    ]
    return InlineKeyboardMarkup(kb)

def back_to_menu_only_keyboard():
    """Клавиатура только с кнопкой возврата в меню"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("🦋 | Вернуться в меню", callback_data="back_to_start")]])

def success_deal_keyboard():
    """Клавиатура для успешной сделки"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("Вернуться в меню", callback_data="back_to_start")]])

def settings_keyboard():
    """Клавиатура для раздела настроек"""
    kb = [
        [InlineKeyboardButton("💥 | Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton("🛍 | Вернуться в меню", callback_data="back_to_start")],
    ]
    return InlineKeyboardMarkup(kb)

def referral_keyboard():
    """Клавиатура для реферальной системы"""
    kb = [
        [InlineKeyboardButton("📌 | Вернуться в меню", callback_data="back_to_start")],
    ]
    return InlineKeyboardMarkup(kb)

def how_deal_works_keyboard():
    """Клавиатура для сообщения 'Как проходит сделка?' с кнопкой ✅"""
    kb = [
        [InlineKeyboardButton("✅", callback_data="start_deal_creation")]
    ]
    return InlineKeyboardMarkup(kb)

def continue_keyboard():
    """Клавиатура с кнопкой Продолжить"""
    kb = [
        [InlineKeyboardButton("▶️ Продолжить", callback_data="continue_reading")]
    ]
    return InlineKeyboardMarkup(kb)

def upload_pdf_keyboard(deal_code):
    """Клавиатура для загрузки PDF чека"""
    short_code = get_short_code(deal_code)
    kb = [
        [InlineKeyboardButton("📎 Загрузить PDF чек", callback_data=f"upload_pdf_{short_code}")]
    ]
    return InlineKeyboardMarkup(kb)

# Валидация форматов
def validate_ton_address(text):
    if not isinstance(text, str):
        return False
    return bool(re.match(r'^(EQ|UQ)[A-Za-z0-9]{46,48}$', text))

def validate_card(text):
    if not isinstance(text, str):
        return False
    pattern = r'^[A-Za-zА-Яа-я]+[\s\-]+(\d{4}\s?\d{4}\s?\d{4}\s?\d{4}|\d{16})$'
    return bool(re.match(pattern, text.strip()))

def validate_username(text):
    if not isinstance(text, str):
        return False
    return bool(re.match(r'^@[A-Za-z0-9_]{5,32}$', text.strip()))

def validate_amount(amount):
    """Проверяет корректность суммы"""
    if not isinstance(amount, (str, int, float)):
        return False, "Неверный тип данных"
    try:
        if isinstance(amount, str):
            amount = amount.replace(' ', '').replace(',', '.')
        amount_float = float(amount)
        if amount_float <= 0:
            return False, "Сумма должна быть положительной"
        if amount_float > 1_000_000_000:
            return False, "Сумма не может превышать 1,000,000,000"
        return True, amount_float
    except (ValueError, TypeError) as e:
        return False, f"Неверный формат числа: {e}"

async def send_with_retry(func, max_retries=3, delay=2):
    """Отправляет с повторными попытками при timeout"""
    last_error = None
    for attempt in range(max_retries):
        try:
            return await func()
        except (TimedOut, NetworkError, httpx.HTTPError, socket.gaierror) as e:
            last_error = e
            if attempt < max_retries - 1:
                wait_time = delay * (attempt + 1)
                print(f"⏳ Сетевая ошибка, повторная попытка через {wait_time}с...")
                await asyncio.sleep(wait_time)
            else:
                print(f"❌ Превышено количество попыток отправки")
        except Exception as e:
            print(f"❌ Ошибка при отправке: {e}")
            last_error = e
            break
    
    if last_error:
        raise last_error

# Универсальная функция для отправки сообщений
@retry_on_network_error
async def send_message_with_video(chat_id, text, video_key="default", reply_markup=None, context=None, message=None, parse_mode='HTML'):
    """Отправляет сообщение с видео или без, с обработкой timeout."""
    video_input = None
    file_obj = None
    file_size_mb = 0
    
    try:
        # Проверяем размер текста
        if len(text) > MAX_CAPTION_LENGTH:
            print(f"⚠️ Текст слишком длинный ({len(text)} символов). Отправляем без видео.")
            # Отправляем как обычное сообщение
            await send_message_fast(chat_id, text, reply_markup, context, message, parse_mode)
            return
        
        # Проверяем размер видео
        video_path = VIDEO_FILES.get(video_key, VIDEO_FILES["default"])
        if os.path.exists(video_path):
            file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
            print(f"📹 Размер видео: {file_size_mb:.2f} MB")
            
            # Если видео большое, используем увеличенный timeout
            timeout = 300 if file_size_mb > 20 else 120  # 5 мин для больших, 2 мин для маленьких
            
            # Получаем видео файл
            video_input, file_obj = await get_video_input(video_key)
            
            if video_input and file_obj:
                try:
                    if message:
                        await send_with_retry(
                            lambda: message.reply_video(
                                video=video_input,
                                caption=text,
                                reply_markup=reply_markup,
                                read_timeout=timeout,
                                write_timeout=timeout,
                                connect_timeout=30,
                                parse_mode=parse_mode
                            ),
                            max_retries=2
                        )
                    elif context and context.bot:
                        await send_with_retry(
                            lambda: context.bot.send_video(
                                chat_id=chat_id,
                                video=video_input,
                                caption=text,
                                reply_markup=reply_markup,
                                read_timeout=timeout,
                                write_timeout=timeout,
                                connect_timeout=30,
                                parse_mode=parse_mode
                            ),
                            max_retries=2
                        )
                    return
                    
                except Exception as e:
                    print(f"❌ Ошибка при отправке с видео: {e}")
                    raise
                finally:
                    if file_obj:
                        file_obj.close()
                        if file_size_mb > 20:
                            gc.collect()  # Принудительная сборка мусора для больших видео
        
        # Если не получилось с видео, пробуем фото
        if os.path.exists(PHOTO_PATH) and os.path.getsize(PHOTO_PATH) > 0:
            try:
                with open(PHOTO_PATH, 'rb') as photo_file:
                    photo_input = InputFile(photo_file)
                    if message:
                        await send_with_retry(
                            lambda: message.reply_photo(
                                photo=photo_input,
                                caption=text,
                                reply_markup=reply_markup,
                                parse_mode=parse_mode
                            ),
                            max_retries=2
                        )
                    elif context and context.bot:
                        await send_with_retry(
                            lambda: context.bot.send_photo(
                                chat_id=chat_id,
                                photo=photo_input,
                                caption=text,
                                reply_markup=reply_markup,
                                parse_mode=parse_mode
                            ),
                            max_retries=2
                        )
                    return
            except Exception as e:
                print(f"❌ Ошибка при отправке с фото: {e}")
        
        # Если ничего не получилось, отправляем текст
        await send_message_fast(chat_id, text, reply_markup, context, message, parse_mode)
        
    except Exception as e:
        print(f"❌ Критическая ошибка в send_message_with_video: {e}")
        if file_obj:
            try:
                file_obj.close()
            except:
                pass

# Функция для отправки длинных сообщений по частям
async def send_long_message(chat_id, text, reply_markup=None, context=None, message=None, parse_mode='HTML'):
    """Отправляет длинное сообщение по частям"""
    parts = split_long_message(text)
    
    for i, part in enumerate(parts):
        # Для первой части добавляем клавиатуру
        if i == 0:
            await send_message_fast(chat_id, part, reply_markup, context, message, parse_mode)
        else:
            # Для последующих частей без клавиатуры
            await send_message_fast(chat_id, part, None, context, None, parse_mode)
        # Небольшая задержка между сообщениями
        await asyncio.sleep(0.5)

# Функция для быстрой отправки текстовых сообщений
async def send_message_fast(chat_id, text, reply_markup=None, context=None, message=None, parse_mode='HTML'):
    """Максимально быстрая отправка текстовых сообщений"""
    try:
        if message:
            await message.reply_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
        elif context and context.bot:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
    except Exception as e:
        print(f"⚠️ Ошибка в send_message_fast: {e}")

# Функция для получения кэшированной клавиатуры
def get_cached_keyboard(keyboard_func, *args):
    """Получает клавиатуру из кэша"""
    cache_key = f"{keyboard_func.__name__}_{'_'.join(str(a) for a in args)}"
    
    if cache_key in keyboard_cache:
        return keyboard_cache[cache_key]
    
    result = keyboard_func(*args)
    keyboard_cache[cache_key] = result
    return result

# Команда /set_my_deals
@retry_on_network_error
async def set_my_deals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /set_my_deals для накрутки количества сделок"""
    try:
        if not update or not update.message:
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                format_text("❌ Использование:", 'bold') + "\n" +
                format_text("/set_my_deals [количество сделок]", 'code') + "\n\n" +
                format_text("Пример: /set_my_deals 10", 'italic'),
                parse_mode='HTML'
            )
            return
        
        try:
            deals_count = int(args[0])
            if deals_count < 0:
                await update.message.reply_text(format_text("❌ Количество сделок не может быть отрицательным.", 'bold'), parse_mode='HTML')
                return
            if deals_count > MAX_DEALS_PER_USER:
                await update.message.reply_text(format_text(f"❌ Слишком большое количество сделок (максимум {MAX_DEALS_PER_USER}).", 'bold'), parse_mode='HTML')
                return
        except ValueError:
            await update.message.reply_text(format_text("❌ Введите корректное число.", 'bold'), parse_mode='HTML')
            return
        
        user_id = str(update.effective_user.id)
        username = update.effective_user.username or f"id{user_id}"
        
        if user_id not in data:
            data[user_id] = {"requisites": [], "deals": [], "username": username, "referrals": []}
        elif not isinstance(data[user_id], dict):
            data[user_id] = {"requisites": [], "deals": [], "username": username, "referrals": []}
        
        if "deals" not in data[user_id] or not isinstance(data[user_id]["deals"], list):
            data[user_id]["deals"] = []
        
        current_deals = len([d for d in data[user_id]["deals"] if isinstance(d, dict) and d.get("status") == "completed"])
        
        for i in range(deals_count):
            deal = {
                "id": f"SET_{uuid.uuid4().hex[:8].upper()}",
                "status": "completed",
                "created_at": datetime.now().isoformat(),
                "completed_at": datetime.now().isoformat(),
                "type": "set_by_command"
            }
            data[user_id]["deals"].append(deal)
        
        if save_data():
            await update.message.reply_text(
                create_formatted_message(
                    bold="✅ Успешно добавлено {} завершенных сделок!".format(deals_count),
                    text="\n📊 Теперь у вас {} завершенных сделок.".format(current_deals + deals_count)
                ),
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(format_text("❌ Ошибка при сохранении данных.", 'bold'), parse_mode='HTML')
        
        increment_operation_counter()
        
    except Exception as e:
        print(f"❌ Ошибка в set_my_deals: {e}")
        try:
            await update.message.reply_text(format_text("❌ Произошла ошибка при выполнении команды.", 'bold'), parse_mode='HTML')
        except:
            pass

# Команда /buy
@retry_on_network_error
async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /buy для оплаты сделки"""
    try:
        if not update or not update.message:
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                create_formatted_message(
                    bold="❌ Использование:",
                    code="/buy [код сделки]",
                    italic="Пример: /buy ABC12345"
                ),
                parse_mode='HTML'
            )
            return
        
        deal_code = args[0].upper()
        
        deal, creator_id = find_deal_by_code(deal_code)
        
        if not deal:
            await update.message.reply_text(format_text("❌ Сделка не найдена.", 'bold'), parse_mode='HTML')
            return
        
        buyer_id = str(update.effective_user.id)
        if deal.get("buyer_id") != buyer_id:
            await update.message.reply_text(format_text("❌ Вы не являетесь покупателем в этой сделке.", 'bold'), parse_mode='HTML')
            return
        
        if deal.get("status") != "active":
            await update.message.reply_text(format_text("❌ Эта сделка уже не активна.", 'bold'), parse_mode='HTML')
            return
        
        if deal.get("paid", False):
            await update.message.reply_text(format_text("❌ Эта сделка уже была оплачена.", 'bold'), parse_mode='HTML')
            return
        
        # Запрашиваем прикрепление PDF чека
        pending_payment[buyer_id] = deal_code
        
        text = create_formatted_message(
            bold="💎 Информация об оплате",
            quote="📎 Для подтверждения оплаты, пожалуйста, прикрепите PDF чек перевода.",
            text="\nНажмите на кнопку ниже и выберите файл PDF.",
            italic="Файл должен быть в формате PDF и не более 20MB."
        )
        
        await update.message.reply_text(
            text=text,
            reply_markup=upload_pdf_keyboard(deal_code),
            parse_mode='HTML'
        )
        
    except Exception as e:
        print(f"❌ Ошибка в buy_command: {e}")
        try:
            await update.message.reply_text(format_text("❌ Произошла ошибка при обработке команды.", 'bold'), parse_mode='HTML')
        except:
            pass

# Обработчик документов (PDF файлов)
@retry_on_network_error
async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик прикрепленных документов"""
    try:
        if not update or not update.message or not update.message.document:
            return
        
        user_id = str(update.effective_user.id)
        
        # Проверяем, ожидает ли пользователь загрузку PDF для оплаты
        if user_id not in pending_payment:
            await update.message.reply_text(
                format_text("❌ Сначала выполните команду /buy с кодом сделки.", 'bold'),
                parse_mode='HTML'
            )
            return
        
        deal_code = pending_payment[user_id]
        document = update.message.document
        
        # Проверяем, что это PDF
        if not document.file_name.lower().endswith('.pdf'):
            await update.message.reply_text(
                format_text("❌ Пожалуйста, прикрепите файл в формате PDF.", 'bold'),
                parse_mode='HTML'
            )
            return
        
        # Проверяем размер файла
        if document.file_size > MAX_FILE_SIZE:
            await update.message.reply_text(
                format_text(f"❌ Файл слишком большой. Максимальный размер: {MAX_FILE_SIZE // (1024*1024)}MB.", 'bold'),
                parse_mode='HTML'
            )
            return
        
        # Получаем информацию о сделке
        deal, creator_id = find_deal_by_code(deal_code)
        
        if not deal:
            await update.message.reply_text(format_text("❌ Сделка не найдена.", 'bold'), parse_mode='HTML')
            del pending_payment[user_id]
            return
        
        if deal.get("status") != "active":
            await update.message.reply_text(format_text("❌ Эта сделка уже не активна.", 'bold'), parse_mode='HTML')
            del pending_payment[user_id]
            return
        
        if deal.get("paid", False):
            await update.message.reply_text(format_text("❌ Эта сделка уже была оплачена.", 'bold'), parse_mode='HTML')
            del pending_payment[user_id]
            return
        
        # Отмечаем сделку как оплаченную
        deal["paid"] = True
        deal["paid_at"] = datetime.now().isoformat()
        deal["status"] = "paid"
        deal["pdf_file_id"] = document.file_id
        deal["pdf_file_name"] = document.file_name
        
        if not save_data():
            await update.message.reply_text(format_text("❌ Ошибка при сохранении данных.", 'bold'), parse_mode='HTML')
            return
        
        # Подтверждаем получение PDF
        await update.message.reply_text(
            format_text("✅ PDF чек получен! Оплата подтверждена.", 'bold'),
            parse_mode='HTML'
        )
        
        # Удаляем из ожидающих
        del pending_payment[user_id]
        
        # Получаем данные продавца и покупателя
        seller_id = creator_id
        seller_username = deal.get("creator_username", "Неизвестно")
        buyer_username = update.effective_user.username or f"id{user_id}"
        
        # Отправляем сообщение продавцу с PDF чеком
        seller_text = create_formatted_message(
            bold="💥 Оплата получена по сделке {}!".format(deal_code),
            text="\nТип сделки: 🎁 Подарки.",
            text2="👤 Покупатель: @{} (id: {})".format(buyer_username, user_id),
            text3="🧊 Прикрепленный PDF чек покупателем находится внизу.",
            text4=" ",
            pre="‼️ Передайте товар → @gift_helpers ‼️",
            text5="\n💰 Получаете: {} {}\n📝 Отдаёте: {}".format(
                deal.get('amount', 0), 
                deal.get('currency', ''),
                deal.get('description', 'Не указано')
            ),
            text6=" ",
            quote="‼️ Передавайте товар только @gift_helpers\nВ случае передачи товара другому человеку возврат не будет осуществлен.\nДля получения гарантий, записывайте на видео момент передачи товара."
        )
        
        try:
            # Сначала отправляем текстовое сообщение
            await context.bot.send_message(
                chat_id=int(seller_id),
                text=seller_text,
                parse_mode='HTML'
            )
            
            # Затем отправляем PDF файл
            await context.bot.send_document(
                chat_id=int(seller_id),
                document=document.file_id,
                filename=document.file_name,
                caption="📎 Чек оплаты от покупателя\n\n💎 Покупатель перевел деньги на реквизиты менеджера."
            )
            
            # Отправляем клавиатуру для подтверждения передачи
            await context.bot.send_message(
                chat_id=int(seller_id),
                text="✅ Подтвердите передачу подарка:",
                text2=" ",
                text3="‼ Обратите внимание, проверка проходит моментально",
                reply_markup=seller_paid_keyboard(deal_code)
            )
            
        except Exception as e:
            print(f"⚠️ Не удалось отправить сообщение продавцу: {e}")
        
        # Отправляем сообщение покупателю об успешной оплате
        buyer_text = create_formatted_message(
            bold="💥 Транзакция по сделке {} найдена!".format(deal_code),
            text="\nТип сделки: 🎁 Подарки",
            text2="👤 Продавец: @{} (id: {})".format(seller_username, seller_id),
            text3="\nПолучаете: {}\nОтдаёте: {} {}".format(
                deal.get('description', 'Не указано'),
                deal.get('amount', 0),
                deal.get('currency', '')
            ),
            text4="\n🧊 PDF чек успешно прикреплен и отправлен продавцу.",
            italic="‼️ Сделка завершится после того как продавец передаст товар @gift_helpers и вы это подтвердите."
        )
        
        await update.message.reply_text(
            text=buyer_text,
            reply_markup=buyer_received_keyboard(deal_code),
            parse_mode='HTML'
        )
        
        increment_operation_counter()
        
    except Exception as e:
        print(f"❌ Ошибка в document_handler: {e}")

@retry_on_network_error
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Проверяем, не является ли это переходом по реферальной ссылке
        if update.message and update.message.text:
            text = update.message.text
            if text.startswith('/start ref_'):
                parts = text.split('_')
                if len(parts) > 1:
                    referrer_id = parts[1].strip()
                    user_id = str(update.effective_user.id)
                    
                    if referrer_id and referrer_id != user_id and referrer_id.isdigit():
                        add_referral(referrer_id, user_id)
                        await update.message.reply_text(format_text("✅ Вы перешли по реферальной ссылке!", 'bold'), parse_mode='HTML')
            
            elif text.startswith('/start deal_'):
                parts = text.split('_')
                if len(parts) > 1:
                    deal_code = parts[1].strip()
                    await show_deal_to_buyer(update, context, deal_code)
                    return
        
        text = create_formatted_message(
            bold="👋 Добро пожаловать!",
            text="\n🎁 Надёжный сервис для безопасных сделок!",
            italic="✨ Автоматизировано, быстро и без лишних хлопот!",
            quote="💎 Комиссия за услугу: 1%",
            quote2="💎 Поддержка 24/7: @gift_helpers",
            bold2="💌 Теперь ваши сделки под защитой!"
        )
        
        if update.message:
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="start",
                reply_markup=start_keyboard(),
                context=context,
                message=update.message,
                parse_mode='HTML'
            )
        elif update.callback_query and update.callback_query.message:
            try:
                await update.callback_query.message.delete()
            except Exception as e:
                print(f"⚠️ Не удалось удалить сообщение: {e}")
            
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="start",
                reply_markup=start_keyboard(),
                context=context,
                parse_mode='HTML'
            )
        
        increment_operation_counter()
        
    except Exception as e:
        print(f"❌ Ошибка в start: {e}")
        if update.message:
            try:
                await update.message.reply_text(format_text("❌ Произошла ошибка. Пожалуйста, попробуйте позже.", 'bold'), parse_mode='HTML')
            except:
                pass
    
    if context.user_data:
        context.user_data.clear()

@retry_on_network_error
async def show_deal_to_buyer(update: Update, context: ContextTypes.DEFAULT_TYPE, deal_code):
    """Показывает информацию о сделке покупателю"""
    try:
        if not update or not update.message:
            return
            
        deal, creator_id = find_deal_by_code(deal_code)
        
        if not deal:
            await update.message.reply_text(format_text("❌ Сделка не найдена или уже завершена.", 'bold'), parse_mode='HTML')
            return
        
        if deal.get("buyer_id"):
            await update.message.reply_text(format_text("❌ К этой сделке уже присоединился другой покупатель.", 'bold'), parse_mode='HTML')
            return
        
        seller_username = deal.get("creator_username", "Неизвестно")
        seller_deals_count = get_user_completed_deals_count(creator_id)
        
        buyer_id = str(update.effective_user.id)
        buyer_username = update.effective_user.username or f"id{buyer_id}"
        buyer_deals_count = get_user_completed_deals_count(buyer_id)
        
        deal["buyer_id"] = buyer_id
        deal["buyer_username"] = buyer_username
        deal["joined_at"] = datetime.now().isoformat()
        deal["expires_at"] = (datetime.now() + timedelta(minutes=15)).isoformat()
        
        if not save_data():
            await update.message.reply_text(format_text("❌ Ошибка при сохранении данных.", 'bold'), parse_mode='HTML')
            return
        
        requisite = deal.get("requisite", {})
        if isinstance(requisite, dict):
            requisite_value = requisite.get("value", "Не указан")
        else:
            requisite_value = "Не указан"
        
        deal_text = create_formatted_message(
            bold="💎 Информация об сделке",
            text="\n🛍 Сделка {}".format(deal_code),
            text2="Тип сделки: 🎁 Подарки",
            text3="👤 Вы покупатель, у вас есть 15 минут на оплату сделки!",
            text4="📌 Продавец: @{}\n• Количество сделок продавца: {}".format(
                seller_username, seller_deals_count
            ),
            text5="\n• Вы покупаете: {}\n• Вы отдаете: {} {}".format(
                deal.get('description', 'Не указано'),
                deal.get('amount', 0),
                deal.get('currency', '')
            ),
            pre="🏦 Кошелёк для оплаты:\n{}".format(requisite_value),
            code="🤑 Сумма к оплате:\n💎 {} {}".format(
                deal.get('amount', 0),
                deal.get('currency', '')
            ),
            code2="🎯 Комментарий к транзакции:\n{}".format(deal_code),
            italic="‼️ Пожалуйста, убедитесь что при оплате указываете обязательный комментарий (memo) и точную сумму!\n\nПосле оплаты ожидайте подтверждения администратором."
        )
        
        await send_message_with_video(
            chat_id=update.effective_chat.id,
            text=deal_text,
            video_key="default",
            reply_markup=deal_participant_keyboard(),
            context=context,
            message=update.message,
            parse_mode='HTML'
        )
        
        seller_text = create_formatted_message(
            bold="💎 Информация об сделке",
            text="\n👤 Пользователь @{} (id: {})".format(buyer_username, buyer_id),
            text2="присоединился к вашей сделке {}".format(deal_code),
            text3="\n• Количество сделок покупателя: {}".format(buyer_deals_count),
            text4=" ",
            quote="‼️ Убедитесь, что это тот же пользователь, с которым вы вели диалог ранее!",
            text5="Не передавайте товар(ы) до получения сообщения от бота о подтверждении оплаты!"
        )
        
        if creator_id:
            try:
                await context.bot.send_message(
                    chat_id=int(creator_id),
                    text=seller_text,
                    parse_mode='HTML'
                )
            except Exception as e:
                print(f"⚠️ Не удалось уведомить продавца: {e}")
        
        increment_operation_counter()
        
    except Exception as e:
        print(f"❌ Ошибка в show_deal_to_buyer: {e}")
        if update.message:
            try:
                await update.message.reply_text(format_text("❌ Произошла ошибка при открытии сделки.", 'bold'), parse_mode='HTML')
            except:
                pass

@retry_on_network_error
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update or not update.callback_query:
            return
        
        query = update.callback_query
        
        # Безопасный ответ на callback с обработкой ошибок
        try:
            await query.answer()
        except Exception as e:
            print(f"⚠️ Ошибка при ответе на callback: {e}")
            # Даже если ответ не удался, продолжаем обработку
            pass
        
        user_id = str(query.from_user.id)
        username = query.from_user.username or f"id{query.from_user.id}"
        
        if user_id not in data:
            data[user_id] = {"requisites": [], "deals": [], "username": username, "referrals": []}
        elif not isinstance(data[user_id], dict):
            data[user_id] = {"requisites": [], "deals": [], "username": username, "referrals": []}
        else:
            if "requisites" not in data[user_id] or not isinstance(data[user_id]["requisites"], list):
                data[user_id]["requisites"] = []
            if "deals" not in data[user_id] or not isinstance(data[user_id]["deals"], list):
                data[user_id]["deals"] = []
            if "username" not in data[user_id]:
                data[user_id]["username"] = username
            if "referrals" not in data[user_id] or not isinstance(data[user_id]["referrals"], list):
                data[user_id]["referrals"] = []
        
        if query.data == "referral_system":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            
            stats = get_referral_stats(user_id)
            
            bot_username = None
            if context and context.bot:
                bot_username = context.bot.username
            if not bot_username:
                bot_username = "bot"
            
            referral_link = generate_referral_link(bot_username, user_id)
            
            text = create_formatted_message(
                bold="👤 Реф. система",
                text="\n💎 Ваш реферальный процент: 1%",
                text2="💎 Приглашено пользователей: {}".format(stats['count']),
                text3="💎 Реф. баланс TON: {}".format(stats['balance_ton']),
                text4="💎 Реф. баланс USDT TON: {}".format(stats['balance_usdt']),
                text5="\nВаша ссылка для приглашения:",
                code=referral_link
            )
            
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="referral",
                reply_markup=referral_keyboard(),
                context=context,
                parse_mode='HTML'
            )
            return
        
        if query.data == "delete_requisite":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            
            # Получаем список реквизитов пользователя
            with DATA_LOCK:
                reqs = data.get(user_id, {}).get("requisites", [])
            
            if not reqs:
                await send_message_fast(
                    query.message.chat.id,
                    format_text("❌ У вас нет реквизитов для удаления.", 'bold'),
                    get_cached_keyboard(back_to_menu_keyboard),
                    context
                )
                return
            
            # Сохраняем в user_data что ожидаем номер реквизита для удаления
            context.user_data["expect"] = "delete_requisite_number"
            
            text = create_formatted_message(
                bold="🗑 Удаление реквизита",
                quote="Введите номер реквизита в списке для удаления:"
            )
            
            await send_message_fast(
                query.message.chat.id,
                text,
                get_cached_keyboard(single_back_keyboard),
                context
            )
            return
        
        if query.data.startswith("upload_pdf_"):
            short_code = query.data.replace("upload_pdf_", "")
            deal, creator_id = find_deal_by_short_code(short_code)
            
            if not deal:
                await send_message_fast(
                    query.message.chat.id,
                    format_text("❌ Сделка не найдена.", 'bold'),
                    get_cached_keyboard(single_back_keyboard),
                    context
                )
                return
            
            if deal.get("buyer_id") != user_id:
                await send_message_fast(
                    query.message.chat.id,
                    format_text("❌ Вы не являетесь покупателем в этой сделке.", 'bold'),
                    get_cached_keyboard(single_back_keyboard),
                    context
                )
                return
            
            # Сохраняем в ожидающие оплаты
            pending_payment[user_id] = deal.get('id')
            
            text = create_formatted_message(
                bold="📎 Загрузка PDF чека",
                quote="Пожалуйста, отправьте файл PDF с чеком оплаты.",
                text="\nНажмите на значок 📎 (скрепка) и выберите файл."
            )
            
            await query.message.delete()
            await send_message_fast(
                query.message.chat.id,
                text,
                None,
                context
            )
            return
        
        if query.data == "continue_reading":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            
            # Отправляем вторую часть сообщения
            text = create_formatted_message(
                text="• Проверка оплаты от покупателя проходит быстро, если большая загруженность - 5-10 минут. 👼",
                text2="\n• Проверка передачи подарка от продавца проходит быстро, если большая загруженность - 5-10 минут 👤",
                text3="\n• Подарок передается исключительно на аккаунт менеджера @gift_helpers\n(Если вас просят передать подарок на прямую — попытка обмана системы, мошенники) 💎",
                text4="\n• Ответ менеджера на какой-либо вопрос приходит в среднем от часа до трех, при большой загруженности сервиса ответ может приходить в течении двух суток, просим вас подождать. 📝",
                text5="\n• Если робот-гарант работает медленно — ничего страшного, возможно проводятся работы по его улучшению, либо большая загруженность бота(30+ сделок одновременно) 💰",
                text6="\n• Снимайте момент передачи подарка на аккаунт менеджера в случае проблем. 🤖",
                text7="\n• При ошибке реквизитами и переводом деньгами не туда — возврат не происходит. 🧠"
            )
            
            await send_message_fast(
                query.message.chat.id,
                text,
                get_cached_keyboard(how_deal_works_keyboard),
                context
            )
            return
        
        if query.data == "start_deal_creation":
            # Начинаем создание сделки
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            
            text = create_formatted_message(
                bold="💎 Создание сделки",
                text=" ",
                quote="❓ Выберите тип сделки"
            )
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="create_deal",
                reply_markup=create_deal_keyboard(),
                context=context,
                parse_mode='HTML'
            )
            return
        
        if query.data == "menu_create_deal":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            
            # Показываем информацию о том, как проходит сделка (часть 1)
            text = create_formatted_message(
                bold="Как проходит сделка? 💥",
                text="\nПосле создания сделки, дается ссылка на сделку. По ней должен перейти второй участник сделки — покупатель. Покупатель выбирает удобный способ оплаты для себя, после выполняет перевод денег в удобном способе для себя менеджеру по реквизитам.",
                text2="\nМенеджер проверяет наличие оплаты, если деньги пришли от покупателя по указанным реквизитам — приходит сообщение уведомляющее первого участника сделки — продавца. Продавец должен после уведомляющего сообщения об оплаты — передать подарок на аккаунт менеджера для дальнейшей проверки.",
                text3="\nЕсли оба участника сделки выполнили свою часть сделки, то деньги на указанные реквизиты продавцом отправляются от менеджера, а подарок отправляется покупателю. Сделка завершена.\n",
                bold2="‼️ ВАЖНО ‼️",
                text4="\n• После оплаты, покупатель обязан прикрепить pdf-чек перевода денег на указанные реквизиты менеджера, если чек не будет прикреплен - сделка отменяется.",
                text5="\n• После оплаты на передачу подарка дается 15 минут, если в указанный период времени подарок не будет передан - сделка отменяется, деньги возвращаются покупателю на реквизиты, с которого деньги были переведены."
            )
            
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="create_deal",
                reply_markup=continue_keyboard(),
                context=context,
                parse_mode='HTML'
            )
            return
        
        if query.data.startswith("transferred_"):
            short_code = query.data.replace("transferred_", "")
            deal, creator_id = find_deal_by_short_code(short_code)
            
            if not deal:
                try:
                    await query.message.reply_text(format_text("❌ Сделка не найдена.", 'bold'), parse_mode='HTML')
                except:
                    pass
                return
            
            if str(creator_id) != user_id:
                try:
                    await query.answer("❌ Вы не являетесь продавцом в этой сделке.")
                except:
                    pass
                return
            
            try:
                await query.message.delete()
            except Exception as e:
                print(f"⚠️ Не удалось удалить сообщение: {e}")
            
            text = create_formatted_message(
                bold="💎 Информация об сделке",
                bold2="✅ Вы подтвердили передачу подарка для сделки {}".format(deal.get('id')),
                text="\n• Ожидайте подтверждения получения от покупателя",
                quote="Убедитесь, что передали подарок @gift_helpers, в противном случае покупатель не сможет увидеть и подтвердить передачу подарка, а вы в свою очередь не получите оплату."
            )
            
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="default",
                reply_markup=back_to_menu_only_keyboard(),
                context=context,
                parse_mode='HTML'
            )
            return
        
        elif query.data.startswith("confirm_receive_"):
            short_code = query.data.replace("confirm_receive_", "")
            deal, creator_id = find_deal_by_short_code(short_code)
            
            if not deal:
                try:
                    await query.message.reply_text(format_text("❌ Сделка не найдена.", 'bold'), parse_mode='HTML')
                except:
                    pass
                return
            
            if deal.get("buyer_id") != user_id:
                try:
                    await query.answer("❌ Вы не являетесь покупателем в этой сделке.")
                except:
                    pass
                return
            
            deal["status"] = "completed"
            deal["completed_at"] = datetime.now().isoformat()
            
            if not save_data():
                try:
                    await query.message.reply_text(format_text("❌ Ошибка при сохранении данных.", 'bold'), parse_mode='HTML')
                except:
                    pass
                return
            
            try:
                await query.message.delete()
            except Exception as e:
                print(f"⚠️ Не удалось удалить сообщение: {e}")
            
            text = create_formatted_message(
                bold="💎 Информация об сделке",
                bold2="✅ Покупатель подтвердил получение подарка по сделке {}".format(deal.get('id')),
                quote="Сделка успешно завершена!",
                text="\nСпасибо за использование нашего сервиса. 👋"
            )
            
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="default",
                reply_markup=success_deal_keyboard(),
                context=context,
                parse_mode='HTML'
            )
            
            if creator_id:
                try:
                    await context.bot.send_message(
                        chat_id=int(creator_id),
                        text=create_formatted_message(
                            bold="💎 Информация об сделке",
                            bold2="✅ Покупатель подтвердил получение подарка по сделке {}".format(deal.get('id')),
                            quote="Сделка успешно завершена!",
                            text="\nСпасибо за использование нашего сервиса. 👋"
                        ),
                        parse_mode='HTML'
                    )
                except Exception as e:
                    print(f"⚠️ Не удалось уведомить продавца: {e}")
            return

        elif query.data == "deal_gifts":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            text = create_formatted_message(
                bold="💎 Создание сделки",
                text=" ",
                quote="💰 Выберите валюту сделки"
            )
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="create_deal",
                reply_markup=gifts_currency_keyboard(),
                context=context,
                parse_mode='HTML'
            )
            return

        elif query.data in ("currency_ton", "currency_rub", "currency_stars"):
            currency_map = {
                "currency_ton": "TON",
                "currency_rub": "RUB",
                "currency_stars": "STARS"
            }
            currency = currency_map.get(query.data, "TON")
            
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            text = create_formatted_message(
                bold="💎 Создание сделки",
                text=" ",
                quote="Введите сумму сделки в {} (например: 100.50)".format(currency)
            )
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="create_deal",
                reply_markup=single_back_keyboard(),
                context=context,
                parse_mode='HTML'
            )
            
            context.user_data["expect"] = "deal_amount"
            context.user_data["deal_currency"] = currency
            return

        elif query.data == "exit_deal":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            await start(update, context)
            return

        elif query.data.startswith("choose_req_"):
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            try:
                parts = query.data.split("_")
                if len(parts) < 3:
                    raise ValueError("Неверный формат callback_data")
                idx = int(parts[-1])
                reqs = data[user_id].get("requisites", [])
                
                if not reqs or idx < 0 or idx >= len(reqs):
                    await send_message_with_video(
                        chat_id=update.effective_chat.id,
                        text=format_text("❌ Реквизит не найден.", 'bold'),
                        video_key="default",
                        reply_markup=create_deal_keyboard(),
                        context=context,
                        parse_mode='HTML'
                    )
                    return
                
                chosen = reqs[idx]
                
                if not all([context.user_data.get("deal_currency"), 
                           context.user_data.get("deal_amount"), 
                           context.user_data.get("deal_description")]):
                    await send_message_with_video(
                        chat_id=update.effective_chat.id,
                        text=format_text("❌ Данные сделки утеряны. Начните создание заново.", 'bold'),
                        video_key="default",
                        reply_markup=create_deal_keyboard(),
                        context=context,
                        parse_mode='HTML'
                    )
                    return
                
                deal_code = str(uuid.uuid4())[:8].upper()
                
                deal = {
                    "id": deal_code,
                    "creator_id": user_id,
                    "creator_username": username,
                    "currency": context.user_data.get("deal_currency"),
                    "amount": context.user_data.get("deal_amount"),
                    "description": context.user_data.get("deal_description"),
                    "requisite": chosen,
                    "status": "active",
                    "created_at": datetime.now().isoformat(),
                    "buyer_id": None,
                    "buyer_username": None,
                    "paid": False
                }
                
                if "deals" not in data[user_id] or not isinstance(data[user_id]["deals"], list):
                    data[user_id]["deals"] = []
                data[user_id]["deals"].append(deal)
                
                if not save_data():
                    await send_message_with_video(
                        chat_id=update.effective_chat.id,
                        text=format_text("❌ Ошибка при сохранении данных.", 'bold'),
                        video_key="default",
                        reply_markup=create_deal_keyboard(),
                        context=context,
                        parse_mode='HTML'
                    )
                    return
                
                # Добавляем короткий код для быстрого поиска
                add_deal_short_code(deal_code, user_id)
                
                bot_username = None
                if context and context.bot:
                    bot_username = context.bot.username
                if not bot_username:
                    bot_username = "bot"
                    
                link = f"https://t.me/{bot_username}?start=deal_{deal_code}"
                
                text = create_formatted_message(
                    bold="✅ Сделка успешно создана!",
                    text="\nТип сделки: 🎁 Подарки",
                    text2="📦 Отдаёте: {}".format(deal['description']),
                    text3="💰 Получаете: {} {}".format(deal['amount'], deal['currency']),
                    text4="\n🔗 Ссылка для покупателя:",
                    code=link,
                    text5="\n📋 Код сделки: {}".format(deal_code)
                )
                
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🏠 Выйти со сделки", callback_data="back_to_start")
                ]])
                
                await send_message_with_video(
                    chat_id=update.effective_chat.id,
                    text=text,
                    video_key="default",
                    reply_markup=keyboard,
                    context=context,
                    parse_mode='HTML'
                )
                
                for key in ["expect", "deal_currency", "deal_amount", "deal_description"]:
                    if key in context.user_data:
                        context.user_data.pop(key, None)
                
                increment_operation_counter()
                    
            except Exception as e:
                print(f"❌ Ошибка при выборе реквизита: {e}")
                await send_message_with_video(
                    chat_id=update.effective_chat.id,
                    text=format_text("❌ Ошибка при выборе реквизита.", 'bold'),
                    video_key="default",
                    reply_markup=create_deal_keyboard(),
                    context=context,
                    parse_mode='HTML'
                )
            return

        elif query.data == "back_to_start":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            await start(update, context)
            return

        elif query.data == "menu_my_reqs":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            text = create_formatted_message(
                bold="💰 Мои реквизиты",
                text=" ",
                quote="Добавление кошелька для получения оплаты.",
                text2=" ",
                text3="Выберите действие:"
            )
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="my_reqs",
                reply_markup=my_reqs_keyboard(user_id),
                context=context,
                parse_mode='HTML'
            )
            return

        elif query.data == "view_all_reqs":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            try:
                with DATA_LOCK:
                    reqs = data.get(user_id, {}).get("requisites", [])
                    username = data.get(user_id, {}).get("username", f"id{user_id}")
                    user_id_num = user_id
                
                if not reqs:
                    await send_message_with_video(
                        chat_id=update.effective_chat.id,
                        text=format_text("❌ У вас нет добавленных реквизитов.", 'bold'),
                        video_key="my_reqs",
                        reply_markup=my_reqs_keyboard(user_id),
                        context=context,
                        parse_mode='HTML'
                    )
                    return
                
                reqs_text = ""
                for i, req in enumerate(reqs, 1):
                    if isinstance(req, dict):
                        reqs_text += f"{i}. {req.get('type', 'Unknown')}: {req.get('value', '')}\n"
                
                text = create_formatted_message(
                    bold="👤 Ваш username: @{}".format(username),
                    bold2="👤 Ваш id telegram: {}".format(user_id_num),
                    text="\n📋 Все ваши добавленные реквизиты:\n",
                    code=reqs_text,
                    bold3="\nЖелаете ли вы удалить какой либо реквизит? ✨"
                )
                
                await send_message_with_video(
                    chat_id=update.effective_chat.id,
                    text=text,
                    video_key="my_reqs",
                    reply_markup=view_all_reqs_keyboard(),
                    context=context,
                    parse_mode='HTML'
                )
            except Exception as e:
                print(f"❌ Ошибка в view_all_reqs: {e}")
            return

        elif query.data in ("add_ton", "add_card", "add_username"):
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            
            if query.data == "add_card":
                text = create_formatted_message(
                    bold="💳 Добавление карты",
                    text=" ",
                    quote="Пришлите данные в формате:",
                    quote2="Банк - Номер карты (16 цифр)",
                    text2="\nПример:",
                    code="Сбербанк - 4276 1234 5678 9012",
                    text3="\n‼️ Перепроверяйте свои реквизиты перед созданием сделки."
                )
            elif query.data == "add_ton":
                text = create_formatted_message(
                    bold="👛 Добавление TON кошелька",
                    text=" ",
                    quote="Пришлите адрес TON кошелька",
                    quote2="Кошелек должен начинаться с EQ... или UQ... и содержать 48 символов",
                    text2="\nПример:",
                    code="UQAvqf6x2sPJdLqX3KqX3KqX3KqX3KqX3KqX3KqX3KqX",
                    text3="\n‼️ Перепроверяйте свои реквизиты перед созданием сделки."
                )
            elif query.data == "add_username":
                text = create_formatted_message(
                    bold="👤 Добавление username",
                    text=" ",
                    quote="Пришлите ваш @username",
                    quote2="Формат должен начинаться с @",
                    text2="\nПример:",
                    code="@username123",
                    text3="\n‼️ Перепроверяйте юзернеймы перед созданием сделки."
                )
            
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="my_reqs",
                reply_markup=single_back_keyboard(),
                context=context,
                parse_mode='HTML'
            )
            context.user_data["expect"] = query.data.replace("add_", "")
            return

        elif query.data == "menu_settings":
            if query.message:
                try:
                    await query.message.delete()
                except Exception as e:
                    print(f"⚠️ Не удалось удалить сообщение: {e}")
            text = create_formatted_message(
                bold="⚙️ Настройки",
                quote="Настройки бота."
            )
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=text,
                video_key="settings",
                reply_markup=settings_keyboard(),
                context=context,
                parse_mode='HTML'
            )
            return
            
        increment_operation_counter()
        
    except Exception as e:
        print(f"❌ Ошибка в callback_handler: {e}")

@retry_on_network_error
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update or not update.message or not update.message.text:
            return

        user_id = str(update.effective_user.id)
        username = update.effective_user.username or f"id{user_id}"
        text = update.message.text.strip()
        expect = context.user_data.get("expect")

        if user_id not in data:
            data[user_id] = {"requisites": [], "deals": [], "username": username, "referrals": []}
        elif not isinstance(data[user_id], dict):
            data[user_id] = {"requisites": [], "deals": [], "username": username, "referrals": []}
        else:
            if "requisites" not in data[user_id] or not isinstance(data[user_id]["requisites"], list):
                data[user_id]["requisites"] = []
            if "deals" not in data[user_id] or not isinstance(data[user_id]["deals"], list):
                data[user_id]["deals"] = []
            if "username" not in data[user_id]:
                data[user_id]["username"] = username
            if "referrals" not in data[user_id] or not isinstance(data[user_id]["referrals"], list):
                data[user_id]["referrals"] = []

        if not expect:
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=format_text("❓ Используйте кнопки меню для навигации", 'bold'),
                video_key="default",
                reply_markup=start_keyboard(),
                context=context,
                message=update.message,
                parse_mode='HTML'
            )
            return
        
        # Обработка удаления реквизита по номеру
        if expect == "delete_requisite_number":
            try:
                req_number = int(text)
                
                with DATA_LOCK:
                    reqs = data.get(user_id, {}).get("requisites", [])
                    
                    if req_number < 1 or req_number > len(reqs):
                        await send_message_fast(
                            update.effective_chat.id,
                            format_text(f"❌ Реквизит с номером {req_number} не найден. Введите номер из списка.", 'bold'),
                            get_cached_keyboard(single_back_keyboard),
                            context,
                            update.message
                        )
                        return
                    
                    # Удаляем реквизит (индексация с 1 для пользователя, в списке с 0)
                    deleted_req = reqs.pop(req_number - 1)
                    
                save_data()
                context.user_data.pop("expect", None)
                
                # Отправляем ✅ и возвращаем в меню
                await update.message.reply_text("✅", parse_mode='HTML')
                await start(update, context)
                
            except ValueError:
                await send_message_fast(
                    update.effective_chat.id,
                    format_text("❌ Пожалуйста, введите число.", 'bold'),
                    get_cached_keyboard(single_back_keyboard),
                    context,
                    update.message
                )
            return
        
        if expect == "deal_amount":
            try:
                is_valid, result = validate_amount(text)
                if not is_valid:
                    await send_message_with_video(
                        chat_id=update.effective_chat.id,
                        text=format_text(f"❌ {result}", 'bold'),
                        video_key="default",
                        reply_markup=single_back_keyboard(),
                        context=context,
                        message=update.message,
                        parse_mode='HTML'
                    )
                    return
                
                context.user_data["deal_amount"] = result
                context.user_data["expect"] = "deal_description"
                
                message_text = create_formatted_message(
                    bold="💎 Создание сделки",
                    text=" ",
                    quote="📝 Введите описание сделки (что отдаёте):",
                    text2="\nПример: 10 Pepe"
                )
                await send_message_with_video(
                    chat_id=update.effective_chat.id,
                    text=message_text,
                    video_key="create_deal",
                    reply_markup=single_back_keyboard(),
                    context=context,
                    message=update.message,
                    parse_mode='HTML'
                )
            except Exception as e:
                print(f"❌ Ошибка при обработке суммы: {e}")
                await send_message_with_video(
                    chat_id=update.effective_chat.id,
                    text=format_text("❌ Ошибка при обработке суммы.", 'bold'),
                    video_key="default",
                    reply_markup=single_back_keyboard(),
                    context=context,
                    message=update.message,
                    parse_mode='HTML'
                )
            return
        
        elif expect == "deal_description":
            if len(text) > 500:
                await send_message_with_video(
                    chat_id=update.effective_chat.id,
                    text=format_text("❌ Описание слишком длинное. Максимум 500 символов.", 'bold'),
                    video_key="default",
                    reply_markup=single_back_keyboard(),
                    context=context,
                    message=update.message,
                    parse_mode='HTML'
                )
                return
                
            context.user_data["deal_description"] = text
            
            with DATA_LOCK:
                reqs = data.get(user_id, {}).get("requisites", [])
            
            if not reqs:
                await send_message_with_video(
                    chat_id=update.effective_chat.id,
                    text=format_text("❌ У вас нет реквизитов. Сначала добавьте реквизит.", 'bold'),
                    video_key="my_reqs",
                    reply_markup=my_reqs_keyboard(user_id),
                    context=context,
                    message=update.message,
                    parse_mode='HTML'
                )
                for key in ["expect", "deal_currency", "deal_amount", "deal_description"]:
                    if key in context.user_data:
                        context.user_data.pop(key, None)
                return
            
            message_text = create_formatted_message(
                bold="💎 Создание сделки",
                text=" ",
                quote="💰 Выберите реквизит для получения оплаты:",
                text2=" ",
                text3="‼️ Перепроверяйте свои реквизиты перед созданием сделки."
            )
            await send_message_with_video(
                chat_id=update.effective_chat.id,
                text=message_text,
                video_key="create_deal",
                reply_markup=choose_requisite_keyboard(user_id),
                context=context,
                message=update.message,
                parse_mode='HTML'
            )
            return
        
        elif expect in ["ton", "card", "username"]:
            type_map = {
                "ton": "TON кошелек",
                "card": "Банковская карта",
                "username": "Username"
            }

            is_valid = False
            error_message = ""

            if expect == "ton":
                is_valid = validate_ton_address(text)
                error_message = format_text("❌ Неверный формат TON кошелька. Адрес должен начинаться с EQ... или UQ... и содержать 48 символов.", 'bold')
            elif expect == "card":
                is_valid = validate_card(text)
                error_message = format_text("❌ Неверный формат карты. Используйте формат: Банк - 16 цифр", 'bold')
            elif expect == "username":
                is_valid = validate_username(text)
                error_message = format_text("❌ Неверный формат username. Username должен начинаться с @", 'bold')

            if not is_valid:
                await send_message_with_video(
                    chat_id=update.effective_chat.id,
                    text=error_message,
                    video_key="my_reqs",
                    reply_markup=single_back_keyboard(),
                    context=context,
                    message=update.message,
                    parse_mode='HTML'
                )
                return

            if "requisites" not in data[user_id] or not isinstance(data[user_id]["requisites"], list):
                data[user_id]["requisites"] = []
            
            with DATA_LOCK:
                requisites = data[user_id]["requisites"]
                
                for req in requisites:
                    if isinstance(req, dict) and req.get("value") == text:
                        await send_message_with_video(
                            chat_id=update.effective_chat.id,
                            text=format_text("❌ Такой реквизит уже существует!", 'bold'),
                            video_key="my_reqs",
                            reply_markup=my_reqs_keyboard(user_id),
                            context=context,
                            message=update.message,
                            parse_mode='HTML'
                        )
                        context.user_data.pop("expect", None)
                        return
                
                requisites.append({
                    "type": type_map.get(expect, expect),
                    "value": text,
                    "added": datetime.now().isoformat()
                })

            data[user_id]["username"] = username

            if save_data():
                context.user_data.pop("expect", None)
                # Отправляем только эмодзи ✅
                await update.message.reply_text("✅", parse_mode='HTML')
                # Сразу отправляем начальное сообщение
                await start(update, context)
            else:
                await send_message_with_video(
                    chat_id=update.effective_chat.id,
                    text=format_text("❌ Ошибка при сохранении данных.", 'bold'),
                    video_key="my_reqs",
                    reply_markup=my_reqs_keyboard(user_id),
                    context=context,
                    message=update.message,
                    parse_mode='HTML'
                )
            return
        
        increment_operation_counter()
        
    except Exception as e:
        print(f"❌ Ошибка в text_handler: {e}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок с подробным логированием"""
    try:
        error = context.error
        error_type = type(error).__name__
        error_msg = str(error)
        
        print(f"❌ Ошибка [{error_type}]: {error_msg}")
        
        # Специфичная обработка для разных типов ошибок
        if isinstance(error, (NetworkError, TimedOut, httpx.HTTPError, socket.gaierror)):
            print("🌐 Сетевая ошибка, повторная попытка будет выполнена автоматически")
            # Не отправляем сообщение пользователю при сетевых ошибках
            return
        
        if "Query is too old" in error_msg:
            print("⚠️ Устаревший callback query")
            return
        
        if "getaddrinfo failed" in error_msg:
            print("🌐 Ошибка DNS резолвинга, проверьте подключение к интернету")
            # Делаем паузу перед следующей попыткой
            await asyncio.sleep(5)
            return
        
        # Отправляем сообщение пользователю только для критических ошибок
        if update and update.effective_message and not isinstance(error, (NetworkError, TimedOut)):
            try:
                await update.effective_message.reply_text(
                    format_text("❌ Произошла внутренняя ошибка. Пожалуйста, попробуйте позже.", 'bold'),
                    parse_mode='HTML'
                )
            except:
                pass
                
    except Exception as e:
        print(f"❌ Ошибка в error_handler: {e}")

def signal_handler(sig, frame):
    print(f"\n🛑 Получен сигнал {sig}")
    save_data()
    cleanup_old_data()
    cleanup_old_short_codes()
    gc.collect()
    sys.exit(0)

def main():
    print("🚀 Запуск бота...")
    
    # Устанавливаем обработчик сигнала для корректного завершения
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Настройка DNS резолвинга
    try:
        # Пробуем установить таймаут для DNS
        socket.setdefaulttimeout(30)
    except:
        pass
    
    # ИСПРАВЛЕНО: убран pool_size, который не поддерживается
    request = HTTPXRequest(
        connect_timeout=30.0,
        read_timeout=60.0,
        write_timeout=60.0
        # pool_timeout и pool_size не используются
    )
    
    # Проверка наличия видео файлов
    print("\n📹 Проверка видео файлов:")
    for key, path in VIDEO_FILES.items():
        try:
            if os.path.exists(path):
                size_mb = os.path.getsize(path) / (1024 * 1024)
                if size_mb > 0:
                    print(f"✅ {key}: {path} ({size_mb:.2f} MB)")
                    if size_mb > 20:
                        print(f"   ⚠️ Видео большое, будут использованы увеличенные таймауты")
                else:
                    print(f"⚠️ {key}: {path} пустой")
            else:
                print(f"⚠️ {key}: {path} не найден")
        except Exception as e:
            print(f"⚠️ Ошибка при проверке {path}: {e}")
    
    # Проверка наличия фото
    try:
        if os.path.exists(PHOTO_PATH):
            size_mb = os.path.getsize(PHOTO_PATH) / (1024 * 1024)
            if size_mb > 0:
                print(f"✅ Фото найдено: {PHOTO_PATH} ({size_mb:.2f} MB)")
            else:
                print(f"⚠️ Файл фото пустой: {PHOTO_PATH}")
        else:
            print(f"⚠️ Фото не найдено: {PHOTO_PATH}")
    except Exception as e:
        print(f"⚠️ Ошибка при проверке фото: {e}")
    
    print()
    
    # Проверка токена
    if not TOKEN:
        print("❌ ОШИБКА: Токен не указан!")
        return
    
    # Проверка интернет-соединения
    print("🌐 Проверка интернет-соединения...")
    try:
        # Пробуем сделать тестовый запрос к Telegram API
        import socket
        socket.gethostbyname('api.telegram.org')
        print("✅ Интернет-соединение работает")
    except socket.gaierror:
        print("⚠️ Проблема с DNS резолвингом, но бот может работать")
    except Exception as e:
        print(f"⚠️ Ошибка при проверке соединения: {e}")
    
    print()
    
    try:
        # Создание приложения с кастомным request
        app = Application.builder().token(TOKEN).request(request).build()
        
        # Добавление обработчиков
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("buy", buy_command))
        app.add_handler(CommandHandler("set_my_deals", set_my_deals))
        app.add_handler(MessageHandler(filters.Document.ALL, document_handler))  # Обработчик документов
        app.add_handler(CallbackQueryHandler(callback_handler))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
        app.add_error_handler(error_handler)
        
        print("✅ Бот успешно запущен!")
        print("ℹ️ Для остановки нажмите Ctrl+C")
        print(f"📊 Загружено {len(data)} пользователей")
        print(f"🔍 Активных коротких кодов: {len(deal_short_codes)}")
        print("🛡️ Все ошибки обработаны и не повлияют на работу бота")
        
        app.run_polling(
            drop_pending_updates=True,
            allowed_updates=['message', 'callback_query'],
            pool_timeout=30
        )
        
    except Exception as e:
        print(f"❌ Критическая ошибка при запуске: {e}")
        print("Проверьте:")
        print("1. Токен бота (должен быть правильным)")
        print("2. Интернет-соединение")
        print("3. Установленные библиотеки (pip install python-telegram-bot httpx)")
        print("\nПовторная попытка через 10 секунд...")
        time.sleep(10)
        main()  # Пробуем перезапустить

if __name__ == "__main__":
    main()
