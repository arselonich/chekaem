"""
Модуль для работы с SQLite базой данных.
Хранит места и соотношения пабрикушек.
"""
import sqlite3
from pathlib import Path
from typing import List, Dict, Optional, Tuple

DB_PATH = Path(__file__).parent / "data.db"


def get_connection():
    """Получить соединение с БД."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Инициализировать базу данных (создать таблицы)."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Таблица мест
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS places (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)
    
    # Таблица соотношений пабрикушек
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            place_name TEXT NOT NULL,
            pab1 TEXT NOT NULL,
            pab2 TEXT,
            is_excluded INTEGER DEFAULT 0,
            UNIQUE(place_name, pab1),
            FOREIGN KEY (place_name) REFERENCES places(name)
        )
    """)
    
    # Таблица соотношений названий полей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS field_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            place_name TEXT NOT NULL,
            field1 TEXT NOT NULL,
            field2 TEXT,
            is_excluded INTEGER DEFAULT 0,
            UNIQUE(place_name, field1),
            FOREIGN KEY (place_name) REFERENCES places(name)
        )
    """)

    # Таблица классификаций номеров (пользовательские классификации)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS number_classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            place_name TEXT NOT NULL,
            raw_number TEXT NOT NULL,
            number_type TEXT NOT NULL,
            extracted_value TEXT,
            UNIQUE(place_name, raw_number),
            FOREIGN KEY (place_name) REFERENCES places(name)
        )
    """)

    # Добавить стандартные места (1-5), если их нет
    default_places = ['1', '2', '3', '4', '5']
    for place in default_places:
        cursor.execute("INSERT OR IGNORE INTO places (name) VALUES (?)", (place,))

    conn.commit()
    conn.close()


def get_all_places() -> List[str]:
    """Получить список всех мест."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM places ORDER BY name")
    places = [row['name'] for row in cursor.fetchall()]
    conn.close()
    return places


def add_place(name: str) -> bool:
    """Добавить новое место. Возвращает True если успешно, False если уже существует."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO places (name) VALUES (?)", (name,))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def get_mapping(place_name: str, pab1: str) -> Optional[str]:
    """Получить соотношение для пабрикушки из 1-го файла."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT pab2, is_excluded FROM mappings WHERE place_name = ? AND pab1 = ?",
        (place_name, pab1)
    )
    row = cursor.fetchone()
    conn.close()
    if row:
        return row['pab2'] if not row['is_excluded'] else None
    return None


def get_all_mappings(place_name: str) -> Dict[str, Optional[str]]:
    """Получить все соотношения для места."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT pab1, pab2, is_excluded FROM mappings WHERE place_name = ?",
        (place_name,)
    )
    mappings = {}
    for row in cursor.fetchall():
        if row['is_excluded']:
            mappings[row['pab1']] = None  # Исключённая пабрикушка
        else:
            mappings[row['pab1']] = row['pab2']
    conn.close()
    return mappings


def save_mappings(place_name: str, mappings: Dict[str, Optional[str]]):
    """
    Сохранить соотношения для места.
    mappings: {pab1: pab2} или {pab1: None} для исключённых
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    for pab1, pab2 in mappings.items():
        is_excluded = 1 if pab2 is None else 0
        cursor.execute("""
            INSERT OR REPLACE INTO mappings (place_name, pab1, pab2, is_excluded)
            VALUES (?, ?, ?, ?)
        """, (place_name, pab1, pab2, is_excluded))
    
    conn.commit()
    conn.close()


def get_excluded_pabricushki(place_name: str) -> List[str]:
    """Получить список исключённых пабрикушек для места."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT pab1 FROM mappings WHERE place_name = ? AND is_excluded = 1",
        (place_name,)
    )
    excluded = [row['pab1'] for row in cursor.fetchall()]
    conn.close()
    return excluded


def get_pab2_to_pab1_mapping(place_name: str) -> Dict[str, List[str]]:
    """
    Получить обратное соотношение: pab2 -> [pab1, ...].
    Нужно для связи 1:многие.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT pab1, pab2 FROM mappings WHERE place_name = ? AND is_excluded = 0",
        (place_name,)
    )
    result = {}
    for row in cursor.fetchall():
        pab2 = row['pab2']
        pab1 = row['pab1']
        if pab2 not in result:
            result[pab2] = []
        result[pab2].append(pab1)
    conn.close()
    return result


# ==================== Функции для работы с соотношениями полей ====================

def get_field_mapping(place_name: str, field1: str) -> Optional[str]:
    """Получить соотношение для названия поля из 1-го файла."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT field2, is_excluded FROM field_mappings WHERE place_name = ? AND field1 = ?",
        (place_name, field1)
    )
    row = cursor.fetchone()
    conn.close()
    if row:
        return row['field2'] if not row['is_excluded'] else None
    return None


def get_all_field_mappings(place_name: str) -> Dict[str, Optional[str]]:
    """Получить все соотношения для места."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT field1, field2, is_excluded FROM field_mappings WHERE place_name = ?",
        (place_name,)
    )
    mappings = {}
    for row in cursor.fetchall():
        if row['is_excluded']:
            mappings[row['field1']] = None
        else:
            mappings[row['field1']] = row['field2']
    conn.close()
    return mappings


def save_field_mappings(place_name: str, mappings: Dict[str, Optional[str]]):
    """
    Сохранить соотношения для места.
    mappings: {field1: field2} или {field1: None} для исключённых
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    for field1, field2 in mappings.items():
        is_excluded = 1 if field2 is None else 0
        cursor.execute("""
            INSERT OR REPLACE INTO field_mappings (place_name, field1, field2, is_excluded)
            VALUES (?, ?, ?, ?)
        """, (place_name, field1, field2, is_excluded))
    
    conn.commit()
    conn.close()


def get_excluded_fields(place_name: str) -> List[str]:
    """Получить список исключённых названий полей для места."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT field1 FROM field_mappings WHERE place_name = ? AND is_excluded = 1",
        (place_name,)
    )
    excluded = [row['field1'] for row in cursor.fetchall()]
    conn.close()
    return excluded


# ==================== Функции для работы с классификациями номеров ====================

def get_all_number_classifications(place_name: str) -> Dict[str, Tuple[str, str]]:
    """
    Получить все классификации номеров для места.
    Возвращает словарь {raw_number: (number_type, extracted_value)}
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT raw_number, number_type, extracted_value FROM number_classifications WHERE place_name = ?",
        (place_name,)
    )
    classifications = {}
    for row in cursor.fetchall():
        classifications[row['raw_number']] = (row['number_type'], row['extracted_value'])
    conn.close()
    return classifications


def save_number_classifications(place_name: str, classifications: Dict[str, Tuple[str, str]]):
    """
    Сохранить классификации номеров для места.
    classifications: {raw_number: (number_type, extracted_value)}
    """
    conn = get_connection()
    cursor = conn.cursor()

    for raw_number, (number_type, extracted_value) in classifications.items():
        cursor.execute("""
            INSERT OR REPLACE INTO number_classifications (place_name, raw_number, number_type, extracted_value)
            VALUES (?, ?, ?, ?)
        """, (place_name, raw_number, number_type, extracted_value))

    conn.commit()
    conn.close()


def get_number_classification(place_name: str, raw_number: str) -> Tuple[str, str]:
    """
    Получить классификацию для конкретного номера.
    Возвращает (number_type, extracted_value) или None
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT number_type, extracted_value FROM number_classifications WHERE place_name = ? AND raw_number = ?",
        (place_name, raw_number)
    )
    row = cursor.fetchone()
    conn.close()
    if row:
        return (row['number_type'], row['extracted_value'])
    return None


# Инициализировать БД при импорте
init_db()
