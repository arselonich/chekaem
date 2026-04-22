import pandas as pd
import numpy as np
import re
import zipfile
import os
from pathlib import Path
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Optional, Literal

# Типы номеров
NumberType = Literal['Э', 'К', 'Склад', 'Неопределённый']


def classify_number(raw_num: str) -> Tuple[NumberType, Optional[str]]:
    """
    Классифицирует номер на один из типов: Э, К, Склад, Неопределённый.
    Возвращает кортеж (тип, извлечённое_значение).
    
    Тип Э: начинается с цифры, формат XX-XX-XX (до второго дефиса)
    Тип К: содержит число с тремя двоеточиями между цифрами
    Тип Склад: содержит слово "Склад"
    Неопределённый: не подошёл ни под один тип
    """
    if not isinstance(raw_num, str):
        raw_num = str(raw_num) if pd.notna(raw_num) else ''
    
    raw_num = raw_num.strip()
    
    # Проверка на тип Склад
    if 'склад' in raw_num.lower():
        return ('Склад', None)
    
    # Проверка на тип Э: начинается с цифры и содержит минимум 2 дефиса между цифрами
    if raw_num and raw_num[0].isdigit():
        # Ищем дефисы между цифрами
        dash_pattern = r'\d+-\d+'
        dashes = re.findall(dash_pattern, raw_num)
        if len(dashes) >= 2:
            # Извлекаем до второго дефиса
            first_dash = raw_num.find('-')
            if first_dash != -1:
                second_dash = raw_num.find('-', first_dash + 1)
                if second_dash != -1:
                    extracted = raw_num[:second_dash]
                    return ('Э', extracted)
    
    # Проверка на тип К: ищем число с тремя двоеточиями между цифрами
    # Паттерн: цифры:цифры:цифры:цифры (три двоеточия)
    colon_pattern = r'\b(\d+:\d+:\d+:\d+)\b'
    match = re.search(colon_pattern, raw_num)
    if match:
        extracted = match.group(1)
        return ('К', extracted)
    
    # Не подошёл ни под один тип
    return ('Неопределённый', None)

# ------------------------ Fuzzy matching ------------------------
def fuzzy_match(pab1: str, pab2_list: List[str], threshold: float = 0.6) -> Optional[str]:
    """
    Найти наиболее похожее название из списка pab2_list для pab1.
    Возвращает лучшее совпадение, если similarity >= threshold.
    """
    if not pab1 or not pab2_list:
        return None
    
    best_match = None
    best_score = threshold
    
    for pab2 in pab2_list:
        if not pab2:
            continue
        score = SequenceMatcher(None, pab1.lower(), pab2.lower()).ratio()
        if score > best_score:
            best_score = score
            best_match = pab2
    
    return best_match


def auto_map_pabricushki(
    pab1_list: List[str],
    pab2_list: List[str],
    existing_mappings: Dict[str, Optional[str]]
) -> Dict[str, dict]:
    """
    Автоматически сопоставить пабрикушки используя fuzzy matching.
    existing_mappings: уже сохранённые соотношения для этого места.
    Возвращает полный словарь {pab1: {'value': pab2 или None, 'source': 'db'|'auto'|None}}.

    source:
      - 'db' — значение взято из БД и есть в текущем файле 2
      - 'db_missing' — значение из БД, но его НЕТ в текущем файле 2 (не выбрано)
      - 'auto' — авто-сопоставление через fuzzy matching
      - 'excluded' — исключено (семена, НЕ ВХОДИТ В ПАТ)
      - None — нет совпадения, пользователь должен выбрать вручную
    """
    pab2_set = set(pab2_list)
    result = {}

    # Сначала применяем существующие соотношения из БД
    for pab1 in pab1_list:
        if pab1 in existing_mappings:
            db_value = existing_mappings[pab1]
            if db_value is None:
                # Исключено в БД
                result[pab1] = {'value': None, 'source': 'excluded'}
            elif db_value in pab2_set:
                # Значение из БД есть в текущем файле 2
                result[pab1] = {'value': db_value, 'source': 'db'}
            else:
                # Значение из БД отсутствует в текущем файле 2 — не выбираем автоматически
                result[pab1] = {'value': None, 'source': 'db_missing'}

    # Для новых пабрикушек пытаемся найти авто-сопоставление
    for pab1 in pab1_list:
        if pab1 in result:
            continue

        # Проверяем, не исключённая ли это пабрикушка (начинается с "Семена" или содержит)
        if pab1.lower().startswith('семена') or 'семена' in pab1.lower() or 'не входит в пат' in pab1.lower():
            result[pab1] = {'value': None, 'source': 'excluded'}
            continue

        # Ищем похожее название во 2-м файле
        match = fuzzy_match(pab1, pab2_list)
        if match:
            result[pab1] = {'value': match, 'source': 'auto'}
        else:
            result[pab1] = {'value': None, 'source': None}  # Нет совпадения — пользователь должен выбрать вручную

    return result


# ------------------------ Чтение первого файла ------------------------
def read_first_file(file_path):
    """
    Читает первый файл (лист "TDSheet"), пропуская первые 9 строк.
    Возвращает DataFrame с колонками:
        пабрикушка (A), номер (D), название_номера (G), вес (H)
    """
    # Проверяем существование файла
    if not os.path.exists(file_path):
        raise ValueError(f"Файл не найден: {file_path}")
    
    # Проверяем, является ли файл валидным Excel-файлом
    def is_valid_excel_file(file_path):
        """Проверяет, является ли файл валидным Excel файлом."""
        try:
            # Проверяем расширение
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.xlsx':
                # Проверяем, является ли файл валидным ZIP-архивом
                try:
                    with zipfile.ZipFile(file_path, 'r') as zf:
                        # Проверяем наличие обязательных файлов в .xlsx
                        required_files = ['xl/workbook.xml', 'xl/worksheets/']
                        for file in zf.namelist():
                            if file.startswith('xl/worksheets/'):
                                return True
                        return False
                except zipfile.BadZipFile:
                    return False
            elif ext == '.xls':
                # Для .xls файлов просто проверяем существование
                return True
            else:
                return False
        except Exception:
            return False
    
    if not is_valid_excel_file(file_path):
        raise ValueError(
            f"Файл '{os.path.basename(file_path)}' не является валидным Excel-файлом. "
            "Возможные причины:\n"
            "1. Файл повреждён или неполный\n"
            "2. Файл имеет расширение .xlsx, но на самом деле это .xls\n"
            "3. Файл был создан некорректно\n"
            "Пожалуйста, откройте файл в Excel и сохраните как новый .xlsx файл."
        )
    
    try:
        # Определяем движок в зависимости от расширения файла
        ext = os.path.splitext(file_path)[1].lower()
        engine = 'openpyxl' if ext == '.xlsx' else 'xlrd'
        
        # Читаем без заголовков, пропускаем 9 строк, данные начинаются с 10-й (индекс 9 в pandas)
        df = pd.read_excel(file_path, sheet_name="TDSheet", header=None, skiprows=9, dtype=str, engine=engine)
    except Exception as e:
        # Проверяем, является ли ошибка связанной с отсутствием sharedStrings.xml
        error_msg = str(e)
        if 'sharedStrings.xml' in error_msg:
            raise ValueError(
                f"Ошибка чтения Excel-файла: файл повреждён или не является валидным .xlsx файлом.\n"
                f"Ошибка: {error_msg}\n"
                "Пожалуйста, откройте файл в Excel и сохраните как новый .xlsx файл, "
                "или убедитесь, что файл не был создан некорректной программой."
            )
        else:
            raise ValueError(f"Ошибка чтения первого файла: {error_msg}")

    # Выбираем нужные столбцы (по индексам: A=0, D=3, G=6, H=7)
    if df.shape[1] < 8:
        raise ValueError("В первом файле недостаточно столбцов (нужны A, D, G, H)")

    df = df.iloc[:, [0, 3, 5, 7]].copy()   # A=0 (пабрикушка), D=3 (номер), G=6 (название_номера), H=7 (вес)
    df.columns = ['пабрикушка', 'номер', 'название_номера', 'вес']

    # Заменяем NaN на пустые строки/0
    df['пабрикушка'] = df['пабрикушка'].fillna('').astype(str).str.strip()
    df['номер'] = df['номер'].fillna('').astype(str).str.strip()
    df['название_номера'] = df['название_номера'].fillna('').astype(str).str.strip()
    df['вес'] = pd.to_numeric(df['вес'], errors='coerce').fillna(0)

    # Удаляем строки, где и номер, и название пусты (игнорируем)
    df = df[~((df['номер'] == '') & (df['название_номера'] == ''))]

    return df

# ------------------------ Чтение справочника ------------------------
def read_spravochnik(file_path):
    """
    Читает третий файл (лист "кадастровые номера полей").
    Предполагается, что первая строка - заголовок.
    Возвращает DataFrame с колонками:
        список_номеров (U), финальный_номер (D), название_номера (F)
    """
    try:
        df = pd.read_excel(file_path, sheet_name="кадастровые номера полей", header=0, dtype=str)
    except Exception as e:
        raise ValueError(f"Ошибка чтения справочника: {e}")

    # Проверяем наличие нужных столбцов (U - 20-й по индексу, D - 3-й, F - 5-й)
    if df.shape[1] < 21:  # минимум до столбца U (индекс 20)
        raise ValueError("В справочнике недостаточно столбцов (нужны U, D, F)")

    # Берем столбец U (индекс 20), D (индекс 3) и F (индекс 5)
    df = df.iloc[:, [20, 3, 5]].copy()
    df.columns = ['список_номеров', 'финальный_номер', 'название_номера_справочник']

    # Обрабатываем NaN
    df['список_номеров'] = df['список_номеров'].fillna('').astype(str).str.strip()
    df['финальный_номер'] = df['финальный_номер'].fillna('').astype(str).str.strip()
    df['название_номера_справочник'] = df['название_номера_справочник'].fillna('').astype(str).str.strip()

    # Удаляем строки, где список пуст
    df = df[df['список_номеров'] != '']

    return df

# ------------------------ Преобразование номера (до второго тире) ------------------------
def transform_number(raw_num):
    """
    Обрезает строку до второго тире (не включая его).
    Если второго тире нет, возвращает исходную строку.
    """
    if not isinstance(raw_num, str):
        raw_num = str(raw_num) if pd.notna(raw_num) else ''
    # Ищем позицию второго тире
    parts = raw_num.split('-')
    if len(parts) >= 3:
        # Есть минимум два тире: объединяем первые две части через тире? Нет, нужно до второго тире не включая его.
        # Фактически: берем подстроку до начала второго тире.
        # Находим индекс второго тире:
        first_dash = raw_num.find('-')
        if first_dash != -1:
            second_dash = raw_num.find('-', first_dash + 1)
            if second_dash != -1:
                return raw_num[:second_dash]  # обрезаем до второго тире (не включая)
    return raw_num

# ------------------------ Чтение второго файла с обработкой по справочнику ------------------------
def read_second_file(file_path, sprav_df, number_classifications: Dict[str, Tuple[NumberType, str]] = None):
    """
    Читает второй файл (лист "Лист1").
    Файл 2 уже модифицирован preprocess_second_file — имеет заголовки.
    Обрабатывает исходный номер с учётом типа (Э, К, Склад, Неопределённый),
    ищет финальный номер в справочнике.

    Args:
        file_path: путь ко второму файлу
        sprav_df: DataFrame справочника
        number_classifications: словарь {исходный_номер: (тип, извлечённое_значение)}
                                для неопределённых номеров, классифицированных пользователем

    Возвращает:
        df_ok - DataFrame с валидными строками (финальный номер найден),
        df_errors - DataFrame с ошибочными строками (для листа ошибок),
        classification_stats - статистика классификации номеров
    """
    try:
        # Файл 2 уже модифицирован — имеет заголовки
        df = pd.read_excel(file_path, sheet_name="Лист1", header=0, dtype=str)
    except Exception as e:
        raise ValueError(f"Ошибка чтения второго файла: {e}")

    # Проверяем наличие столбцов (модифицированный файл)
    if 'пабрикушка' in df.columns and 'исходный_номер' in df.columns:
        # Модифицированный файл — используем именованные столбцы
        needed_cols = ['пабрикушка', 'вес', 'название_номера', 'исходный_номер']
        for col in needed_cols:
            if col not in df.columns:
                df[col] = ''
        df = df[needed_cols].copy()
    elif df.shape[1] >= 6:
        # Fallback: читаем по индексам A=0, B=1, D=3, F=5
        df = df.iloc[:, [0, 1, 3, 5]].copy()
        df.columns = ['пабрикушка', 'вес', 'название_номера', 'исходный_номер']
    else:
        raise ValueError("Во втором файле недостаточно столбцов (нужны A, B, D, F)")

    # Заменяем NaN
    df['пабрикушка'] = df['пабрикушка'].fillna('').astype(str).str.strip()
    df['название_номера'] = df['название_номера'].fillna('').astype(str).str.strip()
    df['исходный_номер'] = df['исходный_номер'].fillna('').astype(str).str.strip()
    df['вес'] = pd.to_numeric(df['вес'], errors='coerce').fillna(0)

    # Удаляем строки с пустыми ключевыми полями (и номер, и название пусты)
    df = df[~((df['исходный_номер'] == '') & (df['название_номера'] == ''))]

    # Классифицируем номера и извлекаем значения
    classification_stats = {'Э': 0, 'К': 0, 'Склад': 0, 'Неопределённый': 0, 'Всего': len(df)}
    
    def process_number(row):
        raw_num = row['исходный_номер']
        
        # Сначала проверяем пользовательские классификации
        if number_classifications and raw_num in number_classifications:
            num_type, extracted = number_classifications[raw_num]
            classification_stats[num_type] += 1
            return pd.Series({'тип_номера': num_type, 'переработанный_номер': extracted})
        
        # Автоматическая классификация
        num_type, extracted = classify_number(raw_num)
        classification_stats[num_type] += 1
        
        return pd.Series({'тип_номера': num_type, 'переработанный_номер': extracted})
    
    # Применяем классификацию
    classified = df.apply(process_number, axis=1)
    df['тип_номера'] = classified['тип_номера']
    df['переработанный_номер'] = classified['переработанный_номер']
    
    # Исключаем строки с типом "Склад"
    df_sklad = df[df['тип_номера'] == 'Склад'].copy()
    df = df[df['тип_номера'] != 'Склад']
    
    # Исключаем строки с типом "Неопределённый" (они пойдут в ошибки)
    df_undefined = df[df['тип_номера'] == 'Неопределённый'].copy()
    df = df[df['тип_номера'] != 'Неопределённый']
    
    # Создаем словари для быстрого поиска финального номера
    # Для типа Э: ищем по столбцу U (список_номеров)
    lookup_e = {}  # {номер: финальный_номер}
    # Для типа К: ищем по столбцу K (индекс 10)
    lookup_k = {}  # {номер: финальный_номер}
    
    # Строим lookup для типа Э (столбец U → D)
    for _, row in sprav_df.iterrows():
        numbers = row['список_номеров'].split(',')
        final_num = row['финальный_номер']
        for num in numbers:
            num = num.strip()
            if num:
                lookup_e[num] = final_num

    # Строим словарь {финальный_номер: название_номера_справочник} для заполнения пустых названий
    field_name_lookup = {}  # {финальный_номер: название_номера_справочник}
    for _, row in sprav_df.iterrows():
        final_num = row['финальный_номер']
        field_name = row['название_номера_справочник']
        if final_num and field_name:
            # Если для одного финального номера несколько названий — берём первое
            if final_num not in field_name_lookup:
                field_name_lookup[final_num] = field_name
    
    # Строим lookup для типа К (столбец K → D)
    # Читаем полный справочник для получения столбца K
    try:
        sprav_full_path = str(file_path).replace('Копия report_', 'Plan ').replace('.xlsx', '.xlsx')
        if not Path(sprav_full_path).exists():
            # Пробуем другие варианты имени
            sprav_full_path = str(file_path.parent / 'Plan_SPP_2026_ENAPKH_V16_02.02.202.xlsx')

        if Path(sprav_full_path).exists():
            sprav_k = pd.read_excel(sprav_full_path, sheet_name="кадастровые номера полей", header=0, dtype=str)
            if sprav_k.shape[1] > 10:  # Есть столбец K (индекс 10)
                for _, row in sprav_k.iterrows():
                    k_val = row.iloc[10] if len(row) > 10 else None  # Столбец K
                    d_val = row.iloc[3] if len(row) > 3 else None    # Столбец D
                    f_val = row.iloc[5] if len(row) > 5 else None    # Столбец F (название)
                    if pd.notna(k_val) and pd.notna(d_val):
                        k_str = str(k_val).strip()
                        d_str = str(d_val).strip()
                        if k_str:
                            lookup_k[k_str] = d_str
                            # Добавляем название из столбца F для заполнения пустых названий
                            if pd.notna(f_val):
                                f_str = str(f_val).strip()
                                if d_str and f_str and d_str not in field_name_lookup:
                                    field_name_lookup[d_str] = f_str
    except Exception as e:
        print(f"Предупреждение: не удалось прочитать справочник для типа К: {e}")
    
    # Если lookup_k пуст, пробуем использовать sprav_df (столбец U → D) для типа К
    # Это fallback на случай, если файл для столбца K не найден
    if not lookup_k and len(lookup_e) > 0:
        # Используем тот же lookup для типа К
        lookup_k = lookup_e.copy()
    
    # Применяем поиск финального номера в зависимости от типа
    def find_final_number(row):
        num_type = row['тип_номера']
        extracted = row['переработанный_номер']
        
        if num_type == 'Э':
            return lookup_e.get(extracted, '')
        elif num_type == 'К':
            return lookup_k.get(extracted, '')
        return ''
    
    df['финальный_номер'] = df.apply(find_final_number, axis=1)

    # Если название_номера пустое — добираем из справочника по финальному номеру
    def fill_field_name(row):
        if row['название_номера']:
            return row['название_номера']
        return field_name_lookup.get(row['финальный_номер'], '')

    df['название_номера'] = df.apply(fill_field_name, axis=1)

    # Разделяем на валидные и ошибочные
    mask_ok = df['финальный_номер'] != ''
    df_ok = df[mask_ok].copy()
    df_errors = df[~mask_ok].copy()
    
    # Добавляем неопределённые номера в ошибки
    if not df_undefined.empty:
        df_undefined['финальный_номер'] = ''
        df_undefined['ошибка'] = 'Неопределённый тип номера'
        df_errors = pd.concat([df_errors, df_undefined], ignore_index=True)
    
    # Добавляем Склад в ошибки (для информации)
    if not df_sklad.empty:
        df_sklad['финальный_номер'] = ''
        df_sklad['ошибка'] = 'Тип Склад - исключено'
    
    # Для ошибок подготовим запись: все поля + сообщение
    if not df_errors.empty:
        df_errors['ошибка'] = df_errors.apply(
            lambda row: row.get('ошибка', 'Не найден финальный номер в справочнике'), 
            axis=1
        )
    
    # Для валидных формируем ключ (без учёта регистра)
    df_ok['ключ'] = df_ok['финальный_номер'].str.lower() + '|' + df_ok['название_номера'].str.lower()
    
    return df_ok, df_errors, classification_stats

# ------------------------ Исключение пабрикушек по соотношениям ------------------------
def exclude_pabricushki(df, excluded_list: List[str]):
    """
    Исключает строки, где пабрикушка есть в списке исключённых.
    excluded_list: список названий пабрикушек, которые нужно исключить
    (включая "семена", "НЕ ВХОДИТ В ПАТ" и пользовательские исключения)
    Возвращает отфильтрованный DataFrame.
    """
    # Создаём маску для строк, которые нужно исключить
    mask = df['пабрикушка'].isin(excluded_list)
    return df[~mask]


def apply_mappings_to_df(df: pd.DataFrame, mappings: Dict[str, Optional[str]]) -> pd.DataFrame:
    """
    Применяет соотношения пабрикушек к DataFrame.
    mappings: {pab1: pab2} или {pab1: None} для исключённых
    
    Возвращает DataFrame с новой колонкой 'пабрикушка_mapped' 
    (содержит pab2 или None для исключённых)
    """
    df = df.copy()
    df['пабрикушка_mapped'] = df['пабрикушка'].map(mappings)
    return df

# ------------------------ Агрегация данных по ключу ------------------------
def aggregate_by_key(df, source_name):
    """
    Агрегирует данные по ключу:
        - список уникальных пабрикушек (через '; ')
        - словарь пабрикушка -> суммарный вес
    Возвращает два словаря:
        pab_dict: {ключ: список_пабрикушек} (уникальные, отсортированные)
        weight_dict: {ключ: {пабрикушка: сумма_весов}}
    """
    pab_dict = {}
    weight_dict = {}

    # Группируем по ключу и пабрикушке, суммируем вес
    grouped = df.groupby(['ключ', 'пабрикушка'])['вес'].sum().reset_index()

    for key in grouped['ключ'].unique():
        subset = grouped[grouped['ключ'] == key]
        # Уникальные пабрикушки (уже уникальны после groupby, но на всякий случай)
        pab_list = sorted(subset['пабрикушка'].tolist())
        pab_dict[key] = pab_list
        weight_dict[key] = dict(zip(subset['пабрикушка'], subset['вес']))

    return pab_dict, weight_dict

# ------------------------ Формирование итоговой таблицы ------------------------
def build_result_table(df1, df2, second_g_values):
    """
    df1 - агрегированные данные первого файла (pab_dict1, weight_dict1)
    df2 - аналогично для второго
    second_g_values - словарь {ключ: список уникальных переработанных номеров G} из второго файла
    Возвращает DataFrame итоговой таблицы.
    """
    pab1, w1 = df1
    pab2, w2 = df2

    all_keys = sorted(set(pab1.keys()) | set(pab2.keys()))

    rows = []
    for key in all_keys:
        # Разделяем ключ на номер и название
        if '|' in key:
            number, name = key.split('|', 1)
        else:
            number, name = key, ''  # на случай некорректного ключа

        # Пабрикушки из первого файла (исключаем только с весом = 0)
        pab_list1 = pab1.get(key, [])
        pab_list2 = pab2.get(key, [])
        
        # Фильтруем пабрикушки с нулевым весом (отрицательные оставляем)
        pab_list1_nonzero = [pab for pab in pab_list1 if w1.get(key, {}).get(pab, 0) != 0]
        pab_list2_nonzero = [pab for pab in pab_list2 if w2.get(key, {}).get(pab, 0) != 0]

        # Множества для сравнения (только с ненулевым весом)
        set1 = set(pab_list1_nonzero)
        set2 = set(pab_list2_nonzero)

        # Общие, только в первом, только во втором
        common = set1 & set2
        only1 = set1 - set2
        only2 = set2 - set1

        # Формируем строки весов
        def format_weight_line(pab, weight):
            # Вес с двумя знаками после запятой и запятой
            w_str = f"{weight:.2f}".replace('.', ',')
            return f"{pab} - {w_str}"

        # E: веса из первого (исключаем только вес = 0)
        lines_e = []
        for pab in pab_list1_nonzero:
            w = w1.get(key, {}).get(pab, 0)
            if w != 0:
                lines_e.append(format_weight_line(pab, w))
        e_text = '\n'.join(lines_e) if lines_e else ''

        # G: веса из второго (исключаем только вес = 0)
        lines_g = []
        for pab in pab_list2_nonzero:
            w = w2.get(key, {}).get(pab, 0)
            if w != 0:
                lines_g.append(format_weight_line(pab, w))
        g_text = '\n'.join(lines_g) if lines_g else ''

        # D: пабрикушки из первого (исключаем только вес = 0)
        d_text = '; '.join(pab_list1_nonzero) if pab_list1_nonzero else ''

        # F: пабрикушки из второго (исключаем только вес = 0)
        f_text = '; '.join(pab_list2_nonzero) if pab_list2_nonzero else ''

        # I: только в первом
        i_text = '; '.join(sorted(only1)) if only1 else ''

        # J: только во втором
        j_text = '; '.join(sorted(only2)) if only2 else ''

                # Функция сравнения весов с округлением до 2 знаков
        def weights_equal(a, b):
            return round(a, 2) == round(b, 2)

        # K: расхождения по весам для общих
        k_lines = []
        for pab in sorted(common):
            w1_val = w1.get(key, {}).get(pab, 0)
            w2_val = w2.get(key, {}).get(pab, 0)
            if not weights_equal(w1_val, w2_val):
                diff = w1_val - w2_val
                line = f"{pab}: {w1_val:.2f} vs {w2_val:.2f} (разница {diff:.2f})"
                line = line.replace('.', ',')
                k_lines.append(line)
        k_text = '\n'.join(k_lines) if k_lines else ''

        # Статус
        same_composition = (set1 == set2)
        weight_mismatch = any(
            not weights_equal(w1.get(key, {}).get(pab, 0), w2.get(key, {}).get(pab, 0))
            for pab in common
        )

        if same_composition:
            if not weight_mismatch:
                status = "все хорошо"
            else:
                status = "состав одинаковый, но есть расхождения веса"
        else:
            if not weight_mismatch:
                status = "состав разный"
            else:
                status = "состав разный и есть расхождения веса по общим позициям"

        # Колонка B: уникальные переработанные номера G для данного ключа
        g_values = second_g_values.get(key, [])
        b_text = '; '.join(sorted(g_values)) if g_values else ''

        # Собираем строку
        rows.append({
            'A': number,
            'B': b_text,
            'C': name,
            'D': d_text,
            'E': e_text,
            'F': f_text,
            'G': g_text,
            'H': status,
            'I': i_text,
            'J': j_text,
            'K': k_text
        })

    result_df = pd.DataFrame(rows, columns=['A','B','C','D','E','F','G','H','I','J','K'])
    return result_df

# ------------------------ Сохранение результата в Excel ------------------------
def save_excel(result_df, errors_df, output_path):
    """
    Сохраняет два листа в один Excel-файл с использованием xlsxwriter для форматирования.
    """
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        # Записываем итоговый лист
        result_df.to_excel(writer, sheet_name='Новый итог', index=False)

        # Записываем ошибки, если есть
        if not errors_df.empty:
            # Выбираем колонки для ошибок
            err_cols = ['пабрикушка', 'вес', 'название_номера', 'исходный_номер', 'переработанный_номер', 'ошибка']
            # Убедимся, что все колонки есть
            for col in err_cols:
                if col not in errors_df.columns:
                    errors_df[col] = ''
            errors_df[err_cols].to_excel(writer, sheet_name='Ошибки', index=False)

        # Доступ к workbook и worksheet для настройки формата
        workbook = writer.book
        worksheet = writer.sheets['Новый итог']

        # Устанавливаем формат для числовых колонок (но они у нас текст, т.к. содержат переносы)
        # Можно задать ширину столбцов
        worksheet.set_column('A:A', 15)
        worksheet.set_column('B:B', 20)
        worksheet.set_column('C:C', 20)
        worksheet.set_column('D:D', 25)
        worksheet.set_column('E:E', 30)
        worksheet.set_column('F:F', 25)
        worksheet.set_column('G:G', 30)
        worksheet.set_column('H:H', 30)
        worksheet.set_column('I:I', 25)
        worksheet.set_column('J:J', 25)
        worksheet.set_column('K:K', 40)

        # Включаем перенос текста для многострочных ячеек
        wrap_format = workbook.add_format({'text_wrap': True})
        for col in ['E', 'G', 'K']:
            worksheet.set_column(f'{col}:{col}', None, wrap_format)

# ------------------------ Основная функция ------------------------
def process_files(first_file, second_file, sprav_file, output_file,
                  place_name: str = None,
                  pab_mappings: Dict[str, Optional[str]] = None,
                  field_mappings: Dict[str, Optional[str]] = None,
                  number_classifications: Dict[str, Tuple[NumberType, str]] = None):
    """
    Полный цикл обработки.

    Args:
        first_file: путь к первому файлу (TDSheet)
        second_file: путь ко второму файлу (Лист1)
        sprav_file: путь к справочнику (кадастровые номера полей)
        output_file: путь для сохранения результата
        place_name: название места (для загрузки соотношений из БД)
        pab_mappings: готовые соотношения пабрикушек {pab1: pab2}
        field_mappings: готовые соотношения названий полей {field1: field2}
        number_classifications: словарь {исходный_номер: (тип, извлечённое_значение)}
    """
    # 1. Чтение справочника
    sprav_df = read_spravochnik(sprav_file)

    # 2. Загружаем соотношения для места из БД если не переданы
    if pab_mappings is None and place_name:
        from database import get_all_mappings, get_excluded_pabricushki, get_all_field_mappings, get_all_number_classifications
        pab_mappings = get_all_mappings(place_name)
        excluded_list = get_excluded_pabricushki(place_name)
        if field_mappings is None:
            field_mappings = get_all_field_mappings(place_name)
        # Загружаем пользовательские классификации номеров из БД
        if number_classifications is None:
            number_classifications = get_all_number_classifications(place_name)
    elif pab_mappings is not None:
        excluded_list = [pab1 for pab1, pab2 in pab_mappings.items() if pab2 is None]
    else:
        pab_mappings = {}
        excluded_list = []
        field_mappings = {}

    # 3. Чтение первого файла
    df1_raw = read_first_file(first_file)

    # Применяем соотношения пабрикушек к первому файлу
    if pab_mappings:
        df1_raw = apply_mappings_to_df(df1_raw, pab_mappings)
        # Исключаем строки с None в пабрикушка_mapped
        df1_raw = df1_raw[df1_raw['пабрикушка_mapped'].notna()]
        # Переименовываем для совместимости
        df1_raw['пабрикушка'] = df1_raw['пабрикушка_mapped']

    # Применяем соотношения названий полей
    if field_mappings:
        df1_raw['название_номера_orig'] = df1_raw['название_номера']
        # Получаем список исключённых полей (где mapping = None)
        excluded_fields = {f for f, m in field_mappings.items() if m is None}

        # Исключаем строки с исключёнными названиями полей ДО применения маппинга
        df1_raw = df1_raw[~df1_raw['название_номера'].isin(excluded_fields)]

        # Применяем маппинг к оставшимся полям
        df1_raw['название_номера'] = df1_raw['название_номера'].apply(
            lambda x: field_mappings[x] if (x in field_mappings and field_mappings[x] is not None) else x
        )

    # Дополнительно исключаем по списку (для старых записей)
    if excluded_list:
        df1_raw = exclude_pabricushki(df1_raw, excluded_list)

    # Формируем ключ (номер в файле 1 уже финальный)
    df1_raw['ключ'] = df1_raw['номер'].str.lower() + '|' + df1_raw['название_номера'].str.lower()

    # Агрегируем
    pab1, w1 = aggregate_by_key(df1_raw, 'first')

    # 4. Чтение второго файла с обработкой по справочнику
    df2_ok, df2_errors, classification_stats = read_second_file(second_file, sprav_df, number_classifications)

    # Исключаем пабрикушки по списку исключений
    if excluded_list:
        df2_ok = exclude_pabricushki(df2_ok, excluded_list)

    # Для файла 2 маппинг field1→field2 НЕ применяем: там уже стоят итоговые названия (field2).
    # Только исключаем поля, у которых mapping = None (исключённые пользователем).
    # Исключённые поля определяются по значениям field2 (правая часть маппинга = None)
    if field_mappings:
        excluded_field1_keys = {f for f, m in field_mappings.items() if m is None}
        # Получаем field2-значения исключённых field1, чтобы найти их в файле 2
        # (если field1 исключён — его field2 пары нет, но на всякий случай проверяем оба)
        df2_ok = df2_ok[~df2_ok['название_номера'].isin(excluded_field1_keys)]

    # Агрегируем (уже есть ключ)
    pab2, w2 = aggregate_by_key(df2_ok, 'second')

    # 5. Собираем уникальные переработанные номера G для колонки B по ключу
    g_by_key = {}
    for _, row in df2_ok.iterrows():
        key = row['ключ']
        g_val = row['переработанный_номер']
        if key not in g_by_key:
            g_by_key[key] = set()
        g_by_key[key].add(g_val)
    g_by_key = {k: list(v) for k, v in g_by_key.items()}

    # 6. Формируем итоговую таблицу
    result_df = build_result_table((pab1, w1), (pab2, w2), g_by_key)

    # 7. Сохраняем результат
    save_excel(result_df, df2_errors, output_file)

    # 8. Модифицируем исходные файлы в папке сессии — заполняем пустые названия и финальные номера
    # Сохраняем модифицированный файл 1 (с финальными номерами и названиями)
    df1_save = df1_raw[['пабрикушка', 'номер', 'название_номера', 'вес']].copy()
    with pd.ExcelWriter(first_file, engine='openpyxl') as writer:
        df1_save.to_excel(writer, sheet_name="TDSheet", index=False)

    # Сохраняем модифицированный файл 2 (с финальными номерами и заполненными названиями)
    df2_save = df2_ok[['пабрикушка', 'вес', 'название_номера', 'финальный_номер', 'переработанный_номер']].copy()
    with pd.ExcelWriter(second_file, engine='openpyxl') as writer:
        df2_save.to_excel(writer, sheet_name="Лист1", index=False)

    print(f"Обработка завершена. Результат сохранён в {output_file}")
    print(f"Файлы сессии модифицированы: заполнены пустые названия, номера заменены на финальные")
    if not df2_errors.empty:
        print(f"Количество ошибочных строк во втором файле: {len(df2_errors)}")


# ------------------------ Получение уникальных пабрикушек для сопоставления ------------------------
def get_unique_pabricushki(first_file, second_file, sprav_file) -> Tuple[List[str], List[str]]:
    """
    Получить уникальные названия пабрикушек из первого и второго файлов.
    Возвращает кортеж (pab1_list, pab2_list) отсортированных списков.
    """
    # Чтение первого файла
    df1 = read_first_file(first_file)
    pab1_list = sorted(df1['пабрикушка'].unique().tolist())

    # Чтение второго файла - берём ВСЕ пабрикушки напрямую, без проверки справочника
    # Файл 2 уже модифицирован preprocess_second_file — имеет заголовок в первой строке
    try:
        df2_raw = pd.read_excel(second_file, sheet_name="Лист1", header=0, dtype=str)
    except Exception as e:
        raise ValueError(f"Ошибка чтения второго файла: {e}")

    print(f"[get_unique_pabricushki] Столбцы файла 2: {list(df2_raw.columns)}")
    print(f"[get_unique_pabricushki] Строк в файле 2: {len(df2_raw)}")

    # Проверяем наличие столбца 'пабрикушка' (модифицированный файл)
    if 'пабрикушка' in df2_raw.columns:
        df2_raw['пабрикушка'] = df2_raw['пабрикушка'].fillna('').astype(str).str.strip()
        df2_raw = df2_raw[df2_raw['пабрикушка'] != '']
        pab2_list = sorted(df2_raw['пабрикушка'].unique().tolist())
        print(f"[get_unique_pabricushki] Найдено пабрикушек из файла 2: {len(pab2_list)}")
        if pab2_list:
            print(f"[get_unique_pabricushki] Примеры: {pab2_list[:3]}")
    else:
        # Fallback: читаем столбец A (индекс 0)
        if df2_raw.shape[1] < 1:
            raise ValueError("Во втором файле недостаточно столбцов")
        df2_raw = df2_raw.iloc[:, [0]].copy()
        df2_raw.columns = ['пабрикушка']
        df2_raw['пабрикушка'] = df2_raw['пабрикушка'].fillna('').astype(str).str.strip()
        df2_raw = df2_raw[df2_raw['пабрикушка'] != '']
        pab2_list = sorted(df2_raw['пабрикушка'].unique().tolist())
        print(f"[get_unique_pabricushki] Fallback: найдено пабрикушек: {len(pab2_list)}")

    return pab1_list, pab2_list


# ------------------------ Вспомогательная: словарь названий из справочника ------------------------
def build_field_name_lookup(sprav_df):
    """
    Строит словарь {финальный_номер: название_номера} из справочника.
    Используется для заполнения пустых названий полей.
    """
    lookup = {}
    for _, row in sprav_df.iterrows():
        final_num = row['финальный_номер']
        field_name = row['название_номера_справочник']
        if final_num and field_name and final_num not in lookup:
            lookup[final_num] = field_name
    return lookup


# ------------------------ Вспомогательная: классификация и поиск финального номера ------------------------
def classify_and_find_final(raw_num, lookup_e, lookup_k):
    """
    Классифицирует номер и находит финальный номер в справочнике.
    Возвращает (тип_номера, финальный_номер).
    """
    if not raw_num:
        return (None, '')

    num_type, extracted = classify_number(raw_num)

    if num_type == 'Э':
        return (num_type, lookup_e.get(extracted or '', ''))
    elif num_type == 'К':
        return (num_type, lookup_k.get(extracted or '', ''))

    return (num_type, '')


# ------------------------ Предобработка файла 2: заполнение пустых названий и финальных номеров ------------------------
def preprocess_second_file(file_path, sprav_file):
    """
    Предобработка файла 2: классифицирует номера, находит финальные номера,
    заполняет пустые названия из справочника.
    Модифицирует существующие столбцы на месте, сохраняет обратно.
    """
    # Чтение справочника
    sprav_df = read_spravochnik(sprav_file)

    # Строим lookup для типа Э
    lookup_e = {}
    for _, row in sprav_df.iterrows():
        numbers = row['список_номеров'].split(',')
        final_num = row['финальный_номер']
        for num in numbers:
            num = num.strip()
            if num:
                lookup_e[num] = final_num

    # Строим lookup для типа К и словарь названий
    lookup_k = {}
    field_name_lookup = {}
    try:
        sprav_full_path = str(sprav_file).replace('Копия report_', 'Plan ').replace('.xlsx', '.xlsx')
        if not Path(sprav_full_path).exists():
            sprav_full_path = str(sprav_file.parent / 'Plan_SPP_2026_ENAPKH_V16_02.02.202.xlsx')

        if Path(sprav_full_path).exists():
            sprav_k = pd.read_excel(sprav_full_path, sheet_name="кадастровые номера полей", header=0, dtype=str)
            if sprav_k.shape[1] > 10:
                for _, row in sprav_k.iterrows():
                    k_val = row.iloc[10] if len(row) > 10 else None
                    d_val = row.iloc[3] if len(row) > 3 else None
                    f_val = row.iloc[5] if len(row) > 5 else None
                    if pd.notna(k_val) and pd.notna(d_val):
                        k_str = str(k_val).strip()
                        d_str = str(d_val).strip()
                        if k_str:
                            lookup_k[k_str] = d_str
                            if pd.notna(f_val):
                                f_str = str(f_val).strip()
                                if d_str and f_str and d_str not in field_name_lookup:
                                    field_name_lookup[d_str] = f_str
    except Exception as e:
        print(f"[preprocess] Предупреждение: не удалось прочитать справочник для типа К: {e}")

    if not lookup_k and len(lookup_e) > 0:
        lookup_k = lookup_e.copy()

    # Заполняем field_name_lookup из основного справочника
    for _, row in sprav_df.iterrows():
        final_num = row['финальный_номер']
        field_name = row['название_номера_справочник']
        if final_num and field_name and final_num not in field_name_lookup:
            field_name_lookup[final_num] = field_name

    # Читаем ВЕСЬ файл 2 (без skiprows, чтобы сохранить структуру)
    try:
        df_full = pd.read_excel(file_path, sheet_name="Лист1", header=None, dtype=str)
    except Exception as e:
        raise ValueError(f"Ошибка чтения файла 2: {e}")

    # Данные начинаются с 5-й строки (индекс 4), пропуская 4 строки заголовков
    data_start_row = 4

    # Столбцы: A=0(пабрикушка), B=1(вес), D=3(название_номера), F=5(исходный_номер)
    if df_full.shape[1] < 6:
        raise ValueError("Во втором файле недостаточно столбцов")

    # Извлекаем только строки с данными
    df_data = df_full.iloc[data_start_row:].copy()
    df_data.columns = ['пабрикушка', 'вес', 'col_2', 'название_номера', 'col_4', 'исходный_номер']

    # Добавляем столбец для финальных номеров
    df_data['финальный_номер'] = ''

    # Обрабатываем все строки с данными
    for idx in df_data.index:
        raw_num = str(df_data.at[idx, 'исходный_номер']).strip() if pd.notna(df_data.at[idx, 'исходный_номер']) else ''
        name_val = str(df_data.at[idx, 'название_номера']).strip() if pd.notna(df_data.at[idx, 'название_номера']) else ''

        # Находим финальный номер
        final_num = ''
        if raw_num:
            num_type, extracted = classify_number(raw_num)
            if num_type == 'Э':
                final_num = lookup_e.get(extracted or '', '')
            elif num_type == 'К':
                final_num = lookup_k.get(extracted or '', '')

        # Записываем финальный номер
        df_data.at[idx, 'финальный_номер'] = final_num

        # Заполняем пустое название из справочника
        if not name_val and final_num:
            df_data.at[idx, 'название_номера'] = field_name_lookup.get(final_num, '')

    # Сохраняем только строки данных с заголовками
    df_save = df_data[['пабрикушка', 'вес', 'название_номера', 'исходный_номер', 'финальный_номер']].copy()
    
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df_save.to_excel(writer, sheet_name="Лист1", index=False)

    # Отладка
    pab_count = (df_save['пабрикушка'] != '').sum()
    filled_final = (df_save['финальный_номер'] != '').sum()
    empty_names = (df_save['название_номера'] == '').sum()
    print(f"[preprocess] Файл 2 модифицирован: {file_path}")
    print(f"[preprocess] Сохранено строк с данными: {len(df_save)}, пабрикушек: {pab_count}")
    print(f"[preprocess] Столбцы: {list(df_save.columns)}")
    print(f"[preprocess] Пустых названий осталось: {empty_names}")
    print(f"[preprocess] Финальных номеров заполнено: {filled_final}")
    if pab_count > 0:
        print(f"[preprocess] Примеры пабрикушек: {df_save[df_save['пабрикушка'] != '']['пабрикушка'].head(3).tolist()}")


# ------------------------ Получение уникальных названий полей для сопоставления ------------------------
def get_unique_fields(first_file, second_file, sprav_file) -> Tuple[List[str], List[str]]:
    """
    Получить уникальные названия полей из первого файла (столбец G)
    и второго файла (столбец D). Файл 2 уже модифицирован (названия заполнены).
    Возвращает кортеж (field1_list, field2_list) отсортированных списков.
    """
    # Чтение первого файла - столбец G (индекс 6)
    df1 = read_first_file(first_file)
    field1_list = sorted(df1['название_номера'].unique().tolist())

    # Чтение второго файла - столбец D (название_номера, индекс 3)
    # Файл 2 уже модифицирован - названия заполнены из справочника
    try:
        df2_raw = pd.read_excel(second_file, sheet_name="Лист1", header=0, dtype=str)
    except Exception as e:
        raise ValueError(f"Ошибка чтения второго файла: {e}")

    # Проверяем наличие столбца 'название_номера' (модифицированный файл)
    if 'название_номера' in df2_raw.columns:
        field2_list = sorted(df2_raw['название_номера'].dropna().astype(str).str.strip().unique().tolist())
        field2_list = [f for f in field2_list if f]  # Убираем пустые
    else:
        # Fallback: читаем столбец D (индекс 3)
        if df2_raw.shape[1] < 4:
            raise ValueError("Во втором файле недостаточно столбцов")
        df2_raw = df2_raw.iloc[:, [3]].copy()
        df2_raw.columns = ['название_номера']
        df2_raw['название_номера'] = df2_raw['название_номера'].fillna('').astype(str).str.strip()
        df2_raw = df2_raw[df2_raw['название_номера'] != '']
        field2_list = sorted(df2_raw['название_номера'].unique().tolist())

    return field1_list, field2_list


# ------------------------ Получение неопределённых номеров для классификации ------------------------
def get_undefined_numbers(second_file, saved_classifications: Dict[str, Tuple[str, str]] = None) -> Tuple[List[str], Dict[str, int]]:
    """
    Получить уникальные неопределённые номера из второго файла (столбец F).
    Возвращает кортеж (undefined_numbers, stats) где:
        - undefined_numbers: список уникальных номеров, которые не определились автоматически
        - stats: полная статистика классификации {Э: N, К: N, Склад: N, Неопределённый: N, Всего: N}
    
    Args:
        second_file: путь ко второму файлу
        saved_classifications: словарь {raw_number: (number_type, extracted_value)} - сохранённые классификации из БД
    """
    try:
        # Файл 2 уже модифицирован preprocess_second_file — имеет заголовки
        df = pd.read_excel(second_file, sheet_name="Лист1", header=0, dtype=str)
    except Exception as e:
        raise ValueError(f"Ошибка чтения второго файла: {e}")

    # Столбец 'исходный_номер' (был F)
    if 'исходный_номер' in df.columns:
        df = df[['исходный_номер']].copy()
    elif df.shape[1] > 5:
        # Fallback: столбец F (индекс 5)
        df = df.iloc[:, [5]].copy()
        df.columns = ['исходный_номер']
    else:
        raise ValueError("Во втором файле недостаточно столбцов")

    # Заменяем NaN и пустые значения
    df['исходный_номер'] = df['исходный_номер'].fillna('').astype(str).str.strip()

    # Удаляем строки с пустым номером
    df = df[df['исходный_номер'] != '']

    # Считаем статистику по ВСЕМ строкам с учётом сохранённых классификаций
    stats = {'Э': 0, 'К': 0, 'Склад': 0, 'Неопределённый': 0, 'Всего': len(df)}
    undefined_set = set()

    # Считаем статистику по всем строкам
    for raw_num in df['исходный_номер']:
        # Сначала проверяем сохранённые классификации
        if saved_classifications and raw_num in saved_classifications:
            num_type = saved_classifications[raw_num][0]
            stats[num_type] += 1
            # Сохранённые номера не добавляем в неопределённые
        else:
            # Автоматическая классификация
            num_type, _ = classify_number(raw_num)
            stats[num_type] += 1
            if num_type == 'Неопределённый':
                undefined_set.add(raw_num)

    # Уникальных номеров для отображения
    stats['Уникальных всего'] = len(df['исходный_номер'].unique())

    return sorted(list(undefined_set)), stats


def auto_map_fields(
    field1_list: List[str],
    field2_list: List[str],
    existing_mappings: Dict[str, Optional[str]]
) -> Dict[str, dict]:
    """
    Автоматически сопоставить названия полей используя fuzzy matching.
    existing_mappings: уже сохранённые соотношения для этого места.
    Возвращает полный словарь {field1: {'value': field2 или None, 'source': 'db'|'auto'|None}}.

    source:
      - 'db' — значение взято из БД и есть в текущем файле 2
      - 'db_missing' — значение из БД, но его НЕТ в текущем файле 2 (не выбрано)
      - 'auto' — авто-сопоставление через fuzzy matching
      - 'excluded' — исключено
      - None — нет совпадения, пользователь должен выбрать вручную
    """
    field2_set = set(field2_list)
    result = {}

    # Сначала применяем существующие соотношения из БД
    for field1 in field1_list:
        if field1 in existing_mappings:
            db_value = existing_mappings[field1]
            if db_value is None:
                # Исключено в БД
                result[field1] = {'value': None, 'source': 'excluded'}
            elif db_value in field2_set:
                # Значение из БД есть в текущем файле 2
                result[field1] = {'value': db_value, 'source': 'db'}
            else:
                # Значение из БД отсутствует в текущем файле 2 — не выбираем автоматически
                result[field1] = {'value': None, 'source': 'db_missing'}

    # Для новых полей пытаемся найти авто-сопоставление
    for field1 in field1_list:
        if field1 in result:
            continue

        # Ищем похожее название во 2-м файле
        match = fuzzy_match(field1, field2_list)
        if match:
            result[field1] = {'value': match, 'source': 'auto'}
        else:
            result[field1] = {'value': None, 'source': None}  # Нет совпадения — пользователь должен выбрать вручную

    return result

# ------------------------ Пример использования ------------------------
if __name__ == "__main__":
    # Укажите пути к вашим файлам
    file1_path = "/Users/arsa/Downloads/Копия применение пат на культуру и поле СНТ 2025 v2.xlsx"
    file2_path = "/Users/arsa/Downloads/Копия report_2026-03-12T12_39_18.320Z Северная Нива Татарстан.xlsx"
    file3_path = "/Users/arsa/Downloads/Plan SPP 2026 ENAPKH V16 02.02.202.xlsx"
    output_path = "result.xlsx"

    process_files(file1_path, file2_path, file3_path, output_path)
