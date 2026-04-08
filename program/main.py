import os
import tempfile
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from database import get_all_places, add_place, get_all_mappings, save_mappings, get_all_field_mappings, save_field_mappings, get_all_number_classifications, save_number_classifications
from comparator import (
    process_files,
    get_unique_pabricushki,
    auto_map_pabricushki,
    get_unique_fields,
    auto_map_fields,
    read_first_file,
    read_spravochnik,
    read_second_file,
    get_undefined_numbers,
    preprocess_second_file
)

BASE_DIR = Path(__file__).parent

# Конфигурация безопасности
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB

app = FastAPI(title="Объединение и сравнение данных из Excel")

# Настройка CORS для публичного доступа
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене заменить на конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Настройка шаблонов и статики
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Создаём временную директорию для загруженных файлов
# Используем папку uploads в проекте
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)  # Гарантируем существование

# Хранилище сессий с путями к файлам и временем создания
SESSIONS = {}

def cleanup_old_sessions(max_age_hours=24):
    """Очистить старые сессии и удалить соответствующие файлы."""
    now = datetime.now()
    to_delete = []
    
    for session_id, session_data in list(SESSIONS.items()):
        created_time = session_data.get("created_time")
        if created_time and (now - created_time) > timedelta(hours=max_age_hours):
            to_delete.append(session_id)
    
    for session_id in to_delete:
        # Удаляем файлы сессии
        session_dir = UPLOAD_DIR / session_id
        if session_dir.exists():
            shutil.rmtree(session_dir, ignore_errors=True)
        # Удаляем из памяти
        SESSIONS.pop(session_id, None)
    
    return len(to_delete)


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/api/places")
async def get_places():
    """Получить список всех мест."""
    places = get_all_places()
    return {"places": places}


@app.post("/api/places")
async def create_place(name: str = Form(...)):
    """Добавить новое место."""
    if not name or not name.strip():
        raise HTTPException(status_code=400, detail="Название места не может быть пустым")
    
    if add_place(name.strip()):
        return {"success": True, "name": name.strip()}
    else:
        raise HTTPException(status_code=400, detail="Такое место уже существует")


@app.post("/api/analyze")
async def analyze_files(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    file3: UploadFile = File(...)
):
    """
    Проанализировать загруженные файлы, получить уникальные пабрикушки и названия полей.
    Возвращает session_id и списки для сопоставления.
    """
    try:
        # Проверка размера файлов
        for file in [file1, file2, file3]:
            # Перематываем в начало файла
            await file.seek(0, 2)  # Переход в конец
            file_size = await file.tell()
            await file.seek(0)  # Возврат в начало
            
            if file_size > MAX_FILE_SIZE:
                raise HTTPException(
                    status_code=400,
                    detail=f"Файл {file.filename} слишком большой. Максимальный размер: {MAX_FILE_SIZE // (1024*1024)} MB"
                )
        
        # Создаём уникальную сессию с именем на основе времени начала
        timestamp = datetime.now().strftime("%H%M%S")
        session_dir = UPLOAD_DIR / f"session_{timestamp}"

        # Если папка уже есть (загрузка в ту же секунду), добавляем счётчик
        counter = 0
        while session_dir.exists():
            counter += 1
            session_dir = UPLOAD_DIR / f"session_{timestamp}_{counter}"

        session_dir.mkdir(parents=True, exist_ok=True)
        session_id = session_dir.name

        # Сохраняем файлы
        paths = []
        for i, file in enumerate([file1, file2, file3], start=1):
            suffix = Path(file.filename).suffix
            file_path = session_dir / f"file{i}{suffix}"
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            paths.append(file_path)

        # Сохраняем пути к файлам в сессии с временем создания
        SESSIONS[session_id] = {
            "file1": paths[0],
            "file2": paths[1],
            "file3": paths[2],
            "created_time": datetime.now()
        }

        # Предобработка файла 2: заполняем пустые названия, находим финальные номера
        preprocess_second_file(paths[1], paths[2])

        # Получаем уникальные пабрикушки
        pab1_list, pab2_list = get_unique_pabricushki(paths[0], paths[1], paths[2])

        # Получаем уникальные названия полей (из уже модифицированного файла 2)
        field1_list, field2_list = get_unique_fields(paths[0], paths[1], paths[2])

        return {
            "session_id": session_id,
            "pab1_list": pab1_list,
            "pab2_list": pab2_list,
            "field1_list": field1_list,
            "field2_list": field2_list
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/classify-numbers")
async def classify_numbers(
    session_id: str = Form(...),
    place_name: str = Form(...),
    classifications: str = Form(...)  # JSON строка {номер: тип}
):
    """
    Сохранить пользовательскую классификацию неопределённых номеров в БД.
    classifications: JSON строка вида {"номер": "Э"|"К"|"Склад"}
    """
    import json
    import re
    from comparator import classify_number

    try:
        session = SESSIONS.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Сессия не найдена")

        # Парсим classifications из JSON
        classifications_dict = json.loads(classifications)

        # Преобразуем в формат {номер: (тип, извлечённое_значение)}
        processed_classifications = {}
        for raw_num, num_type in classifications_dict.items():
            # Извлекаем значение на основе типа
            extracted = None
            
            # Для типа К ищем число с тремя двоеточиями
            if num_type == 'К':
                colon_pattern = r'\b(\d+:\d+:\d+:\d+)\b'
                match = re.search(colon_pattern, raw_num)
                if match:
                    extracted = match.group(1)
            
            # Для типа Э обрезаем до второго дефиса
            elif num_type == 'Э':
                first_dash = raw_num.find('-')
                if first_dash != -1:
                    second_dash = raw_num.find('-', first_dash + 1)
                    if second_dash != -1:
                        extracted = raw_num[:second_dash]
                    else:
                        extracted = raw_num
                else:
                    extracted = raw_num
            
            # Для типа Склад значение не нужно
            elif num_type == 'Склад':
                extracted = None

            processed_classifications[raw_num] = (num_type, extracted)

        # Сохраняем в сессии для последующего использования
        session["number_classifications"] = processed_classifications
        
        # Сохраняем в БД
        save_number_classifications(place_name, processed_classifications)

        return {"success": True}

    except HTTPException:
        raise
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Неверный формат classifications")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/saved-classifications/{place_name}")
async def get_saved_classifications(place_name: str):
    """
    Получить сохранённые классификации номеров для места из БД.
    """
    try:
        classifications = get_all_number_classifications(place_name)
        # Преобразуем в формат {номер: тип} для фронтенда
        result = {raw_num: num_type for raw_num, (num_type, _) in classifications.items()}
        return {"classifications": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/undefined-numbers/{session_id}")
async def get_undefined_numbers_endpoint(session_id: str, place_name: str = None):
    """
    Получить список неопределённых номеров и статистику классификации.
    Если указан place_name, учитываются сохранённые классификации из БД.
    """
    try:
        session = SESSIONS.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Сессия не найдена")

        # Загружаем сохранённые классификации из БД для этого места
        saved_classifications = {}
        if place_name:
            from database import get_all_number_classifications
            saved_classifications = get_all_number_classifications(place_name)

        # Получаем неопределённые номера и статистику с учётом сохранённых классификаций
        undefined_list, stats = get_undefined_numbers(session["file2"], saved_classifications)

        # Исключаем уже классифицированные номера из списка неопределённых
        # (они уже учтены в статистике, но не должны показываться для повторной классификации)
        if saved_classifications:
            undefined_list = [num for num in undefined_list if num not in saved_classifications]

        return {
            "undefined_numbers": undefined_list,
            "stats": stats,
            "saved_classifications": {k: v[0] for k, v in saved_classifications.items()}
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/auto-map")
async def auto_map(
    session_id: str = Form(...),
    place_name: str = Form(...)
):
    """
    Автоматически сопоставить пабрикушки используя fuzzy matching
    и сохранённые соотношения для места.
    Возвращает {mappings: {pab1: {value: ..., source: ...}}, pab2_list: [...]}
    """
    try:
        session = SESSIONS.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Сессия не найдена")

        # Получаем уникальные пабрикушки
        pab1_list, pab2_list = get_unique_pabricushki(
            session["file1"],
            session["file2"],
            session["file3"]
        )

        # Получаем существующие соотношения для места
        existing_mappings = get_all_mappings(place_name)

        # Автоматически сопоставляем
        mappings = auto_map_pabricushki(pab1_list, pab2_list, existing_mappings)

        return {
            "mappings": mappings,
            "pab2_list": pab2_list
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/mappings")
async def save_mappings_endpoint(
    session_id: str = Form(...),
    place_name: str = Form(...),
    mappings: str = Form(...)  # JSON строка {pab1: pab2 или null}
):
    """
    Сохранить соотношения пабрикушек для места.
    mappings: JSON строка вида {"pab1_name": "pab2_name" или null}
    null означает, что пабрикушка исключается (семена, НЕ ВХОДИТ В ПАТ)
    """
    import json
    
    try:
        session = SESSIONS.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Сессия не найдена")
        
        # Парсим mappings из JSON
        mappings_dict = json.loads(mappings)
        
        # Сохраняем в БД
        save_mappings(place_name, mappings_dict)
        
        return {"success": True}
        
    except HTTPException:
        raise
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Неверный формат mappings")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/auto-map-fields")
async def auto_map_fields_endpoint(
    session_id: str = Form(...),
    place_name: str = Form(...)
):
    """
    Автоматически сопоставить названия полей используя fuzzy matching
    и сохранённые соотношения для места.
    Возвращает {mappings: {field1: {value: ..., source: ...}}, field2_list: [...]}
    """
    try:
        session = SESSIONS.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Сессия не найдена")

        # Получаем уникальные названия полей (с заполнением пустых из справочника)
        field1_list, field2_list = get_unique_fields(
            session["file1"],
            session["file2"],
            session["file3"]
        )

        # Получаем существующие соотношения для места
        existing_mappings = get_all_field_mappings(place_name)

        # Автоматически сопоставляем
        mappings = auto_map_fields(field1_list, field2_list, existing_mappings)

        return {
            "mappings": mappings,
            "field2_list": field2_list
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/field-mappings")
async def save_field_mappings_endpoint(
    session_id: str = Form(...),
    place_name: str = Form(...),
    mappings: str = Form(...)  # JSON строка {field1: field2 или null}
):
    """
    Сохранить соотношения названий полей для места.
    mappings: JSON строка вида {"field1_name": "field2_name" или null}
    null означает, что название поля исключается
    """
    import json

    try:
        session = SESSIONS.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Сессия не найдена")

        # Парсим mappings из JSON
        mappings_dict = json.loads(mappings)

        # Сохраняем в БД
        save_field_mappings(place_name, mappings_dict)

        return {"success": True}

    except HTTPException:
        raise
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Неверный формат mappings")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/process")
async def process_files_endpoint(
    session_id: str = Form(...),
    place_name: str = Form(...)
):
    """
    Обработать файлы с использованием сохранённых соотношений для места.
    Результат сохраняется прямо в ~/Downloads/result.xlsx
    """
    try:
        session = SESSIONS.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Сессия не найдена")

        # Сохраняем результат в сессионную папку
        session_dir = UPLOAD_DIR / session_id
        session_dir.mkdir(exist_ok=True)
        output_path = session_dir / "result.xlsx"

        # Получаем классификации номеров из сессии (если есть)
        number_classifications = session.get("number_classifications", {})

        # Запускаем обработку с использованием соотношений из БД и классификаций номеров
        process_files(
            session["file1"],
            session["file2"],
            session["file3"],
            output_path,
            place_name=place_name,
            number_classifications=number_classifications
        )

        # Возвращаем путь для скачивания
        download_url = f"/download-file/{session_id}/result.xlsx"
        return {"download_url": download_url}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/upload-simple")
async def upload_files_simple(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...),
    file3: UploadFile = File(...),
    place_name: str = Form(...)
):
    """
    Упрощённая загрузка и обработка (без интерфейса сопоставления).
    Использует сохранённые соотношения из БД.
    """
    try:
        # Создаём уникальную подпапку для этой сессии
        session_dir = tempfile.mkdtemp(dir=UPLOAD_DIR)
        session_name = Path(session_dir).name
        
        paths = []
        for i, file in enumerate([file1, file2, file3], start=1):
            suffix = Path(file.filename).suffix
            file_path = Path(session_dir) / f"file{i}{suffix}"
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            paths.append(file_path)
        
        # Путь для выходного файла
        output_path = Path(session_dir) / "result.xlsx"
        
        # Запускаем обработку с использованием соотношений из БД
        process_files(paths[0], paths[1], paths[2], output_path, place_name=place_name)
        
        # Возвращаем ссылку на скачивание
        download_url = f"/download/{session_name}/result.xlsx"
        return {"download_url": download_url}
        
    except Exception as e:
        # В случае ошибки удаляем временную папку
        if 'session_dir' in locals():
            shutil.rmtree(session_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/download-file/{session_id}/{filename}")
async def download_from_session(session_id: str, filename: str):
    """Скачать файл из сессионной папки."""
    file_path = UPLOAD_DIR / session_id / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Файл не найден")
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.get("/download/{session_dir:path}/{filename}")
async def download_file(session_dir: str, filename: str):
    """Старый эндпоинт для совместимости."""
    file_path = UPLOAD_DIR / session_dir / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Файл не найден")
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


# Запуск (для разработки и продакшена)
if __name__ == "__main__":
    import os
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host=host, port=port)
