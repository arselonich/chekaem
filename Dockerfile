FROM python:3.9-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Копирование requirements и установка Python зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

# Создание необходимых директорий
RUN mkdir -p uploads static templates

# Инициализация базы данных
RUN python -c "from database import init_db; init_db()"

# Открытие порта
EXPOSE 8000

# Запуск приложения
CMD ["python", "main.py"]