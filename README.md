# Twitch Bot v3.0

FastAPI + SQLite + Twitch IRC + Overlay Editor

## Установка

```bash
pip install -r requirements.txt
```

## Запуск

```bash
python main.py
```

## Файлы

Положи в папку `frontend/`:
- `index.html` - панель управления
- `overlay_editor.html` - редактор оверлеев

## Адреса

- Панель: http://localhost:8000
- Редактор оверлеев: http://localhost:8000/editor
- OBS Browser Source: http://localhost:8000/overlay/scene/{id}

## Настройка Twitch

1. Открой панель -> Настройки
2. Заполни: канал, токен бота, ник бота
3. Токен получи на: https://twitchapps.com/tmi/

## Структура папок

```
main.py
requirements.txt
frontend/
    index.html
    overlay_editor.html
bot_data.db  (создаётся автоматически)
```
