# my_first_bot

## Запуск

1. Установите зависимости:

```bash
pip install -r requirements.txt
```

2. Задайте токен бота через переменную окружения `TELEGRAM_BOT_TOKEN`.

Windows (PowerShell):

```powershell
$env:TELEGRAM_BOT_TOKEN="123:ABC"
python bot.py
```

Windows (перманентно):
python bot.py
```powershell
setx TELEGRAM_BOT_TOKEN "123:ABC"
```

Или создайте файл `.env` рядом с `bot.py`:

```dotenv
TELEGRAM_BOT_TOKEN=123:ABC
```

3. Запустите:

```bash
python bot.py
```

## Примечания

- База SQLite по умолчанию: `recipes.db` (не коммитится в GitHub