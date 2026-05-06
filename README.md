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

Windows (перманентно, PowerShell):

```powershell
setx TELEGRAM_BOT_TOKEN "123:ABC"
```

Откройте новое окно терминала и снова запустите `python bot.py`.

Или создайте файл `.env` рядом с `bot.py`:

```dotenv
TELEGRAM_BOT_TOKEN=123:ABC
```

3. Запустите:

```bash
python bot.py
```

## Примечания

- База SQLite по умолчанию: `recipes.db` (файл в `.gitignore`, в репозиторий не попадает).
- Массовый импорт рецептов с Hugging Face ([povarenok-recipes](https://huggingface.co/datasets/rogozinushka/povarenok-recipes)):  
  `pip install -r requirements-import.txt`, затем `python scripts/import_povarenok.py --limit 3000`  
  (`--limit 0` — без ограничения; файл БД будет очень большим. Поиск блюда по имени ускорен индексом `name_search`, но сообщение `/recipes` показывает только первые 100 названий из базы.)
- Если рецепты импортировались **до** появления колонки `source_url`, прямые ссылки на страницы Поварёнка можно один раз подставить командой  
  `python scripts/backfill_recipe_urls.py` (из корня проекта, рядом с `recipes.db`).
- Аудит падежей и уменьшительных по всем названиям и ингредиентам в `recipes.db` (pymorphy, кластеры лемм):  
  `python scripts/audit_vocabulary_morphology.py` (при большой БД прогон может занять несколько минут).