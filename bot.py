import asyncio
import logging
import os
import re
import sqlite3
from urllib.parse import quote

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

# `transliterate` используется только для авто-преобразования латиницы в кириллицу.
# Если библиотека не установлена — бот продолжит работать, просто вернув исходную строку.
try:
    from transliterate import translit
except ImportError:  # pragma: no cover
    translit = None

# Токен бота нельзя хранить в репозитории. Берём из переменной окружения.
# Пример: setx TELEGRAM_BOT_TOKEN "123:ABC"  (Windows)
def _load_token_from_dotenv(path: str = ".env") -> str | None:
    """
    Очень простой парсер .env (без зависимостей).
    Ожидаем строку вида: TELEGRAM_BOT_TOKEN=...
    """
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key.strip() == "TELEGRAM_BOT_TOKEN":
                    return value.strip().strip("'\"")
    except OSError:
        logging.exception("Не удалось прочитать %s", path)
    return None


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or _load_token_from_dotenv()

# Telegram ID администратора: только он может добавлять блюда.
ADMIN_ID = 69026978

# Путь к SQLite-базе данных.
DB_PATH = "recipes.db"

# Отдельный роутер помогает держать обработчики в одном месте.
router = Router()

# Регулярка для проверки "только латинские буквы".
_LATIN_ONLY_RE = re.compile(r"^[A-Za-z]+$")
# Регулярка для проверки "содержит кириллицу".
_CYRILLIC_RE = re.compile(r"[а-яё]", re.IGNORECASE)


def normalize_recipe_name(name: str) -> str:
    """
    Нормализует название блюда:
    - приводит к нижнему регистру;
    - если строка состоит ТОЛЬКО из латинских букв, транслитерирует в кириллицу
      (например: "borsh" -> "борщ");
    - при отсутствии библиотеки `transliterate` просто возвращает строку в нижнем регистре.
    """
    normalized = (name or "").strip().lower()
    if not normalized:
        return ""

    if translit is None:
        return normalized

    if normalized.isascii() and _LATIN_ONLY_RE.fullmatch(normalized):
        # Важно: у разных версий/настроек `transliterate` флаг `reversed`
        # может означать обратное направление. Поэтому пробуем оба направления
        # и выбираем тот результат, который действительно содержит кириллицу.
        candidates: list[str] = []
        try:
            candidates.append(translit(normalized, "ru", reversed=True).strip())
        except Exception:
            logging.exception("Ошибка транслитерации (reversed=True): %r", name)
        try:
            candidates.append(translit(normalized, "ru", reversed=False).strip())
        except Exception:
            logging.exception("Ошибка транслитерации (reversed=False): %r", name)

        for candidate in candidates:
            if _CYRILLIC_RE.search(candidate):
                candidate = candidate.lower()

                # Эвристика для частого сценария "borsh/borch" -> "борщ".
                # В некоторых сборках `transliterate` "sh/ch" транслитерируются как
                # "ш/ч" вместо "щ". Мы исправляем только самый конец слова.
                if normalized.endswith(("sh", "ch")) and candidate.endswith(("ш", "ч")):
                    candidate = candidate[:-1] + "щ"

                return candidate

        # Если оба варианта не дали кириллицу — возвращаем нормализованную строку.
        # (Например, если строка не поддерживается маппингом.)
        return normalized

    return normalized


def build_magnit_search_url(product_name: str) -> str:
    """Формирует ссылку на поиск товара в Магните (magnit.ru)."""
    return f"https://magnit.ru/search?term={quote(product_name)}"


def unique_sorted_casefold(items: list[str]) -> list[str]:
    """
    Удаляет дубликаты и сортирует по алфавиту без учёта регистра (Unicode).
    При дубликатах сохраняет первое встреченное написание.
    """
    seen: dict[str, str] = {}
    for item in items:
        cleaned = (item or "").strip()
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key not in seen:
            seen[key] = cleaned
    return sorted(seen.values(), key=lambda x: x.casefold())


class AddRecipeStates(StatesGroup):
    """Состояния диалога добавления нового рецепта администратором."""

    waiting_recipe_name = State()
    waiting_ingredients = State()


class QuizStates(StatesGroup):
    """Состояния квиза по ингредиентам выбранного блюда."""

    in_progress = State()
    awaiting_next_dish_name = State()
    awaiting_addition_choice = State()


def init_db() -> None:
    """
    Создаем таблицы при старте бота, если их еще нет.
    Структура полностью соответствует заданию:
      - recipes (id INTEGER PRIMARY KEY, name TEXT UNIQUE)
      - ingredients (id INTEGER PRIMARY KEY, recipe_id INTEGER, name TEXT)
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS recipes (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ingredients (
                id INTEGER PRIMARY KEY,
                recipe_id INTEGER,
                name TEXT,
                FOREIGN KEY(recipe_id) REFERENCES recipes(id) ON DELETE CASCADE
            )
            """
        )
        conn.commit()


def get_all_recipes() -> list[str]:
    """Возвращает список названий всех блюд, отсортированный по алфавиту."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM recipes ORDER BY name COLLATE NOCASE")
        rows = cursor.fetchall()
    return [row[0] for row in rows]


def add_recipe_to_db(recipe_name: str, ingredients: list[str]) -> bool:
    """
    Добавляет рецепт и его ингредиенты в БД.
    Возвращает:
      - True, если успешно добавлено;
      - False, если блюдо уже существует (нарушение UNIQUE).
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO recipes (name) VALUES (?)", (recipe_name,))
            recipe_id = cursor.lastrowid

            cursor.executemany(
                "INSERT INTO ingredients (recipe_id, name) VALUES (?, ?)",
                [(recipe_id, ingredient) for ingredient in ingredients],
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def find_recipe_with_ingredients(recipe_name: str) -> tuple[int, str, list[str]] | None:
    """
    Ищет блюдо по названию без учета регистра.
    Возвращает кортеж (recipe_id, exact_recipe_name, ingredients) либо None.
    """
    # Важно: SQLite `LOWER()` и `COLLATE NOCASE` по умолчанию могут
    # корректно работать только с ASCII. Поэтому делаем сравнение в Python
    # через `.casefold()` — это надежнее для Unicode (кириллицы).
    target = normalize_recipe_name(recipe_name).casefold()

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM recipes")
        recipes_rows = cursor.fetchall()

        recipe_id: int | None = None
        exact_name: str | None = None
        for rid, name in recipes_rows:
            if str(name).casefold() == target:
                recipe_id = int(rid)
                exact_name = str(name)
                break

        if recipe_id is None or exact_name is None:
            return None

        cursor.execute(
            "SELECT name FROM ingredients WHERE recipe_id = ? ORDER BY id",
            (recipe_id,),
        )
        ingredients_rows = cursor.fetchall()

    ingredients = [row[0] for row in ingredients_rows]
    return recipe_id, exact_name, ingredients


def ingredient_quiz_keyboard(recipe_id: int, ingredient_index: int) -> InlineKeyboardMarkup:
    """Создает inline-клавиатуру для вопроса про текущий ингредиент."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Есть",
                    callback_data=f"quiz:{recipe_id}:{ingredient_index}:have",
                ),
                InlineKeyboardButton(
                    text="🛒 Нужно купить",
                    callback_data=f"quiz:{recipe_id}:{ingredient_index}:buy",
                ),
            ]
        ]
    )


def addition_prompt_keyboard() -> InlineKeyboardMarkup:
    """
    Кнопки после завершения квиза:
    - добавить ещё блюдо
    - завершить и получить список покупок
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ Добавить ещё блюдо",
                    callback_data="multi:add_more",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🏁 Завершить",
                    callback_data="multi:finish",
                )
            ],
        ]
    )


def render_shopping_list(products: list[str]) -> str:
    """
    Рендерит маркированный список покупок со ссылками на magnit.ru,
    убирая дубликаты и сортируя без учёта регистра.
    """
    unique_products = unique_sorted_casefold(products)
    if not unique_products:
        return "• (пусто)"
    return "\n".join(
        f"• {product} — {build_magnit_search_url(product)}"
        for product in unique_products
    )


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    """Приветствие и краткая инструкция по использованию бота."""
    await state.clear()
    text = (
        "Привет! 👋 Я помогу проверить ингредиенты для блюда.\n\n"
        "Что умею:\n"
        "• /recipes — показать список блюд 📋\n"
        "• /add_recipe — добавить новое блюдо (только админ) 🧑‍🍳\n"
        "• /cancel — выйти из текущего диалога ❌\n\n"
        "Просто напиши название блюда, и я запущу мини-квиз по ингредиентам 😉"
    )
    await message.answer(text)


@router.message(Command("recipes"))
async def cmd_recipes(message: Message) -> None:
    """Показывает все доступные блюда из базы."""
    recipes = get_all_recipes()
    if not recipes:
        await message.answer(
            "Пока нет ни одного блюда 😕\n"
            "Администратор может добавить первое через /add_recipe."
        )
        return

    # Показываем названия с большой буквы (как требовалось).
    lines = [f"• {name.capitalize()}" for name in recipes]
    await message.answer("Доступные блюда 🍽️:\n" + "\n".join(lines))


@router.message(Command("add_recipe"))
async def cmd_add_recipe(message: Message, state: FSMContext) -> None:
    """
    Старт диалога добавления рецепта.
    Доступно только ADMIN_ID.
    """
    if message.from_user is None or message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет прав для этой команды.")
        return

    await state.set_state(AddRecipeStates.waiting_recipe_name)
    await message.answer(
        "Отлично! 🧑‍🍳\n"
        "Введите название нового блюда:"
    )


@router.message(AddRecipeStates.waiting_recipe_name)
async def process_recipe_name(message: Message, state: FSMContext) -> None:
    """Получает название блюда от администратора."""
    # Сохраняем в нижнем регистре, как требовалось.
    recipe_name = normalize_recipe_name((message.text or "").strip())
    if not recipe_name:
        await message.answer("Название не должно быть пустым. Попробуйте еще раз ✍️")
        return

    await state.update_data(recipe_name=recipe_name)
    await state.set_state(AddRecipeStates.waiting_ingredients)
    await message.answer(
        "Теперь отправьте ингредиенты через запятую 🥕\n"
        "Например: картошка, морковь, лук"
    )


@router.message(AddRecipeStates.waiting_ingredients)
async def process_ingredients(message: Message, state: FSMContext) -> None:
    """Получает ингредиенты, валидирует и сохраняет новый рецепт в БД."""
    raw_text = (message.text or "").strip()
    ingredients = [item.strip() for item in raw_text.split(",") if item.strip()]

    if not ingredients:
        await message.answer(
            "Не удалось распознать ингредиенты 😅\n"
            "Введите список через запятую."
        )
        return

    data = await state.get_data()
    recipe_name = data.get("recipe_name")
    if not recipe_name:
        await state.clear()
        await message.answer(
            "Произошла ошибка состояния. Начните заново через /add_recipe."
        )
        return

    success = add_recipe_to_db(recipe_name=recipe_name, ingredients=ingredients)
    await state.clear()

    if not success:
        await message.answer(f"⚠️ Блюдо «{recipe_name}» уже существует.")
        return

    await message.answer(
        f"✅ Блюдо «{recipe_name}» успешно сохранено!\n"
        "Теперь пользователи могут найти его по названию."
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    """Выход из любого активного диалога/квиза с очисткой FSM-состояния."""
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Сейчас нет активного диалога 🙂")
        return

    await state.clear()
    await message.answer("Диалог отменен ✅ Можете выбрать другое блюдо.")


@router.message(F.text & ~F.text.startswith("/"))
async def handle_recipe_search(message: Message, state: FSMContext) -> None:
    """
    Любой текст (не команда) считаем названием блюда.
    Если блюдо найдено — запускаем квиз по ингредиентам.
    """
    user_text = (message.text or "").strip()
    if not user_text:
        return

    current_state = await state.get_state()
    # Если бот ждёт нажатия кнопки/перехода — не начинаем новый квиз по тексту.
    if current_state in {
        QuizStates.awaiting_addition_choice.state,
    }:
        await message.answer("Сначала выберите кнопки ниже ✅")
        return
    if current_state == QuizStates.in_progress.state:
        await message.answer("Сначала ответьте на вопросы квиза кнопками ✅")
        return

    # Нормализуем запрос пользователя перед поиском в базе:
    # нижний регистр + транслит латиницы в кириллицу.
    normalized_name = normalize_recipe_name(user_text)
    found = find_recipe_with_ingredients(normalized_name)
    if not found:
        await message.answer(
            "Блюдо не найдено. Попробуйте написать по-русски или проверьте список командой /recipes."
        )
        return

    recipe_id, recipe_name, ingredients = found
    if not ingredients:
        await message.answer(
            f"Для блюда «{recipe_name}» пока нет ингредиентов в базе."
        )
        return

    # Начинаем квиз по выбранному блюду.
    # В режиме накопления (awaiting_next_dish_name) накопленные данные не очищаются,
    # а только добавляются новые поля квиза.
    await state.set_state(QuizStates.in_progress)
    await state.update_data(
        quiz_recipe_id=recipe_id,
        quiz_recipe_name=recipe_name,
        quiz_ingredients=ingredients,
        quiz_index=0,
        shopping_list=[],
    )

    first_ingredient = ingredients[0]
    await message.answer(
        f"Начинаем квиз по блюду «{recipe_name}» 🍳\n"
        f"У тебя есть {first_ingredient}?",
        reply_markup=ingredient_quiz_keyboard(recipe_id, 0),
    )


@router.callback_query(F.data.startswith("quiz:"))
async def process_quiz_answer(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Обрабатывает выбор inline-кнопок:
      - have -> ингредиент есть;
      - buy  -> добавляем ингредиент в список покупок.
    """
    if callback.message is None:
        await callback.answer()
        return

    current_state = await state.get_state()
    if current_state != QuizStates.in_progress.state:
        await callback.answer("Квиз уже завершен или отменен.", show_alert=False)
        return

    try:
        _, cb_recipe_id, cb_index, action = callback.data.split(":")
        cb_recipe_id = int(cb_recipe_id)
        cb_index = int(cb_index)
    except (AttributeError, ValueError):
        await callback.answer("Некорректные данные кнопки.", show_alert=True)
        return

    data = await state.get_data()
    recipe_id = data.get("quiz_recipe_id")
    ingredients = data.get("quiz_ingredients", [])
    current_index = data.get("quiz_index", 0)
    shopping_list = data.get("shopping_list", [])

    # Дополнительная проверка: если callback устарел, аккуратно игнорируем.
    if recipe_id != cb_recipe_id or current_index != cb_index or cb_index >= len(ingredients):
        await callback.answer("Этот шаг уже неактуален.", show_alert=False)
        return

    ingredient = ingredients[current_index]
    if action == "buy":
        shopping_list.append(ingredient)
    elif action != "have":
        await callback.answer("Неизвестный вариант ответа.", show_alert=True)
        return

    next_index = current_index + 1
    await state.update_data(quiz_index=next_index, shopping_list=shopping_list)
    await callback.answer("Ответ сохранен 👍")

    if next_index < len(ingredients):
        next_ingredient = ingredients[next_index]
        try:
            await callback.message.edit_text(
                f"У тебя есть {next_ingredient}?",
                reply_markup=ingredient_quiz_keyboard(recipe_id, next_index),
            )
        except TelegramBadRequest:
            # Если Telegram не дал отредактировать (редкий случай), отправляем новое сообщение.
            await callback.message.answer(
                f"У тебя есть {next_ingredient}?",
                reply_markup=ingredient_quiz_keyboard(recipe_id, next_index),
            )
        return

    # Квиз завершен.
    # По требованиям:
    # - не выводим список покупок после каждого блюда (в т.ч. после первого);
    # - список показываем только после нажатия "🏁 Завершить";
    # - после квиза показываем кнопки "Добавить ещё блюдо" / "Завершить".
    recipe_name = data.get("quiz_recipe_name", "выбранного блюда")

    accumulated_missing = data.get("accumulated_missing", [])
    if not isinstance(accumulated_missing, list):
        accumulated_missing = list(accumulated_missing) if accumulated_missing else []

    accumulated_missing.extend(shopping_list)

    await state.update_data(
        accumulated_missing=accumulated_missing,
        last_completed_recipe_name=recipe_name,
    )
    await state.set_state(QuizStates.awaiting_addition_choice)

    prompt_text = "✅ Блюдо добавлено! Хотите добавить ещё одно блюдо или завершить?"
    try:
        await callback.message.edit_text(prompt_text, reply_markup=addition_prompt_keyboard())
    except TelegramBadRequest:
        await callback.message.answer(prompt_text, reply_markup=addition_prompt_keyboard())


@router.callback_query(F.data == "multi:add_more")
async def multi_add_more(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор «➕ Добавить ещё блюдо»: включаем режим накопления и ждём следующее блюдо."""
    current_state = await state.get_state()
    if current_state != QuizStates.awaiting_addition_choice.state:
        await callback.answer("Сейчас нельзя добавить ещё блюдо.", show_alert=False)
        return

    await callback.answer()
    await state.update_data(accumulation_started=True)
    await state.set_state(QuizStates.awaiting_next_dish_name)
    if callback.message:
        await callback.message.answer("Напишите название следующего блюда")


@router.callback_query(F.data == "multi:finish")
async def multi_finish(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор «🏁 Завершить»: выводим список покупок и очищаем состояние."""
    current_state = await state.get_state()
    if current_state != QuizStates.awaiting_addition_choice.state:
        await callback.answer("Сейчас нельзя завершить.", show_alert=False)
        return

    data = await state.get_data()
    accumulated_missing = data.get("accumulated_missing", [])
    if not isinstance(accumulated_missing, list):
        accumulated_missing = list(accumulated_missing) if accumulated_missing else []

    accumulation_started = bool(data.get("accumulation_started", False))
    last_recipe_name = str(data.get("last_completed_recipe_name") or "выбранного блюда")

    shopping_text = render_shopping_list(accumulated_missing)

    if accumulation_started:
        if unique_sorted_casefold(accumulated_missing):
            result_text = f"📦 Общий список покупок:\n{shopping_text}"
        else:
            result_text = "📦 Общий список покупок пуст — у вас всё уже есть ✅"
    else:
        # Обычный режим: пользователь не выбирал «Добавить ещё»,
        # поэтому показываем список покупок только для одного блюда.
        if unique_sorted_casefold(accumulated_missing):
            result_text = f"Для блюда «{last_recipe_name}» нужно купить:\n{shopping_text}"
        else:
            result_text = f"Для блюда «{last_recipe_name}» у вас уже есть все ингредиенты ✅"

    await callback.answer()
    await state.clear()

    if callback.message:
        await callback.message.edit_text(result_text)


@router.errors()
async def global_error_handler(event) -> bool:
    """
    Глобальный обработчик ошибок aiogram.
    Логируем исключение и не даем боту упасть.
    """
    logging.exception("Необработанная ошибка во время апдейта: %s", event.exception)
    return True


async def main() -> None:
    """Точка входа: инициализация базы, бота и запуск long-polling."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logging.info("Запуск бота...")

    if not TOKEN:
        raise RuntimeError(
            "Не задан TELEGRAM_BOT_TOKEN. "
            "Установите переменную окружения и перезапустите приложение."
        )

    init_db()

    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        logging.info("Бот остановлен.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот выключен пользователем.")