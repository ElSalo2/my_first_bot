import asyncio
import copy
import html
import logging
import os
import re
import sqlite3
from urllib.parse import quote

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from food_emojis import emoji_for_dish, emoji_for_ingredient
from ingredient_synonyms import (
    canonical_ingredient_display,
    ingredient_merge_key,
    shopping_lines_from_buckets,
)

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

START_HELP_TEXT = (
    "Привет! 👋 Я помогу проверить ингредиенты для блюда.\n\n"
    "Что умею:\n"
    "• /recipes — показать список блюд 📋\n"
    "• /add_recipe — добавить новое блюдо (только админ) 🧑‍🍳\n"
    "• /cancel — выйти из текущего диалога ❌\n\n"
    "Просто напиши название блюда, и я запущу мини-квиз по ингредиентам 😉"
)

# Telegram ID администратора: только он может добавлять блюда.
ADMIN_ID = 69026978

# Путь к SQLite-базе данных.
DB_PATH = "recipes.db"

# Отдельный роутер помогает держать обработчики в одном месте.
router = Router()

# Один активный запуск «Проверить ингредиенты» на чат (защита от двойного нажатия).
_quiz_edit_locks: dict[int, asyncio.Lock] = {}


def _quiz_edit_lock_for_chat(chat_id: int) -> asyncio.Lock:
    if chat_id not in _quiz_edit_locks:
        _quiz_edit_locks[chat_id] = asyncio.Lock()
    return _quiz_edit_locks[chat_id]


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


def format_dish_title(name: str) -> str:
    """Название блюда с эмодзи без кавычек."""
    return f"{emoji_for_dish(name)} {name}"


def format_ingredient_display(name: str) -> str:
    """Строка ингредиента с подходящим эмодзи."""
    return f"{emoji_for_ingredient(name)} {name}"


def build_magnit_search_url(product_name: str) -> str:
    """Формирует ссылку на поиск товара в Магните (magnit.ru)."""
    return f"https://magnit.ru/search?term={quote(product_name)}"


def _coerce_extras_list(raw: object) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [str(x).strip() for x in raw if str(x).strip()]


def _dedupe_extras_preserve_order(items: list[str]) -> list[str]:
    """Убирает дубликаты по ключу синонимов, канонизирует отображение."""
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        mk = ingredient_merge_key(x)
        if not mk or mk in seen:
            continue
        seen.add(mk)
        out.append(canonical_ingredient_display(x))
    return out


class AddRecipeStates(StatesGroup):
    """Состояния диалога добавления нового рецепта администратором."""

    waiting_recipe_name = State()
    waiting_ingredients = State()


class QuizStates(StatesGroup):
    """Состояния квиза по ингредиентам выбранного блюда."""

    in_progress = State()
    awaiting_next_dish_name = State()
    awaiting_addition_choice = State()
    after_shopping_list = State()  # список уже показан; можно снова проверить последнее блюдо
    awaiting_manual_product = State()  # ждём текст: продукты в список вручную


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


def find_recipe_by_id(recipe_id: int) -> tuple[int, str, list[str]] | None:
    """Блюдо по id из БД: (id, точное имя, ингредиенты)."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM recipes WHERE id = ?", (int(recipe_id),))
        row = cursor.fetchone()
        if not row:
            return None
        exact_name = str(row[0])
        cursor.execute(
            "SELECT name FROM ingredients WHERE recipe_id = ? ORDER BY id",
            (int(recipe_id),),
        )
        ing_rows = cursor.fetchall()
    ingredients = [r[0] for r in ing_rows]
    return int(recipe_id), exact_name, ingredients


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
    Кнопки после завершения квиза под последним блюдом.
    «Проверить ингредиенты» — только под сообщением после последнего шага квиза.
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
                    text="🛒 Добавить продукт",
                    callback_data="multi:add_product",
                ),
                InlineKeyboardButton(
                    text="📋 Посмотреть список",
                    callback_data="multi:finish",
                ),
            ],
        ]
    )


def shopping_list_actions_keyboard() -> InlineKeyboardMarkup:
    """Под финальным списком покупок после «Посмотреть список»."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🛒 Добавить продукт",
                    callback_data="multi:add_product",
                ),
            ],
            [
                InlineKeyboardButton(text="🆕 Создать новый список", callback_data="multi:new_planning"),
            ],
        ]
    )


def quiz_finished_recheck_keyboard(recipe_id: int) -> InlineKeyboardMarkup:
    """Под строкой после квиза — повторить квиз для этого блюда."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Проверить ингредиенты",
                    callback_data=f"multi:edit_dish:{int(recipe_id)}",
                ),
            ],
        ]
    )


def render_shopping_list_html_from_buckets(
    buckets: list[dict],
    extras: list[str] | None = None,
) -> str | None:
    """
    Список покупок по корзинам блюд: синонимы объединяются, подпись вида
    «Морковь (для борща и для плова)». Каждая строка — ссылка на поиск в Магните.
    extras — дополнительные продукты от пользователя (тот же формат, эмодзи и ссылки).
    Возвращает None, если покупать нечего.
    """
    rows = shopping_lines_from_buckets(buckets, extras=extras)
    if not rows:
        return None
    lines: list[str] = []
    for search_term, label_plain in rows:
        url = build_magnit_search_url(search_term)
        inner = f"{emoji_for_ingredient(search_term)} {label_plain}"
        lines.append(f"• <a href=\"{html.escape(url)}\">{html.escape(inner)}</a>")
    return "\n".join(lines)


def _upsert_bucket(buckets: list[dict], recipe: str, missing: list[str]) -> list[dict]:
    """
    Добавляет или заменяет корзинку для блюда `recipe`.
    """
    recipe_key = str(recipe or "").casefold()
    out: list[dict] = []
    replaced = False
    for bucket in buckets or []:
        b_recipe = str(bucket.get("recipe", ""))
        if b_recipe.casefold() == recipe_key:
            out.append({"recipe": str(recipe), "missing": list(missing)})
            replaced = True
        else:
            # нормализуем структуру на всякий
            b_missing = bucket.get("missing", [])
            out.append(
                {
                    "recipe": b_recipe,
                    "missing": list(b_missing) if isinstance(b_missing, list) else [],
                }
            )
    if not replaced:
        out.append({"recipe": str(recipe), "missing": list(missing)})
    return out


async def _clear_last_keyboard(bot: Bot, state: FSMContext) -> None:
    """
    Снимает inline-клавиатуру с последнего сообщения с кнопками (если оно есть).
    Храним идентификаторы в состоянии: last_keyboard_chat_id, last_keyboard_msg_id.
    """
    data = await state.get_data()
    chat_id = data.get("last_keyboard_chat_id")
    msg_id = data.get("last_keyboard_msg_id")
    if not chat_id or not msg_id:
        return
    try:
        await bot.edit_message_reply_markup(chat_id=int(chat_id), message_id=int(msg_id), reply_markup=None)
    except Exception:
        # сообщение могло быть удалено/уже без клавиатуры — это ок
        logging.exception("Не удалось снять клавиатуру с сообщения %s/%s", chat_id, msg_id)
    finally:
        await state.update_data(last_keyboard_chat_id=None, last_keyboard_msg_id=None)


async def _send_keyboard_message(message: Message, state: FSMContext, text: str) -> None:
    """
    Перед отправкой нового сообщения с кнопками снимает кнопки с предыдущего,
    затем отправляет новое и сохраняет его message_id/chat_id в состоянии.
    """
    await _clear_last_keyboard(message.bot, state)
    sent = await message.answer(text, reply_markup=addition_prompt_keyboard())
    await state.update_data(last_keyboard_chat_id=sent.chat.id, last_keyboard_msg_id=sent.message_id)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    """Приветствие и краткая инструкция по использованию бота."""
    await _clear_last_keyboard(message.bot, state)
    await state.clear()
    await message.answer(START_HELP_TEXT)


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
    lines = [f"• {emoji_for_dish(name)} {name.capitalize()}" for name in recipes]
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
        await message.answer(f"⚠️ Блюдо {format_dish_title(recipe_name)} уже существует.")
        return

    await message.answer(
        f"✅ Блюдо {format_dish_title(recipe_name)} успешно сохранено!\n"
        "Теперь пользователи могут найти его по названию."
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    """Выход из любого активного диалога/квиза с очисткой FSM-состояния."""
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Сейчас нет активного диалога 🙂")
        return

    if current_state == QuizStates.awaiting_manual_product.state:
        data = await state.get_data()
        ret_state = data.get("manual_product_return_state")
        await _clear_last_keyboard(message.bot, state)
        await state.update_data(manual_product_return_state=None)
        await state.set_state(ret_state if ret_state else QuizStates.after_shopping_list.state)
        await message.answer("Добавление продуктов отменено.")
        return

    await _clear_last_keyboard(message.bot, state)
    await state.clear()
    await message.answer("Диалог отменен ✅ Можете выбрать другое блюдо.")


@router.message(StateFilter(QuizStates.awaiting_manual_product), F.text & ~F.text.startswith("/"))
async def handle_manual_shopping_items(message: Message, state: FSMContext) -> None:
    """Продукты в список вручную (одна или несколько позиций через запятую)."""
    user_text = (message.text or "").strip()
    parts = [p.strip() for p in user_text.split(",") if p.strip()]
    if not parts:
        await message.answer(
            "Укажите хотя бы один продукт или отправьте /cancel.",
        )
        return

    data = await state.get_data()
    ret_state = data.get("manual_product_return_state")
    buckets_raw = data.get("accumulated_buckets")
    buckets = copy.deepcopy(buckets_raw) if isinstance(buckets_raw, list) else []

    extras_prev = _dedupe_extras_preserve_order(_coerce_extras_list(data.get("shopping_list_extras")))
    keys_seen: set[str] = {ingredient_merge_key(x) for x in extras_prev}
    for b in buckets:
        miss = b.get("missing")
        if not isinstance(miss, list):
            continue
        for m in miss:
            mk = ingredient_merge_key(str(m))
            if mk:
                keys_seen.add(mk)
    added_labels: list[str] = []
    for part in parts:
        mk = ingredient_merge_key(part)
        if not mk:
            continue
        if mk in keys_seen:
            continue
        keys_seen.add(mk)
        disp = canonical_ingredient_display(part)
        extras_prev.append(disp)
        added_labels.append(disp)

    extras_prev = _dedupe_extras_preserve_order(extras_prev)

    await state.update_data(
        shopping_list_extras=extras_prev,
        accumulated_buckets=buckets,
        manual_product_return_state=None,
    )

    target_state = ret_state if ret_state else QuizStates.after_shopping_list.state
    await state.set_state(target_state)

    if target_state == QuizStates.after_shopping_list.state:
        body = render_shopping_list_html_from_buckets(buckets, extras_prev)
        if body:
            head = ""
            if added_labels:
                head = "Добавлено: " + ", ".join(added_labels) + "\n\n"
            await message.answer(
                head + f"📦 Общий список покупок:\n{body}",
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=shopping_list_actions_keyboard(),
            )
        else:
            await message.answer(
                "В списке пока пусто — добавьте продукты или пройдите квиз по блюду.",
                reply_markup=shopping_list_actions_keyboard(),
            )
        return

    if added_labels:
        await message.answer(
            "Добавлено в список: " + ", ".join(added_labels),
            reply_markup=addition_prompt_keyboard(),
        )
    else:
        await message.answer(
            "Эти позиции уже есть в списке или совпадают с тем, что из блюд.",
            reply_markup=addition_prompt_keyboard(),
        )


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

    if current_state == QuizStates.awaiting_manual_product.state:
        return

    # После показа итогового списка можно просто написать новое блюдо — новый квиз без нажатия кнопки.
    # Сохраняем корзину и доп. продукты (state.clear() их стирает).
    if current_state == QuizStates.after_shopping_list.state:
        await _clear_last_keyboard(message.bot, state)
        snap = await state.get_data()
        buckets_raw = snap.get("accumulated_buckets")
        buckets_copy = copy.deepcopy(buckets_raw) if isinstance(buckets_raw, list) else []
        extras_copy = _dedupe_extras_preserve_order(_coerce_extras_list(snap.get("shopping_list_extras")))
        lr = snap.get("last_recipe_name")
        ff = bool(snap.get("final_list_shown", False))
        acc = bool(snap.get("accumulation_started", False))
        await state.clear()
        await state.update_data(
            accumulated_buckets=buckets_copy,
            shopping_list_extras=extras_copy,
            last_recipe_name=lr,
            final_list_shown=ff,
            accumulation_started=acc,
        )

    if current_state == QuizStates.awaiting_addition_choice.state:
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
            f"Для блюда {format_dish_title(recipe_name)} пока нет ингредиентов в базе."
        )
        return

    # Запускаем опрос ингредиентов для выбранного блюда.
    prev_data = await state.get_data()
    extras_keep = _dedupe_extras_preserve_order(_coerce_extras_list(prev_data.get("shopping_list_extras")))
    await state.set_state(QuizStates.in_progress)
    await state.update_data(
        quiz_recipe_id=recipe_id,
        quiz_recipe_name=recipe_name,
        quiz_ingredients=ingredients,
        quiz_index=0,
        shopping_list=[],
        editing_last=False,
        shopping_list_extras=extras_keep,
    )

    first_ingredient = ingredients[0]
    await message.answer(
        f"Проверим, всё ли у тебя есть для того, чтобы приготовить {format_dish_title(recipe_name)}\n"
        f"У тебя есть {format_ingredient_display(first_ingredient)}?",
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

    # Устаревший callback (двойное нажатие и т.п.) — тихо игнорируем.
    if recipe_id != cb_recipe_id or current_index != cb_index or cb_index >= len(ingredients):
        await callback.answer()
        return

    ingredient = ingredients[current_index]
    if action == "buy":
        shopping_list.append(ingredient)
    elif action != "have":
        await callback.answer("Неизвестный вариант ответа.", show_alert=True)
        return

    next_index = current_index + 1
    await state.update_data(quiz_index=next_index, shopping_list=shopping_list)
    await callback.answer()

    if next_index < len(ingredients):
        next_ingredient = ingredients[next_index]
        try:
            await callback.message.edit_text(
                f"У тебя есть {format_ingredient_display(next_ingredient)}?",
                reply_markup=ingredient_quiz_keyboard(recipe_id, next_index),
            )
        except TelegramBadRequest:
            # Если Telegram не дал отредактировать (редкий случай), отправляем новое сообщение.
            await callback.message.answer(
                f"У тебя есть {format_ingredient_display(next_ingredient)}?",
                reply_markup=ingredient_quiz_keyboard(recipe_id, next_index),
            )
        return

    recipe_name = str(data.get("quiz_recipe_name") or "блюда")
    rid = int(data.get("quiz_recipe_id") or 0)

    # Заменяем текст последнего вопроса и показываем «Проверить ингредиенты» здесь же (не под списком покупок).
    closing_text = (
        "Сохранил ингредиенты в список покупок. "
        "Добавим новое блюдо или посмотрим список?"
    )
    kb = quiz_finished_recheck_keyboard(rid) if rid > 0 else None
    try:
        await callback.message.edit_text(closing_text, reply_markup=kb)
    except TelegramBadRequest:
        if callback.message:
            await callback.message.answer(closing_text, reply_markup=kb)

    # Квиз завершён: общий список с Магнитом после «Посмотреть список» или сразу после обновления блюда,
    # если список уже показывали ранее (final_list_shown).

    # buckets: [{"recipe": "...", "missing": ["..."]}, ...]
    buckets = data.get("accumulated_buckets", [])
    if not isinstance(buckets, list):
        buckets = []

    final_list_shown = bool(data.get("final_list_shown", False))
    was_editing = bool(data.get("editing_last", False))

    # missing для этого блюда (как пользователь отметил "Нужно купить")
    dish_missing = [str(x).strip() for x in (shopping_list or []) if str(x).strip()]

    buckets = _upsert_bucket(buckets, recipe=str(recipe_name), missing=dish_missing)

    extras_keep = _dedupe_extras_preserve_order(_coerce_extras_list(data.get("shopping_list_extras")))

    await state.update_data(
        accumulated_buckets=buckets,
        last_recipe_name=str(recipe_name),
        editing_last=False,
        shopping_list_extras=extras_keep,
    )

    await state.set_state(QuizStates.awaiting_addition_choice)
    keyboard_text = (
        f"✅ Блюдо {format_dish_title(recipe_name)} обновлено! Что дальше?"
        if was_editing
        else f"✅ Блюдо {format_dish_title(recipe_name)} добавлено! Что дальше?"
    )

    # Уже выводили общий список и пользователь заново прошёл квиз по блюду — обновляем список сразу.
    if final_list_shown and was_editing and callback.message:
        shop_body = render_shopping_list_html_from_buckets(buckets, extras_keep)
        if shop_body:
            await callback.message.answer(
                f"📦 Общий список покупок:\n{shop_body}",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await callback.message.answer(
                "📦 Общий список покупок пуст — у вас всё уже есть ✅"
            )

    if callback.message:
        await _send_keyboard_message(callback.message, state, keyboard_text)
    else:
        await _clear_last_keyboard(callback.bot, state)


@router.callback_query(F.data == "multi:add_product")
async def multi_add_product(callback: CallbackQuery, state: FSMContext) -> None:
    """Запрос строки с продуктами для ручного добавления в общий список покупок."""
    current_state = await state.get_state()
    if current_state not in {
        QuizStates.awaiting_addition_choice.state,
        QuizStates.after_shopping_list.state,
    }:
        await callback.answer("Сейчас недоступно.", show_alert=False)
        return

    await callback.answer()
    await state.update_data(manual_product_return_state=current_state)
    await state.set_state(QuizStates.awaiting_manual_product)
    if callback.message:
        await callback.message.answer(
            "Напишите продукты для списка покупок через запятую.\n"
            "Например: хлеб, молоко, масло\n\n"
            "Отмена — /cancel."
        )


@router.callback_query(F.data == "multi:add_more")
async def multi_add_more(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор «➕ Добавить ещё блюдо»: включаем режим накопления и ждём следующее блюдо."""
    current_state = await state.get_state()
    if current_state != QuizStates.awaiting_addition_choice.state:
        await callback.answer("Сейчас нельзя добавить ещё блюдо.", show_alert=False)
        return

    await callback.answer()
    await _clear_last_keyboard(callback.bot, state)
    await state.update_data(accumulation_started=True)
    await state.set_state(QuizStates.awaiting_next_dish_name)
    if callback.message:
        await callback.message.answer("Напишите название следующего блюда")


@router.callback_query(F.data.startswith("multi:edit_dish:"))
async def multi_edit_dish(callback: CallbackQuery, state: FSMContext) -> None:
    """Повторный квиз для выбранного блюда (кнопка под сообщением после квиза)."""
    if callback.message is None:
        await callback.answer()
        return

    chat_id = callback.message.chat.id
    lock = _quiz_edit_lock_for_chat(chat_id)
    if lock.locked():
        await callback.answer("Редактирование уже запускается.", show_alert=False)
        return

    async with lock:
        current_state = await state.get_state()
        if current_state == QuizStates.in_progress.state:
            await callback.answer("Сначала завершите квиз по ингредиентам.", show_alert=False)
            return

        if current_state not in {
            QuizStates.awaiting_addition_choice.state,
            QuizStates.after_shopping_list.state,
            QuizStates.awaiting_next_dish_name.state,
        }:
            await callback.answer("Сейчас недоступно.", show_alert=False)
            return

        parts = callback.data.split(":")
        if len(parts) != 3:
            await callback.answer()
            return
        try:
            target_rid = int(parts[2])
        except ValueError:
            await callback.answer()
            return

        found = find_recipe_by_id(target_rid)
        if not found:
            await callback.answer("Блюдо не найдено в базе.", show_alert=True)
            return

        recipe_id, recipe_name, ingredients = found
        if not ingredients:
            await callback.answer("Для этого блюда нет ингредиентов в базе.", show_alert=True)
            return

        await callback.answer()

        try:
            await callback.bot.edit_message_reply_markup(
                chat_id=callback.message.chat.id,
                message_id=callback.message.message_id,
                reply_markup=None,
            )
        except TelegramBadRequest:
            pass

        await _clear_last_keyboard(callback.bot, state)

        data = await state.get_data()
        buckets = data.get("accumulated_buckets", [])
        if not isinstance(buckets, list):
            buckets = []
        rk = recipe_name.casefold()
        buckets = [b for b in buckets if str(b.get("recipe", "")).casefold() != rk]
        extras_keep = _dedupe_extras_preserve_order(_coerce_extras_list(data.get("shopping_list_extras")))
        await state.update_data(
            accumulated_buckets=buckets,
            editing_last=True,
            last_recipe_name=recipe_name,
            shopping_list_extras=extras_keep,
        )

        await state.set_state(QuizStates.in_progress)
        await state.update_data(
            quiz_recipe_id=recipe_id,
            quiz_recipe_name=recipe_name,
            quiz_ingredients=ingredients,
            quiz_index=0,
            shopping_list=[],
            shopping_list_extras=extras_keep,
        )

        await callback.message.answer(
            f"Редактируем блюдо {format_dish_title(recipe_name)} ✏️\n"
            f"У тебя есть {format_ingredient_display(ingredients[0])}?",
            reply_markup=ingredient_quiz_keyboard(recipe_id, 0),
        )


@router.callback_query(F.data == "multi:finish")
async def multi_finish(callback: CallbackQuery, state: FSMContext) -> None:
    """«Посмотреть список»: показываем список; «Проверить ингредиенты» остаётся под сообщением после квиза."""
    current_state = await state.get_state()
    if current_state != QuizStates.awaiting_addition_choice.state:
        await callback.answer("Сейчас нельзя завершить.", show_alert=False)
        return

    await callback.answer()
    data = await state.get_data()

    buckets = data.get("accumulated_buckets", [])
    if not isinstance(buckets, list):
        buckets = []

    last_recipe = data.get("last_recipe_name")
    if not last_recipe and buckets:
        last_recipe = str(buckets[-1].get("recipe", ""))

    extras = _dedupe_extras_preserve_order(_coerce_extras_list(data.get("shopping_list_extras")))
    saved_extras = copy.deepcopy(extras)

    saved_buckets = copy.deepcopy(buckets)

    shop_body = render_shopping_list_html_from_buckets(buckets, extras)
    if shop_body:
        result_text = f"📦 Общий список покупок:\n{shop_body}"
        parse_mode = "HTML"
    else:
        result_text = "📦 Общий список покупок пуст — у вас всё уже есть ✅"
        parse_mode = None

    await _clear_last_keyboard(callback.bot, state)

    keep_after_list = (
        bool(saved_buckets)
        or bool(saved_extras)
        or bool(str(last_recipe or "").strip())
    )

    await state.clear()
    await state.set_state(QuizStates.after_shopping_list)
    payload: dict = {}
    if keep_after_list:
        payload.update(
            {
                "accumulated_buckets": saved_buckets,
                "shopping_list_extras": saved_extras,
                "last_recipe_name": str(last_recipe),
                "accumulation_started": True,
                "final_list_shown": True,
            }
        )
    if payload:
        await state.update_data(**payload)

    if callback.message:
        send_kw: dict = {
            "reply_markup": shopping_list_actions_keyboard(),
            "disable_web_page_preview": True,
        }
        if parse_mode:
            send_kw["parse_mode"] = parse_mode
        await callback.message.answer(result_text, **send_kw)


@router.callback_query(F.data == "multi:new_planning")
async def multi_new_planning(callback: CallbackQuery, state: FSMContext) -> None:
    """Сброс как после /start."""
    if callback.message is None:
        await callback.answer()
        return

    await callback.answer()

    chat_id = callback.message.chat.id
    bot = callback.bot

    try:
        await callback.message.delete()
    except TelegramBadRequest:
        logging.exception("Не удалось удалить сообщение со списком")

    await _clear_last_keyboard(bot, state)
    await state.clear()
    await bot.send_message(chat_id, START_HELP_TEXT)


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
