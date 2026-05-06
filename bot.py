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
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from food_emojis import emoji_for_dish, emoji_for_ingredient
from ingredient_synonyms import (
    canonical_ingredient_display,
    exclude_home_pantry_ingredients,
    is_always_home_pantry_ingredient,
    ingredient_merge_key,
    shopping_lines_from_buckets,
)
from recipe_normalize import normalize_recipe_name, recipe_search_key

# Транслитерация для названий блюд подключена в recipe_normalize.py (опционально transliterate).

_MSG_ONLY_HOME_PANTRY_INGREDIENTS = (
    "В этом рецепте только базовые позиции: вода, соль, перец для приправы и лавровый лист — "
    "считаем, что они есть дома и не включаем их в список покупок."
)

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

START_HELP_TEXT = "Привет, что готовим? Напиши название блюда, а я подберу рецепт"

ASK_DISH_NAME_TEXT = "Напишите название блюда, которое будем готовить."

_SIM_WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)
_CY_SOFT_SIGN_RE = re.compile(r"[ьъ]")

# Telegram ID администратора: только он может добавлять блюда.
ADMIN_ID = 69026978

# Путь к SQLite-базе данных.
DB_PATH = "recipes.db"
# Сколько названий показывать в /recipes (лимит Telegram ~4096 символов на сообщение).
RECIPES_LIST_LIMIT = 100


def _migrate_recipe_name_search(conn: sqlite3.Connection) -> None:
    """Колонка и индекс для O(1) поиска по нормализованному имени блюда."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(recipes)")
    cols = [row[1] for row in cur.fetchall()]
    if "name_search" not in cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN name_search TEXT")
        conn.commit()

    cur.execute(
        """
        SELECT id, name FROM recipes
        WHERE name_search IS NULL OR TRIM(COALESCE(name_search, '')) = ''
        """
    )
    pending = cur.fetchall()
    if pending:
        for rid, name in pending:
            cur.execute(
                "UPDATE recipes SET name_search = ? WHERE id = ?",
                (recipe_search_key(str(name)), int(rid)),
            )
        conn.commit()

    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_recipes_name_search'"
    )
    if cur.fetchone() is not None:
        return

    cur.execute(
        """
        SELECT 1 FROM (
            SELECT name_search FROM recipes
            WHERE name_search IS NOT NULL AND TRIM(name_search) != ''
            GROUP BY name_search
            HAVING COUNT(*) > 1
        ) LIMIT 1
        """
    )
    has_dup = cur.fetchone() is not None
    try:
        if has_dup:
            logging.warning(
                "В recipes есть разные блюда с одинаковым name_search — индекс не UNIQUE."
            )
            cur.execute(
                "CREATE INDEX idx_recipes_name_search ON recipes(name_search)"
            )
        else:
            cur.execute(
                "CREATE UNIQUE INDEX idx_recipes_name_search ON recipes(name_search)"
            )
    except sqlite3.OperationalError as e:
        logging.warning("Индекс name_search: %s — создаётся обычный INDEX.", e)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_recipes_name_search ON recipes(name_search)"
        )
    conn.commit()


def _migrate_recipe_source_url(conn: sqlite3.Connection) -> None:
    """Опциональная ссылка на страницу рецепта (из импорта HF или вручную)."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(recipes)")
    cols = [row[1] for row in cur.fetchall()]
    if "source_url" not in cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN source_url TEXT")
        conn.commit()


# Отдельный роутер помогает держать обработчики в одном месте.
router = Router()

# Один активный запуск «Проверить ингредиенты» на чат (защита от двойного нажатия).
_quiz_edit_locks: dict[int, asyncio.Lock] = {}


def _quiz_edit_lock_for_chat(chat_id: int) -> asyncio.Lock:
    if chat_id not in _quiz_edit_locks:
        _quiz_edit_locks[chat_id] = asyncio.Lock()
    return _quiz_edit_locks[chat_id]


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


class RecipeBrowseStates(StatesGroup):
    """Просмотр карточки рецепта перед запуском квиза."""

    viewing_offer = State()


def init_db() -> None:
    """
    Создаём таблицы при старте бота.
    Дополнительно: колонка recipes.name_search и индекс для быстрого поиска по имени.
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
        _migrate_recipe_name_search(conn)
        _migrate_recipe_source_url(conn)


def get_recipes_list_preview(limit: int = RECIPES_LIST_LIMIT) -> tuple[list[str], int]:
    """Первые `limit` названий (по имени) и общее число блюд — для /recipes без полного скана в память."""
    cap = max(1, min(limit, 500))
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM recipes")
        total = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT name FROM recipes ORDER BY name COLLATE NOCASE LIMIT ?",
            (cap,),
        )
        names = [str(row[0]) for row in cursor.fetchall()]
    return names, total


def add_recipe_to_db(
    recipe_name: str,
    ingredients: list[str],
    *,
    source_url: str | None = None,
) -> bool:
    """
    Добавляет рецепт и его ингредиенты в БД.
    Возвращает:
      - True, если успешно добавлено;
      - False, если блюдо уже существует (нарушение UNIQUE).
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO recipes (name, name_search, source_url) VALUES (?, ?, ?)",
                (
                    recipe_name,
                    recipe_search_key(recipe_name),
                    (source_url.strip() if isinstance(source_url, str) and source_url.strip() else None),
                ),
            )
            recipe_id = cursor.lastrowid

            cursor.executemany(
                "INSERT INTO ingredients (recipe_id, name) VALUES (?, ?)",
                [(recipe_id, ingredient) for ingredient in ingredients],
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def sql_like_escape(fragment: str) -> str:
    return fragment.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def collect_search_keys(normalized_name: str) -> list[str]:
    """
    Несколько ключей для LIKE по name_search: базовый + упрощение укр./рус.
    (ь/ъ, і→и, ї→и, …) чтобы находились «український» vs «украинский» и т.п.
    """
    nm = normalize_recipe_name(normalized_name).strip()
    nm = " ".join(nm.split())
    if not nm:
        return []
    keys: list[str] = []
    seen: set[str] = set()

    def push(fragment: str) -> None:
        k = recipe_search_key(fragment)
        if len(k) < 2 or k in seen:
            return
        seen.add(k)
        keys.append(k)

    push(nm)
    cf = nm.casefold()
    push(_CY_SOFT_SIGN_RE.sub("", cf))
    _uk_ru = str.maketrans({"і": "и", "ї": "и", "є": "е", "ґ": "г"})
    push(cf.translate(_uk_ru))
    push(_CY_SOFT_SIGN_RE.sub("", cf.translate(_uk_ru)))
    return keys


def similarity_tokens(query_key: str) -> list[str]:
    """Токены для подбора похожих рецептов (длинные — раньше) + укр./рус. варианты слов."""
    query_key_cf = (query_key or "").strip().casefold()
    out: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        if len(s) < 3 or s in seen:
            return
        seen.add(s)
        out.append(s)

    for sk in collect_search_keys(query_key_cf):
        add(sk)

    words = _SIM_WORD_RE.findall(query_key_cf)
    for w in sorted(words, key=len, reverse=True):
        cf = w.casefold()
        add(cf)
        for sk in collect_search_keys(cf):
            add(sk)
    return out


def search_recipe_ids_substring(normalized_query: str) -> list[int]:
    """Рецепты, где name_search содержит запрос (или его укр./рус. вариант) как подстроку."""
    ordered: list[int] = []
    seen: set[int] = set()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        for key in collect_search_keys(normalized_query):
            pat = f"%{sql_like_escape(key)}%"
            cur.execute(
                "SELECT id FROM recipes WHERE name_search LIKE ? ESCAPE '\\' ORDER BY id",
                (pat,),
            )
            for (rid,) in cur.fetchall():
                rid = int(rid)
                if rid not in seen:
                    seen.add(rid)
                    ordered.append(rid)
    return ordered


def search_recipe_ids_all_significant_words(normalized_query: str) -> list[int]:
    """
    Все значимые слова запроса (≥3 символа) должны встречаться в названии
    (каждое — как подстрока, с вариантами collect_search_keys).
    """
    nm = normalize_recipe_name(normalized_query).strip()
    nm = " ".join(nm.split())
    words = [w.casefold() for w in _SIM_WORD_RE.findall(nm) if len(w.casefold()) >= 3]
    if len(words) < 2:
        return []
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        conj_parts: list[str] = []
        params: list[str] = []
        for w in words:
            variants = collect_search_keys(w)
            if not variants:
                return []
            or_parts = ["name_search LIKE ? ESCAPE '\\'" for _ in variants]
            conj_parts.append("(" + " OR ".join(or_parts) + ")")
            for vk in variants:
                params.append(f"%{sql_like_escape(vk)}%")
        sql = "SELECT id FROM recipes WHERE " + " AND ".join(conj_parts) + " ORDER BY id"
        cur.execute(sql, params)
        return [int(r[0]) for r in cur.fetchall()]


def fetch_similar_recipe_ids(query_key: str, exclude: set[int], limit: int = 40) -> list[int]:
    ban = set(exclude)
    out: list[int] = []
    tokens = similarity_tokens(query_key)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        for tok in tokens:
            need = limit - len(out)
            if need <= 0:
                break
            pat = f"%{sql_like_escape(tok)}%"
            if ban:
                qs = ",".join("?" * len(ban))
                sql = (
                    f"SELECT id FROM recipes WHERE name_search LIKE ? ESCAPE '\\' "
                    f"AND id NOT IN ({qs}) ORDER BY id LIMIT ?"
                )
                cur.execute(sql, (pat, *ban, need))
            else:
                cur.execute(
                    "SELECT id FROM recipes WHERE name_search LIKE ? ESCAPE '\\' ORDER BY id LIMIT ?",
                    (pat, need),
                )
            for (rid,) in cur.fetchall():
                rid = int(rid)
                if rid in ban:
                    continue
                out.append(rid)
                ban.add(rid)
                if len(out) >= limit:
                    return out
    return out


def filter_recipe_ids_with_ingredients(ids: list[int]) -> list[int]:
    """Оставляет только те id, для которых в БД есть хотя бы одна строка ингредиентов."""
    if not ids:
        return []
    uniq = list(dict.fromkeys(int(i) for i in ids))
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        qs = ",".join("?" * len(uniq))
        cur.execute(
            f"SELECT DISTINCT recipe_id FROM ingredients WHERE recipe_id IN ({qs})",
            uniq,
        )
        have = {int(r[0]) for r in cur.fetchall()}
    return [i for i in uniq if i in have]


def resolve_recipe_external_url(source_url: str | None) -> str | None:
    """
    Только прямая ссылка на страницу рецепта, как в датасете HF (поле url).
    Fallback-поиск по имени на povarenok.ru с кириллицей в query давал 404 и кракозябры.
    """
    if not isinstance(source_url, str):
        return None
    u = source_url.strip()
    if u.startswith(("http://", "https://")):
        return u
    return None


def format_recipe_offer_html(
    recipe_name: str, ingredients: list[str], link: str | None
) -> str:
    lines: list[str] = []
    cap = 100
    for ing in ingredients[:cap]:
        lines.append(f"• {html.escape(format_ingredient_display(str(ing)))}")
    if len(ingredients) > cap:
        lines.append("• …")
    ing_block = "\n".join(lines) if lines else "• (нет списка)"
    title = html.escape(format_dish_title(recipe_name))
    if link:
        safe_link = html.escape(link, quote=True)
        link_block = (
            f"<a href=\"{safe_link}\">Открыть полный рецепт на сайте</a>\n\n"
            f"<i>Пошаговое приготовление — на странице по ссылке.</i>"
        )
    else:
        link_block = (
            "<i>Для этого блюда нет сохранённой ссылки на Поварёнок "
            "(рецепт добавлен вручную или название не совпало с датасетом).</i>"
        )
    return (
        f"<b>{title}</b>\n\n"
        f"<b>Ингредиенты:</b>\n{ing_block}\n\n"
        f"{link_block}"
    )


def recipe_offer_keyboard(recipe_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👍 Делаем", callback_data=f"recipe:do:{recipe_id}"),
                InlineKeyboardButton(
                    text="🔎 Найти другой рецепт",
                    callback_data="recipe:next",
                ),
            ],
            [
                InlineKeyboardButton(text="👎 Не делаем", callback_data="recipe:cancel"),
            ],
        ]
    )


def load_recipe_row(recipe_id: int) -> tuple[int, str, list[str], str | None] | None:
    rid = int(recipe_id)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name, source_url FROM recipes WHERE id = ?", (rid,))
        row = cur.fetchone()
        if not row:
            return None
        name = str(row[0])
        raw_url = row[1]
        url = str(raw_url).strip() if raw_url else None
        cur.execute(
            "SELECT name FROM ingredients WHERE recipe_id = ? ORDER BY id",
            (rid,),
        )
        ing_rows = cur.fetchall()
    ingredients = [str(r[0]) for r in ing_rows]
    return rid, name, ingredients, url


def find_recipe_by_id(recipe_id: int) -> tuple[int, str, list[str]] | None:
    """Блюдо по id из БД: (id, точное имя, ингредиенты)."""
    row = load_recipe_row(recipe_id)
    if row is None:
        return None
    rid, name, ingredients, _url = row
    return rid, name, ingredients


async def append_similar_to_browse(state: FSMContext) -> bool:
    data = await state.get_data()
    ids = list(data.get("browse_offer_ids") or [])
    qkey = str(data.get("browse_query_key") or "")
    batch = fetch_similar_recipe_ids(qkey, set(ids), limit=80)
    batch = filter_recipe_ids_with_ingredients(batch)
    if not batch:
        return False
    ids.extend(batch)
    await state.update_data(browse_offer_ids=ids)
    return True


async def recipe_browse_advance(state: FSMContext) -> tuple[bool, int | None]:
    data = await state.get_data()
    ids = list(data.get("browse_offer_ids") or [])
    pos = int(data.get("browse_offer_pos", 0)) + 1

    while pos >= len(ids):
        if not await append_similar_to_browse(state):
            prev = max(0, len(ids) - 1)
            await state.update_data(browse_offer_pos=prev)
            return False, None
        data = await state.get_data()
        ids = list(data.get("browse_offer_ids") or [])

    rid = ids[pos]
    await state.update_data(browse_offer_pos=pos, browse_current_recipe_id=rid)
    return True, rid


async def send_recipe_offer_card(
    message: Message,
    state: FSMContext,
    recipe_id: int,
    *,
    edit_target: Message | None = None,
) -> None:
    row = load_recipe_row(recipe_id)
    if row is None:
        return
    _rid, name, ingredients, src_url = row
    if not ingredients:
        txt = f"Для блюда {format_dish_title(name)} пока нет ингредиентов в базе."
        if edit_target:
            await edit_target.edit_text(txt)
        else:
            await message.answer(txt)
        return
    link = resolve_recipe_external_url(src_url)
    html_body = format_recipe_offer_html(name, ingredients, link)
    kb = recipe_offer_keyboard(recipe_id)
    await state.update_data(browse_current_recipe_id=recipe_id)
    if edit_target:
        await edit_target.edit_text(
            html_body,
            reply_markup=kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    else:
        await message.answer(
            html_body,
            reply_markup=kb,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


async def start_ingredient_quiz_for_message(message: Message, state: FSMContext, recipe_id: int) -> None:
    row = load_recipe_row(recipe_id)
    if row is None:
        await message.answer("Рецепт не найден в базе.")
        return
    rid, recipe_name, ingredients, _url = row
    if not ingredients:
        await message.answer(
            f"Для блюда {format_dish_title(recipe_name)} пока нет ингредиентов в базе."
        )
        return
    ingredients_quiz = exclude_home_pantry_ingredients(ingredients)
    prev_data = await state.get_data()
    extras_keep = _dedupe_extras_preserve_order(
        _coerce_extras_list(prev_data.get("shopping_list_extras"))
    )
    if not ingredients_quiz:
        await state.update_data(
            browse_offer_ids=None,
            browse_offer_pos=None,
            browse_current_recipe_id=None,
            browse_query_key=None,
            quiz_recipe_id=rid,
            quiz_recipe_name=recipe_name,
            quiz_ingredients=[],
            quiz_index=0,
            shopping_list=[],
            editing_last=False,
            shopping_list_extras=extras_keep,
        )
        await _finalize_ingredient_quiz_flow(
            message.bot,
            state,
            message,
            quiz_surface_message=None,
            recipe_id=rid,
            recipe_name=str(recipe_name),
            shopping_list_buy=[],
            preamble=_MSG_ONLY_HOME_PANTRY_INGREDIENTS,
        )
        return

    await state.update_data(
        browse_offer_ids=None,
        browse_offer_pos=None,
        browse_current_recipe_id=None,
        browse_query_key=None,
        quiz_recipe_id=rid,
        quiz_recipe_name=recipe_name,
        quiz_ingredients=ingredients_quiz,
        quiz_index=0,
        shopping_list=[],
        editing_last=False,
        shopping_list_extras=extras_keep,
    )
    await state.set_state(QuizStates.in_progress)
    first_ingredient = ingredients_quiz[0]
    await message.answer(
        f"Проверим, всё ли у тебя есть для того, чтобы приготовить {format_dish_title(recipe_name)}\n"
        f"У тебя есть {format_ingredient_display(first_ingredient)}?",
        reply_markup=ingredient_quiz_keyboard(rid, 0),
    )


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


async def _finalize_ingredient_quiz_flow(
    bot: Bot,
    state: FSMContext,
    anchor_chat_message: Message,
    *,
    quiz_surface_message: Message | None,
    recipe_id: int,
    recipe_name: str,
    shopping_list_buy: list,
    preamble: str | None = None,
) -> None:
    """
    Общий хвост после последнего шага квиза: закрывающее сообщение, корзины, переход и клавиатура.
    Если нечего было спрашивать (только базовый набор) — передай preamble и quiz_surface_message=None.
    """
    data = await state.get_data()

    buckets = data.get("accumulated_buckets", [])
    if not isinstance(buckets, list):
        buckets = []

    final_list_shown = bool(data.get("final_list_shown", False))
    was_editing = bool(data.get("editing_last", False))

    dish_missing = exclude_home_pantry_ingredients(
        [str(x).strip() for x in (shopping_list_buy or []) if str(x).strip()]
    )

    buckets = _upsert_bucket(buckets, recipe=str(recipe_name), missing=dish_missing)

    extras_keep = _dedupe_extras_preserve_order(_coerce_extras_list(data.get("shopping_list_extras")))

    closing_text = (
        "Сохранил ингредиенты в список покупок. "
        "Добавим новое блюдо или посмотрим список?"
    )
    kb_finish = quiz_finished_recheck_keyboard(recipe_id) if recipe_id > 0 else None

    if preamble:
        await anchor_chat_message.answer(preamble)

    if quiz_surface_message is not None:
        try:
            await quiz_surface_message.edit_text(closing_text, reply_markup=kb_finish)
        except TelegramBadRequest:
            await quiz_surface_message.answer(closing_text, reply_markup=kb_finish)
    else:
        await anchor_chat_message.answer(closing_text, reply_markup=kb_finish)

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

    resend_shop_here = quiz_surface_message or anchor_chat_message
    if final_list_shown and was_editing:
        shop_body = render_shopping_list_html_from_buckets(buckets, extras_keep)
        if shop_body:
            await resend_shop_here.answer(
                f"📦 Общий список покупок:\n{shop_body}",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await resend_shop_here.answer(
                "📦 Общий список покупок пуст — у вас всё уже есть ✅"
            )

    await _send_keyboard_message(anchor_chat_message, state, keyboard_text)


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
    """Показывает доступные блюда из базы (первые N и общее число)."""
    recipes, total = get_recipes_list_preview()
    if not recipes or total == 0:
        await message.answer(
            "Пока нет ни одного блюда 😕\n"
            "Администратор может добавить первое через /add_recipe."
        )
        return

    lines = [f"• {emoji_for_dish(name)} {name.capitalize()}" for name in recipes]
    body = "Доступные блюда 🍽️:\n" + "\n".join(lines)
    if total > len(recipes):
        body += (
            f"\n\n… и ещё {total - len(recipes)} в базе "
            f"(показаны первые {len(recipes)} по алфавиту). "
            "Уточните название или напишите его для поиска рецепта."
        )
    await message.answer(body)


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

    if current_state == RecipeBrowseStates.viewing_offer.state:
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
        await message.answer(f"Отменено ✅\n{ASK_DISH_NAME_TEXT}")
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
    for part in parts:
        if is_always_home_pantry_ingredient(part):
            continue
        mk = ingredient_merge_key(part)
        if not mk:
            continue
        if mk in keys_seen:
            continue
        keys_seen.add(mk)
        disp = canonical_ingredient_display(part)
        extras_prev.append(disp)

    extras_prev = _dedupe_extras_preserve_order(extras_prev)

    await state.update_data(
        shopping_list_extras=extras_prev,
        accumulated_buckets=buckets,
        manual_product_return_state=None,
    )

    target_state = ret_state if ret_state else QuizStates.after_shopping_list.state
    await state.set_state(target_state)

    body = render_shopping_list_html_from_buckets(buckets, extras_prev)
    kb = (
        shopping_list_actions_keyboard()
        if target_state == QuizStates.after_shopping_list.state
        else addition_prompt_keyboard()
    )

    if body:
        await message.answer(
            f"📦 Общий список покупок:\n{body}",
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=kb,
        )
        return

    await message.answer(
        "В списке пока пусто — добавьте продукты или пройдите квиз по блюду."
        if not buckets and not extras_prev
        else "Эти позиции уже есть в списке или совпадают с тем, что из блюд.",
        reply_markup=kb,
    )


@router.message(F.text & ~F.text.startswith("/"))
async def handle_recipe_search(message: Message, state: FSMContext) -> None:
    """
    Любой текст (не команда) считаем названием блюда.
    Показываем карточку: название, ингредиенты, ссылка и кнопки Делаем / Другой / Не делаем.
    """
    user_text = (message.text or "").strip()
    if not user_text:
        return

    current_state = await state.get_state()

    if current_state == QuizStates.awaiting_manual_product.state:
        await message.answer(
            "Сейчас жду продукты для списка через запятую или отправьте /cancel."
        )
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

    if current_state == RecipeBrowseStates.viewing_offer.state:
        await state.update_data(
            browse_offer_ids=None,
            browse_offer_pos=None,
            browse_current_recipe_id=None,
            browse_query_key=None,
        )
        await state.set_state(None)

    normalized_name = " ".join(normalize_recipe_name(user_text).split())
    query_key = recipe_search_key(normalized_name)

    primary_ids = search_recipe_ids_substring(normalized_name)
    recipe_ids = filter_recipe_ids_with_ingredients(list(primary_ids))
    if not recipe_ids:
        recipe_ids = filter_recipe_ids_with_ingredients(
            search_recipe_ids_all_significant_words(normalized_name)
        )
    if not recipe_ids:
        recipe_ids = filter_recipe_ids_with_ingredients(
            fetch_similar_recipe_ids(query_key, set(), limit=80)
        )

    if not recipe_ids:
        await message.answer(
            "Блюдо не найдено. Попробуйте другое название или команду /recipes."
        )
        return

    await state.update_data(
        browse_offer_ids=recipe_ids,
        browse_offer_pos=0,
        browse_query_key=query_key,
        browse_current_recipe_id=recipe_ids[0],
    )
    await state.set_state(RecipeBrowseStates.viewing_offer)
    await send_recipe_offer_card(message, state, recipe_ids[0], edit_target=None)


@router.callback_query(F.data.startswith("recipe:"))
async def recipe_offer_callbacks(callback: CallbackQuery, state: FSMContext) -> None:
    """Кнопки под карточкой рецепта до запуска квиза."""
    if callback.message is None:
        await callback.answer()
        return

    current_state = await state.get_state()
    parts = (callback.data or "").split(":")

    if len(parts) < 2:
        await callback.answer()
        return

    if parts[1] == "cancel":
        if current_state != RecipeBrowseStates.viewing_offer.state:
            await callback.answer("Это меню уже неактуально.", show_alert=False)
            return
        await callback.answer()
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
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await callback.message.answer(ASK_DISH_NAME_TEXT)
        return

    if current_state != RecipeBrowseStates.viewing_offer.state:
        await callback.answer("Сначала найдите блюдо по названию.", show_alert=False)
        return

    if parts[1] == "next":
        ok, rid = await recipe_browse_advance(state)
        if not ok or rid is None:
            await callback.answer(
                "Больше нет других подходящих рецептов 😕",
                show_alert=True,
            )
            return
        await callback.answer()
        await send_recipe_offer_card(
            callback.message, state, rid, edit_target=callback.message
        )
        return

    if parts[1] == "do":
        if len(parts) != 3:
            await callback.answer()
            return
        try:
            rid = int(parts[2])
        except ValueError:
            await callback.answer()
            return
        data = await state.get_data()
        expected = data.get("browse_current_recipe_id")
        if expected is not None and int(expected) != rid:
            await callback.answer("Устаревшая кнопка.", show_alert=False)
            return
        await callback.answer()
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await start_ingredient_quiz_for_message(callback.message, state, rid)
        return

    await callback.answer()


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

    anchor = callback.message
    await _finalize_ingredient_quiz_flow(
        callback.bot,
        state,
        anchor,
        quiz_surface_message=anchor,
        recipe_id=rid,
        recipe_name=recipe_name,
        shopping_list_buy=shopping_list,
    )


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

        ingredients_quiz = exclude_home_pantry_ingredients(ingredients)
        if not ingredients_quiz:
            await _finalize_ingredient_quiz_flow(
                callback.bot,
                state,
                callback.message,
                quiz_surface_message=None,
                recipe_id=recipe_id,
                recipe_name=str(recipe_name),
                shopping_list_buy=[],
                preamble=_MSG_ONLY_HOME_PANTRY_INGREDIENTS,
            )
            return

        await state.set_state(QuizStates.in_progress)
        await state.update_data(
            quiz_recipe_id=recipe_id,
            quiz_recipe_name=recipe_name,
            quiz_ingredients=ingredients_quiz,
            quiz_index=0,
            shopping_list=[],
            shopping_list_extras=extras_keep,
        )

        await callback.message.answer(
            f"Редактируем блюдо {format_dish_title(recipe_name)} ✏️\n"
            f"У тебя есть {format_ingredient_display(ingredients_quiz[0])}?",
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
    # FSM только в RAM: полный сброс состояний всех пользователей при каждом перезапуске процесса.
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    logging.info(
        "Состояния диалогов (FSM) в памяти: после перезапуска бота у всех пользователей чистый сеанс."
    )

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
