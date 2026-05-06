"""
Синонимы ингредиентов и подписи «для …» для списка покупок.

Алиасы ингредиентов читаются из JSON (см. data/ingredient_aliases.json).
Файл перечитывается при изменении на диске — правки без правки кода.

Переопределить путь: переменная окружения INGREDIENT_ALIASES_JSON.

Родительный падеж названий блюд («для борща») — pymorphy3 / pymorphy2,
если установлены; иначе запасной вариант: строка в нижнем регистре.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    from pymorphy3 import MorphAnalyzer as _MorphAnalyzerCls
except ImportError:  # pragma: no cover
    try:
        from pymorphy2 import MorphAnalyzer as _MorphAnalyzerCls
    except ImportError:
        _MorphAnalyzerCls = None

_PACKAGE_DIR = Path(__file__).resolve().parent
_DEFAULT_ALIASES_PATH = _PACKAGE_DIR / "data" / "ingredient_aliases.json"

_ALIASES_CACHE: dict[str, str] | None = None
_ALIASES_MTIME: float | None = None
_ALIASES_SOURCE: Path | None = None

_MORPH_ANALYZER: Any | None | bool = False


def _aliases_json_path() -> Path:
    raw = (os.getenv("INGREDIENT_ALIASES_JSON") or "").strip()
    return Path(raw).expanduser().resolve() if raw else _DEFAULT_ALIASES_PATH


def _load_aliases_from_disk(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logging.exception("Не удалось прочитать %s", path)
        return {}
    raw = data.get("aliases")
    if raw is None:
        raw = data.get("ingredient_aliases")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        ks = " ".join(k.strip().split()).casefold()
        vs = " ".join(v.strip().split())
        if ks and vs:
            out[ks] = vs
    return out


def get_ingredient_aliases() -> dict[str, str]:
    """
    Алиасы variant_lc → каноническая строка для списка покупок и для объединения строк.
    Кэш сбрасывается при изменении mtime файла.
    """
    global _ALIASES_CACHE, _ALIASES_MTIME, _ALIASES_SOURCE
    path = _aliases_json_path()
    try:
        mtime = path.stat().st_mtime if path.is_file() else None
    except OSError:
        mtime = None

    if (
        _ALIASES_CACHE is not None
        and path == _ALIASES_SOURCE
        and mtime is not None
        and _ALIASES_MTIME == mtime
    ):
        return _ALIASES_CACHE

    _ALIASES_SOURCE = path
    _ALIASES_MTIME = mtime
    _ALIASES_CACHE = _load_aliases_from_disk(path)
    return _ALIASES_CACHE


def _morph_analyzer():
    global _MORPH_ANALYZER
    if _MORPH_ANALYZER is False:
        if _MorphAnalyzerCls is None:
            _MORPH_ANALYZER = None
            logging.warning(
                "Морфология для «для …» недоступна: установите pymorphy3 "
                "(pip install pymorphy3). Используется запасной вариант без склонения."
            )
        else:
            try:
                _MORPH_ANALYZER = _MorphAnalyzerCls()
            except Exception:
                logging.exception("Не удалось инициализировать MorphAnalyzer")
                _MORPH_ANALYZER = None
    return _MORPH_ANALYZER


_RX_HAS_CYRILLIC = re.compile(r"[а-яё]", re.IGNORECASE)
_RX_LATIN_WORD = re.compile(r"^[a-z]+$", re.IGNORECASE)
_PAREN_CHUNK_RE = re.compile(r"\([^()]*\)")


def strip_parenthetical_segments(text: str) -> str:
    """
    Убирает все фрагменты в круглых скобках — для поиска на сайте магазина
    без количеств вида «(1 шт)», «(250 мл)» и т.п.
    Повторяет удаление, чтобы снять несколько пар подряд.
    """
    s = " ".join((text or "").strip().split())
    if not s:
        return ""
    while True:
        t = _PAREN_CHUNK_RE.sub("", s)
        t = " ".join(t.split())
        if t == s:
            break
        s = t
    return s.strip()


def _word_genitive(word: str, morph: Any) -> str:
    """Одно слово в родительном падеже для подписи после «для»."""
    if not word:
        return ""
    if _RX_LATIN_WORD.fullmatch(word):
        return word.lower()
    if not _RX_HAS_CYRILLIC.search(word):
        return word.lower()

    parsed = morph.parse(word)
    if not parsed:
        return word.lower()
    p = parsed[0]
    pos = getattr(p.tag, "POS", None)
    if pos in {"PREP", "CONJ", "PRCL", "INTJ"}:
        return word.lower()
    gent = p.inflect({"gent"})
    return gent.word.lower() if gent else word.lower()


def phrase_to_genitive(phrase: str) -> str:
    """Фраза названия блюда целиком в родительном падеже (слова по отдельности)."""
    name = " ".join((phrase or "").strip().split())
    if not name:
        return ""
    morph = _morph_analyzer()
    if morph is None:
        return name.lower()

    parts_out: list[str] = []
    for raw_word in name.split():
        hyphens = raw_word.split("-")
        gh = [_word_genitive(h.strip(), morph) for h in hyphens if h.strip()]
        parts_out.append("-".join(gh) if gh else raw_word.lower())
    return " ".join(parts_out)


def recipe_name_genitive(recipe_name: str) -> str:
    """Краткая подпись «для чего» в строке списка покупок."""
    return phrase_to_genitive(recipe_name)


def ingredient_merge_key(raw: str) -> str:
    """Стабильный ключ группировки (нижний регистр канонического названия)."""
    cleaned = " ".join((raw or "").strip().split())
    if not cleaned:
        return ""
    variant = cleaned.casefold()
    aliases = get_ingredient_aliases()
    display = aliases.get(variant, cleaned)
    return display.casefold()


def canonical_ingredient_display(raw: str) -> str:
    """Как показывать ингредиент после объединения синонимов."""
    cleaned = " ".join((raw or "").strip().split())
    if not cleaned:
        return ""
    aliases = get_ingredient_aliases()
    return aliases.get(cleaned.casefold(), cleaned)


# Точные синонимы «домашнего» набора после ingredient_merge_key;
# ключи считаются лениво, чтобы успели загрузиться алиасы с диска.
_PANTRY_SEED_PHRASES: tuple[str, ...] = (
    "вода",
    "кипяток",
    "соль",
    "соль по вкусу",
    "перец",
    "перец черный",
    "перец чёрный",
    "чёрный перец",
    "черный перец",
    "белый перец",
    "перец белый",
    "перец молотый",
    "молотый перец",
    "перец горошком",
    "лавровый лист",
    "лавровые листы",
    "лист лавровый",
)
_pantry_exact_merge_keys_cache: frozenset[str] | None = None

_RX_WORD_SALT = re.compile(
    r"(^|[\s,;])(щепотк[аи]\s+)?сол[ьюи]([\s,.;]|$)",
    re.IGNORECASE | re.UNICODE,
)
_RX_WORD_WATER = re.compile(
    r"(^|[\s,;])(стакан|чашка|ст\.?)?\s*(\d+([.,]\d+)?\s*)?"
    r"(мили|санти)?литр(а|ов|ы)?([\s,.;]|$)|"
    r"(^|[\s,;])вод[уы]([\s,.;]|$)",
    re.IGNORECASE | re.UNICODE,
)
_WATER_EXCLUDE_MARKER = ("минеральн", "газированн", "апельсинов", "лимонная вода")


def _pantry_exact_merge_keys() -> frozenset[str]:
    global _pantry_exact_merge_keys_cache
    if _pantry_exact_merge_keys_cache is None:
        _pantry_exact_merge_keys_cache = frozenset(
            k for p in _PANTRY_SEED_PHRASES if (k := ingredient_merge_key(p))
        )
    return _pantry_exact_merge_keys_cache


def is_always_home_pantry_ingredient(raw: str) -> bool:
    """
    Вода / соль / пряный перец (не болгарский и т.п.) / лавровый лист —
    считаем, что дома есть: не показываем в квизе и не выводим в списке покупок.
    """
    mk = ingredient_merge_key(raw)
    if mk in _pantry_exact_merge_keys():
        return True

    disp = canonical_ingredient_display(raw)
    ck = strip_parenthetical_segments(disp).casefold()
    if not ck:
        return False

    if "лавров" in ck:
        return True

    if _RX_WORD_SALT.search(ck):
        return True

    if _RX_WORD_WATER.search(f" {ck} "):
        if any(x in ck for x in _WATER_EXCLUDE_MARKER):
            return False
        return True

    if "перец" in ck:
        veg_markers = (
            "болгарск",
            "сладк",
            "чили",
            "стручков",
            "кайен",
            "халапен",
            "пепперон",
            "капсу",
            "перчин",
        )
        if any(m in ck for m in veg_markers):
            return False
        if "душист" in ck:
            return False
        return True

    return False


def exclude_home_pantry_ingredients(lines: list[str] | tuple[str, ...] | None) -> list[str]:
    """Строки ингредиентов без «домашнего» набора."""
    out: list[str] = []
    for raw in lines or []:
        s = str(raw).strip()
        if s and not is_always_home_pantry_ingredient(s):
            out.append(s)
    return out


def format_dishes_clause(recipe_names: list[str]) -> str:
    """
    «для борща», «для борща и для плова», «для борща, для плова и для салата».
    """
    names = sorted({r.strip() for r in recipe_names if str(r).strip()}, key=lambda x: x.casefold())
    if not names:
        return ""
    chunks = [f"для {recipe_name_genitive(r)}" for r in names]
    if len(chunks) == 1:
        return chunks[0]
    if len(chunks) == 2:
        return f"{chunks[0]} и {chunks[1]}"
    return ", ".join(chunks[:-1]) + f" и {chunks[-1]}"


def shopping_lines_from_buckets(
    buckets: list[dict],
    extras: list[str] | None = None,
) -> list[tuple[str, str]]:
    """
    Собирает строки списка покупок из корзин по блюдам и дополнительных позиций (вручную).

    extras — продукты, добавленные пользователем; попадают в тот же формат (ссылка + эмодзи снаружи).
    Дубликаты по синонимам с позициями из блюд объединяются.

    Возвращает список пар (текст для поиска на magnit.ru без скобок «(1 шт)» и т.д.,
    подпись для пользователя в HTML — тоже без таких скобок).
    Фраза «(для борща и для плова, …)» добавляется только если один и тот же ингредиент
    к покупке относится к двум и более блюдам.

    Подпись уже без экранирования HTML — вызывающий делает escape.

    Порядок — по алфавиту канонической подписи ингредиента.
    """
    grouped: dict[str, dict] = defaultdict(lambda: {"display": "", "recipes": set()})

    for bucket in buckets or []:
        recipe = str(bucket.get("recipe", "") or "").strip()
        missing = bucket.get("missing", [])
        if not recipe or not isinstance(missing, list):
            continue
        for raw in missing:
            item = str(raw).strip()
            if not item:
                continue
            if is_always_home_pantry_ingredient(item):
                continue
            mk = ingredient_merge_key(item)
            if not mk:
                continue
            entry = grouped[mk]
            if not entry["display"]:
                entry["display"] = canonical_ingredient_display(item)
            entry["recipes"].add(recipe)

    for raw in extras or []:
        item = str(raw).strip()
        if not item:
            continue
        if is_always_home_pantry_ingredient(item):
            continue
        mk = ingredient_merge_key(item)
        if not mk:
            continue
        entry = grouped[mk]
        if not entry["display"]:
            entry["display"] = canonical_ingredient_display(item)

    rows: list[tuple[str, str]] = []
    for mk in sorted(grouped.keys(), key=lambda k: grouped[k]["display"].casefold()):
        info = grouped[mk]
        display = info["display"] or mk
        recipes_list = sorted(info["recipes"], key=lambda x: x.casefold())
        clause = (
            format_dishes_clause(recipes_list) if len(recipes_list) >= 2 else ""
        )
        base = strip_parenthetical_segments(display) or display.strip()
        search_term = base
        if clause:
            label_core = f"{base} ({clause})"
        else:
            label_core = base
        rows.append((search_term, label_core))

    return rows
