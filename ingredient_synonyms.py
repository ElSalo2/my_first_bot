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

    Возвращает список пар (текст ссылки для magnit.ru, подпись для пользователя в HTML).
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
        clause = format_dishes_clause(recipes_list)
        search_term = display
        if clause:
            label_core = f"{display} ({clause})"
        else:
            label_core = display
        rows.append((search_term, label_core))

    return rows
