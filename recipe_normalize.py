"""
Нормализация названий блюд для поиска в базе (единые правила для бота и скриптов импорта).
"""

from __future__ import annotations

import logging
import re

try:
    from transliterate import translit
except ImportError:  # pragma: no cover
    translit = None

_LATIN_ONLY_RE = re.compile(r"^[A-Za-z]+$")
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

                if normalized.endswith(("sh", "ch")) and candidate.endswith(("ш", "ч")):
                    candidate = candidate[:-1] + "щ"

                return candidate

        return normalized

    return normalized


def recipe_search_key(name: str) -> str:
    """Ключ для индексированного поиска в SQLite (= normalize + Unicode casefold)."""
    return normalize_recipe_name(name).casefold()
