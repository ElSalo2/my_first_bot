#!/usr/bin/env python3
"""
Импорт рецептов из Hugging Face: rogozinushka/povarenok-recipes → SQLite recipes.db.

Зависимость (один раз):  pip install datasets

В датасете ~147k записей. Полная загрузка сильно раздувает recipes.db.

По умолчанию добавляется не более --limit новых рецептов (не считая уже существующих имён).

Примеры:
  python scripts/import_povarenok.py --limit 3000
  python scripts/import_povarenok.py --limit 0 --db recipes.db   # всё (долго, большой файл)
"""

from __future__ import annotations

import argparse
import ast
import re
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from recipe_normalize import recipe_search_key


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS recipes (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            name_search TEXT,
            source_url TEXT
        );
        CREATE TABLE IF NOT EXISTS ingredients (
            id INTEGER PRIMARY KEY,
            recipe_id INTEGER,
            name TEXT,
            FOREIGN KEY(recipe_id) REFERENCES recipes(id) ON DELETE CASCADE
        );
        """
    )
    conn.commit()


def ensure_name_search_column(conn: sqlite3.Connection) -> None:
    """Для старых БД без столбца name_search."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(recipes)")
    cols = [row[1] for row in cur.fetchall()]
    if "name_search" not in cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN name_search TEXT")
        conn.commit()


def ensure_source_url_column(conn: sqlite3.Connection) -> None:
    """Для старых БД без столбца source_url."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(recipes)")
    cols = [row[1] for row in cur.fetchall()]
    if "source_url" not in cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN source_url TEXT")
        conn.commit()


def parse_ingredients(raw: object) -> list[str]:
    """Колонка ingredients: dict или строка вида "{'Молоко': '250 мл', ...}"."""
    if raw is None:
        return []
    if isinstance(raw, dict):
        pairs = raw.items()
    elif isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            d = ast.literal_eval(s)
        except (ValueError, SyntaxError, MemoryError):
            return []
        if not isinstance(d, dict):
            return []
        pairs = d.items()
    else:
        return []

    out: list[str] = []
    for k, v in pairs:
        name = str(k).strip()
        if not name:
            continue
        if v is not None and str(v).strip():
            name = f"{name} ({str(v).strip()})"
        out.append(name)
    return out


# Частые commit на каждую строку сильно замедляют полный импорт (~147k записей).
_COMMIT_EVERY = 500

_JUNK_NAME = re.compile(
    r"главная|версия\s+для\s+печати|главная\s*>",
    re.IGNORECASE | re.UNICODE,
)


def acceptable_title(name: str) -> bool:
    n = " ".join(name.split()).strip()
    if len(n) < 3:
        return False
    if _JUNK_NAME.search(n):
        return False
    if "http://" in n.lower() or "https://" in n.lower():
        return False
    return True


def existing_names_casefold(conn: sqlite3.Connection) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM recipes")
    return {str(r[0]).casefold() for r in cur.fetchall()}


def main() -> int:
    ap = argparse.ArgumentParser(description="Импорт povarenok-recipes в SQLite.")
    ap.add_argument("--db", type=Path, default=Path("recipes.db"), help="Файл базы")
    ap.add_argument(
        "--limit",
        type=int,
        default=3000,
        help="Максимум новых рецептов для добавления (0 = без ограничения)",
    )
    ap.add_argument("--skip", type=int, default=0, help="Пропустить первые N строк датасета")
    args = ap.parse_args()

    try:
        from datasets import load_dataset
    except ImportError:
        print("Нужен пакет datasets:  pip install datasets", file=sys.stderr)
        return 1

    db_path = args.db.resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        ensure_schema(conn)
        ensure_name_search_column(conn)
        ensure_source_url_column(conn)
        seen_cf = existing_names_casefold(conn)

        ds = load_dataset(
            "rogozinushka/povarenok-recipes",
            split="train",
            streaming=True,
        )

        it = iter(ds)
        for _ in range(max(0, args.skip)):
            next(it, None)

        added = skipped_dup = skipped_bad = 0

        for row in it:
            if args.limit > 0 and added >= args.limit:
                break

            title = str(row.get("name") or "").strip()
            title = " ".join(title.split())
            if not acceptable_title(title):
                skipped_bad += 1
                continue

            tcf = title.casefold()
            if tcf in seen_cf:
                skipped_dup += 1
                continue

            ingredients = parse_ingredients(row.get("ingredients"))
            if not ingredients:
                skipped_bad += 1
                continue

            src_url = row.get("url")
            if src_url is None:
                url_val = None
            else:
                url_val = str(src_url).strip()
                if url_val and not url_val.startswith(("http://", "https://")):
                    url_val = None

            cur = conn.cursor()
            try:
                cur.execute("SAVEPOINT pov_import")
                cur.execute(
                    "INSERT INTO recipes (name, name_search, source_url) VALUES (?, ?, ?)",
                    (title, recipe_search_key(title), url_val),
                )
                rid = cur.lastrowid
                cur.executemany(
                    "INSERT INTO ingredients (recipe_id, name) VALUES (?, ?)",
                    [(rid, ing) for ing in ingredients],
                )
                cur.execute("RELEASE SAVEPOINT pov_import")
            except sqlite3.IntegrityError:
                cur.execute("ROLLBACK TO SAVEPOINT pov_import")
                skipped_dup += 1
                continue

            seen_cf.add(tcf)
            added += 1
            if added % _COMMIT_EVERY == 0:
                conn.commit()

        conn.commit()

        print(
            f"Готово: добавлено новых рецептов: {added}; "
            f"пропущено (дубликаты имён): {skipped_dup}; "
            f"пропущено (без ингредиентов / мусор в названии): {skipped_bad}."
        )
        print(f"База: {db_path}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
