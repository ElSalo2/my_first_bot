#!/usr/bin/env python3
"""
Подставить source_url из Hugging Face povarenok-recipes для уже импортированных строк,
где ссылка ещё пустая (повторный прогон после добавления колонки).

  python scripts/backfill_recipe_urls.py
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = _ROOT / "recipes.db"

if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from recipe_normalize import recipe_search_key


def _target_recipe_id(cur: sqlite3.Cursor, title: str) -> int | None:
    """Строка без URL: сначала точное имя, иначе единственное совпадение по name_search."""
    cur.execute(
        """
        SELECT id FROM recipes
        WHERE name = ?
          AND (source_url IS NULL OR TRIM(COALESCE(source_url, '')) = '')
        """,
        (title,),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])
    nsk = recipe_search_key(title)
    cur.execute(
        """
        SELECT id FROM recipes
        WHERE name_search = ?
          AND (source_url IS NULL OR TRIM(COALESCE(source_url, '')) = '')
        """,
        (nsk,),
    )
    rows = cur.fetchall()
    if len(rows) == 1:
        return int(rows[0][0])
    return None


def main() -> int:
    try:
        from datasets import load_dataset
    except ImportError:
        print("Нужен пакет datasets: pip install datasets", file=sys.stderr)
        return 1

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(recipes)")
        cols = [row[1] for row in cur.fetchall()]
        if "source_url" not in cols:
            cur.execute("ALTER TABLE recipes ADD COLUMN source_url TEXT")
            conn.commit()

        ds = load_dataset(
            "rogozinushka/povarenok-recipes",
            split="train",
            streaming=True,
        )
        updated = skipped = 0
        for row in ds:
            title = str(row.get("name") or "").strip()
            title = " ".join(title.split())
            raw_u = row.get("url")
            if not title or raw_u is None:
                skipped += 1
                continue
            url = str(raw_u).strip()
            if not url.startswith(("http://", "https://")):
                skipped += 1
                continue
            rid = _target_recipe_id(cur, title)
            if rid is None:
                skipped += 1
                continue
            cur.execute(
                """
                UPDATE recipes SET source_url = ?
                WHERE id = ?
                  AND (source_url IS NULL OR TRIM(COALESCE(source_url, '')) = '')
                """,
                (url, rid),
            )
            if cur.rowcount:
                updated += 1
            else:
                skipped += 1
            if (updated + skipped) % 5000 == 0:
                conn.commit()
        conn.commit()
        print(f"Готово: обновлено ссылок: {updated}; строк без обновления: {skipped}.")
        print(f"База: {DB_PATH.resolve()}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
