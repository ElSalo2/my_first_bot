#!/usr/bin/env python3
"""
Аудит словаря в recipes.db: токены в названиях блюд и в строках ингредиентов.

Проверяет:
  • падежи и распознавание pymorphy3/2 (normal_form, score);
  • уменьшительные формы (граммема Dimin в разборе, если есть);
  • частотные токены с низким score (возможные опечатки / не-словарь);
  • кластеры лемма → несколько поверхностных форм (падежи и варианты написания).

Запуск из корня проекта (рядом с recipes.db):

  python scripts/audit_vocabulary_morphology.py
  python scripts/audit_vocabulary_morphology.py --db path/to/recipes.db --top 80

При отсутствии pymorphy: pip install pymorphy3
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ingredient_synonyms import ingredient_merge_key, strip_parenthetical_segments  # noqa: E402
from recipe_normalize import normalize_recipe_name  # noqa: E402

try:
    from pymorphy3 import MorphAnalyzer
except ImportError:  # pragma: no cover
    try:
        from pymorphy2 import MorphAnalyzer
    except ImportError:
        MorphAnalyzer = None  # type: ignore[misc, assignment]


_SIM_WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
_CYR = re.compile(r"[а-яё]", re.IGNORECASE)
_SCORE_LOW = 0.45


def _words(text: str) -> list[str]:
    return [
        m.group(0).casefold()
        for m in _SIM_WORD.finditer(text or "")
        if len(m.group(0)) >= 3
    ]


def _prep_morph():
    if MorphAnalyzer is None:
        return None
    try:
        return MorphAnalyzer()
    except Exception:
        print("Не удалось инициализировать MorphAnalyzer.", file=sys.stderr)
        return None


def _parse_primary(
    morph, w: str
) -> tuple[str | None, float | None, bool]:
    """(normal_form, score, is_diminutive)."""
    if morph is None or not _CYR.search(w):
        return None, None, False
    try:
        p = morph.parse(w)[0]
    except Exception:
        return None, None, False
    nf = str(p.normal_form).casefold()
    sc = getattr(p, "score", None)
    tag_s = str(p.tag)
    dimin = "Dimin" in tag_s
    return nf, float(sc) if sc is not None else None, dimin


def run_audit(db_path: Path, *, top_unknown: int, top_clusters: int) -> int:
    if not db_path.is_file():
        print(f"Файл БД не найден: {db_path}", file=sys.stderr)
        print("Укажите --db или положите recipes.db в корень проекта.", file=sys.stderr)
        return 1

    morph = _prep_morph()

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM recipes WHERE TRIM(name) != ''")
        rcount = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM ingredients")
        icount = int(cur.fetchone()[0])
        cur.execute('SELECT COUNT(*) FROM ingredients WHERE TRIM(name) != "" AND name IS NOT NULL')
        ing_nonempty = int(cur.fetchone()[0])
    finally:
        conn.close()

    print(f"БД: {db_path}")
    print(f"Строк recipes (имя не пустое): {rcount}")
    print(f"Строк ingredients: {icount}, с непустым name: {ing_nonempty}")

    freq_recipes: Counter[str] = Counter()
    freq_ingredients: Counter[str] = Counter()
    merge_key_sample: dict[str, str] = {}

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM recipes")
        for (name,) in cur:
            nm = str(name or "").strip()
            if not nm:
                continue
            nm_n = " ".join(normalize_recipe_name(nm).split())
            for w in _words(nm_n.casefold()):
                freq_recipes[w] += 1
        cur.execute("SELECT name FROM ingredients")
        for (line,) in cur:
            raw = str(line or "").strip()
            if not raw:
                continue
            core = strip_parenthetical_segments(raw)
            text = " ".join(normalize_recipe_name(core).split()).casefold()
            for w in _words(text):
                freq_ingredients[w] += 1
            mk = ingredient_merge_key(raw)
            if mk and mk not in merge_key_sample:
                merge_key_sample[mk] = raw[:120]
    finally:
        conn.close()

    all_tokens = set(freq_recipes) | set(freq_ingredients)
    union_freq: Counter[str] = freq_recipes + freq_ingredients

    print(f"\nУникальных токенов (не меньше 3 симв., recipes): {len(freq_recipes)}")
    print(f"Уникальных токенов (не меньше 3 симв., ingredients, без скобок): {len(freq_ingredients)}")
    print(f"Уникальных токенов в объединении: {len(all_tokens)}")
    print(f"Различных merge_key ингредиентов (по ingredient_merge_key): {len(merge_key_sample)}")

    only_rec = sorted(set(freq_recipes) - set(freq_ingredients), key=lambda t: (-freq_recipes[t], t))[:40]
    only_ing = sorted(set(freq_ingredients) - set(freq_recipes), key=lambda t: (-freq_ingredients[t], t))[:40]
    print("\n- Только в названиях блюд (частота, до 40):")
    print(", ".join(f"{x} ({freq_recipes[x]})" for x in only_rec) or "(нет)")
    print("\n- Только в ингредиентах (до 40):")
    print(", ".join(f"{x} ({freq_ingredients[x]})" for x in only_ing) or "(нет)")

    if morph is None:
        print("\n[!] pymorphy не установлен - морфологический разбор пропущен.")
        print("  pip install pymorphy3")
        return 0

    low_score: Counter[str] = Counter()
    lemma_r: defaultdict[str, set[str]] = defaultdict(set)
    lemma_i: defaultdict[str, set[str]] = defaultdict(set)
    dimin_tokens: list[tuple[str, str, int]] = []

    for w in sorted(all_tokens):
        if not _CYR.search(w):
            continue
        nf, sc, dimin = _parse_primary(morph, w)
        weight = union_freq[w]
        if nf is None:
            low_score[w] += weight
            continue
        if sc is not None and sc < _SCORE_LOW:
            low_score[w] += weight
        if w in freq_recipes:
            lemma_r[nf].add(w)
        if w in freq_ingredients:
            lemma_i[nf].add(w)
        if dimin:
            dimin_tokens.append((w, nf, weight))

    print("\n=== Токены с низкой уверенностью разбора pymorphy (score < {:.2f}), топ {} ===".format(
        _SCORE_LOW,
        top_unknown,
    ))
    for tok, ww in low_score.most_common(top_unknown):
        nf, sc, _ = _parse_primary(morph, tok)
        sc_s = f"{sc:.3f}" if sc is not None else "-"
        print(f"  {tok}\t(freq_sum={ww})\tlemma~{nf}\tscore={sc_s}")

    def print_clusters(title: str, lemma_map: defaultdict[str, set[str]]) -> None:
        multi = [(lem, vals) for lem, vals in lemma_map.items() if len(vals) >= 3]
        multi.sort(key=lambda x: (-len(x[1]), -sum(union_freq.get(t, 0) for t in x[1]), x[0]))
        print(f"\n=== {title} (леммы с не меньше 3 разными формами в тексте), топ {top_clusters} ===")
        for lem, vals in multi[:top_clusters]:
            vv = sorted(vals, key=lambda t: (-union_freq.get(t, 0), t))
            fr = ",".join(f"{v}({union_freq[v]})" for v in vv[:12])
            more = len(vv) - 12
            if more > 0:
                fr += f" ...+{more}"
            print(f"  {lem}\t<- {fr}")

    print_clusters("Рецепты", lemma_r)
    print_clusters("Ингредиенты", lemma_i)

    print(f"\n=== Уменьшительные формы (Dimin в разборе), до {top_clusters} по частоте ===")
    dimin_tokens.sort(key=lambda x: -x[2])
    for surf, nf, ww in dimin_tokens[:top_clusters]:
        print(f"  {surf}\t<- {nf}\t(freq_sum={ww})")

    print(
        "\nПодсказка: для поиска блюд в боте используются lemma/pymorphy + "
        "_RECIPE_SCORING_ALIASES (см. bot.py); для ингредиентов - data/ingredient_aliases.json "
        "(ingredient_merge_key). Дополняйте алиасы по кластерам выше или по списку low-score."
    )
    return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    ap = argparse.ArgumentParser(description="Аудит токенов recipes/ingredients в SQLite")
    ap.add_argument(
        "--db",
        type=Path,
        default=_ROOT / "recipes.db",
        help="Путь к recipes.db",
    )
    ap.add_argument("--top", type=int, default=60, metavar="N", help="Сколько строк в топах")
    args = ap.parse_args()
    return run_audit(args.db.expanduser().resolve(), top_unknown=args.top, top_clusters=args.top)


if __name__ == "__main__":
    sys.exit(main())
