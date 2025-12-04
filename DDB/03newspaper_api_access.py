#!/usr/bin/env python
# coding: utf-8

# In[2]:


from __future__ import annotations

"""
Fetch page-level fulltext for selected DDB newspapers.

For each newspaper in PAPERS, this script:

1. Uses `zp_issues` to retrieve issues in a given year range and obtain the ZDB-ID(s).
2. Uses `zp_pages(zdb_id=...)` to fetch all pages for that newspaper.
3. Keeps only pages within the given year range (START_YEAR–END_YEAR).
4. Parses `publication_date` into year, month, day.
5. Outputs:
    - one CSV per newspaper in data/ddb/processed/by_newspaper/
    - one master CSV with all pages in data/ddb/processed/ddb_pages_all.csv

Final columns in all outputs:
    title, year, month, day, pagenumber, text
"""

import os
import re
from pathlib import Path
from typing import List

import pandas as pd
from ddbapi import zp_issues, zp_pages  # ddbapi wrapper


# ========================
# Konfiguration
# ========================

# Zeitungen (Titel wie im Zeitungsportal / deinem Issues-DF)
PAPERS: List[str] = [
    "Honnefer Volkszeitung",
    "Jeversches Wochenblatt",
    "Kölnische Zeitung",
    "Neckar-Bote",
    "Oberkasseler Zeitung",
    "Schwäbischer Merkur",
]

# Inklusiver Jahresbereich
START_YEAR = 1871
END_YEAR = 1954

# Sprache im Zeitungsportal (deutsch)
LANGUAGE = "ger"

# Optional: auf bestimmte Seiten begrenzen (z.B. 1–5 wie im alten Skript)
# Wenn du ALLE Seiten willst, setze PAGE_MIN/PAGE_MAX = None
PAGE_MIN = 1  # z.B. 1
PAGE_MAX = 5  # z.B. 5

# Output-Pfade
OUT_MASTER = Path("data/ddb/processed/ddb_pages_all.csv")
OUT_PER_PAPER_DIR = Path("data/ddb/processed/by_newspaper")
OUT_PER_PAPER_DIR.mkdir(parents=True, exist_ok=True)


# ========================
# Hilfsfunktionen
# ========================

def slugify(title: str) -> str:
    s = title.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "paper"


def build_publication_date_range(start_year: int, end_year: int) -> str:
    """
    DDB-konformer Datumsbereichs-String für `publication_date`,
    verwendet bei `zp_issues`.
    """
    date_from = f"{start_year}-01-01T12:00:00Z"
    date_to = f"{end_year}-12-31T12:00:00Z"
    return f"[{date_from} TO {date_to}]"


def fetch_zdb_ids_for_paper(paper_title: str, start_year: int, end_year: int, language: str) -> List[str]:
    """
    Holt Issues für eine Zeitung im gewünschten Zeitraum und liefert
    die einzigartigen ZDB-IDs zurück.
    """
    pub_range = build_publication_date_range(start_year, end_year)

    issues_df = zp_issues(
        paper_title=paper_title,
        publication_date=pub_range,
        language=language,
    )

    if issues_df.empty:
        print(f"⚠️ Keine Issues für '{paper_title}' im Zeitraum {start_year}-{end_year}.")
        return []

    if "zdb_id" not in issues_df.columns:
        raise RuntimeError("Erwartete Spalte 'zdb_id' nicht in Issues-DataFrame enthalten.")

    zdb_ids = sorted(issues_df["zdb_id"].dropna().unique().tolist())
    print(f"   → Gefundene ZDB-ID(s) für '{paper_title}': {', '.join(zdb_ids)}")
    return zdb_ids


def fetch_pages_for_zdb(zdb_id: str) -> pd.DataFrame:
    """
    Holt alle Seiten für eine gegebene ZDB-ID über `zp_pages`.
    """
    pages_df = zp_pages(zdb_id=zdb_id)
    if pages_df.empty:
        print(f"   ⚠️ Keine Seiten für ZDB-ID {zdb_id}.")
    return pages_df


def preprocess_pages_df(df: pd.DataFrame, paper_title_fallback: str) -> pd.DataFrame:
    """
    - relevante Spalten auswählen/umbenennen
    - Datum parsen
    - year/month/day hinzufügen
    - optional page-range filtern
    - auf finale Spalten reduzieren
    """
    # Sicherstellen, dass die erwarteten Spalten existieren
    needed_cols = {"pagenumber", "publication_date", "plainpagefulltext"}
    missing = needed_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"Erwartete Spalten fehlen in pages_df: {missing}")

    # Paper-Titel-Spalte
    if "paper_title" in df.columns:
        title_series = df["paper_title"].astype(str)
    else:
        title_series = pd.Series([paper_title_fallback] * len(df))

    out = pd.DataFrame(
        {
            "title": title_series,
            "pagenumber": df["pagenumber"],
            "publication_date": df["publication_date"],
            "text": df["plainpagefulltext"],
        }
    )

    # Datumsfeld parsen
    out["publication_date"] = pd.to_datetime(out["publication_date"], errors="coerce")
    out = out.dropna(subset=["publication_date"])

    out["year"] = out["publication_date"].dt.year
    out["month"] = out["publication_date"].dt.month
    out["day"] = out["publication_date"].dt.day

    # Seitenbereich filtern (optional)
    out["pagenumber"] = pd.to_numeric(out["pagenumber"], errors="coerce")
    out = out.dropna(subset=["pagenumber"])
    out["pagenumber"] = out["pagenumber"].astype(int)

    if PAGE_MIN is not None:
        out = out[out["pagenumber"] >= PAGE_MIN]
    if PAGE_MAX is not None:
        out = out[out["pagenumber"] <= PAGE_MAX]

    # Nach Jahrbereich filtern
    out = out[(out["year"] >= START_YEAR) & (out["year"] <= END_YEAR)]

    # Sortierung
    out = out.sort_values(["year", "month", "day", "pagenumber"])

    # Finale Spaltenreihenfolge
    out = out[["title", "year", "month", "day", "pagenumber", "text"]]

    return out


# ========================
# Hauptlogik
# ========================

def main() -> None:
    # Hinweis: ddbapi nutzt intern den API-Key; hier nur Info, falls keiner gesetzt ist.
    if not os.environ.get("DDB_API_KEY"):
        print("⚠️ Hinweis: DDB_API_KEY ist nicht gesetzt. "
              "Stelle sicher, dass ddbapi deinen API-Key anderweitig findet.\n")

    all_pages: List[pd.DataFrame] = []

    print(f"📅 Zeitraum: {START_YEAR}–{END_YEAR}")
    print(f"🗞️ Zeitungen: {', '.join(PAPERS)}\n")

    for paper in PAPERS:
        print(f"➡️ Verarbeite Zeitung: {paper}")
        zdb_ids = fetch_zdb_ids_for_paper(paper, START_YEAR, END_YEAR, LANGUAGE)
        if not zdb_ids:
            continue

        paper_frames: List[pd.DataFrame] = []

        for zdb_id in zdb_ids:
            print(f"   → Hole Seiten für ZDB-ID {zdb_id} …")
            pages_df = fetch_pages_for_zdb(zdb_id)
            if pages_df.empty:
                continue

            processed = preprocess_pages_df(pages_df, paper_title_fallback=paper)
            if processed.empty:
                print("   ⚠️ Keine Seiten im gewünschten Jahrbereich nach Preprocessing.")
                continue

            paper_frames.append(processed)

        if not paper_frames:
            print(f"   ❌ Keine Seiten für '{paper}' im Zeitraum {START_YEAR}–{END_YEAR}.")
            continue

        paper_all = pd.concat(paper_frames, ignore_index=True)

        # Pro Zeitung speichern
        slug = slugify(paper)
        filename = f"{slug}_{START_YEAR}_to_{END_YEAR}.csv"
        out_paper_path = OUT_PER_PAPER_DIR / filename
        paper_all.to_csv(out_paper_path, index=False, encoding="utf-8")
        print(f"   💾 {len(paper_all)} Seiten gespeichert unter: {out_paper_path}\n")

        all_pages.append(paper_all)

    if not all_pages:
        print("❌ Keine Seiten für die angegebenen Zeitungen/Zeiträume gefunden.")
        return

    master = pd.concat(all_pages, ignore_index=True)
    OUT_MASTER.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(OUT_MASTER, index=False, encoding="utf-8")

    print(f"✅ Master-Datensatz mit {len(master)} Seiten gespeichert unter: {OUT_MASTER}")


if __name__ == "__main__":
    main()


# In[ ]:




