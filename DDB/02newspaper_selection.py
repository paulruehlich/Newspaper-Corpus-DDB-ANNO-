#!/usr/bin/env python
# coding: utf-8

# In[3]:


"""
Select DDB newspapers for the German corpus.

Logic (as used in the thesis project):

1. Start from all DDB newspapers with loaded issues and computed time spans
   (zeitungszeiträume_alle.csv from newspaper_timespans.py).

2. Keep only titles with a minimum temporal coverage (years_covered >= MIN_YEARS).

3. Inspect two groups of candidates:
   - titles that start close to 1871 and run into the 20th century
   - titles that end around 1954 and start before 1900

4. From these candidates, select:
   - three titles that start close to 1871 with long spans ("early" group)
   - three titles that end in 1954 with long spans ("late" group)

5. Titles with very poor digitization coverage in DDB were excluded manually
   (despite long theoretical spans in the metadata).

The final selection is written to config/ddb_newspapers_selection.csv.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

# --- Pfade ---
TIMESPANS_CSV = Path("zeitungszeiträume_alle.csv")
OUTPUT_SELECTION = Path("config/ddb_newspapers_selection.csv")

# --- Parameter für die Kandidatensuche ---
MIN_YEARS = 20         # Mindestlaufzeit
EARLY_START_REF = 1871 # Ziel: frühe Titel um 1871
LATE_END_REF = 1954    # Ziel: späte Titel um 1954

# --- Deine final ausgewählten Zeitungen (aus Notebook + manueller Prüfung) ---
# Bitte genaue Schreibweise wie in zeitungszeiträume_alle.csv verwenden!
SELECTED_TITLES: List[str] = [
    "Honnefer Volkszeitung",
    "Jeversches Wochenblatt",
    "Kölnische Zeitung",
    "Neckar-Bote",
    "Oberkasseler Zeitung",
    "Schwäbischer Merkur",
]


def main() -> None:
    if not TIMESPANS_CSV.exists():
        raise FileNotFoundError(
            f"Eingabedatei nicht gefunden: {TIMESPANS_CSV}. "
            "Bitte zuerst newspaper_timespans.py ausführen."
        )

    df = pd.read_csv(TIMESPANS_CSV)

    # 1. Mindestlaufzeit filtern
    df_long = df[df["years_covered"] >= MIN_YEARS].copy()
    print(f"Gesamtzahl Titel: {len(df)}")
    print(f"➡ Titel mit mindestens {MIN_YEARS} Jahren Coverage: {len(df_long)}\n")

    # 2. Kandidaten rund um 1954 (späte Titel) – rein informativ, wie in deinem Notebook
    print("Kandidaten: end_year > 1945 und start_year < 1900\n")
    late_candidates = df_long[(df_long["end_year"] > 1945) & (df_long["start_year"] < 1900)]
    print(
        late_candidates.sort_values("years_covered", ascending=False).to_string(
            index=False
        )
    )
    print("\n" + "=" * 80 + "\n")

    # 3. Kandidaten mit sehr langer Spanne (früh startend, spät endend)
    print("Kandidaten: end_year > 1946 und start_year < 1871\n")
    span_candidates = df_long[
        (df_long["end_year"] > 1946) & (df_long["start_year"] < EARLY_START_REF)
    ]
    print(
        span_candidates.sort_values("years_covered", ascending=False).to_string(
            index=False
        )
    )
    print("\n" + "=" * 80 + "\n")

    # 4. Finale Auswahl per Titelliste (entspricht deiner manuellen Auswahl)
    df_sel = df_long[df_long["title"].isin(SELECTED_TITLES)].copy()

    if df_sel.empty:
        raise RuntimeError(
            "Die finale Auswahl ist leer. "
            "Stimmen die Titel in SELECTED_TITLES exakt mit der CSV überein?"
        )

    # Rolle kennzeichnen: 'early' vs 'late' (für Dokumentation)
    df_sel["role"] = "unspecified"
    # frühe Titel: Start vor oder um 1871
    df_sel.loc[df_sel["start_year"] <= EARLY_START_REF, "role"] = "early"
    # späte Titel: Ende 1954
    df_sel.loc[df_sel["end_year"] >= LATE_END_REF, "role"] = "late"

    # Platz für Anmerkungen (z.B. Ausschlusskriterien / DDB-Coverage)
    df_sel["notes"] = ""

    # schön sortieren: erst „early“, dann „late“, dann nach years_covered
    role_order = {"early": 0, "late": 1, "unspecified": 2}
    df_sel["role_order"] = df_sel["role"].map(role_order)
    df_sel = df_sel.sort_values(
        by=["role_order", "years_covered", "start_year"], ascending=[True, False, True]
    ).drop(columns=["role_order"])

    OUTPUT_SELECTION.parent.mkdir(parents=True, exist_ok=True)
    df_sel.to_csv(OUTPUT_SELECTION, index=False)

    print("✅ Finale Auswahl gespeichert unter:", OUTPUT_SELECTION)
    print("\n📊 Ausgewählte DDB-Zeitungen:\n")
    print(df_sel.to_string(index=False))


if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:




