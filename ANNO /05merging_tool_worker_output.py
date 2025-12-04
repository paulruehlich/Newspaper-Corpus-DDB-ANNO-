#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
Merge all worker output files from the ANNO scraper into one unified dataset.

Pipeline step:
    - input:  data/anno/processed/output_worker_*.csv
    - output: data/anno/processed/anno_pages.csv

This script is step 4 of the ANNO pipeline and combines all page-level
scraper output files into a single dataset suitable for analysis or publication.
"""

import pandas as pd
from pathlib import Path

# === Config ===
INPUT_DIR = Path("data/anno/processed")
OUTPUT_FILE = INPUT_DIR / "anno_pages.csv"


def main():
    print(f"📥 Suche Worker-Output in: {INPUT_DIR}")

    worker_files = sorted(INPUT_DIR.glob("output_worker_*.csv"))
    if not worker_files:
        raise FileNotFoundError(
            f"Keine Worker-Dateien gefunden in {INPUT_DIR}. "
            "Bitte Scraper zunächst ausführen."
        )

    print(f"🔍 Gefundene Dateien: {len(worker_files)}")

    dfs = []
    for file in worker_files:
        print(f"   ➕ Lade {file.name}")
        df = pd.read_csv(file)
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)

    # Sortiere optional für Stabilität (nice for publication)
    merged = merged.sort_values(["aid", "year", "month", "day", "page"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    print(f"\n✅ Fertig! Zusammengeführt: {len(merged)} Seiten")
    print(f"📄 Gespeichert unter: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

