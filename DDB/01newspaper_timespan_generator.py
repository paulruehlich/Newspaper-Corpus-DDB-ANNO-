#!/usr/bin/env python
# coding: utf-8

# In[9]:


from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import requests


API_KEY: str = #add API Key


BASE_URL: str = (
    "https://api.deutsche-digitale-bibliothek.de/search/index/newspaper/select"
)

ROWS_PER_PAGE: int = 500


@dataclass
class NewspaperPeriod:

    id: str
    title: str
    start_year: Optional[int]
    end_year: Optional[int]

    @property
    def years_covered(self) -> Optional[int]:
        if self.start_year is not None and self.end_year is not None:
            return self.end_year - self.start_year + 1
        return None


def get_api_key() -> str:
   

    key = API_KEY or os.environ.get("DDB_API_KEY")
    if not key or key == "YOUR_API_KEY":
        raise RuntimeError(
            "No API Key added"
        )
    return key


def fetch_newspapers(api_key: str) -> List[dict]:
   
    all_docs: List[dict] = []
    start = 0
    while True:
        params = {
            "q": "hasLoadedIssues:true",
            "fl": "id,title,progress",
            "wt": "json",
            "start": start,
            "rows": ROWS_PER_PAGE,
            "oauth_consumer_key": api_key,
        }
        response = requests.get(BASE_URL, params=params)
        try:
            response.raise_for_status()
        except Exception as exc:
            raise RuntimeError(
                f"Fehler beim Abruf der Newspaper‑Dokumente: {exc}\n"
                f"URL: {response.url}\n"
                f"Antwortcode: {response.status_code}\n"
                f"Antworttext: {response.text[:200]}..."
            ) from exc

        data = response.json()
        docs = data.get("response", {}).get("docs", [])
        num_found = data.get("response", {}).get("numFound", 0)

        all_docs.extend(docs)
        start += ROWS_PER_PAGE

        
        if start >= num_found:
            break

    return all_docs


def parse_progress(progress_list: List[str]) -> Tuple[Optional[int], Optional[int]]:
    
    years: List[int] = []
    if not progress_list:
        return None, None

    
    year_pattern = re.compile(r"\b(1[5-9]\d{2}|20\d{2})\b")

    for entry in progress_list:
        
        for match in year_pattern.findall(entry):
            try:
                years.append(int(match))
            except ValueError:
                continue

    if not years:
        return None, None
    return min(years), max(years)


def compute_periods(docs: List[dict]) -> List[NewspaperPeriod]:
   
    periods: List[NewspaperPeriod] = []
    for doc in docs:
        raw_title = doc.get("title")
        
        if isinstance(raw_title, list):
            title = "; ".join([t for t in raw_title if isinstance(t, str)])
        else:
            title = str(raw_title) if raw_title is not None else ""

        progress = doc.get("progress") or []
        start_year, end_year = parse_progress(progress)
        periods.append(
            NewspaperPeriod(
                id=doc.get("id", ""),
                title=title,
                start_year=start_year,
                end_year=end_year,
            )
        )
    return periods


def main() -> None:
    api_key = get_api_key()
    print("Lade Zeitungstitel …", flush=True)
    docs = fetch_newspapers(api_key)
    print(f"Abgerufen: {len(docs)} Titel")
    periods = compute_periods(docs)
    df = pd.DataFrame(
        [
            {
                "id": p.id,
                "title": p.title,
                "start_year": p.start_year,
                "end_year": p.end_year,
                "years_covered": p.years_covered,
            }
            for p in periods
        ]
    )
  
    df_sorted = df.sort_values(
        by=["years_covered", "start_year"], ascending=[False, True]
    )
    
    print(df_sorted.to_string(index=False))
   
    out_file = "zeitungszeiträume_alle.csv"
    df_sorted.to_csv(out_file, index=False)
    print(f"Die komplette Tabelle wurde als '{out_file}' gespeichert.")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        sys.stderr.write(f"Fehler: {err}\n")
        sys.exit(1)

