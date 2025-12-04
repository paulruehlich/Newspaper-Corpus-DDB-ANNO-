#!/usr/bin/env python
# coding: utf-8



"""
Generate list of ANNO newspaper issues by date.

This script:
1. Fetches the alphabetical newspaper list from ANNO.
2. Filters newspapers by TARGET_CITIES (Erscheinungsort).
3. For each selected newspaper, collects available years within TARGET_YEAR_RANGE.
4. For each year, extracts all issue dates (YYYYMMDD).
5. Writes a CSV with columns: aid, title, date.

Intended as step 1 of the ANNO data pipeline for the Masterarbeit.
"""

import requests
from bs4 import BeautifulSoup
import re
import csv
import time
from pathlib import Path

# === Config ===
BASE_URL = "https://anno.onb.ac.at"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Cities to include based on "Erscheinungsort" (lowercase)
TARGET_CITIES = {"wien"}

# Inclusive year range
TARGET_YEAR_RANGE = (1871, 1954)

# Output path (adjust to your repo structure)
OUTPUT_PATH = Path("data/anno/raw/anno_issues_all_filtered.csv")


# === Utilities ===
def get_soup(url: str, retries: int = 3) -> BeautifulSoup:
    """
    Fetch URL and return a BeautifulSoup object.

    Uses a polite delay and simple retry logic.
    """
    last_exc = None
    for attempt in range(retries):
        try:
            time.sleep(0.5)  # polite delay to avoid blocking
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            last_exc = e
            print(f" Fehler bei {url} (Versuch {attempt+1}/{retries}): {e}")
            time.sleep(2)
    raise last_exc


def extract_newspapers():
    """
    Extract all newspapers from the alphabetical list and
    filter by TARGET_CITIES based on the 'Erscheinungsort' field.
    """
    newspapers = []
    soup = get_soup(f"{BASE_URL}/alph_list.htm")

    for div in soup.find_all("div", class_="list-item"):
        try:
            aid_link = div.find("a", href=True)["href"]
            aid_match = re.search(r"aid=([a-zA-Z0-9]+)", aid_link)
            if not aid_match:
                continue
            aid = aid_match.group(1)

            title_tag = div.find("h4")
            title = title_tag.get_text(strip=True) if title_tag else "Unknown Title"

            z_table = div.find("table", class_="zusatz")
            if not z_table:
                continue

            rows = z_table.find_all("tr")
            place = None
            for row in rows:
                cells = row.find_all("td")
                if len(cells) == 2:
                    key = cells[0].get_text(strip=True).lower()
                    val = cells[1].get_text(strip=True)
                    if "erscheinungsort" in key:
                        place = val.lower()
                        break

            if place in TARGET_CITIES:
                newspapers.append({"aid": aid, "title": title})
        except Exception as e:
            print(f" Fehler beim Parsen eines Eintrags: {e}")
    return newspapers


def extract_available_years(aid: str):
    """
    For a given newspaper (aid), extract all available years and
    filter them to TARGET_YEAR_RANGE.
    """
    url = f"{BASE_URL}/cgi-content/anno?aid={aid}"
    soup = get_soup(url)
    year_links = soup.select("#content.view-year a[href*='datum=']")

    years = []
    for link in year_links:
        href = link.get("href", "")
        m = re.search(r"datum=(\d{4})", href)
        if m:
            years.append(int(m.group(1)))

    return [y for y in years if TARGET_YEAR_RANGE[0] <= y <= TARGET_YEAR_RANGE[1]]


def extract_issue_dates(aid: str, year: int):
    """
    For a given newspaper (aid) and year, extract all issue dates (YYYYMMDD).
    """
    url = f"{BASE_URL}/cgi-content/anno?aid={aid}&datum={year}"
    soup = get_soup(url)
    active_links = soup.select("td.active a[href*='datum=']")

    dates = []
    for a in active_links:
        href = a.get("href", "")
        m = re.search(r"datum=(\d{8})", href)
        if m:
            dates.append(m.group(1))

    return dates


# === Main Execution ===
def main():
    rows = []

    print(" Hole Zeitungen aus der alphabetischen Liste …")
    newspapers = extract_newspapers()
    print(f" Gefundene Zeitungen (gefiltert nach Städten {TARGET_CITIES}): {len(newspapers)}\n")

    for paper in newspapers:
        aid = paper["aid"]
        title = paper["title"]
        print(f"🔍 Verarbeite Zeitung: {aid} – {title}")
        try:
            years = extract_available_years(aid)
            print(f"   Gefundene Jahre im Zielbereich {TARGET_YEAR_RANGE}: {years}")
            for year in years:
                dates = extract_issue_dates(aid, year)
                print(f"      {year}: {len(dates)} Ausgaben")
                for date in dates:
                    rows.append(
                        {
                            "aid": aid,
                            "title": title,
                            "date": date,
                        }
                    )
        except Exception as e:
            print(f" Fehler bei Zeitung {aid}: {e}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["aid", "title", "date"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✅ Fertig. {len(rows)} Ausgaben gespeichert unter {OUTPUT_PATH}")


if __name__ == "__main__":
    main()







