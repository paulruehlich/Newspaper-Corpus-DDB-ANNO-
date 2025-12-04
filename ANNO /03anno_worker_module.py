#!/usr/bin/env python
# coding: utf-8

# In[9]:


import requests
import pandas as pd
import time
from random import uniform
from pathlib import Path
from datetime import datetime
import re
import json

BASE_URL = "https://anno.onb.ac.at"
HEADERS = {"User-Agent": "Mozilla/5.0"}
CSV_ISSUES = "metadata_selected_np.csv"  
BACKUP_DIR = Path("backups")
PROXY = {
    "http": "http://XXX@gate.decodo.com:7000",
    "https": "http://XXX@gate.decodo.com:7000",  # add Decodo details
}

session = requests.Session()
session.headers.update(HEADERS)
BACKUP_DIR.mkdir(exist_ok=True)


def get_issue_progress_file(worker_id):
    return BACKUP_DIR / f"progress_worker_{worker_id}.json"


def load_progress(worker_id):
    progress_file = get_issue_progress_file(worker_id)
    if progress_file.exists():
        with open(progress_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_progress(worker_id, progress):
    progress_file = get_issue_progress_file(worker_id)
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump(progress, f)


def scrape_page(aid, date_str, page_index):
    url = f"{BASE_URL}/cgi-content/annoshow?text={aid}|{date_str}|{page_index}"
    try:
        r = session.get(url, proxies=PROXY, timeout=30)
        time.sleep(uniform(0.0, 0.01))
        if r.status_code == 403:
            raise PermissionError("403 Forbidden")
        if r.status_code == 500:
            return None, "server_error_500"
        r.raise_for_status()
        if re.fullmatch(
            r"\[\s*\d{4}-\d{2}-\d{2}\s*-\s*\d{8}\s*-\s*Seite\s+\d+\s*\]", r.text.strip()
        ):
            return None, "invalid_index"
        content = re.sub(r"^.*?\]\s*", "", r.text, flags=re.DOTALL).strip()
        return (content, None) if content else (None, "no_content")
    except Exception as e:
        print(f"Fehler bei Seite {page_index}: {e}")
        return None, "error"


def scrape_issue(aid, title, date_str, worker_id, progress, max_pages=100):
    results = []
    already_done_pages = progress.get(aid + "_" + date_str, [])

    for page_index in range(1, max_pages + 1):
        if str(page_index) in already_done_pages:
            continue  # Seite bereits gesichert

        text, err = scrape_page(aid, date_str, page_index)
        if err in ["server_error_500", "no_content", "invalid_index"]:
            break
        if text:
            d = datetime.strptime(str(date_str), "%Y%m%d")
            record = {
                "title": title,
                "aid": aid,
                "year": d.year,
                "month": d.month,
                "day": d.day,
                "page": page_index,
                "text": text,
            }

            output_file = BACKUP_DIR / f"output_worker_{worker_id}.csv"
            pd.DataFrame([record]).to_csv(
                output_file,
                mode="a",
                header=not output_file.exists(),
                index=False,
            )

            # Fortschritt aktualisieren
            progress.setdefault(aid + "_" + date_str, []).append(str(page_index))
            save_progress(worker_id, progress)

            results.append(record)
        else:
            break
    return results


def run_worker(worker_id, total_workers):
    df = pd.read_csv(CSV_ISSUES)
    df = df.reset_index(drop=True)
    df = df[df.index % total_workers == (worker_id - 1)]
    progress = load_progress(worker_id)

    completed = 0
    for idx, row in df.iterrows():
        aid, title, date = row["aid"], row["title"], str(row["date"])
        key = aid + "_" + date
        if key in progress and len(progress[key]) >= 1:
            print(
                f"[Worker {worker_id}] ⏭️ Bereits begonnen: {aid} {date} "
                f"(Seiten: {len(progress[key])})"
            )
        else:
            print(f"[Worker {worker_id}] ➡️ Beginne: {aid} {date}")

        try:
            result = scrape_issue(aid, title, date, worker_id, progress)
            if result:
                completed += 1
                print(
                    f"[Worker {worker_id}] ✅ Fertig: {aid} {date} mit {len(result)} Seiten. "
                    f"Gesamt: {completed}/{len(df)}"
                )
        except PermissionError:
            print(f"[Worker {worker_id}] ⛔ Zugriff blockiert. Stoppe...")
            break

    print(
        f"[Worker {worker_id}] 🏁 Alle verarbeitet oder abgebrochen. "
        f"Gesamt bearbeitet: {completed}"
    )

