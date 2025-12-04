#!/usr/bin/env python
# coding: utf-8

# In[11]:


import pandas as pd
import numpy as np
from pathlib import Path

import matplotlib.pyplot as plt

pd.set_option("display.max_rows", 50)
pd.set_option("display.max_colwidth", 80)


# In[12]:


DATA_PATH = Path("metadata.csv")

df = pd.read_csv(DATA_PATH)

df["date"] = df["date"].astype(str)
df["year"] = df["date"].str[:4].astype(int)

df.head()


# In[13]:


coverage_per_title = (
    df.groupby("title")["year"]
      .agg(start_jahr="min", end_jahr="max", anzahl_jahre=lambda x: x.nunique())
      .reset_index()
)

coverage_per_title.sort_values("anzahl_jahre", ascending=False).head(50)


# In[14]:


selected_titles = [
    "Wiener Zeitung",
    "Wienerisches Diarium",
    "Neues Wiener Tagblatt (Tagesausgabe)",
    "Neues Wiener Tagblatt (Wochenausgabe)",
    "Arbeiter-Zeitung",
    "Christlich-sociale Arbeiter-Zeitung",
    "Oesterreichische Arbeiter-Zeitung",
    "Deutsche Arbeiter-Zeitung",
    "Neue Freie Presse",
    "Das Vaterland",
    "Neues Österreich",
]

coverage_sel = coverage_per_title[
    coverage_per_title["title"].isin(selected_titles)
].copy()

coverage_sel.sort_values("start_jahr")


# In[15]:


# Mapping: Zeitungsfamilie -> Einzeltitel
families = {
    "Wiener Zeitung": [
        "Wiener Zeitung",
        "Wienerisches Diarium",
    ],
    "Neues Wiener Tagblatt": [
        "Neues Wiener Tagblatt (Tagesausgabe)",
        "Neues Wiener Tagblatt (Wochenausgabe)",
    ],
    "Arbeiter-Zeitung": [
        "Arbeiter-Zeitung",
        "Christlich-sociale Arbeiter-Zeitung",
        "Oesterreichische Arbeiter-Zeitung",
        "Deutsche Arbeiter-Zeitung",
    ],
    "Neue Freie Presse": [
        "Neue Freie Presse",
    ],
    "Das Vaterland": [
        "Das Vaterland",
    ],
    "Neues Österreich": [
        "Neues Österreich",
    ],
}


# In[16]:


family_rows = []

for fam_name, titles in families.items():
    sub = df[df["title"].isin(titles)]
    years = sorted(sub["year"].unique())
    if not years:
        continue

    family_rows.append({
        "familie": fam_name,
        "titel_anzahl": len(titles),
        "start_jahr": years[0],
        "end_jahr": years[-1],
        "anzahl_jahre": len(years),
    })

coverage_families = pd.DataFrame(family_rows)
coverage_families.sort_values("start_jahr")


# In[17]:


coverage_families.sort_values("anzahl_jahre", ascending=False)


# In[18]:


df["dekade"] = (df["year"] // 10) * 10

rows = []
for fam_name, titles in families.items():
    sub = df[df["title"].isin(titles)]
    dekaden = sub.groupby("dekade")["year"].nunique().reset_index(name="anzahl_jahre")
    dekaden["familie"] = fam_name
    rows.append(dekaden)

dekaden_fam = pd.concat(rows, ignore_index=True)

pivot_dekaden = dekaden_fam.pivot_table(
    index="dekade",
    columns="familie",
    values="anzahl_jahre",
    fill_value=0
).sort_index()

pivot_dekaden


# In[19]:


# Einfacher Linienplot pro Familie über die Zeit (Dekaden)

plt.figure(figsize=(10, 6))

for fam in pivot_dekaden.columns:
    plt.plot(pivot_dekaden.index, pivot_dekaden[fam], marker="o", label=fam)

plt.xlabel("Dekade")
plt.ylabel("Anzahl Jahre mit Ausgaben")
plt.title("Dekadenweise Abdeckung pro Zeitungsfamilie (ANNO, 1870–1950er)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()


# In[20]:


def summarize_family(row):
    return (
        f"{row['familie']}: {int(row['start_jahr'])}–{int(row['end_jahr'])} "
        f"({int(row['anzahl_jahre'])} Jahre)"
    )

for _, r in coverage_families.sort_values("start_jahr").iterrows():
    print("-", summarize_family(r))

