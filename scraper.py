import argparse
import itertools
import math
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse

import pandas as pd
from google_play_scraper import app as fetch_app
from google_play_scraper import search as play_search
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm

OUTPUT_FILE = "puzzle.xlsx"

TARGET_DEVELOPERS = 10_000   # stop once this many qualifying developers are found
RESULTS_PER_QUERY = 250      # Play search caps out around here per query
MAX_WORKERS = 6
MAX_RETRIES = 3
BASE_BACKOFF = 1.5
MONGO_BATCH_SIZE = 100       # buffered writes per bulk_write call
DB_NAME = os.environ.get("MONGODB_DB", "db2")  # MongoDB database to store data in

LANG = "en"
COUNTRY = "in"

STALE_AFTER_DAYS = 1460
MIN_INSTALLS = 10_000

BASE_TERMS = [
    "puzzle", "puzzle game", "brain teaser", "brain training", "brain game",
    "logic puzzle", "logic game", "match 3", "match puzzle", "block puzzle",
    "block game", "tile puzzle", "tile match", "merge puzzle", "color puzzle",
    "color match", "sorting puzzle", "ball sort", "water sort", "jigsaw",
    "jigsaw puzzle", "sudoku", "crossword", "word puzzle", "word game",
    "word search", "word connect", "anagram", "number puzzle", "number game",
    "2048", "math puzzle", "nonogram", "hidden object", "spot the difference",
    "connect dots", "maze", "sliding puzzle", "slide puzzle", "pipe puzzle",
    "flow puzzle", "physics puzzle", "rope puzzle", "bubble shooter",
    "bubble puzzle", "marble shooter", "riddle", "riddle game", "escape room",
    "escape game", "mystery puzzle", "detective puzzle", "3d puzzle",
    "shape puzzle", "pattern puzzle", "kids puzzle", "educational puzzle",
    "sokoban", "mahjong", "solitaire", "chess puzzle",
]

MODIFIERS = [
    "game", "games", "app", "free", "offline", "online",
    "2 player", "multiplayer", "for kids", "no wifi",
]

SEARCH_QUERIES = sorted({
    f"{term} {modifier}".strip()
    for term, modifier in itertools.product(BASE_TERMS, MODIFIERS)
})


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def normalize_downloads(real_installs, installs_str=""):
    if real_installs:
        try:
            return int(real_installs)
        except (TypeError, ValueError):
            pass

    digits = re.sub(r"[^\d]", "", str(installs_str or ""))
    return int(digits) if digits else 0


def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def epoch_to_date(value):
    if not value:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).date()
    except (TypeError, ValueError, OSError):
        return None


def days_since(date_obj):
    if date_obj is None:
        return None
    return (datetime.now(timezone.utc).date() - date_obj).days


def qualifies(record):
    """Same filters as build_dataframe, so the developer count we stop on
    matches what ends up in the Excel file."""
    installs = normalize_downloads(record.get("realInstalls"), record.get("installs"))
    if installs < MIN_INSTALLS:
        return False
    if not (record.get("containsAds") or record.get("adSupported")):
        return False
    age = days_since(epoch_to_date(record.get("updated")))
    return age is None or age <= STALE_AFTER_DAYS


# --------------------------------------------------------------------------
# storage
# --------------------------------------------------------------------------

class Store:
    """MongoDB-backed cache.

    Everything we need for hot-path lookups (known package names, qualified
    developer IDs) is loaded ONCE at startup and kept in memory. Writes are
    buffered and sent with bulk_write, so we make a handful of round trips
    instead of one per app.
    """

    def __init__(self):
        uri = os.environ.get("MONGODB_URI")
        if not uri:
            raise RuntimeError("MONGODB_URI environment variable is not set")

        self.client = MongoClient(uri, serverSelectionTimeoutMS=30000)
        self.db = self.client[DB_NAME]
        self.apps = self.db["apps"]
        self.apps.create_index("package_name", unique=True)
        self.client.admin.command("ping")
        print("Connected to MongoDB Atlas")

        self.known = set()
        self.qualified_devs = set()
        self._buffer = []
        self._load_state()

    def _load_state(self):
        for r in self.apps.find({}, {"package_name": 1, "payload": 1, "_id": 0}):
            self.known.add(r["package_name"])
            payload = r.get("payload")
            if payload and payload.get("developerId") and qualifies(payload):
                self.qualified_devs.add(str(payload["developerId"]))

    def save(self, package_name, payload, ok=True):
        self.known.add(package_name)
        if ok and payload and payload.get("developerId") and qualifies(payload):
            self.qualified_devs.add(str(payload["developerId"]))

        self._buffer.append(UpdateOne(
            {"package_name": package_name},
            {"$set": {
                "package_name": package_name,
                "payload": payload,
                "ok": bool(ok),
                "fetched_at": time.time(),
            }},
            upsert=True,
        ))
        if len(self._buffer) >= MONGO_BATCH_SIZE:
            self.flush()

    def flush(self):
        if self._buffer:
            self.apps.bulk_write(self._buffer, ordered=False)
            self._buffer = []

    def all_records(self):
        return [
            r["payload"]
            for r in self.apps.find(
                {"ok": True, "payload": {"$ne": None}},
                {"payload": 1, "_id": 0},
            )
            if r.get("payload")
        ]

    def close(self):
        self.flush()
        self.client.close()


# --------------------------------------------------------------------------
# fetching / discovery
# --------------------------------------------------------------------------

def with_retry(fn, *args, **kwargs):
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            message = str(exc).lower()
            if "not found" in message or "404" in message:
                raise
            time.sleep(BASE_BACKOFF * (2 ** attempt))
    raise last_error


KEEP_FIELDS = (
    "title", "url", "appId", "realInstalls", "installs", "containsAds",
    "offersIAP", "genre", "genreId", "score", "ratings", "reviews",
    "developer", "developerId", "developerEmail", "developerWebsite",
    "developerAddress", "updated", "released", "free", "adSupported",
    "contentRating", "privacyPolicy",
)


def fetch_one(package_name):
    details = with_retry(fetch_app, package_name, lang=LANG, country=COUNTRY)
    return {k: details.get(k) for k in KEEP_FIELDS}


def fetch_many(package_names, store):
    if not package_names:
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_one, p): p for p in package_names}
        for future in as_completed(futures):
            package_name = futures[future]
            try:
                store.save(package_name, future.result(), ok=True)
            except Exception:
                store.save(package_name, None, ok=False)


def run_discovery(store, target_devs):
    """Run search queries one by one. For each query, fetch full details only
    for apps we haven't seen AND whose developer isn't already qualified.
    Stops as soon as `target_devs` qualifying developers are collected."""
    print(f"\nCache: {len(store.known)} apps, "
          f"{len(store.qualified_devs)} qualifying developers already.")
    print(f"Target: {target_devs} developers | {len(SEARCH_QUERIES)} queries available.\n")

    queries = SEARCH_QUERIES[:]
    random.shuffle(queries)  # spread across puzzle types instead of alphabetical order

    bar = tqdm(queries, desc="Searching")
    for query in bar:
        if len(store.qualified_devs) >= target_devs:
            break

        try:
            results = with_retry(
                play_search, query,
                lang=LANG, country=COUNTRY, n_hits=RESULTS_PER_QUERY,
            )
        except Exception as exc:
            tqdm.write(f"  search failed: {query!r} -> {exc}")
            continue

        candidates = []
        for result in results:
            package_name = result.get("appId")
            if not package_name or package_name in store.known:
                continue
            dev_id = str(result.get("developerId") or "")
            if dev_id and dev_id in store.qualified_devs:
                continue  # already have this developer, skip the extra fetch
            candidates.append(package_name)

        fetch_many(candidates, store)
        store.flush()

        bar.set_postfix(devs=len(store.qualified_devs), apps=len(store.known))
        time.sleep(0.3)

    bar.close()

    if len(store.qualified_devs) >= target_devs:
        print(f"\nReached target: {len(store.qualified_devs)} developers.")
    else:
        print(f"\nRan out of queries at {len(store.qualified_devs)} developers "
              f"(target {target_devs}). Add more BASE_TERMS / MODIFIERS to go further.")


# --------------------------------------------------------------------------
# scoring / export
# --------------------------------------------------------------------------

def lead_score(row):
    score = 0.0

    installs = row["Downloads"]
    if installs > 0:
        score += min(35.0, (math.log10(installs) - 3) * 9)

    if row["Developer Email"]:
        score += 16
    if row["Developer Website"]:
        score += 6
    if row["Developer Address"]:
        score += 3

    age = row["Days Since Update"]
    if age is not None:
        if age <= 90:
            score += 20
        elif age <= 365:
            score += 13
        elif age <= 730:
            score += 5

    if row["Ratings Count"] >= 500 and row["Rating"] >= 4.0:
        score += 10
    elif row["Ratings Count"] >= 100 and row["Rating"] >= 3.5:
        score += 5

    if row["Contains Ads"] == "Yes":
        score += 10
    elif row["Offers IAP"] == "Yes":
        score += 7

    return round(min(score, 100.0), 1)


def build_dataframe(store):
    rows = []

    for record in store.all_records():
        package_name = record.get("appId")
        updated = epoch_to_date(record.get("updated"))
        contains_ads = bool(record.get("containsAds") or record.get("adSupported"))

        rows.append({
            "Developer Name": record.get("developer") or "",
            "Developer Email": record.get("developerEmail") or "",
            "Developer Website": record.get("developerWebsite") or "",
            "Developer Address": record.get("developerAddress") or "",
            "Developer ID": str(record.get("developerId") or ""),
            "App Name": record.get("title") or "",
            "App Link": record.get("url")
                        or f"https://play.google.com/store/apps/details?id={package_name}",
            "Package Name": package_name,
            "Downloads": normalize_downloads(
                record.get("realInstalls"), record.get("installs")
            ),
            "Contains Ads": "Yes" if contains_ads else "No",
            "Offers IAP": "Yes" if record.get("offersIAP") else "No",
            "Category": record.get("genre") or "",
            "Rating": round(to_float(record.get("score")), 2),
            "Ratings Count": to_int(record.get("ratings")),
            "Last Updated": updated,
            "Days Since Update": days_since(updated),
            "Privacy Policy": record.get("privacyPolicy") or "",
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["Package Name"])
    df = df[df["Downloads"] >= MIN_INSTALLS]
    df = df[df["Contains Ads"] == "Yes"]
    df = df[
        df["Days Since Update"].isna()
        | (df["Days Since Update"] <= STALE_AFTER_DAYS)
    ]

    if df.empty:
        return df

    df["Lead Score"] = df.apply(lead_score, axis=1)
    df = df.sort_values(
        by=["Lead Score", "Downloads"], ascending=[False, False]
    ).reset_index(drop=True)

    return df


def build_developer_sheet(df):
    grouped = df.groupby("Developer ID", dropna=False).agg(
        **{
            "Developer Name": ("Developer Name", "first"),
            "Developer Email": ("Developer Email", "first"),
            "Developer Website": ("Developer Website", "first"),
            "Developer Address": ("Developer Address", "first"),
            "App Count": ("Package Name", "count"),
            "Total Installs": ("Downloads", "sum"),
            "Best App Installs": ("Downloads", "max"),
            "Apps With Ads": ("Contains Ads", lambda s: (s == "Yes").sum()),
            "Avg Rating": ("Rating", "mean"),
            "Freshest Update (days)": ("Days Since Update", "min"),
            "Best Lead Score": ("Lead Score", "max"),
        }
    ).reset_index()

    grouped["Avg Rating"] = grouped["Avg Rating"].round(2)
    grouped["Top App"] = grouped["Developer ID"].map(
        df.sort_values("Downloads", ascending=False)
          .drop_duplicates("Developer ID")
          .set_index("Developer ID")["App Name"]
    )

    columns = [
        "Developer Name", "Developer Email", "Developer Website",
        "Developer Address", "Developer ID", "Top App", "App Count",
        "Total Installs", "Best App Installs", "Apps With Ads", "Avg Rating",
        "Freshest Update (days)", "Best Lead Score",
    ]
    return grouped[columns].sort_values(
        by=["Best Lead Score", "Total Installs"], ascending=[False, False]
    )


def autoformat(worksheet, df, link_column=None):
    from openpyxl.styles import Alignment, Font, PatternFill

    header_fill = PatternFill("solid", start_color="1F3864")
    header_font = Font(color="FFFFFF", bold=True)

    for cell in worksheet[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(vertical="center", wrap_text=True)

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions
    worksheet.row_dimensions[1].height = 28

    for index, column_name in enumerate(df.columns, start=1):
        letter = worksheet.cell(row=1, column=index).column_letter
        longest = max(
            [len(str(column_name))]
            + [len(str(v)) for v in df[column_name].head(300).tolist()]
        )
        worksheet.column_dimensions[letter].width = min(max(longest + 2, 11), 48)

    if link_column and link_column in df.columns:
        column_index = list(df.columns).index(link_column) + 1
        link_font = Font(color="0563C1", underline="single")
        for row_index in range(2, worksheet.max_row + 1):
            cell = worksheet.cell(row=row_index, column=column_index)
            if cell.value:
                cell.hyperlink = cell.value
                cell.value = "Open on Play"
                cell.font = link_font


def export(df):
    developers = build_developer_sheet(df)
    hot = df[(df["Lead Score"] >= 60) & (df["Developer Email"] != "")]

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        hot.to_excel(writer, sheet_name="Hot Leads", index=False)
        df.to_excel(writer, sheet_name="All Apps", index=False)
        developers.to_excel(writer, sheet_name="Developers", index=False)

        autoformat(writer.sheets["Hot Leads"], hot, link_column="App Link")
        autoformat(writer.sheets["All Apps"], df, link_column="App Link")
        autoformat(writer.sheets["Developers"], developers)

    return hot, developers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-devs", type=int, default=TARGET_DEVELOPERS,
                        help="Stop discovery once this many qualifying developers are found.")
    parser.add_argument("--export-only", action="store_true",
                        help="Skip all discovery/fetching, just rebuild the Excel from cache.")
    args = parser.parse_args()

    store = Store()

    try:
        if not args.export_only:
            run_discovery(store, args.target_devs)
    finally:
        store.flush()  # make sure buffered writes survive Ctrl+C / errors

    df = build_dataframe(store)

    if df.empty:
        print("\nNo qualifying apps found.")
        store.close()
        return

    hot, developers = export(df)

    print("\n" + "=" * 44)
    print("SCRAPING COMPLETE")
    print("=" * 44)
    print(f"Apps cached          : {len(store.known)}")
    print(f"Apps kept            : {len(df)}")
    print(f"Unique developers    : {df['Developer ID'].nunique()}")
    print(f"Hot leads (score 60+): {len(hot)}")
    print(f"Excel                : {OUTPUT_FILE}")
    print(f"MongoDB              : {DB_NAME}.apps")
    print("=" * 44)

    store.close()


if __name__ == "__main__":
    main()
