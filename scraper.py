import argparse
import json
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlparse

import itertools
import os

import pandas as pd
from google_play_scraper import app as fetch_app
from google_play_scraper import search as play_search
from tqdm import tqdm
from pymongo import MongoClient

OUTPUT_FILE = "puzzle.xlsx"

RESULTS_PER_QUERY = 250  # Play search realistically caps out around here per query
MAX_WORKERS = 4
MAX_RETRIES = 3
BASE_BACKOFF = 1.5

LANG = "en"
COUNTRY = "in"

STALE_AFTER_DAYS = 1460
MIN_INSTALLS = 10_000

# One query only gets you ~200-250 results from Play's search backend, no matter
# what n_hits is set to (this is a server-side cap, not something the library or
# n_hits controls). This installed version of google_play_scraper also has no
# `list` (charts) or `similar` endpoint -- only app, search, reviews, permissions.
# So the only levers available are: (a) many distinct search queries, and
# (b) re-searching by developer name to pull the rest of their catalog.
#
# BASE_TERMS x MODIFIERS is expanded into many queries automatically below so you
# don't have to hand-write hundreds of query strings.

# BASE_TERMS = [
#     "puzzle", "match 3", "block puzzle", "brain teaser", "brain training",
#     "logic puzzle", "jigsaw", "number puzzle", "sudoku", "crossword",
#     "tile puzzle", "physics puzzle", "escape room", "hidden object",
#     "sliding puzzle", "bubble shooter", "merge puzzle", "picture puzzle",
#     "riddle", "quiz", "IQ test", "spot the difference", "maze",
#     "word search", "anagram", "connect dots", "match puzzle", "3d puzzle",
#     "kids puzzle", "puzzle adventure",
# ]
# MODIFIERS = ["game", "games", "app", "free", "offline", "2 player", "for kids"]
BASE_TERMS = [
    "puzzle",
    "puzzle game",
    "brain teaser",
    "brain training",
    "brain game",
    "logic puzzle",
    "logic game",
    "match 3",
    "match puzzle",
    "block puzzle",
    "block game",
    "tile puzzle",
    "tile match",
    "merge puzzle",
    "color puzzle",
    "color match",
    "sorting puzzle",
    "ball sort",
    "water sort",
    "jigsaw",
    "jigsaw puzzle",
    "sudoku",
    "crossword",
    "word puzzle",
    "word game",
    "word search",
    "word connect",
    "anagram",
    "number puzzle",
    "number game",
    "2048",
    "math puzzle",
    "nonogram",
    "hidden object",
    "spot the difference",
    "connect dots",
    "maze",
    "sliding puzzle",
    "slide puzzle",
    "pipe puzzle",
    "flow puzzle",
    "physics puzzle",
    "rope puzzle",
    "bubble shooter",
    "bubble puzzle",
    "marble shooter",
    "riddle",
    "riddle game",
    "escape room",
    "escape game",
    "mystery puzzle",
    "detective puzzle",
    "3d puzzle",
    "shape puzzle",
    "pattern puzzle",
    "kids puzzle",
    "educational puzzle",
    "sokoban",
    "mahjong",
    "solitaire",
    "chess puzzle",
]

MODIFIERS = [
    "game",
    "games",
    "app",
    "free",
    "offline",
    "online",
    "2 player",
    "multiplayer",
    "for kids",
    "no wifi",
]
SEARCH_QUERIES = sorted({
    f"{term} {modifier}".strip()
    for term, modifier in itertools.product(BASE_TERMS, MODIFIERS)
})

MAX_DISCOVERY_ROUNDS = 6  # how many developer-catalog expansion passes to run

INDIA_PLACE_KEYWORDS = [
    "india", "indian", "bharat", "new delhi", "delhi", "mumbai", "bombay",
    "bangalore", "bengaluru", "hyderabad", "chennai", "kolkata", "calcutta",
    "pune", "ahmedabad", "surat", "jaipur", "lucknow", "kanpur", "nagpur",
    "indore", "bhopal", "patna", "chandigarh", "kochi", "coimbatore",
    "noida", "gurgaon", "gurugram", "ghaziabad", "faridabad", "vadodara",
    "rajkot", "nashik", "thane", "visakhapatnam", "vijayawada", "mysore",
    "mysuru", "madurai", "trichy", "thiruvananthapuram", "guwahati",
    "bhubaneswar", "raipur", "ranchi", "dehradun", "jodhpur", "udaipur",
    "kerala", "karnataka", "maharashtra", "telangana", "tamil nadu",
    "west bengal", "uttar pradesh", "rajasthan", "gujarat", "punjab",
    "bihar", "odisha", "jharkhand", "assam", "haryana", "madhya pradesh",
    "andhra pradesh", "chhattisgarh", "uttarakhand", "himachal pradesh",
    "goa",
]

_NEGATIVE_HINTS = re.compile(r"\b(indiana|indianapolis)\b", re.I)
_PLACE_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in INDIA_PLACE_KEYWORDS) + r")\b",
    re.I,
)
_PIN_RE = re.compile(r"\b[1-9][0-9]{5}\b")


def is_indian_developer(address, website=""):
    addr = str(address or "")

    if addr:
        if _NEGATIVE_HINTS.search(addr) and not _PLACE_RE.search(
            _NEGATIVE_HINTS.sub("", addr)
        ):
            return False
        if _PLACE_RE.search(addr):
            return True
        if _PIN_RE.search(addr):
            return True

    host = (urlparse(str(website or "")).hostname or "").lower()
    if host.endswith(".in") or host.endswith(".co.in") or host.endswith(".org.in"):
        return True

    return False


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


class Store:
    def __init__(self):
        uri = os.environ.get("MONGODB_URI")
        if not uri:
            raise RuntimeError("MONGODB_URI environment variable is not set")

        self.client = MongoClient(uri, serverSelectionTimeoutMS=30000)
        self.db = self.client["puzzledb"]
        self.apps = self.db["apps"]
        self.expanded_devs = self.db["expanded_devs"]

        self.apps.create_index("package_name", unique=True)
        self.expanded_devs.create_index("developer_id", unique=True)

        self.client.admin.command("ping")
        print("Connected to MongoDB Atlas")

    def known_ids(self):
        return {
            r["package_name"]
            for r in self.apps.find({}, {"package_name": 1, "_id": 0})
        }

    def save(self, package_name, payload, ok=True):
        self.apps.update_one(
            {"package_name": package_name},
            {
                "$set": {
                    "package_name": package_name,
                    "payload": payload,
                    "ok": bool(ok),
                    "fetched_at": time.time(),
                }
            },
            upsert=True,
        )

    def all_records(self):
        return [
            r["payload"]
            for r in self.apps.find(
                {"ok": True, "payload": {"$ne": None}},
                {"payload": 1, "_id": 0},
            )
            if r.get("payload")
        ]

    def expanded(self):
        return {
            r["developer_id"]
            for r in self.expanded_devs.find({}, {"developer_id": 1, "_id": 0})
        }

    def mark_expanded(self, developer_id):
        self.expanded_devs.update_one(
            {"developer_id": developer_id},
            {
                "$set": {
                    "developer_id": developer_id,
                    "done_at": time.time(),
                }
            },
            upsert=True,
        )

    def close(self):
        self.client.close()


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


def fetch_many(package_names, store, desc="Fetching apps"):
    todo = [p for p in package_names if p not in store.known_ids()]
    if not todo:
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_one, p): p for p in todo}
        for future in tqdm(as_completed(futures), total=len(futures), desc=desc):
            package_name = futures[future]
            try:
                store.save(package_name, future.result(), ok=True)
            except Exception:
                store.save(package_name, None, ok=False)


def discover_from_queries():
    """Search discovery. Each distinct query yields its own ~200-250 result
    batch (Play's search backend caps out there regardless of n_hits), so we
    lean on having many varied queries rather than one huge n_hits value."""
    found = set()
    for query in tqdm(SEARCH_QUERIES, desc="Searching"):
        try:
            results = with_retry(
                play_search, query,
                lang=LANG, country=COUNTRY, n_hits=RESULTS_PER_QUERY,
            )
        except Exception as exc:
            tqdm.write(f"  search failed: {query!r} -> {exc}")
            continue

        for result in results:
            package_name = result.get("appId")
            if package_name:
                found.add(package_name)
        time.sleep(0.4)

    return found


def expand_developer_catalogs(store):
    records = store.all_records()
    already = store.expanded()

    targets = {}
    for record in records:
        developer_id = str(record.get("developerId") or "")
        name = record.get("developer")
        if developer_id and name and developer_id not in already:
            targets[developer_id] = name

    if not targets:
        return set()

    new_ids = set()
    known = store.known_ids()

    for developer_id, name in tqdm(targets.items(), desc="Expanding developers"):
        try:
            results = with_retry(
                play_search, f'"{name}"',
                lang=LANG, country=COUNTRY, n_hits=RESULTS_PER_QUERY,
            )
        except Exception as exc:
            tqdm.write(f"  expand failed: {name!r} -> {exc}")
            continue

        for result in results:
            if str(result.get("developer") or "").strip().lower() != name.strip().lower():
                continue
            package_name = result.get("appId")
            if package_name and package_name not in known:
                new_ids.add(package_name)

        store.mark_expanded(developer_id)
        time.sleep(0.4)

    return new_ids


def run_discovery(store, no_expand):
    """Round 0: seed from many distinct search queries (each query gets its own
    ~200-250 result batch from Play's search backend, since that cap applies
    per-query regardless of n_hits). Then iteratively expand via developer
    catalogs -- re-searching each developer's name to surface their other
    apps -- until nothing new turns up or we hit MAX_DISCOVERY_ROUNDS."""
    print(f"\nCache holds {len(store.known_ids())} apps already.\n")
    print(f"Running {len(SEARCH_QUERIES)} search queries this pass.\n")

    seed = discover_from_queries()
    print(f"\nInitial search discovery surfaced {len(seed)} unique package names.")
    fetch_many(seed, store, desc="Fetching seed app details")

    if no_expand:
        return

    for round_num in range(1, MAX_DISCOVERY_ROUNDS + 1):
        dev_new = expand_developer_catalogs(store)
        fetch_many(dev_new, store, desc=f"Fetching dev-expanded apps (round {round_num})")

        print(f"\nRound {round_num}: +{len(dev_new)} from developer catalogs. "
              f"Cache now {len(store.known_ids())} apps.")

        if not dev_new:
            print("No new apps found — discovery has plateaued.")
            break


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
    df = df[df["Contains Ads"] == "Yes"]
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
    parser.add_argument("--no-expand", action="store_true",
                         help="Skip developer-catalog and similar-apps expansion rounds.")
    parser.add_argument("--export-only", action="store_true",
                         help="Skip all discovery/fetching, just rebuild the Excel from cache.")
    args = parser.parse_args()

    store = Store()

    if not args.export_only:
        run_discovery(store, args.no_expand)

    df = build_dataframe(store)

    if df.empty:
        print("\nNo qualifying apps found.")
        return

    hot, developers = export(df)

    print("\n" + "=" * 44)
    print("SCRAPING COMPLETE")
    print("=" * 44)
    print(f"Apps cached          : {len(store.known_ids())}")
    print(f"Apps kept            : {len(df)}")
    print(f"  ...with ads        : {(df['Contains Ads'] == 'Yes').sum()}")
    print(f"Unique developers    : {df['Developer ID'].nunique()}")
    print(f"Hot leads (score 60+): {len(hot)}")
    print(f"Excel                : {OUTPUT_FILE}")
    print("MongoDB              : puzzledb.apps")
    print("=" * 44)


if __name__ == "__main__":
    main()
