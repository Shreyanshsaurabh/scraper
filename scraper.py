import argparse
import itertools
import math
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
from google_play_scraper import app as fetch_app
from google_play_scraper import search as play_search
from google_play_scraper.exceptions import NotFoundError
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm

OUTPUT_FILE = "puzzle.xlsx"
DB_NAME = os.environ.get("MONGODB_DB", "db2")

# ---- goals / filters -------------------------------------------------------
TARGET_DEVELOPERS = 10_000
STALE_AFTER_DAYS = 1460
MIN_INSTALLS = 10_000
MAX_RUNTIME_MINUTES = 330          # stop cleanly before GitHub's 6h cut-off

# ---- rate limiting ---------------------------------------------------------
# One global limiter is shared by ALL threads, so speed is controlled by
# REQUEST_INTERVAL, not by MAX_WORKERS. Raise it if you still get throttled.
REQUEST_INTERVAL = float(os.environ.get("REQUEST_INTERVAL", "0.6"))  # seconds per detail fetch
SEARCH_WEIGHT = 4.0        # a search (n_hits=250) makes several HTTP calls internally
MAX_WORKERS = 4
MAX_RETRIES = 5
BASE_COOLDOWN = 20         # seconds; doubles on each consecutive rate-limit strike
MAX_COOLDOWN = 240
MAX_STRIKES = 6            # this many strikes in a row -> assume we're blocked, stop cleanly

# ---- discovery -------------------------------------------------------------
RESULTS_PER_QUERY = 250
LANG = "en"
# Searched in this order; later countries only get used if the target isn't hit.
# Different stores rank different apps, which surfaces more developers.
SEARCH_COUNTRIES = ["in", "us", "gb", "ca", "au", "ph", "id", "pk", "bd", "za", "ng", "ae", "sg", "my"]

MONGO_BATCH_SIZE = 100

BASE_TERMS = [
    # core puzzle
    "puzzle", "puzzle game", "brain teaser", "brain training", "brain game", "brain puzzle",
    "brain test", "brain out", "tricky puzzle", "logic puzzle", "logic game", "logic riddles",
    "match 3", "match puzzle", "match 3 adventure", "block puzzle", "block game", "block blast",
    "woodoku", "wood puzzle", "hexa puzzle", "hexa sort", "tile puzzle", "tile match",
    "tile connect", "triple tile", "triple match", "merge puzzle", "number merge", "color puzzle",
    "color match", "color sort", "sorting puzzle", "ball sort", "water sort", "bottle sort",
    "tube sort", "number sort", "screw puzzle", "nuts and bolts", "jigsaw", "jigsaw puzzle",
    "jigsaw for kids", "jigsaw for adults", "photo puzzle", "art puzzle", "landscape jigsaw",
    "tangram", "sliding puzzle", "slide puzzle", "sliding block", "15 puzzle", "sliding tile",
    "sudoku", "sudoku classic", "sudoku 9x9", "killer sudoku", "kakuro", "nonogram", "minesweeper",
    "crossword", "crossword puzzle", "codeword", "cryptogram", "acrostic",
    # words / numbers / math / quiz
    "word puzzle", "word game", "word search", "word connect", "word cookies", "word crush",
    "word blocks", "word stack", "word scramble", "word ladder", "wordle", "hangman",
    "anagram", "guess the word", "guess the picture", "4 pics", "spelling game",
    "vocabulary game", "alphabet game", "number puzzle", "number game", "number match", "2048",
    "math puzzle", "math game", "math quiz", "mental math", "multiplication game",
    "quiz", "quiz game", "trivia", "trivia game", "general knowledge quiz", "IQ test", "riddle",
    "riddle game", "memory game", "memory match", "matching pairs", "pair game", "memory training",
    "concentration game", "focus game", "reflex game", "thinking game", "mind game",
    # objects / mystery / adventure
    "hidden object", "hidden objects mystery", "find hidden objects", "hidden numbers",
    "seek and find", "spot the difference", "find the difference", "connect dots", "one line",
    "draw puzzle", "draw to save", "maze", "labyrinth", "escape room", "escape game",
    "room escape", "escape puzzle", "escape adventure", "point and click", "adventure puzzle",
    "story puzzle", "mystery puzzle", "detective puzzle", "detective game", "murder mystery",
    # physics / shooters / pipes
    "pipe puzzle", "flow puzzle", "physics puzzle", "physics game", "rope puzzle", "cut the rope",
    "chain reaction", "bubble shooter", "bubble puzzle", "bubble pop", "marble shooter", "marble",
    "zuma", "rolling ball", "stack game", "tower building", "unblock puzzle", "unblock car",
    "parking puzzle", "traffic puzzle", "puzzle platformer",
    # classic / board / card
    "mahjong", "mahjong solitaire", "mahjong connect", "onet", "solitaire", "spider solitaire",
    "freecell", "klondike", "tetris", "brick breaker", "chess", "chess puzzle", "chess offline",
    "checkers", "reversi", "othello", "connect four", "gomoku", "tic tac toe", "sokoban",
    "dominoes", "domino puzzle", "ludo", "carrom", "snakes and ladders", "card game", "rummy",
    "blackjack", "strategy puzzle", "tower defense puzzle",
    # casual / kids / creative
    "casual game", "arcade game", "hyper casual", "idle game", "clicker game", "gem match",
    "jewel match", "fruit match", "fruit crush", "diamond match", "candy match", "pop it",
    "3d puzzle", "shape puzzle", "pattern puzzle", "kids puzzle", "educational puzzle",
    "toddler puzzle", "baby puzzle", "animal puzzle", "car puzzle", "preschool game",
    "kids learning game", "educational game", "coloring book", "color by number",
    "paint by number", "pixel art", "drawing game", "diamond painting", "puzzle for adults",
]

MODIFIERS = [
    "", "game", "games", "app", "free", "offline", "online", "2 player", "multiplayer",
    "for kids", "for adults", "no wifi", "classic", "new", "best", "hd", "3d", "pro",
    "master", "challenge", "levels", "adventure", "fun", "casual", "relaxing",
]

SEARCH_QUERIES = sorted({
    f"{term} {modifier}".strip()
    for term, modifier in itertools.product(BASE_TERMS, MODIFIERS)
})


# --------------------------------------------------------------------------
# rate limiting
# --------------------------------------------------------------------------

class BlockedError(Exception):
    """Raised when Play keeps throttling us and we should stop for now."""


_RATE_LIMIT_RE = re.compile(
    r"\b(429|503|403)\b|too many|rate.?limit|quota|captcha|unusual traffic|"
    r"timed out|timeout|connection (reset|aborted|refused)|remote end closed|temporar",
    re.I,
)


def looks_rate_limited(exc):
    return bool(_RATE_LIMIT_RE.search(str(exc)))


def is_not_found(exc):
    if isinstance(exc, NotFoundError):
        return True
    message = str(exc).lower()
    return "not found" in message or "404" in message


class RateLimiter:
    """Thread-safe: spaces requests out globally, adds jitter, and backs off
    exponentially when Google starts throttling."""

    def __init__(self, interval):
        self.interval = interval
        self.lock = threading.Lock()
        self.next_slot = 0.0
        self.cooldown_until = 0.0
        self.strikes = 0
        self.blocked = False

    def wait(self, weight=1.0):
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next_slot, self.cooldown_until)
            self.next_slot = start + self.interval * weight * random.uniform(0.8, 1.4)
        if start > now:
            time.sleep(start - now)

    def success(self):
        with self.lock:
            if self.strikes > 0:
                self.strikes -= 1

    def failure(self, exc):
        rate_limited = looks_rate_limited(exc)
        cooldown = 2.0
        with self.lock:
            if rate_limited:
                self.strikes += 1
                cooldown = min(MAX_COOLDOWN, BASE_COOLDOWN * 2 ** (self.strikes - 1))
                if self.strikes >= MAX_STRIKES:
                    self.blocked = True
            self.cooldown_until = max(self.cooldown_until, time.monotonic() + cooldown)
            strikes = self.strikes
        if rate_limited:
            tqdm.write(f"  rate limited (strike {strikes}/{MAX_STRIKES}) - cooling down {cooldown:.0f}s")


LIMITER = RateLimiter(REQUEST_INTERVAL)


def with_retry(fn, *args, weight=1.0, **kwargs):
    last_error = None
    for _ in range(MAX_RETRIES):
        if LIMITER.blocked:
            raise BlockedError("too many rate-limit strikes")
        LIMITER.wait(weight)
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            if is_not_found(exc):
                raise
            last_error = exc
            LIMITER.failure(exc)
            continue
        LIMITER.success()
        return result
    raise last_error


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
    """MongoDB-backed cache. Known apps and qualified developers are loaded
    into memory once; writes are buffered and sent with bulk_write."""

    def __init__(self):
        uri = os.environ.get("MONGODB_URI")
        if not uri:
            raise RuntimeError("MONGODB_URI environment variable is not set")

        self.client = MongoClient(uri, serverSelectionTimeoutMS=30000)
        self.db = self.client[DB_NAME]
        self.apps = self.db["apps"]
        self.apps.create_index("package_name", unique=True)
        self.client.admin.command("ping")
        print(f"Connected to MongoDB Atlas (database: {DB_NAME})")

        self.known = set()
        self.qualified_devs = set()
        self._buffer = []
        self._load_state()

    def _load_state(self):
        retry_count = 0
        for r in self.apps.find({}, {"package_name": 1, "payload": 1, "ok": 1, "_id": 0}):
            if r.get("ok") is False:
                # Older runs stored rate-limited fetches as "failed" forever.
                # Leave them out of `known` so they get fetched again.
                retry_count += 1
                continue
            self.known.add(r["package_name"])
            payload = r.get("payload")
            if payload and payload.get("developerId") and qualifies(payload):
                self.qualified_devs.add(str(payload["developerId"]))
        if retry_count:
            print(f"Will retry {retry_count} apps that previously failed.")

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

KEEP_FIELDS = (
    "title", "url", "appId", "realInstalls", "installs", "containsAds",
    "offersIAP", "genre", "genreId", "score", "ratings", "reviews",
    "developer", "developerId", "developerEmail", "developerWebsite",
    "developerAddress", "updated", "released", "free", "adSupported",
    "contentRating", "privacyPolicy",
)


def fetch_one(package_name, country):
    details = with_retry(fetch_app, package_name, lang=LANG, country=country)
    return {k: details.get(k) for k in KEEP_FIELDS}


def fetch_many(package_names, store, country):
    if not package_names:
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_one, p, country): p for p in package_names}
        for future in as_completed(futures):
            package_name = futures[future]
            try:
                store.save(package_name, future.result(), ok=True)
            except BlockedError:
                pass  # not saved -> will be retried on the next run
            except Exception as exc:
                if is_not_found(exc):
                    store.save(package_name, None, ok=False)
                # any other error is treated as transient: not saved, retried later


def run_discovery(store, target_devs, deadline):
    jobs = []
    for country in SEARCH_COUNTRIES:
        queries = SEARCH_QUERIES[:]
        random.shuffle(queries)
        jobs.extend((query, country) for query in queries)

    print(f"\nCache: {len(store.known)} apps, {len(store.qualified_devs)} qualifying developers.")
    print(f"Target: {target_devs} developers | {len(SEARCH_QUERIES)} queries x "
          f"{len(SEARCH_COUNTRIES)} countries = {len(jobs)} searches available.")
    print(f"Pace: ~1 request every {REQUEST_INTERVAL}s (searches count x{SEARCH_WEIGHT:g}).\n")

    stop_reason = "ran out of queries"
    bar = tqdm(jobs, desc="Searching")
    for query, country in bar:
        if len(store.qualified_devs) >= target_devs:
            stop_reason = "target reached"
            break
        if LIMITER.blocked:
            stop_reason = "Google Play is rate-limiting this IP"
            break
        if time.monotonic() >= deadline:
            stop_reason = "time limit reached"
            break

        try:
            results = with_retry(
                play_search, query,
                lang=LANG, country=country, n_hits=RESULTS_PER_QUERY,
                weight=SEARCH_WEIGHT,
            )
        except BlockedError:
            stop_reason = "Google Play is rate-limiting this IP"
            break
        except Exception as exc:
            tqdm.write(f"  search failed: {query!r} [{country}] -> {exc}")
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

        fetch_many(candidates, store, country)
        store.flush()

        bar.set_postfix(devs=len(store.qualified_devs), apps=len(store.known), cc=country)

    bar.close()
    store.flush()

    print(f"\nDiscovery stopped: {stop_reason}. "
          f"Qualifying developers: {len(store.qualified_devs)} / {target_devs}.")
    if stop_reason != "target reached":
        print("Progress is saved in MongoDB - run again to continue where this left off.")


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
    parser.add_argument("--max-minutes", type=float, default=MAX_RUNTIME_MINUTES,
                        help="Stop discovery after this many minutes so the Excel still gets exported.")
    parser.add_argument("--export-only", action="store_true",
                        help="Skip all discovery/fetching, just rebuild the Excel from cache.")
    args = parser.parse_args()

    store = Store()
    deadline = time.monotonic() + args.max_minutes * 60

    try:
        if not args.export_only:
            run_discovery(store, args.target_devs, deadline)
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
