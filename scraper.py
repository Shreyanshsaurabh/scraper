import argparse
import itertools
import json
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

try:
    from google_play_scraper.constants.regex import Regex
    from google_play_scraper.constants.request import Formats
    from google_play_scraper.features.app import parse_dom
    from google_play_scraper.utils.request import get as _http_get
    RAW_AVAILABLE = True
except ImportError:
    RAW_AVAILABLE = False

OUTPUT_FILE = "puzzle.xlsx"
DB_NAME = os.environ.get("MONGODB_DB", "db3")

TARGET_DEVELOPERS = 10_000
STALE_AFTER_DAYS = 1460
MIN_INSTALLS = 10_000
MAX_RUNTIME_MINUTES = 330

REQUEST_INTERVAL = float(os.environ.get("REQUEST_INTERVAL", "0.6"))
SEARCH_WEIGHT = 4.0
MAX_WORKERS = 4
MAX_RETRIES = 4
BASE_COOLDOWN = 20
MAX_COOLDOWN = 240
MAX_INTERVAL = 4.0
MAX_STRIKES = 6

RESULTS_PER_QUERY = 250
LANG = "en"
MAX_SEARCHES = 10
SEARCH_COUNTRIES = ["tr", "ma", "eg"]

MONGO_BATCH_SIZE = 100

BASE_TERMS = [
    # Business
    "business app", "business tools", "small business app", "invoice app",
    "invoice maker", "crm app", "accounting app", "bookkeeping app",
    "business card scanner", "office app", "office suite", "expense tracker",
    "employee management app", "payroll app", "b2b app",

    # Games
    "games", "mobile games", "casual games", "action games", "racing games",
    "arcade games", "rpg games", "strategy games", "simulation games",
    "adventure games", "shooting games", "card games", "board games",
    "multiplayer games", "offline games", "kids games",

    # Entertainment
    "entertainment app", "streaming app", "movie app", "tv app",
    "comics app", "anime app", "webtoon app", "live tv app",
    "short video app", "meme app", "fan app",

    # Utility & Tools
    "utility app", "tools app", "file manager", "cleaner app",
    "battery saver", "flashlight app", "calculator app", "vpn app",
    "app locker", "screen recorder", "qr code scanner", "compass app",
    "launcher app", "keyboard app", "backup app",

    # Social
    "social app", "social network app", "chat app", "social media app",
    "community app", "forum app", "social discovery app",

    # Dating
    "dating app", "chat and date app", "meet people app",
    "matchmaking app", "online dating app", "video chat dating app",

    # Sports
    "sports app", "football app", "live score app", "fitness tracker app",
    "cricket app", "sports news app", "workout app", "running app",
    "gym app", "football live scores",

    # Travel
    "travel app", "flight booking app", "hotel booking app",
    "maps and navigation app", "travel guide app", "travel planner",
    "taxi booking app", "car rental app",

    # Lifestyle
    "lifestyle app", "fashion app", "beauty app", "recipe app",
    "home design app", "wedding planner app", "horoscope app",
    "astrology app", "pregnancy tracker app",

    # Productivity
    "productivity app", "to do list app", "note taking app",
    "calendar app", "task manager app", "planner app", "pdf reader app",
    "resume builder app", "scanner app document",

    # Video Players & Downloader
    "video player app", "video downloader app", "mp4 player app",
    "media player app", "video status downloader", "video converter app",

    # Communication
    "communication app", "messenger app", "video call app", "sms app",
    "email app", "voice call app", "group chat app", "translator app",

    # Finance
    "finance app", "banking app", "budget app", "loan app",
    "investment app", "wallet app", "money transfer app",
    "trading app", "crypto app", "personal finance app",

    # Reward
    "reward app", "cashback app", "earn money app", "rewards points app",
    "gift card app", "survey app earn money", "spin and win app",

    # Photography
    "photography app", "photo editor app", "camera app", "collage maker app",
    "photo filter app", "selfie camera app", "photo enhancer app",

    # News
    "news app", "breaking news app", "local news app", "newspaper app",
    "news aggregator app",

    # Music
    "music app", "music player app", "music streaming app",
    "song downloader app", "karaoke app", "lyrics app", "radio app",
]

MODIFIERS = [
    "", "app", "free", "offline", "online", "for android",
    "download", "best",
]

SEARCH_QUERIES = sorted({
    f"{term} {modifier}".strip()
    for term, modifier in itertools.product(BASE_TERMS, MODIFIERS)
})


class BlockedError(Exception):
    pass


_RATE_LIMIT_RE = re.compile(
    r"\b(429|503|403)\b|too many|rate.?limit|quota|captcha|unusual traffic|"
    r"timed out|timeout|connection (reset|aborted|refused)|remote end closed|temporar|gateway",
    re.I,
)


def looks_rate_limited(exc):
    return bool(_RATE_LIMIT_RE.search(str(exc)))


def is_not_found(exc):
    return isinstance(exc, NotFoundError)


class RateLimiter:
    def __init__(self, interval):
        self.base_interval = interval
        self.interval = interval
        self.lock = threading.Lock()
        self.next_slot = 0.0
        self.cooldown_until = 0.0
        self.last_strike_at = 0.0
        self.strikes = 0
        self.blocked = False

    def wait(self, weight=1.0):
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next_slot, self.cooldown_until)
            self.next_slot = start + self.interval * weight * random.uniform(0.8, 1.4)
        if start > now:
            time.sleep(start - now)
        return start

    def success(self):
        with self.lock:
            self.strikes = max(0, self.strikes - 1)
            self.interval = max(self.base_interval, self.interval * 0.97)

    def failure(self, exc, started_at):
        rate_limited = looks_rate_limited(exc)
        message = None
        with self.lock:
            now = time.monotonic()
            if self.blocked:
                pass
            elif rate_limited:
                if started_at > self.last_strike_at:
                    self.strikes += 1
                    self.last_strike_at = now
                    self.interval = min(MAX_INTERVAL, self.interval * 1.5)
                    cooldown = min(MAX_COOLDOWN, BASE_COOLDOWN * 2 ** (self.strikes - 1))
                    self.cooldown_until = max(self.cooldown_until, now + cooldown)
                    if self.strikes >= MAX_STRIKES:
                        self.blocked = True
                    message = (f"  rate limited (strike {self.strikes}/{MAX_STRIKES}) - "
                               f"pausing {cooldown:.0f}s, pace now 1 request / {self.interval:.1f}s")
            else:
                self.cooldown_until = max(self.cooldown_until, now + 2.0)
        if message:
            tqdm.write(message)
        return rate_limited


LIMITER = RateLimiter(REQUEST_INTERVAL)


def with_retry(fn, *args, weight=1.0, **kwargs):
    errors = 0
    while True:
        if LIMITER.blocked:
            raise BlockedError("too many rate-limit strikes")
        started = LIMITER.wait(weight)
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            if is_not_found(exc):
                raise
            if LIMITER.failure(exc, started):
                continue
            errors += 1
            if errors >= MAX_RETRIES:
                raise
            continue
        LIMITER.success()
        return result


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
    installs = normalize_downloads(record.get("realInstalls"), record.get("installs"))
    if installs < MIN_INSTALLS:
        return False
    if not (record.get("containsAds") or record.get("adSupported")):
        return False
    age = days_since(epoch_to_date(record.get("updated")))
    return age is None or age <= STALE_AFTER_DAYS


class Store:
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

    def phone_backfill_candidates(self):
        done = {str(d) for d in self.apps.distinct(
            "payload.developerId", {"payload.developerPhone": {"$exists": True}})}
        seen, packages = set(done), []
        cursor = self.apps.find(
            {"ok": True, "payload": {"$ne": None},
             "payload.developerPhone": {"$exists": False}},
            {"package_name": 1, "payload": 1, "_id": 0},
        )
        for r in cursor:
            payload = r["payload"]
            dev_id = str(payload.get("developerId") or "")
            if dev_id and dev_id not in seen and qualifies(payload):
                seen.add(dev_id)
                packages.append(r["package_name"])
        return packages

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


KEEP_FIELDS = (
    "title", "url", "appId", "realInstalls", "installs", "containsAds",
    "offersIAP", "genre", "genreId", "score", "ratings", "reviews",
    "developer", "developerId", "developerEmail", "developerWebsite",
    "developerAddress", "developerPhone", "updated", "released", "free", "adSupported",
    "contentRating", "privacyPolicy",
)


DEV_DATASET = "ds:5"
DEV_BLOCK_PATHS = ([1, 2, 69], [1, 2, 68])
_PHONE_RE = re.compile(r"^\+?\(?\d[\d\s().\-]{5,18}\d$")


def build_dataset(dom):
    dataset = {}
    for match in Regex.SCRIPT.findall(dom):
        key_match = Regex.KEY.findall(match)
        value_match = Regex.VALUE.findall(match)
        if key_match and value_match:
            try:
                dataset[key_match[0]] = json.loads(value_match[0])
            except ValueError:
                continue
    return dataset


def _dig(node, path):
    for index in path:
        try:
            node = node[index]
        except (IndexError, KeyError, TypeError):
            return None
    return node


def find_phone(node):
    if isinstance(node, str):
        text = node.strip()
        digits = re.sub(r"\D", "", text)
        if _PHONE_RE.match(text) and 7 <= len(digits) <= 15:
            return text
        return ""
    if isinstance(node, (list, tuple)):
        for item in node:
            found = find_phone(item)
            if found:
                return found
    return ""


def extract_developer_phone(dom):
    try:
        root = build_dataset(dom).get(DEV_DATASET)
        for path in DEV_BLOCK_PATHS:
            phone = find_phone(_dig(root, path))
            if phone:
                return phone
    except Exception:
        pass
    return ""


def fetch_details(package_name, country):
    if not RAW_AVAILABLE:
        return fetch_app(package_name, lang=LANG, country=country)

    url = Formats.Detail.build(app_id=package_name, lang=LANG, country=country)
    try:
        dom = _http_get(url)
    except NotFoundError:
        url = Formats.Detail.fallback_build(app_id=package_name, lang=LANG)
        dom = _http_get(url)

    details = parse_dom(dom=dom, app_id=package_name, url=url)
    details["developerPhone"] = extract_developer_phone(dom)
    return details


def fetch_one(package_name, country):
    details = with_retry(fetch_details, package_name, country)
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
                pass
            except Exception as exc:
                if is_not_found(exc):
                    store.save(package_name, None, ok=False)


def run_discovery(store, target_devs, deadline, max_searches):
    jobs = []
    for country in SEARCH_COUNTRIES:
        queries = SEARCH_QUERIES[:]
        random.shuffle(queries)
        jobs.extend((query, country) for query in queries)

    available = len(jobs)
    jobs = jobs[:max_searches]

    print(f"\nCache: {len(store.known)} apps, {len(store.qualified_devs)} qualifying developers.")
    print(f"Target: {target_devs} developers | {len(SEARCH_QUERIES)} queries x "
          f"{len(SEARCH_COUNTRIES)} countries = {available} possible; "
          f"running at most {len(jobs)}.")
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
                continue
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


def backfill_phones(store, deadline, country):
    todo = store.phone_backfill_candidates()
    print(f"\n{len(todo)} developers need a phone lookup (1 request each).")
    chunk = 200
    for start in tqdm(range(0, len(todo), chunk), desc="Backfilling phones"):
        if LIMITER.blocked:
            print("Google Play is rate-limiting this IP - stopping; run again later.")
            break
        if time.monotonic() >= deadline:
            print("Time limit reached - run again to continue.")
            break
        fetch_many(todo[start:start + chunk], store, country)
        store.flush()


def inspect_app(package_name, country):
    if not RAW_AVAILABLE:
        raise SystemExit("This google_play_scraper version doesn't expose the raw page helpers.")
    url = Formats.Detail.build(app_id=package_name, lang=LANG, country=country)
    dom = _http_get(url)
    root = build_dataset(dom).get(DEV_DATASET)
    for path in DEV_BLOCK_PATHS:
        print(f"--- {DEV_DATASET}{path} ---")
        print(json.dumps(_dig(root, path), indent=2, ensure_ascii=False))
    print("\nDetected phone:", extract_developer_phone(dom) or "(none)")


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
            "Developer Phone": record.get("developerPhone") or "",
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


def first_non_empty(series):
    return next((v for v in series if v), "")


def build_developer_sheet(df):
    grouped = df.groupby("Developer ID", dropna=False).agg(
        **{
            "Developer Name": ("Developer Name", "first"),
            "Developer Email": ("Developer Email", first_non_empty),
            "Developer Phone": ("Developer Phone", first_non_empty),
            "Developer Website": ("Developer Website", first_non_empty),
            "Developer Address": ("Developer Address", first_non_empty),
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
        "Developer Name", "Developer Email", "Developer Phone", "Developer Website",
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
    parser.add_argument("--max-searches", type=int, default=MAX_SEARCHES,
                        help="Maximum number of search queries to run in this pass.")
    parser.add_argument("--max-minutes", type=float, default=MAX_RUNTIME_MINUTES,
                        help="Stop discovery after this many minutes so the Excel still gets exported.")
    parser.add_argument("--backfill-phones", action="store_true",
                        help="Re-fetch one app per saved developer to add phone numbers, then export.")
    parser.add_argument("--inspect", metavar="PACKAGE",
                        help="Print the raw developer block for one app (e.g. com.example.game) and exit.")
    parser.add_argument("--export-only", action="store_true",
                        help="Skip all discovery/fetching, just rebuild the Excel from cache.")
    args = parser.parse_args()

    if args.inspect:
        inspect_app(args.inspect, SEARCH_COUNTRIES[0])
        return

    store = Store()
    deadline = time.monotonic() + args.max_minutes * 60

    try:
        if args.backfill_phones:
            backfill_phones(store, deadline, SEARCH_COUNTRIES[0])
        elif not args.export_only:
            run_discovery(store, args.target_devs, deadline, args.max_searches)
    finally:
        store.flush()

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
