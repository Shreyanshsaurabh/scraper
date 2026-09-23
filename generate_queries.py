"""
Turns a plain-language niche ("puzzle game", "finance", "fitness tracking")
into a list of base search terms + modifiers for the Play Store scraper,
using the Mistral API. Writes queries.json, which scraper.py reads if
QUERIES_FILE is set.

Usage:
    python generate_queries.py "puzzle game"
    python generate_queries.py "finance" --out queries.json --count 40
"""

import argparse
import json
import os
import random
import re
import sys
import time
import urllib.request
import urllib.error

MISTRAL_URL = "https://api.mistral.ai/v1/chat/completions"
MODEL = os.environ.get("MISTRAL_MODEL", "mistral-large-latest")

SYSTEM_PROMPT_TEMPLATE = """You generate search-query building blocks for a Google Play \
Store app discovery tool. Given a niche/topic, return STRICT JSON only, no \
prose, no markdown fences, matching exactly this shape:

{"base_terms": ["...", "..."], "modifiers": ["...", "..."]}

Rules:
- base_terms: __MIN__-__MAX__ short phrases (1-4 words) real users would type \
into the Play Store search box to find apps in this niche. Include close \
synonyms, sub-categories, and common app-type words, but NOT the word \
"app" or "game" alone as their own entry.
- modifiers: 8-15 short words/phrases commonly appended to a search, such as \
"free", "offline", "for kids", "pro", "2 player" - adapt them to fit the \
niche (a finance niche might include "tracker", "budget", "calculator" \
instead of "2 player"). Include "" (empty string) as one modifier.
- Every entry is lowercase, has no punctuation besides spaces, and is under \
5 words.
- Output valid JSON and nothing else.
"""


def call_mistral(keyword, count, api_key, timeout=60, max_retries=5):
    system_prompt = (
        SYSTEM_PROMPT_TEMPLATE
        .replace("__MIN__", str(count // 2))
        .replace("__MAX__", str(count))
    )
    payload = {
        "model": MODEL,
        "temperature": 0.4,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Niche: {keyword}"},
        ],
    }
    req = urllib.request.Request(
        MISTRAL_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            return body["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            last_error = RuntimeError(f"Mistral API error {exc.code}: {detail}")
            if exc.code in (429, 500, 502, 503, 504) and attempt < max_retries:
                wait = min(60, 2 ** attempt) + random.uniform(0, 1)
                print(f"  Mistral returned {exc.code} (attempt {attempt}/{max_retries}) "
                      f"- waiting {wait:.0f}s before retrying...")
                time.sleep(wait)
                continue
            raise last_error from exc
        except urllib.error.URLError as exc:
            last_error = RuntimeError(f"Could not reach Mistral: {exc.reason}")
            if attempt < max_retries:
                wait = min(60, 2 ** attempt)
                print(f"  Network error (attempt {attempt}/{max_retries}) - waiting {wait:.0f}s...")
                time.sleep(wait)
                continue
            raise last_error from exc

    raise last_error


_WORD_RE = re.compile(r"^[a-z0-9][a-z0-9 ]{0,40}$")


def clean_list(items, max_words=5):
    seen, out = set(), []
    for item in items:
        if not isinstance(item, str):
            continue
        text = re.sub(r"\s+", " ", item.strip().lower())
        if text == "" or (_WORD_RE.match(text) and len(text.split()) <= max_words):
            if text not in seen:
                seen.add(text)
                out.append(text)
    return out


def parse_and_validate(raw_content):
    try:
        data = json.loads(raw_content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Model did not return valid JSON: {exc}\nRaw: {raw_content[:500]}")

    base_terms = clean_list(data.get("base_terms", []))
    modifiers = clean_list(data.get("modifiers", []))
    if "" not in modifiers:
        modifiers.insert(0, "")

    if len(base_terms) < 5:
        raise ValueError(f"Only {len(base_terms)} usable base_terms came back - too few to search with.")
    if len(modifiers) < 3:
        raise ValueError(f"Only {len(modifiers)} usable modifiers came back - too few to search with.")

    return base_terms, modifiers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("keyword", help='Niche to generate queries for, e.g. "puzzle game"')
    parser.add_argument("--out", default="queries.json", help="Where to write the result")
    parser.add_argument("--count", type=int, default=30, help="Target number of base terms")
    args = parser.parse_args()

    api_key = os.environ.get("MISTRAL_API_KEY")
    if not api_key:
        sys.exit("MISTRAL_API_KEY environment variable is not set")

    print(f"Asking Mistral for search terms covering: {args.keyword!r}")
    raw = call_mistral(args.keyword, args.count, api_key)
    base_terms, modifiers = parse_and_validate(raw)

    queries = sorted({
        f"{term} {mod}".strip()
        for term in base_terms
        for mod in modifiers
    })

    result = {
        "keyword": args.keyword,
        "base_terms": base_terms,
        "modifiers": modifiers,
        "query_count": len(queries),
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)

    print(f"base_terms ({len(base_terms)}): {base_terms}")
    print(f"modifiers  ({len(modifiers)}): {modifiers}")
    print(f"-> {len(queries)} unique queries written to {args.out}")


if __name__ == "__main__":
    main()
