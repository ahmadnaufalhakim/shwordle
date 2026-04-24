import requests
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime
import sys
import time
import random

# ---------------- CONFIG ----------------
MAX_WORKERS = 8
FLUSH_EVERY = 32
TIMEOUT = 10

MAX_RETRIES = 5
BACKOFF_FACTOR = 1.5

BASE_URL = "https://www.merriam-webster.com/dictionary/{}"
NOT_FOUND_TEXT = "The word you've entered isn't in the dictionary"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
]

PROXIES = [
    None,
]

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})
lock = threading.Lock()

# ---------------- LOGGING ----------------
class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, data):
        for f in self.files:
            f.write(data)
            f.flush()

    def flush(self):
        for f in self.files:
            f.flush()

def setup_logging(log_file):
    log_f = open(log_file, "a", encoding="utf-8")

    class TimestampedWriter:
        def __init__(self, f):
            self.f = f
            self.at_line_start = True

        def write(self, data):
            for chunk in data.splitlines(True):
                if self.at_line_start:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.f.write(f"[{ts}] ")
                self.f.write(chunk)
                self.at_line_start = chunk.endswith("\n")
            self.f.flush()

        def flush(self):
            self.f.flush()

    sys.stdout = Tee(sys.stdout, TimestampedWriter(log_f))
    sys.stderr = Tee(sys.stderr, TimestampedWriter(log_f))

# ---------------- SCRAPER ----------------
def clean_chunk(nodes):
    soup = BeautifulSoup("".join(str(n) for n in nodes), "html.parser")

    for em in soup.find_all("em", class_="mw_t_it"):
        em.string = f"<i>{em.get_text(strip=True)}</i>"

    for a in soup.find_all("a"):
        text = a.get_text(strip=True).upper()
        a.string = f"[LNK]{text}[/LNK]"

    text = soup.get_text(" ", strip=True)
    text = text.replace(" ( ", " (").replace(" )", ")")
    text = text.replace("\t", " ").replace("\n", " ")
    text = text.strip(" :")

    if not text or text in [",", ";"]:
        return None

    return text

def extract_dt_parts(tag):
    parts = []
    current = []

    for child in tag.children:
        if getattr(child, "name", None) == "strong":
            if current:
                text = clean_chunk(current)
                if text:
                    parts.append(text)
                current = []
            continue

        current.append(child)

    if current:
        text = clean_chunk(current)
        if text:
            parts.append(text)

    return parts

def extract_definitions(html):
    soup = BeautifulSoup(html, "html.parser")

    for bad_id in ["kidsdictionary", "geographicalDictionary", "medicalDictionary", "legalDictionary"]:
        section = soup.find(id=bad_id)
        if section:
            section.decompose()

    defs = []

    # [DEF]
    for tag in soup.find_all("span", class_="dtText"):
        parts = extract_dt_parts(tag)
        defs.extend([f"[DEF]{p}[/DEF]" for p in parts])

    # [SYN]
    for tag in soup.find_all("span", class_="unText"):
        for em in tag.find_all("em", class_="mw_t_it"):
            em.string = f"<i>{em.get_text(strip=True)}</i>"

        for a in tag.find_all("a"):
            a.string = f"[LNK]{a.get_text(strip=True).upper()}[/LNK]"

        text = tag.get_text(" ", strip=True)
        if text:
            defs.append(f"[SYN]{text}[/SYN]")

    # [CXL]
    for tag in soup.find_all("p", class_="cxl-ref"):
        parts = []

        for span in tag.find_all("span", class_="cxl"):
            parts.append(span.get_text(strip=True))

        for span in tag.find_all("span", class_="ucxt"):
            text = span.get_text(strip=True).upper()
            parts.append(f"[LNK]{text}[/LNK]")

        for a in tag.find_all("a"):
            text = a.get_text(strip=True).upper()
            parts.append(f"[LNK]{text}[/LNK]")

        final_text = " ".join(parts)
        if final_text:
            defs.append(f"[CXL]{final_text}[/CXL]")

    return defs

def process_word(word, index, total):
    url = BASE_URL.format(word.lower())

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session.headers.update({
                "User-Agent": random.choice(USER_AGENTS)
            })

            proxy = random.choice(PROXIES)
            res = session.get(url, timeout=TIMEOUT, proxies=proxy)

            if res.status_code == 200:
                time.sleep(random.uniform(0.6, 1.5))

                # 🔴 NOT FOUND detection
                if NOT_FOUND_TEXT in res.text:
                    print(f"[{index}/{total}] NOT FOUND: {word}")
                    return ("missing", word)

                defs = extract_definitions(res.text)

                if defs:
                    print(f"[{index}/{total}] OK: {word}")
                    return ("ok", [word] + defs)
                else:
                    print(f"[{index}/{total}] EMPTY: {word}")
                    return ("missing", word)

            elif res.status_code in (403, 429):
                wait = BACKOFF_FACTOR ** attempt + random.uniform(0.5, 1.5)
                print(f"[{index}/{total}] BLOCKED ({res.status_code} {res.reason}) {word} | retry {attempt}/{MAX_RETRIES} in {wait:.2f}s")
                time.sleep(wait)

            elif 500 <= res.status_code < 600:
                wait = BACKOFF_FACTOR ** attempt
                print(f"[{index}/{total}] SERVER ERROR {res.status_code} {res.reason} {word} | retry {attempt}/{MAX_RETRIES} in {wait:.2f}s")
                time.sleep(wait)

            else:
                print(f"[{index}/{total}] FAIL: {word} -> {res.status_code} {res.reason}")
                return ("missing", word)

        except Exception as e:
            wait = BACKOFF_FACTOR ** attempt
            print(f"[{index}/{total}] ERROR: {word} -> {e}")
            time.sleep(wait)

        time.sleep(random.uniform(0.6, 1.5))

    print(f"[{index}/{total}] GAVE UP: {word}")
    return ("missing", word)

# ---------------- MAIN ----------------
def main():
    log_file = "scrape_defs_and_ignore_not_found.log"
    setup_logging(log_file)

    input_file = "../words/medium.txt"
    output_file = "defs_medium.csv"
    missing_file = "missing_medium.txt"

    with open(input_file) as f:
        words = [line.strip() for line in f if line.strip()]

    print(f"Loaded {len(words)} words...\n")

    defs_buffer = []
    missing_buffer = []

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile, \
         open(missing_file, "w", encoding="utf-8") as missfile:

        writer = csv.writer(csvfile, delimiter="\t", quoting=csv.QUOTE_MINIMAL)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_word, word, i, len(words)): word
                for i, word in enumerate(words, 1)
            }

            for future in as_completed(futures):
                word = futures[future]
                status, data = future.result()

                if status == "ok":
                    defs_buffer.append(data)
                else:
                    missing_buffer.append(data)

                # flush defs
                if len(defs_buffer) >= FLUSH_EVERY:
                    with lock:
                        try:
                            writer.writerows(defs_buffer)
                        except Exception as e:
                            print("WRITE DEFS ERROR:", e)
                        csvfile.flush()
                    defs_buffer.clear()

                # flush missing
                if len(missing_buffer) >= FLUSH_EVERY:
                    with lock:
                        try:
                            missfile.write("\n".join(missing_buffer) + "\n")
                        except Exception as e:
                            print("WRITE MISS ERROR:", e)
                        missfile.flush()
                    missing_buffer.clear()

        # final flush
        if defs_buffer:
            try:
                writer.writerows(defs_buffer)
            except Exception as e:
                print(f"WRITE DEFS ERROR: {word}, reason:", e)
            csvfile.flush()

        if missing_buffer:
            try:
                missfile.write("\n".join(missing_buffer) + "\n")
            except Exception as e:
                print(f"WRITE MISS ERROR: {word}, reason:", e)
            missfile.flush()

    print("\nScraping complete!")
    print("Definitions ->", output_file)
    print("Missing words ->", missing_file)

if __name__ == "__main__":
    main()