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
MAX_WORKERS = 2   # tweak (8–15 is usually safe)
FLUSH_EVERY = 40
TIMEOUT = 10

MAX_RETRIES = 5
BACKOFF_FACTOR = 1.5

BASE_URL = "https://www.merriam-webster.com/dictionary/{}"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
]

# Optional proxies (leave empty if not using)
PROXIES = [
    None,
    # {"http": "http://IP:PORT", "https": "http://IP:PORT"},
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
    """Clean a chunk of nodes into a proper definition, inject [LNK] for links and <i> for italics."""

    soup = BeautifulSoup("".join(str(n) for n in nodes), "html.parser")

    # 🔹 Replace <em class="mw_t_it"> with <i>
    for em in soup.find_all("em", class_="mw_t_it"):
        em.string = f"<i>{em.get_text(strip=True)}</i>"

    # 🔹 Inject [LNK] around all <a> links
    for a in soup.find_all("a"):
        text = a.get_text(strip=True).upper()
        a.string = f"[LNK]{text}[/LNK]"

    # 🔹 Normal text extraction
    text = soup.get_text(" ", strip=True)

    # Fix spacing issues
    text = text.replace(" ( ", " (").replace(" )", ")")

    # 🔥 make it TSV-safe
    text = text.replace("\t", " ").replace("\n", " ")
    text = text.strip(" :")

    # 🚫 skip junk
    if not text or text in [",", ";"]:
        return None

    return text

def extract_dt_parts(tag):
    """Split dtText into clean definition parts using real DOM structure."""

    parts = []
    current = []

    for child in tag.children:

        # 🔹 Separator → flush current buffer
        if getattr(child, "name", None) == "strong":
            if current:
                text = clean_chunk(current)
                if text:
                    parts.append(text)
                current = []
            continue

        current.append(child)

    # 🔹 flush last chunk
    if current:
        text = clean_chunk(current)
        if text:
            parts.append(text)

    return parts

def extract_definitions(html):
    soup = BeautifulSoup(html, "html.parser")

    # Optional extra cleanup
    for bad_id in ["kidsdictionary", "geographicalDictionary", "medicalDictionary", "legalDictionary"]:
        section = soup.find(id=bad_id)
        if section:
            section.decompose()

    defs = []

    # 1️⃣ dtText → [DEF]
    for tag in soup.find_all("span", class_="dtText"):
        parts = extract_dt_parts(tag)
        defs.extend([f"[DEF]{p}[/DEF]" for p in parts])

    # 2️⃣ unText → [SYN], handle italics
    for tag in soup.find_all("span", class_="unText"):
        # Replace <em class="mw_t_it"> with <i>
        for em in tag.find_all("em", class_="mw_t_it"):
            em.string = f"<i>{em.get_text(strip=True)}</i>"

        # Inject [LNK] around any <a>
        for a in tag.find_all("a"):
            a.string = f"[LNK]{a.get_text(strip=True).upper()}[/LNK]"

        text = tag.get_text(" ", strip=True)
        if text:
            defs.append(f"[SYN]{text}[/SYN]")

    # 3️⃣ cxl-ref → [CXL], inject [LNK] for links
    for tag in soup.find_all("p", class_="cxl-ref"):
        parts = []

        # Explanatory text (span.cxl)
        for span in tag.find_all("span", class_="cxl"):
            parts.append(span.get_text(strip=True))

        # UCXT spans
        for span in tag.find_all("span", class_="ucxt"):
            text = span.get_text(strip=True).upper()
            parts.append(f"[LNK]{text}[/LNK]")

        # All <a> links
        for a in tag.find_all("a"):
            text = a.get_text(strip=True).upper()
            parts.append(f"[LNK]{text}[/LNK]")

        final_text = " ".join(parts)
        defs.append(f"[CXL]{final_text}[/CXL]")

    return defs

def process_word(word, index, total):
    url = BASE_URL.format(word.lower())

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Rotate user-agent each attempt
            session.headers.update({
                "User-Agent": random.choice(USER_AGENTS)
            })

            proxy = random.choice(PROXIES)

            res = session.get(url, timeout=TIMEOUT, proxies=proxy)

            # ✅ SUCCESS
            if res.status_code == 200:
                time.sleep(random.uniform(0.6, 1.5))
                defs = extract_definitions(res.text)

                if defs:
                    print(f"[{index}/{total}] OK: {word}")
                    return [word] + defs
                else:
                    print(f"[{index}/{total}] EMPTY: {word}")
                    return None

            # 🚫 BLOCKED / RATE LIMITED
            elif res.status_code in (403, 429):
                wait = BACKOFF_FACTOR ** attempt + random.uniform(0.5, 1.5)
                print(f"[{index}/{total}] BLOCKED ({res.status_code} {res.reason}) {word} | retry {attempt}/{MAX_RETRIES} in {wait:.2f}s")
                time.sleep(wait)

            # ⚠️ SERVER ERRORS (retryable)
            elif 500 <= res.status_code < 600:
                wait = BACKOFF_FACTOR ** attempt
                print(f"[{index}/{total}] SERVER ERROR {res.status_code} {res.reason} {word} | retry {attempt}/{MAX_RETRIES} in {wait:.2f}s")
                time.sleep(wait)

            # ❌ OTHER FAILURES (don’t retry much)
            else:
                print(f"[{index}/{total}] FAIL: {word} -> {res.status_code} {res.reason}")
                return None

        except requests.Timeout:
            wait = BACKOFF_FACTOR ** attempt
            print(f"[{index}/{total}] TIMEOUT: {word} | retry {attempt}/{MAX_RETRIES} in {wait:.2f}s")
            time.sleep(wait)

        except requests.ConnectionError as e:
            wait = BACKOFF_FACTOR ** attempt
            print(f"[{index}/{total}] CONNECTION ERROR: {word} -> {e} | retry {attempt}/{MAX_RETRIES}")
            time.sleep(wait)

        except Exception as e:
            print(f"[{index}/{total}] ERROR: {word} -> {e}")
            return None

        # 💤 polite delay (even between retries)
        time.sleep(random.uniform(0.6, 1.5))

    print(f"[{index}/{total}] GAVE UP: {word}")
    return None

# ---------------- MAIN ----------------
def main():
    log_file = "scrape_defs.log"
    setup_logging(log_file)

    input_file = "../words/5letter.txt"
    output_file = "defs.csv"

    with open(input_file) as f:
        words = [line.strip() for line in f if line.strip()]

    print(f"Loaded {len(words)} words...\n")

    buffer = []

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter="\t", quoting=csv.QUOTE_MINIMAL)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_word, word, i, len(words)): word
                for i, word in enumerate(words, 1)
            }

            for future in as_completed(futures):
                word = futures[future]
                result = future.result()
                if result:
                    buffer.append(result)
                else :
                    buffer.append(["FAAH", word])

                # batch write
                if len(buffer) >= FLUSH_EVERY:
                    with lock:
                        try:
                            writer.writerows(buffer)
                        except Exception as e:
                            print("WRITE ERROR:", e)
                        csvfile.flush()
                    buffer.clear()

        # flush remaining
        if buffer:
            try:
                writer.writerows(buffer)
            except Exception as e:
                print(f"WRITE ERROR: {word}, reason:", e)
            csvfile.flush()
            buffer.clear()

    print("\nScraping complete! Output saved to:", output_file)

if __name__ == "__main__":
    main()
