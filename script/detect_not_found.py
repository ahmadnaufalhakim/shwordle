import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime
import sys

# ---------------- CONFIG ----------------
MAX_WORKERS = 5        # adjust (5–15 is safe)
FLUSH_EVERY = 20        # batch disk writes
TIMEOUT = 10

NOT_FOUND_TEXT = "The word you've entered isn't in the dictionary"
BASE_URL = "https://www.merriam-webster.com/dictionary/"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# -------------- LOGGING -----------------
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


# -------------- LOAD WORDS ----------------
def load_words(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


# -------------- WORKER ----------------
session = requests.Session()
session.headers.update(HEADERS)

lock = threading.Lock()

def check_word(word, index, total):
    url = BASE_URL + word

    try:
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # FAST check instead of full text extraction
        not_found = soup.find(string=NOT_FOUND_TEXT) is not None

        if not_found:
            print(f"[{index}/{total}] NOT FOUND: {word}")
            return word, False
        else:
            print(f"[{index}/{total}] FOUND: {word}")
            return word, True

    except requests.RequestException as e:
        print(f"[{index}/{total}] ERROR: {word} -> {e}")
        return word, False


# -------------- MAIN PROCESS ----------------
def check_words(words, output_file):
    buffer = []

    with open(output_file, "a", encoding="utf-8") as out_f:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(check_word, word, i, len(words)): word
                for i, word in enumerate(words, 1)
            }

            for count, future in enumerate(as_completed(futures), 1):
                word, found = future.result()

                if not found:
                    buffer.append(word)

                # Batch write (fast!)
                if len(buffer) >= FLUSH_EVERY:
                    with lock:
                        out_f.write("\n".join(buffer) + "\n")
                        out_f.flush()
                    buffer.clear()

        # Write remaining
        if buffer:
            out_f.write("\n".join(buffer) + "\n")
            out_f.flush()


# -------------- ENTRY POINT ----------------
if __name__ == "__main__":
    input_file = "../words/medium.txt"
    output_file = "missing.txt"
    log_file = "run.1.log"

    setup_logging(log_file)

    words = load_words(input_file)
    print(f"Loaded {len(words)} words...\n")

    check_words(words, output_file)

    print(f"\nDone! Missing words saved to: {output_file}")