#!/usr/bin/env python3
"""Download all ELCC 2026 PDFs from ScienceDirect with polite rate limiting."""

import json
import os
import subprocess
import sys
import time
import re

# Paths
BASE_DIR = "/Users/barry/Downloads/code/mg_down"
OUTPUT_DIR = os.path.join(BASE_DIR, "ELCC")
PII_MAP = os.path.join(BASE_DIR, "elcc_pii_map.json")
PROGRESS_FILE = os.path.join(BASE_DIR, "elcc_progress.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load PII mapping
with open(PII_MAP, 'r') as f:
    data = json.load(f)

# Load progress (already downloaded PIIs)
downloaded = set()
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        downloaded = set(json.load(f))

# Also check existing files in output dir
for fname in os.listdir(OUTPUT_DIR):
    if fname.endswith('.pdf'):
        m = re.match(r'PII(S\d+[A-Z]*)\.pdf', fname)
        if m:
            downloaded.add(m.group(1))

remaining = [item for item in data if item.get('pii') and item['pii'] not in downloaded]
print(f"Total: {len(data)}, Already downloaded: {len(downloaded)}, Remaining: {len(remaining)}")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
DELAY = 3  # seconds between requests - be very polite
MAX_CONSECUTIVE_FAILS = 5  # stop if too many consecutive failures

success = 0
fail = 0
consecutive_fails = 0

for i, item in enumerate(remaining):
    pii = item.get('pii')
    title = item.get('title', 'unknown')

    output_file = os.path.join(OUTPUT_DIR, f"PII{pii}.pdf")
    url = f"https://www.sciencedirect.com/science/article/pii/{pii}/pdfft?isDTMRedir=true&download=true"

    try:
        result = subprocess.run(
            [
                'curl', '-s', '-L',
                '-o', output_file,
                '-w', '%{http_code}',
                '-H', f'User-Agent: {UA}',
                '-H', 'Accept: application/pdf,*/*',
                '-H', 'Referer: https://www.sciencedirect.com/',
                '--max-time', '60',
                '--retry', '1',
                url
            ],
            capture_output=True, text=True, timeout=90
        )

        http_code = result.stdout.strip()

        # Verify it's actually a PDF
        is_valid = False
        if os.path.exists(output_file):
            with open(output_file, 'rb') as f:
                header = f.read(5)
            if header == b'%PDF-':
                is_valid = True
                file_size = os.path.getsize(output_file)
                consecutive_fails = 0
            else:
                os.remove(output_file)
                # Check if it's a Cloudflare block page
                with open(output_file, 'rb') as f:
                    pass  # already removed

        if is_valid and http_code == '200':
            success += 1
            downloaded.add(pii)
            print(f"[{i+1}/{len(remaining)}] OK: {pii} ({file_size//1024}KB)")
        else:
            fail += 1
            consecutive_fails += 1
            print(f"[{i+1}/{len(remaining)}] FAIL: {pii} HTTP={http_code}")

            # If too many consecutive fails, we're probably blocked - stop
            if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
                print(f"\n!!! {MAX_CONSECUTIVE_FAILS} consecutive failures - IP likely blocked. Stopping. !!!")
                break

    except Exception as e:
        fail += 1
        consecutive_fails += 1
        print(f"[{i+1}/{len(remaining)}] ERROR: {pii} - {e}")

        if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
            print(f"\n!!! {MAX_CONSECUTIVE_FAILS} consecutive failures - IP likely blocked. Stopping. !!!")
            break

    # Save progress every 10 downloads
    if (success + fail) % 10 == 0:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(list(downloaded), f)
        print(f"--- Progress saved: {len(downloaded)}/{len(data)} ---")

    # Rate limiting - be very polite
    time.sleep(DELAY)

# Final save
with open(PROGRESS_FILE, 'w') as f:
    json.dump(list(downloaded), f)

print(f"\nDone! Success: {success}, Failed: {fail}, Skipped: {len(data) - len(remaining)}")
