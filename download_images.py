#!/usr/bin/env python3
"""
Download SPL images from DailyMed and update HTML files to use local paths.

1. Parse all HTML files to find image references
2. Download images from DailyMed: /dailymed/image.cfm?name=XXX&setid=YYY
3. Save images locally: downloads/fdalabel_images/<set_id>/<filename>
4. Update HTML img src to point to local files
"""

import os, re, json, time, logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import unquote

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

HTML_DIR = "downloads/fdalabel_html"
IMG_DIR = "downloads/fdalabel_images"
BASE_URL = "https://dailymed.nlm.nih.gov/dailymed/image.cfm"


def collect_images():
    """Collect all image references from HTML files."""
    image_map = {}  # set_id -> set of filenames

    for f in sorted(os.listdir(HTML_DIR)):
        if not f.endswith('.html'):
            continue
        parts = f.rsplit('__', 1)
        if len(parts) != 2:
            continue
        set_id = parts[1].replace('.html', '')

        path = os.path.join(HTML_DIR, f)
        with open(path, 'r', encoding='utf-8') as fh:
            content = fh.read()

        imgs = re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', content)
        local_imgs = [i for i in imgs if not i.startswith('http') and not i.startswith('/')]

        if local_imgs:
            if set_id not in image_map:
                image_map[set_id] = set()
            image_map[set_id].update(local_imgs)

    return image_map


def download_image(task):
    """Download a single image."""
    set_id, filename = task
    local_dir = os.path.join(IMG_DIR, set_id)
    local_path = os.path.join(local_dir, filename)

    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return ("ok", set_id, filename)

    os.makedirs(local_dir, exist_ok=True)

    url = f"{BASE_URL}?name={filename}&setid={set_id}"
    try:
        r = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        r.raise_for_status()

        content_type = r.headers.get('Content-Type', '')
        if 'image' in content_type or r.content[:4] in [b'\xff\xd8\xff\xe0', b'\xff\xd8\xff\xe1', b'\x89PNG']:
            with open(local_path, 'wb') as f:
                f.write(r.content)
            return ("ok", set_id, filename)
        else:
            return ("err", set_id, f"{filename}: not an image (got {content_type})")
    except Exception as e:
        return ("err", set_id, f"{filename}: {str(e)[:100]}")


def update_html_paths():
    """Update HTML files to point img src to local image files."""
    updated = 0

    for f in sorted(os.listdir(HTML_DIR)):
        if not f.endswith('.html'):
            continue
        parts = f.rsplit('__', 1)
        if len(parts) != 2:
            continue
        set_id = parts[1].replace('.html', '')

        path = os.path.join(HTML_DIR, f)
        with open(path, 'r', encoding='utf-8') as fh:
            content = fh.read()

        # Replace relative image paths with local paths
        # Pattern: <img src="filename.jpg"> -> <img src="../fdalabel_images/set_id/filename.jpg">
        def replace_img(m):
            prefix = m.group(1)
            src = m.group(2)
            suffix = m.group(3)
            if src.startswith('http') or src.startswith('/') or src.startswith('data:'):
                return m.group(0)
            new_src = f"../fdalabel_images/{set_id}/{src}"
            return f'{prefix}{new_src}{suffix}'

        new_content = re.sub(
            r'(<img[^>]+src=["\'])([^"\']+)(["\'])',
            replace_img,
            content
        )

        if new_content != content:
            with open(path, 'w', encoding='utf-8') as fh:
                fh.write(new_content)
            updated += 1

    log.info("Updated image paths in %d HTML files", updated)


def main():
    log.info("Step 1: Collecting image references from HTML files")
    image_map = collect_images()

    total = sum(len(v) for v in image_map.values())
    log.info("Found %d image references across %d SET IDs", total, len(image_map))

    # Build download tasks
    tasks = []
    for set_id, filenames in image_map.items():
        for fname in filenames:
            tasks.append((set_id, fname))

    log.info("Step 2: Downloading %d images (8 threads)", len(tasks))

    success = 0
    failed = 0
    start = time.time()

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(download_image, t): t for t in tasks}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result[0] == "ok":
                success += 1
            else:
                failed += 1
                if failed <= 20:
                    log.warning("Failed: %s/%s", result[1], result[2])

            if i % 500 == 0:
                elapsed = time.time() - start
                rate = i / elapsed if elapsed > 0 else 0
                log.info("Progress: %d/%d (%.1f/s), ok=%d, err=%d",
                         i, len(tasks), rate, success, failed)

    elapsed = time.time() - start
    log.info("Download done: ok=%d, failed=%d, total=%d (%.1fs)", success, failed, len(tasks), elapsed)

    log.info("Step 3: Updating HTML files to use local image paths")
    update_html_paths()

    log.info("All done!")


if __name__ == "__main__":
    main()
