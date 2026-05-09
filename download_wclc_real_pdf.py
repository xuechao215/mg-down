#!/usr/bin/env python3
"""Download WCLC 2025 PDFs via the shared JTO conference downloader."""

from __future__ import annotations

import sys

from download_jto_real_pdf import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv, default_conference="wclc_2025"))
