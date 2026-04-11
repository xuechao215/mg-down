# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a data repository, not a software project. It contains a single CSV file:

- **肺癌-相关药品列表.csv** — FDA drug labeling data for lung cancer-related drugs (~557 records)

## Data Schema

CSV columns (key fields):
- `Trade Name`, `Generic/Proper Name(s)` — drug identification
- `Active Ingredient(s)`, `Active Moiety Name(s)` — chemical components
- `Dosage Form(s)`, `Route(s) of Administration` — delivery method
- `Marketing Category` — regulatory pathway (NDA, ANDA, BLA)
- `Application Number(s)` — FDA application IDs
- `Company` — manufacturer
- `Initial U.S. Approval` — first approval year
- `SPL Effective Date` — label effective date (YYYY/MM/DD)
- `FDALabel Link`, `DailyMed SPL Link`, `DailyMed PDF Link` — reference URLs
- `NDC(s)`, `SET ID` — identifiers

## Working with the Data

The CSV uses UTF-8 encoding and has a trailing comma issue (extra empty columns at the end of each row). When parsing programmatically, handle or ignore the trailing empty fields.
