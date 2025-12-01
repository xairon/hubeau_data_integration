#!/usr/bin/env python3
"""
Script to find the oldest piezometry measurement date from Hub'Eau API.
This is used to dynamically generate partitions for historical backfill.
"""

import requests
import sys
from datetime import datetime

def find_oldest_date():
    """Query Hub'Eau API to find the oldest piezometry measurement."""

    # First, get a sample of stations
    print("Fetching sample stations...", file=sys.stderr)
    stations_url = "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations.csv"

    try:
        response = requests.get(stations_url, params={"size": 100}, timeout=30)
        response.raise_for_status()

        # Parse CSV to get station codes
        lines = response.text.strip().split('\n')
        if len(lines) < 2:
            print("ERROR: No stations found", file=sys.stderr)
            return None

        # Header is first line, data starts at line 2
        header = lines[0].split(';')
        code_bss_idx = header.index('code_bss') if 'code_bss' in header else 0

        station_codes = []
        for line in lines[1:11]:  # Take first 10 stations
            parts = line.split(';')
            if len(parts) > code_bss_idx:
                station_codes.append(parts[code_bss_idx])

        print(f"Found {len(station_codes)} stations to sample", file=sys.stderr)

        # For each station, try to get the oldest measurement
        oldest_date = None

        for code_bss in station_codes:
            print(f"Checking station {code_bss}...", file=sys.stderr)
            chroniques_url = "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques.csv"

            try:
                resp = requests.get(
                    chroniques_url,
                    params={
                        "code_bss": code_bss,
                        "size": 1,
                        "sort": "asc",
                        "fields": "date_mesure"
                    },
                    timeout=30
                )

                if resp.status_code == 200 and resp.text.strip():
                    lines = resp.text.strip().split('\n')
                    if len(lines) >= 2:
                        # First line is header, second is data
                        date_str = lines[1].strip()
                        if date_str:
                            try:
                                date = datetime.strptime(date_str, "%Y-%m-%d")
                                print(f"  → Oldest: {date_str}", file=sys.stderr)

                                if oldest_date is None or date < oldest_date:
                                    oldest_date = date
                            except ValueError:
                                continue

            except Exception as e:
                print(f"  → Error: {e}", file=sys.stderr)
                continue

        return oldest_date

    except Exception as e:
        print(f"ERROR fetching stations: {e}", file=sys.stderr)
        return None

if __name__ == "__main__":
    oldest = find_oldest_date()

    if oldest:
        print(f"\n✓ Oldest measurement found: {oldest.strftime('%Y-%m-%d')}", file=sys.stderr)
        print(f"  → Oldest year: {oldest.year}", file=sys.stderr)
        print(oldest.year)  # Print just the year to stdout for parsing
    else:
        print("ERROR: Could not determine oldest date", file=sys.stderr)
        # Fallback to 1960
        print(1960)
