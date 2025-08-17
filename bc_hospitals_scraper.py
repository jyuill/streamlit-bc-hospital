#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape BC hospitals from Wikipedia into a single CSV:
- Combines tables for all health authorities from the list page
- Follows each hospital link to pull "Beds" from the infobox (when present)
- Extracts coordinates (lat/lon) from table rows or hospital pages
"""

import argparse
import concurrent.futures as futures
import re
import sys
import time
from typing import Dict, List, Optional, Tuple
import urllib.parse as urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

WIKI_BASE = "https://en.wikipedia.org"
LIST_URL = "https://en.wikipedia.org/wiki/List_of_hospitals_in_British_Columbia"

HEADERS = {
    "User-Agent": "BC-Hospitals-Scraper/1.0 (+for research; contact: your-email@example.com)"
}

HEALTH_SECTIONS = [
    "Fraser Health",
    "Interior Health",
    "Island Health",
    "Northern Health",
    "Vancouver Coastal Health",
    "Provincial Health Services Authority",
    "Providence Health Care",
    "Other",
]

def _fetch(url: str, *, session: Optional[requests.Session] = None, timeout: int = 30) -> Optional[requests.Response]:
    s = session or requests.Session()
    try:
        resp = s.get(url, headers=HEADERS, timeout=timeout)
        if resp.status_code == 200:
            return resp
        else:
            sys.stderr.write(f"[warn] GET {url} -> {resp.status_code}\n")
            return None
    except requests.RequestException as e:
        sys.stderr.write(f"[warn] GET {url} -> {e}\n")
        return None

def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def extract_tables_under_section(soup: BeautifulSoup, section_title: str) -> List[Tag]:
    """Find the heading whose text matches section_title and return wikitable(s) until the next h2/h3)."""
    heading = None
    for hx in soup.select("#bodyContent h2, #bodyContent h3"):
        title = _clean_text(hx.get_text(" ")).replace("[edit]", "").strip()
        if title.lower().startswith(section_title.lower()):
            heading = hx
            break
    if not heading:
        return []

    tables = []
    for sib in heading.find_all_next():
        if sib.name in ("h2", "h3") and sib is not heading:
            break
        if sib.name == "table" and "wikitable" in (sib.get("class") or []):
            tables.append(sib)
    return tables

def parse_list_tables(resp_html: str) -> List[Dict]:
    soup = BeautifulSoup(resp_html, "lxml")
    all_rows: List[Dict] = []
    for section in HEALTH_SECTIONS:
        tables = extract_tables_under_section(soup, section)
        for table in tables:
            for tr in table.select("tr"):
                tds = tr.find_all(["td", "th"])
                if len(tds) < 2:
                    continue
                if tds[0].name == "th":
                    continue

                facility_cell = tds[0]
                facility_name = _clean_text(facility_cell.get_text(" "))
                if not facility_name:
                    continue

                a = facility_cell.find("a", href=True)
                hospital_href = a["href"] if a else None
                hospital_url = urlparse.urljoin(WIKI_BASE, hospital_href) if hospital_href else None

                city_cell = tds[1]
                city_txt = _clean_text(city_cell.get_text(" "))

                lat, lon = extract_coords_from_node(tr)

                all_rows.append({
                    "Health Authority": section,
                    "Facility Name": facility_name,
                    "Location City": city_txt,
                    "Latitude": lat,
                    "Longitude": lon,
                    "Hospital Page URL": hospital_url,
                })
    return all_rows

def extract_coords_from_node(node: Tag) -> Tuple[Optional[float], Optional[float]]:
    """Try to extract coordinates from a node via common Wikipedia microformats."""
    geo = node.select_one(".geo")
    if geo:
        txt = _clean_text(geo.get_text(" "))
        parts = re.split(r"[;, ]+", txt)
        parts = [p for p in parts if p]
        if len(parts) >= 2:
            try:
                return (float(parts[0]), float(parts[1]))
            except ValueError:
                pass

    lat_node = node.select_one(".latitude")
    lon_node = node.select_one(".longitude")
    if lat_node and lon_node:
        try:
            return (float(lat_node.get_text().strip()), float(lon_node.get_text().strip()))
        except ValueError:
            pass

    maplink = node.select_one("a.mw-kartographer-maplink")
    if maplink and maplink.has_attr("data-lat") and maplink.has_attr("data-lon"):
        try:
            return (float(maplink["data-lat"]), float(maplink["data-lon"]))
        except ValueError:
            pass

    return (None, None)

def parse_beds_and_coords_from_hospital(url: str, *, session: requests.Session) -> Tuple[Optional[str], Optional[int], Optional[float], Optional[float]]:
    r = _fetch(url, session=session)
    if not r:
        return (None, None, None, None)
    soup = BeautifulSoup(r.text, "lxml")

    lat, lon = extract_coords_from_node(soup)

    beds_raw = None
    beds_int = None
    infobox = soup.select_one("table.infobox")
    if infobox:
        for tr in infobox.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = _clean_text(th.get_text(" ")).lower()
            if label.startswith("beds"):
                beds_raw = _clean_text(td.get_text(" "))
                m = re.search(r"(\d{1,4})", beds_raw.replace(",", ""))
                if m:
                    try:
                        beds_int = int(m.group(1))
                    except ValueError:
                        beds_int = None
                break

    return (beds_raw, beds_int, lat, lon)

def enrich_with_hospital_pages(rows: List[Dict], max_workers: int = 10, delay: float = 0.0) -> List[Dict]:
    out: List[Dict] = []
    sess = requests.Session()

    def worker(rec: Dict) -> Dict:
        url = rec.get("Hospital Page URL")
        beds_raw = None
        beds_int = None
        lat2 = None
        lon2 = None
        if url:
            br, bi, la, lo = parse_beds_and_coords_from_hospital(url, session=sess)
            beds_raw, beds_int, lat2, lon2 = br, bi, la, lo
            if delay:
                time.sleep(delay)
        lat = rec.get("Latitude") or lat2
        lon = rec.get("Longitude") or lon2
        rec.update({
            "Beds Raw": beds_raw,
            "Beds": beds_int,
            "Beds Source URL": url if beds_raw else None,
            "Latitude": lat,
            "Longitude": lon,
        })
        return rec

    with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        for result in ex.map(worker, rows):
            out.append(result)

    return out

def main():
    ap = argparse.ArgumentParser(description="Scrape BC hospitals from Wikipedia and fetch bed counts + coordinates.")
    ap.add_argument("--out", default="bc_hospitals_from_wikipedia.csv", help="Output CSV path")
    ap.add_argument("--workers", type=int, default=10, help="Concurrent requests for hospital pages")
    ap.add_argument("--delay", type=float, default=0.0, help="Optional sleep (seconds) between requests in worker")
    args = ap.parse_args()

    list_resp = _fetch(LIST_URL)
    if not list_resp:
        sys.stderr.write("Failed to load list page. Aborting.\n")
        sys.exit(2)

    base_rows = parse_list_tables(list_resp.text)
    if not base_rows:
        sys.stderr.write("No tables parsed from list page. Aborting.\n")
        sys.exit(3)

    full_rows = enrich_with_hospital_pages(base_rows, max_workers=args.workers, delay=args.delay)

    df = pd.DataFrame(full_rows, columns=[
        "Health Authority",
        "Facility Name",
        "Location City",
        "Latitude",
        "Longitude",
        "Beds",
        "Beds Raw",
        "Beds Source URL",
        "Hospital Page URL",
    ])

    df.drop_duplicates(subset=["Facility Name","Location City"], inplace=True)
    df.sort_values(["Health Authority","Facility Name"], inplace=True)

    df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {args.out} with {len(df):,} rows.")

if __name__ == "__main__":
    main()
