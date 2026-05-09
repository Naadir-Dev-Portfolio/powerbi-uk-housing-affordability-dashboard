from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests


ROOT = Path(__file__).resolve().parents[2]
SOURCE_DATA_DIR = ROOT / "Source Data"
RAW_DIR = SOURCE_DATA_DIR / "raw"
METADATA_PATH = SOURCE_DATA_DIR / "download_manifest.json"


@dataclass(frozen=True)
class DatasetSpec:
    slug: str
    title: str
    dataset_page_url: str
    output_filename: str
    notes: str
    download_url: str | None = None


DATASETS = [
    DatasetSpec(
        slug="ons_workplace_affordability",
        title="House price to workplace-based earnings ratio",
        dataset_page_url=(
            "https://www.ons.gov.uk/peoplepopulationandcommunity/housing/"
            "datasets/ratioofhousepricetoworkplacebasedearningslowerquartileandmedian"
        ),
        output_filename="ons_house_price_to_workplace_earnings_ratio.xlsx",
        notes=(
            "Core England and Wales local-authority affordability source with "
            "median and lower-quartile house prices, earnings, and ratios."
        ),
    ),
    DatasetSpec(
        slug="ons_private_rental_affordability",
        title="Private rental affordability, England, Wales and Northern Ireland",
        dataset_page_url=(
            "https://www.ons.gov.uk/peoplepopulationandcommunity/housing/"
            "datasets/privaterentalaffordabilityengland"
        ),
        output_filename="ons_private_rental_affordability.xlsx",
        notes=(
            "Rental pressure source covering countries, English regions, and local "
            "authorities in England and Wales."
        ),
    ),
    DatasetSpec(
        slug="ons_uk_purchase_affordability",
        title="Housing purchase affordability, by UK country and English region",
        dataset_page_url=(
            "https://www.ons.gov.uk/peoplepopulationandcommunity/housing/"
            "datasets/housingpurchaseaffordabilityingreatbritain"
        ),
        output_filename="ons_uk_purchase_affordability.xlsx",
        notes=(
            "UK country and English region purchase-affordability context for the "
            "executive summary and national comparison views."
        ),
    ),
    DatasetSpec(
        slug="hmlr_uk_hpi_monthly_full",
        title="UK House Price Index full monthly file (February 2026 release)",
        dataset_page_url=(
            "https://www.gov.uk/government/statistical-data-sets/"
            "uk-house-price-index-data-downloads-february-2026"
        ),
        output_filename="uk_hpi_monthly_full_2026_02.csv",
        notes=(
            "Official monthly UK HPI source with average price, index, and monthly "
            "and annual change at UK, country, region, and local-authority level."
        ),
        download_url=(
            "https://publicdata.landregistry.gov.uk/market-trend-data/"
            "house-price-index-data/UK-HPI-full-file-2026-02.csv"
        ),
    ),
    DatasetSpec(
        slug="boe_bank_rate_history",
        title="Monthly average of Official Bank Rate",
        dataset_page_url="https://www.bankofengland.co.uk/boeapps/database/",
        output_filename="boe_monthly_average_bank_rate.csv",
        notes=(
            "Official Bank of England IADB CSV export for series IUMABEDR, the "
            "monthly average of Official Bank Rate. This is machine-readable and "
            "aligned to the monthly HPI grain."
        ),
        download_url=(
            "https://www.bankofengland.co.uk/boeapps/database/"
            "_iadb-fromshowcolumns.asp?csv.x=yes&Datefrom=01/Jan/1975"
            "&Dateto=now&SeriesCodes=IUMABEDR&UsingCodes=Y&CSVF=TN&VPD=Y&VFD=N"
        ),
    ),
    DatasetSpec(
        slug="ons_awe_total_pay_kab9",
        title="Average weekly earnings, whole economy total pay",
        dataset_page_url=(
            "https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/"
            "earningsandworkinghours/timeseries/kab9/lms"
        ),
        output_filename="ons_awe_total_pay_kab9_lms.json",
        notes=(
            "ONS Labour Market Statistics JSON time-series endpoint for KAB9: "
            "average weekly earnings, whole economy, seasonally adjusted total pay."
        ),
        download_url=(
            "https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/"
            "earningsandworkinghours/timeseries/kab9/lms/data"
        ),
    ),
    DatasetSpec(
        slug="ons_unemployment_rate_mgsx",
        title="Unemployment rate, aged 16 and over, seasonally adjusted",
        dataset_page_url=(
            "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/"
            "unemployment/timeseries/mgsx/lms"
        ),
        output_filename="ons_unemployment_rate_mgsx_lms.json",
        notes=(
            "ONS Labour Market Statistics JSON time-series endpoint for MGSX: "
            "UK unemployment rate, aged 16 and over, seasonally adjusted."
        ),
        download_url=(
            "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/"
            "unemployment/timeseries/mgsx/lms/data"
        ),
    ),
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.text


def extract_first_xlsx_url(html: str, base_url: str) -> str:
    match = re.search(r'href="([^"]+\.xlsx[^"]*)"', html, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError(f"Could not find an .xlsx link on {base_url}")
    return urljoin(base_url, match.group(1))


def download_file(session: requests.Session, url: str, destination: Path) -> None:
    with session.get(url, timeout=120, stream=True) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,*/*;q=0.8",
        }
    )

    manifest: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "datasets": [],
    }

    for dataset in DATASETS:
        if dataset.download_url:
            workbook_url = dataset.download_url
            print(f"Using direct file URL for: {dataset.title}")
        else:
            print(f"Resolving workbook link for: {dataset.title}")
            html = fetch_html(session, dataset.dataset_page_url)
            workbook_url = extract_first_xlsx_url(html, dataset.dataset_page_url)
        destination = RAW_DIR / dataset.output_filename

        print(f"Downloading {workbook_url}")
        download_file(session, workbook_url, destination)

        manifest["datasets"].append(
            {
                "slug": dataset.slug,
                "title": dataset.title,
                "dataset_page_url": dataset.dataset_page_url,
                "download_url": workbook_url,
                "output_path": str(destination.relative_to(ROOT)),
                "bytes": destination.stat().st_size,
                "sha256": sha256_file(destination),
                "notes": dataset.notes,
            }
        )

    METADATA_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote manifest: {METADATA_PATH}")


if __name__ == "__main__":
    main()
