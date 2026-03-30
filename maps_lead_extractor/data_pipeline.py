from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from .models import LeadRecord


class DataPipeline:
    COLUMNS = [
        "business_name",
        "category",
        "rating",
        "review_count",
        "address",
        "locality",
        "city",
        "phone",
        "website",
        "google_maps_url",
        "plus_code",
        "hours",
        "services",
        "query_source",
        "scraped_at",
    ]

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def to_dataframe(self, records: list[LeadRecord]) -> pd.DataFrame:
        rows = [record.to_dict() for record in records]
        if not rows:
            return pd.DataFrame(columns=self.COLUMNS)

        df = pd.DataFrame(rows)
        for col in self.COLUMNS:
            if col not in df.columns:
                df[col] = ""
        df = df[self.COLUMNS]
        df = df.fillna("")

        df["phone"] = df["phone"].map(self.clean_phone)
        df["website"] = df["website"].map(self.normalize_website)
        df["business_name"] = df["business_name"].astype(str).str.strip()
        df["query_source"] = df["query_source"].astype(str).str.strip()
        df["address"] = df["address"].astype(str).str.strip()

        name_key = df["business_name"].str.lower().str.replace(r"\s+", " ", regex=True)
        phone_key = df["phone"].astype(str).str.strip()
        place_key = df["google_maps_url"].map(self.extract_place_key)
        fallback_key = (
            name_key
            + "||"
            + df["address"].str.lower().str.replace(r"\s+", " ", regex=True)
            + "||"
            + place_key
        )
        df["_dedupe_key"] = (name_key + "||" + phone_key).where(phone_key != "", fallback_key)
        df = df.drop_duplicates(subset=["_dedupe_key"], keep="first").drop(columns=["_dedupe_key"])
        return df

    def export(self, df: pd.DataFrame) -> tuple[Path, Path]:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = self.output_dir / f"google_maps_leads_{stamp}.csv"
        json_path = self.output_dir / f"google_maps_leads_{stamp}.json"

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
        return csv_path, json_path

    @staticmethod
    def clean_phone(raw_phone: str) -> str:
        phone = str(raw_phone or "").strip()
        if not phone:
            return ""
        digits = re.sub(r"[^\d+]", "", phone)
        if digits.startswith("+"):
            normalized = "+" + re.sub(r"\D", "", digits)
        else:
            pure = re.sub(r"\D", "", digits)
            if pure.startswith("91") and len(pure) >= 12:
                normalized = "+" + pure
            elif len(pure) == 10:
                normalized = "+91" + pure
            elif pure:
                normalized = "+" + pure
            else:
                normalized = ""
        return normalized

    @staticmethod
    def normalize_website(raw_url: str) -> str:
        url = str(raw_url or "").strip()
        if not url:
            return ""
        lowered = url.lower()
        blocked = ("support.google.com", "google.com/contributionpolicy", "google.com/maps")
        if any(item in lowered for item in blocked):
            return ""
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        parsed = urlparse(url)
        if not parsed.netloc:
            return ""
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path or ''}"

    @staticmethod
    def extract_place_key(url: str) -> str:
        value = str(url or "")
        if not value:
            return ""
        match = re.search(r"!1s([^!]+)", value)
        if match:
            return match.group(1)
        # Fallback to path tail before /data
        match = re.search(r"/maps/place/([^/]+)/data", value)
        if match:
            return match.group(1)
        return value.strip().lower()

