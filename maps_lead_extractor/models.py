from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class LeadRecord:
    business_name: str = ""
    category: str = ""
    rating: str = ""
    review_count: str = ""
    address: str = ""
    locality: str = ""
    city: str = ""
    phone: str = ""
    website: str = ""
    google_maps_url: str = ""
    plus_code: str = ""
    hours: str = ""
    services: str = ""
    query_source: str = ""
    scraped_at: str = ""

    @classmethod
    def for_query(cls, query: str, listing_url: str) -> "LeadRecord":
        return cls(
            query_source=query.strip(),
            google_maps_url=listing_url.strip(),
            scraped_at=datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
        )

    def to_dict(self) -> dict[str, str]:
        return asdict(self)

