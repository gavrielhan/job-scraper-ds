from dataclasses import dataclass
from datetime import date


@dataclass
class JobPosting:
    source: str
    job_title: str
    company: str
    location: str
    url: str
    collected_at: date

    def to_row(self) -> dict:
        return {
            "source": self.source,
            "job_title": self.job_title,
            "company": self.company,
            "location": self.location,
            "url": self.url,
            "collected_at": self.collected_at.isoformat(),
        } 