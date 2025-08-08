from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import date
from typing import List
from ..models import JobPosting


class ScraperBase(ABC):
    @abstractmethod
    def fetch(self, *, as_of: date) -> List[JobPosting]:
        raise NotImplementedError 