import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import yaml
from dotenv import load_dotenv


load_dotenv()


@dataclass
class AppConfig:
    data_dir: str = os.path.abspath(os.path.join(os.getcwd(), "data"))
    csv_path: str = os.path.abspath(os.path.join(os.getcwd(), "data", "jobs.csv"))
    sources_config_path: str = os.path.abspath(os.path.join(os.getcwd(), "config", "sources.yaml"))
    serpapi_api_key: Optional[str] = os.getenv("SERPAPI_API_KEY")


def ensure_dirs(cfg: AppConfig) -> None:
    os.makedirs(os.path.dirname(cfg.csv_path), exist_ok=True)
    os.makedirs(os.path.dirname(cfg.sources_config_path), exist_ok=True)


def load_sources_config(cfg: AppConfig) -> Dict[str, Any]:
    if not os.path.exists(cfg.sources_config_path):
        return {}
    with open(cfg.sources_config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {} 