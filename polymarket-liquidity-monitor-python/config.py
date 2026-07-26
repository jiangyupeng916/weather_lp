from dataclasses import dataclass


@dataclass
class Config:
    MIN_DAILY_REWARDS: float = 20.0
    MIN_DAYS_TO_EXPIRY: int = 0
    MIN_MIDPOINT: float = 0.15
    MAX_MIDPOINT: float = 0.85
    MIN_SIZE_LOWER: float = 0.0
    MIN_SIZE_UPPER: float = 60
    MIN_EXISTING_SIZE: float = 1500.0
    MIN_TOP1_BIDS: float = 50.0
    MIN_TOP2_BIDS: float = 600.0
    MIN_TOP3_BIDS: float = 1500.0
    SEARCH_KEYWORD: str = "temp"
    INTERVAL_MINUTES: int = 1
    CONCURRENCY_LIMIT: int = 5
    CLOB_API: str = "https://clob.polymarket.com"


CONFIG = Config()
