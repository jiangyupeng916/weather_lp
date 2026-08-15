from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class CandidateMarket:
    condition_id: str
    question: str
    end_date: Optional[datetime]   # 无 end_date 的长期/赛季型市场为 None
    days_to_expiry: float
    yes_token_id: str
    no_token_id: str
    total_daily_rewards: float
    min_size: float
    max_spread: float


@dataclass
class ScoredMarket(CandidateMarket):
    midpoint: float = 0.0
    yes_total_size: float = 0.0
    no_total_size: float = 0.0
    existing_total_size: float = 0.0
    reward_per_dollar: float = 0.0
    yes_top3_bids: float = 0.0
    no_top3_bids: float = 0.0
    yes_top2_bids: float = 0.0
    no_top2_bids: float = 0.0
    yes_top1_bids: float = 0.0
    no_top1_bids: float = 0.0


@dataclass
class AllocatedMarket(ScoredMarket):
    allocated_size: float = 0.0
    estimated_daily_rewards: float = 0.0
