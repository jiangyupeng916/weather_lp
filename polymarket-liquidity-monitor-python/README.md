# Polymarket LP Rewards Screener (Python)

扫描 Polymarket 预测市场，识别最佳的流动性提供（LP）收益机会。所有符合条件市场按 **reward-per-dollar（每投入一美元收益率）** 降序排列，一目了然。

**纯监控工具** — 不下单、不交互钱包、不需要私钥。

## 工作原理

1. 从 CLOB `/sampling-markets` 拉取**当前正在分发奖励**的市场（仅数千个，而非几十万个）
2. 获取每个候选市场的 orderbook，计算中点价和竞争份额
3. 按「每投入一美元可获得多少日奖励」打分排名

## 环境要求

- Python >= 3.9
- pip

## 安装

```bash
cd polymarket-liquidity-monitor-python
pip install -r requirements.txt
```

## 使用

```bash
python screener.py
```

交互模式：

- **R** — 重新运行
- **Q** — 退出

## 配置

编辑 `config.py` 中的默认值：

```python
@dataclass
class Config:
    MIN_DAILY_REWARDS: float = 20.0        # 最低日奖励阈值（美元）
    MIN_DAYS_TO_EXPIRY: int = 0            # 排除 N 天内到期的市场
    MIN_MIDPOINT: float = 0.15             # 排除概率过低的市场
    MAX_MIDPOINT: float = 0.85             # 排除概率过高的市场
    MIN_SIZE_LOWER: float = 0.0            # minSize 范围下限
    MIN_SIZE_UPPER: float = float("inf")   # minSize 范围上限
    MIN_EXISTING_SIZE: float = 500.0       # 已有竞争份额下限
    SEARCH_KEYWORD: str = ""               # 按关键词过滤（空则不过滤）
    CONCURRENCY_LIMIT: int = 5             # 最大并发 API 请求数
    CLOB_API: str = "https://clob.polymarket.com"
```

| 参数 | 默认值 | 说明 |
|---|---|---|
| `MIN_DAILY_REWARDS` | 20.0 | 低于此值日奖励的市场排除 |
| `MIN_DAYS_TO_EXPIRY` | 0 | 排除 N 天内到期的市场，设为 0 则不过滤到期 |
| `MIN_MIDPOINT` | 0.15 | 排除中点价太低的市场（概率过低，挂单难以成交） |
| `MAX_MIDPOINT` | 0.85 | 排除中点价太高的市场（概率过高，挂单难以成交） |
| `MIN_SIZE_LOWER` | 0.0 | 市场最低挂单金额下限（低于此值的市场排除） |
| `MIN_SIZE_UPPER` | inf | 市场最低挂单金额上限（高于此值的市场排除） |
| `MIN_EXISTING_SIZE` | 500.0 | 竞争总份额下限。份额太少说明盘口太薄，挂单容易被吃掉 |
| `SEARCH_KEYWORD` | `""` | 按关键词搜索（不区分大小写），空字符串则显示全部 |
| `CONCURRENCY_LIMIT` | 5 | 并发获取 orderbook 的最大并行数 |
| `CLOB_API` | `https://clob.polymarket.com` | CLOB API 基础地址 |

## 过滤流程

```
全部市场 (8745+)
  └─ 只取当前有奖励配置的
  └─ 排除到期、关闭、不可下单的
  └─ MIN_DAILY_REWARDS / MIN_DAYS_TO_EXPIRY   → 过滤奖励 / 到期
  └─ MIN_SIZE_LOWER / MIN_SIZE_UPPER          → 过滤 minSize 范围
  └─ SEARCH_KEYWORD                           → 关键词过滤
       ↓
  候选市场
  └─ 获取 orderbook，计算中点价和竞争份额
  └─ MIN_MIDPOINT / MAX_MIDPOINT              → 过滤概率
  └─ MIN_EXISTING_SIZE                        → 过滤竞争太薄的市场
       ↓
  最终排名（按 reward_per_dollar 降序）
```

## 排名逻辑

每个市场按 **reward_per_dollar = total_daily_rewards / (existing_total_size + min_size)** 打分：

- **分子**：该市场每天的奖励总额（越高的越好）
- **分母**：已有的竞争份额 + 最低挂单门槛（越小越好）
- 得分越高 = 投入产出比越高

## 输出示例

```
══════════════════════════════════════════════════════════════════════════
  POLYMARKET LP SCREENER  —  2026-07-22 10:15:30
══════════════════════════════════════════════════════════════════════════
  Markets: 45    |  Total rewards/day: $234.00
  keyword: "temperature"
──────────────────────────────────────────────────────────────────────────
  #  Market                            minSz  Reward/day   R/$        Competition
──────────────────────────────────────────────────────────────────────────
   1  Will global temperature exceed..   $20      $80.00      0.1234   LOW    (600 sh)
   2  July 2026 avg temperature NYC...   $50      $50.00      0.0456   MED    (1200 sh)
   3  Record high temp Las Vegas JUL..   $20      $35.00      0.0340   LOW    (800 sh)
   4  Arctic sea temperature anomaly.    $20      $25.00      0.0200   HIGH   (3200 sh)
  ...
══════════════════════════════════════════════════════════════════════════
```

| 列名 | 说明 |
|---|---|
| # | 排名 |
| Market | 市场名称（过长会截断） |
| minSz | 该市场奖励要求的最低挂单金额 |
| Reward/day | 该市场每日发放的奖励总额 |
| R/$ | reward-per-dollar，投入产出效率得分 |
| Competition | 竞争程度 + 已有总份额：THIN(<500) → LOW(<1K) → MED(<5K) → HIGH(5K+) |

## 项目结构

```
polymarket-liquidity-monitor-python/
├── config.py         # 配置参数
├── market_types.py   # 数据类定义
├── markets.py        # 从 CLOB 拉取并过滤市场
├── clob.py           # 获取 orderbook 并评分
├── screener.py       # 主入口 + 输出格式化
├── requirements.txt  # Python 依赖
└── README.md
```

## 调用的 API

| API | 用途 |
|---|---|
| `GET /sampling-markets` | 获取当前发放奖励的市场列表（分页） |
| `GET /book?token_id=` | 获取单个 token 的 orderbook 数据 |

两个端点均为只读公开端点，无需认证。
