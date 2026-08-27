# 给 LP 扫描器添加「成交量 / 流动性上限」过滤（双接口方案）

> 可复用到任何基于 Polymarket CLOB `/sampling-markets` 拿奖励市场、但缺成交量/流动性数据的扫描器。全程只读、无需认证。

## 1. 目标

在已有的「奖励市场 → orderbook → 排名」流程里加一层**上限过滤**：排除成交量过高 / 流动性过高的市场（这类市场通常已饱和，LP 收益被稀释）。

- 成交量：gamma 的 `volume24hr`（近 24h）或 `volumeNum`（累计）
- 流动性：gamma 的 `liquidityNum`（当前总流动性）

## 2. 为什么用「双接口」

| 接口 | 有 rewards | 有 volume/liquidity | 覆盖市场数 |
|---|---|---|---|
| CLOB `/sampling-markets` | ✅ | ❌ | 全量（1.2 万+） |
| gamma `/markets` | 部分 | ✅ | 列表不全（~2100 未关闭） |

结论：**以 sampling-markets 为准拿奖励市场，再用 condition_id 去 gamma 补成交量/流动性**。gamma 只做补充查询，漏掉的市场按 0 处理，不影响主流程。

## 3. API 细节

### 3.1 gamma `/markets`

- 端点：`GET https://gamma-api.polymarket.com/markets`
- 关键参数：
  - `condition_ids`：`string[]`，**必须传重复参数**（见坑 1）
  - `limit`：分页大小，**默认 20，要显式传 100**（见坑 2）
- 响应是**裸 JSON 数组**，关键字段：
  - `conditionId`：condition id（关联键）
  - `volumeNum`：累计总成交量（number）
  - `volume24hr`：近 24h 成交量（number）
  - `liquidityNum`：当前总流动性（number）

### 3.2 速率限制（Cloudflare IP 级滑动窗口，超了是延迟/排队不是拒绝）

| 接口 | 限制 |
|---|---|
| gamma `/markets` | 300 req / 10s |
| CLOB `/books` | 500 req / 10s |
| CLOB `/sampling-markets` | 通用 9,000 req / 10s |

## 4. 三个实测坑（务必注意）

1. **`condition_ids` 要传 list（重复参数），不能逗号拼接**
   - `params={'condition_ids': [id1, id2]}` → requests 编码成 `condition_ids=id1&condition_ids=id2` ✅
   - `params={'condition_ids': 'id1,id2'}` → 返回空 `[]` ❌
2. **默认 `limit=20`**，不显式传 `limit=100` 会静默截断整批结果
3. **缺失的 condition_id 被静默跳过**，只返回能找到的市场（2 valid + 1 fake → 返回 2 条）——正好实现"漏市场=0"语义，无需额外处理

## 5. 字段映射（CLOB ↔ gamma）

| 含义 | CLOB sampling-markets | gamma /markets |
|---|---|---|
| condition id | `condition_id` | `conditionId` |
| 奖励日利率 | `rewards.rates[].rewards_daily_rate` | `clobRewards[].rewardsDailyRate` |
| tags | `tags`（string[]） | `tags`（object[]，取 `.label`，需 `include_tag=true`） |
| token id | `tokens[].token_id` | `clobTokenIds`（JSON 字符串数组） |
| 累计成交量 | — | `volumeNum` |
| 24h 成交量 | — | `volume24hr` |
| 总流动性 | — | `liquidityNum` |

## 6. 分步改动清单（6 个文件）

### 步骤 1 — config 加参数

```python
MAX_VOLUME: float = float("inf")      # 24h 成交量上限，inf = 不过滤
MAX_LIQUIDITY: float = float("inf")   # liquidityNum 上限，inf = 不过滤
GAMMA_CONCURRENCY: int = 10           # gamma 并发线程数
GAMMA_API: str = "https://gamma-api.polymarket.com"
```

> ⚠️ 上限过滤默认必须是 `inf`，不能用 `0`——`≤0` 会把所有市场滤掉。

### 步骤 2 — 数据类加字段

```python
@dataclass
class CandidateMarket:
    ...
    volume: float = 0.0        # volumeNum 累计成交量
    volume24hr: float = 0.0    # volume24hr
    liquidity: float = 0.0     # liquidityNum
```

### 步骤 3 — 新增 gamma 模块

```python
BATCH_SIZE = 100

def _fetch_gamma_batch(condition_ids, retries=5):
    url = f"{CONFIG.GAMMA_API}/markets"
    # condition_ids 必须传 list，requests 编码成重复参数；逗号拼接会返回空
    params = {"condition_ids": condition_ids, "limit": BATCH_SIZE}
    # 5 次重试 + 指数退避；遇到 429 单独退避
    # 解析出 {condition_id: {volume, volume24hr, liquidity}}

def fetch_volume_liquidity(condition_ids):
    # 切成 ≤100 一批，ThreadPoolExecutor 并发，返回 {condition_id: {...}}
```

### 步骤 4 — 主拉取流程里 enrich + 过滤（放在 orderbook 之前）

```python
if candidates:
    gamma_data = fetch_volume_liquidity([m.condition_id for m in candidates])
    for m in candidates:
        info = gamma_data.get(m.condition_id, {})
        m.volume = info.get("volume", 0.0)
        m.volume24hr = info.get("volume24hr", 0.0)
        m.liquidity = info.get("liquidity", 0.0)

    candidates = [
        m for m in candidates
        if m.volume24hr <= CONFIG.MAX_VOLUME
        and m.liquidity <= CONFIG.MAX_LIQUIDITY
    ]
```

### 步骤 5 — 透传到评分结果（ScoredMarket 构造处）

```python
volume=market.volume,
volume24hr=market.volume24hr,
liquidity=market.liquidity,
```

### 步骤 6 — CSV 输出加列

```
Volume24h, Liquidity, VolumeTotal
```

## 7. 验证方法

1. `python -m py_compile` 全量编译通过
2. 用「1 个真实 condition_id + 1 个 fake id」调 `fetch_volume_liquidity`，确认只返回真实那条（fake 被跳过）
3. 拿 100 个真实 id + `limit=100`，确认返回 100 条（验证 URL 长度无截断）
4. 构造一个 ScoredMarket 走一遍 CSV 写出，确认表头/行**列数对齐**

## 8. 默认行为

`MAX_VOLUME=inf`、`MAX_LIQUIDITY=inf` 时，任何有限成交量/流动性的市场都通过，**等同于没加过滤**，可先上线再按需调阈值（如 `MAX_LIQUIDITY = 5000.0` 只留流动性 ≤5000 美元的市场）。
