# WebSocket 实时监控交接文档

## 当前状态（2026-08-03）

### 已完成
1. ✅ **架构集成**：市场频道 WS 已融入 `Guardian` 主循环
2. ✅ **连接稳定性**：实测 98.3% 可用率，断线重连正常
3. ✅ **订阅机制**：动态订阅 64 markets，15s 同步
4. ✅ **断线策略 B3**：断线立即撤全部挂单，重连后自然重挂
5. ✅ **统计监控**：每 5min 输出可用率统计
6. ✅ **路由修复**：识别 Polymarket 无 type 字段的消息格式，改用结构推断

### 待验证（需服务器重启）
⚠️ **路由修复后未实盘验证**，需确认：
1. 类型分布从全 `'unknown'` 变为 `{'array': N, 'unknown': M}`
2. 出现大量 `WS bid变化` 日志（之前几乎为 0）
3. 官网手动改 bid 后 1-2s 内触发撤单（不再等 30s）
4. WS:REST 撤单比例从 8:677 反转

---

## 路由修复详情

### 问题现象
- WS 连接稳定，订阅成功，但实时性完全失效
- 8 次 WS 撤单 vs 677 次 REST 撤单
- 所有撤单都在 30s poll 周期，无实时响应

### 根因
Polymarket WebSocket 消息**完全没有 `type` 或 `event_type` 字段**，旧代码依赖 `data.get("event_type")` 路由，导致所有 3100+ 条消息走默认分支（空操作）。

### 实际消息格式
```python
# Book 快照（数组）
[{
    "market": "0x887e...",
    "asset_id": "59148...",
    "timestamp": "1785745870754",
    "hash": "019a8f77...",
    "bids": [{"price": "0.01", "size": "2684.26"}, ...],
    "asks": [{"price": "0.92", "size": "500"}, ...]
}]

# Price change（对象）
{
    "market": "0xc120...",
    "price_changes": [
        {
            "asset_id": "60747...",
            "price": "0.32",
            "size": "27.93",
            "side": "BUY",
            "hash": "6a773e83...",
            "best_bid": "0.64",  # ← 目标字段（小写+下划线）
            "best_ask": "0.65"
        }
    ]
}
```

### 修复内容（commit 31e4d12 + e6cb3ef）

#### 1. `_route()` 方法重写
```python
def _route(self, data) -> None:
    """靠结构推断，不依赖 type 字段"""
    if isinstance(data, list):
        # 数组 → book 快照，递归展开
        for item in data:
            if isinstance(item, dict):
                self._route(item)
        return
    
    if not isinstance(data, dict):
        return
    
    # 对象 → 通过关键字段推断
    if "price_changes" in data:
        self._handle_price_change(data)
    elif "bids" in data or "asks" in data:
        self._handle_book(data)
```

#### 2. `_handle_price_change()` 数据路径修正
```python
# 旧（错误）
payload = data.get("payload", {})
changes = payload.get("priceChanges", [])
token_id = change.get("tokenId", "")
new_bid = _to_decimal(change.get("bestBid"))

# 新（正确）
changes = data.get("price_changes", [])  # 顶层，无 payload
asset_id = change.get("asset_id", "")     # 小写+下划线
new_bid = _to_decimal(change.get("best_bid"))
```

#### 3. 调试代码修复
```python
# 旧（数组会报错）
msg_type = data.get("type", "unknown")

# 新（类型安全）
msg_type = data.get("type", "unknown") if isinstance(data, dict) else "array"
```

---

## 验证步骤

### 服务器重启

> 入口是 `main.py`（不是 `guardian.py`）；服务器用 screen 托管（见 USAGE.md）。
> `INSTANCE="bot1"`（main.py:27），故日志在 `data/bot1/guardian.log`。

```bash
# 1. 进入 screen 优雅关闭旧进程
screen -r bot1
#（在 screen 内）Ctrl+C，等 "系统已停止"

# 2. 拉取最新代码
cd /root/weather_lp && git pull

# 3. 启动新进程
cd /root/weather_lp/guardian_v8 && source venv/bin/activate && python main.py
# 确认启动正常后 Ctrl+A + D 挂后台

# 4. 等待 2-3 分钟让消息累积后再验证
```

### 日志验证

> 注意：日志实际 tag 带 `[MarketWS DEBUG]` 前缀，路径是 `data/bot1/guardian.log`。

#### A. 检查类型分布（应有 array 类型）
```bash
grep "类型分布" data/bot1/guardian.log | tail -5
```
预期：`{'array': 500, 'unknown': 600}` 而非全 `'unknown'`

#### B. 检查 WS bid 变化（应频繁出现）
```bash
grep "WS bid变化" data/bot1/guardian.log | wc -l
```
预期：数十到数百条（之前接近 0）

#### C. 检查前 3 条消息样本（确认真实结构）
```bash
grep "消息样本" data/bot1/guardian.log | tail -3
```
预期：能看到 array/price_changes 结构

#### D. 实时性测试
1. 去 Polymarket 官网手动修改某个市场的 bid
2. 观察日志时间戳：
   ```bash
   tail -f data/bot1/guardian.log | grep --line-buffered -E "WS bid变化|WS断线"
   ```
3. 预期：bid 变化后 **1-2 秒内**触发撤单（不再是 30s）

#### E. 错误检查（应无报错）
```bash
grep -E "ERROR|'list' object has no attribute" data/bot1/guardian.log
```
预期：无 `'list' object has no attribute 'get'` 错误

---

## 关键文件位置

### 实现代码
- `guardian_v8/wss/market_ws.py:337-414`
  - `_route()` - 消息路由（结构推断）
  - `_handle_book()` - 订单簿快照处理
  - `_handle_price_change()` - 价格变动处理
  
- `guardian_v8/guardian.py`
  - `_enqueue_bid_change()` - WS 回调（投队列）
  - `_process_ws_bids()` - 主线程处理队列
  - `_apply_bid_change()` - 共享 helper（WS + REST 公用）
  - `_check_ws_connection()` - 断线检测
  - `_sync_ws_subscriptions()` - 订阅同步

### 配置项
```python
# config.py
WS_MARKET_ENABLED=true              # Kill switch（false 回退纯 REST）
WS_MARKET_URL=wss://...             # WS 服务器
MARKET_PING_INTERVAL=10             # 心跳间隔
ws_rest_reconcile_interval=30       # WS 启用时 REST 对账间隔
ws_sub_sync_interval=15             # 订阅同步间隔
```

---

## 下一步（路由验证通过后）

1. **性能优化**：如果消息量过大导致主线程处理不过来，考虑：
   - 批量处理：`_process_ws_bids()` 一次 drain 多条
   - 去重优化：同一 token 多条更新只取最新

2. **监控增强**：
   - 添加 WS 消息延迟统计（服务器时间戳 vs 收到时间）
   - 添加 WS vs REST 触发撤单的明细日志

3. **可靠性提升**：
   - 如果可用率 <95%，切换到 B2 策略（断线时加速 REST）
   - 添加 WS 消息丢失检测（序列号或时间戳断层）

4. **代码清理**：
   - 删除调试日志（消息样本、类型分布统计）
   - 提取配置常量到 `config.py`

---

## Troubleshooting

### 如果还是全 'unknown' 类型
1. 抓 3 条消息样本：`grep "消息样本" data/bot1/guardian.log | tail -3`
2. 检查是否有新的消息格式（可能 Polymarket 又改了）
3. 对照 `_route()` 的判断条件

### 如果还是没有 WS bid变化
1. 检查 `_handle_price_change()` 是否被调用：
   ```bash
   # 临时在 _handle_price_change 开头加日志
   logger.info("[DEBUG] _handle_price_change 被调用: asset_id=%s", change.get("asset_id"))
   ```
2. 检查 `best_bid` 解析是否正确（可能字段名又变了）
3. 检查 `_on_bid_changed_cb` 是否正确绑定

### 如果出现新的 AttributeError
说明消息格式又变了，用 `grep "消息样本" data/bot1/guardian.log` 看原始结构后分析。

---

## Git 历史

- `31e4d12` - 修复 WS 路由根因：Polymarket 消息无 type 字段，改用结构推断
- `e6cb3ef` - 修复调试代码 bug：data.get() 在数组上报错
- `cf3a305` - 修复 _handle_price_change 读取错误路径（已被 31e4d12 再次修正）
