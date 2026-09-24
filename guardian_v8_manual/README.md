# 手动做市模式

在 Polymarket 官网用对应钱包手动挂 **BUY 限价单**，本程序每 30 秒发现并接管。市场 WS 最优买价变化后，程序撤原单，冷却 120 秒，按 `MAKER_RANK` 对应档位和原订单的 share 数量重挂。用户在官网撤单即退出；再手动挂一个新的订单 ID 才会重新接管。确认 BUY 成交后停止该 token 的买单追价，并即时尝试卖出；每 120 秒兜底查持仓，按 `MAX_HOLD_HOURS` 执行超时 FOK 卖出。

## 启动

```bash
cd guardian_v8_manual
python -m pip install -r requirements.txt
cp .env.example .env.bot1
# 填写 .env.bot1 的钱包凭据
python main.py bot1
```

其他钱包各建 `.env.bot2` 等独立实例，以 `python main.py bot2` 启动。程序从当前目录自己的 `.env.<实例>` 读取配置，不读取自动做市版的凭据。显式实例缺失配置时直接退出。

每个 token 同时只接管一个 BUY 单；同 token 有多个 BUY 单时暂停接管，避免自动追价制造重复订单。手动订单的原始数量按 shares 保留。`CANCEL_ON_TRADE=true` 会在市场有成交时撤当前买单，同样冷却 120 秒后重挂。

用户频道 WS 离线时不创建新买单；若漏掉撤单事件，后续 `open_orders` 对账发现旧买单消失时停止跟踪。程序退出不主动调用 `cancel_all`，但停止 REST 心跳后，交易所会自动撤销该账户的活跃订单。

参考：[Polymarket 用户/市场 WebSocket 协议](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)、[订单与心跳规则](https://github.com/Polymarket/agent-skills/blob/main/order-patterns.md)。
