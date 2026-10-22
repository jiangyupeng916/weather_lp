# 反向买入并合并退出

`EXIT_MODE=hedge_merge` 根据已成交的份额买入同一市场的相反 outcome，再把确认的等量 YES、NO 合并成抵押资产。触发点由 `HEDGE_TRIGGER` 选择：默认 `confirmed` 等成交确认；`matched` 提前到撮合事件。原模式 `EXIT_MODE=sell` 仍为默认值。

本次仅增加退出方式，不更改奖励市场筛选、步长筛选或 midpoint 区间参数。未修改真实 `.env`，未运行实盘订单。改动前 Git 基线为 `bd4bad2`。

### MATCHED 提前触发（可选）

```dotenv
EXIT_MODE=hedge_merge
HEDGE_TRIGGER=matched
```

设置后，自己的原始 BUY 撮合份额单独记为暂记份额，允许触发反向 BUY；后续 `MINED` / `RETRYING` / `CONFIRMED` 更新不会重复增加买入数量。若断线后第一次 REST 补查已处于 MINED / RETRYING，也按已撮合、未确认处理。反向 BUY 自身的撮合仍作为已有订单的预留处理，不递归触发新 BUY。WS 若缺少可选的 `bucket_index`，为避免与 REST 同一成交重复计数，等待原有 REST 补查取得完整字段再处理，不猜 bucket 为 0。

原始成交最终 FAILED 时：先停止继续补买并撤掉已有反向 BUY 的未成交部分，核实所有在途成交终态；合并已确认的配对部分，再卖出实际剩余单边份额。残余 SELL 使用该持仓的实际加权买入成本：首次按 best_bid 与 `SELL_MIN_BID_GAP` 判断，后续按 best_ask 与 `SELL_POSITION_GAP` 判断；启用超时退出时，仍按 `MAX_HOLD_HOURS` 触发份额明确的 FOK SELL。卖出需要链上及 CLOB 可用余额，零头不向上凑量，低于原持仓阈值或市场最小量时保留等待。缺少卖出授权时，持久化 `sell_approve` 交易并等待确认，授权给相应 Exchange 而非合并用的 adapter/Router，再调用 SDK 原有的条件余额缓存刷新。残余 SELL 也持久化签名和订单 ID，响应丢失不创建另一张新签订单。

本次提前触发的代码基线为 `d5ef697`。仅调整触发点和必要的暂记/失败恢复记账；原来的买价反转、gap、撤单确认、REST 补查顺序、5 秒状态轮询、120 秒后续报价周期及 WSS 行情缓存均保留。收到 MATCHED 后尽快进入原下单流程，不承诺零延迟，也不越过价格保护。

## 1. 价格和份额

例如 YES 成交 20 份、价格 0.85，`SELL_MIN_BID_GAP=0.02`：

| 阶段 | 原卖出规则 | 新反向买入规则 |
| --- | --- | --- |
| 即时 | 原 token 买一 ≥ 成本 − gap 时，以买一卖出 | 以 `1 − 原 token 买一` 买入反向 token，且买价 ≤ `1 − 成本 + gap` |
| 定时兜底 | 原 token 卖一 ≥ 成本 − gap 时，以卖一挂卖单 | 以 `1 − 原 token 卖一` 挂反向 Post-Only 买单，使用 `SELL_POSITION_GAP` |
| 超时退出 | 原逻辑 FOK 卖出 | 忽略上述 gap，按相反 token 卖盘的实际深度设置主动 GTC 买价，数量仍为净缺口份额 |

如果原 YES 买一是 0.84，新单为 **NO 20 份、限价 0.16**，保护上限为 0.17。若原买一跌到 0.82，即时阶段跳过，随后按定时兜底规则处理；不会自动把买价抬到保护上限。

同一市场多笔成交按剩余持仓的加权成本计算阈值。两边已持有的份额先抵扣：YES 20 / NO 7 时仅需补 NO 13；YES 20 / NO 20 时直接合并。只在真实成交确认后增加持仓，不把 `matched` 下单响应当作已到账。

反向买价按该 token 当前 tick **向下对齐**，份额按 SDK 精度向下取两位小数，不向上凑数量。即时单是 GTC、可主动成交；定时兜底是 Post-Only GTC。超时单也使用 GTC 的份额语义，因为 SDK 的市价 BUY 参数 `amount` 是美元预算，不能直接套用卖出的 `shares` 参数。超时限价取覆盖所需份额的最深卖档；深度不足时取已有最深卖档，未成交部分保留，后续继续核对和追价。

`SELL_*_GAP` 仍是价格差保护，不是扣除手续费后的收益承诺。主动反向买入可能有 taker 费用，需要可用抵押资产支付买入金额和费用；合并前这部分资金暂时占用。

## 2. 启用

1. 用原 `sell` 模式处理遗留持仓和旧卖单，再停止实例。首次切换时不能带旧持仓：延迟的持仓快照与历史成交重播无法直接相加，否则可能重复买入。检测到旧仓或未结算交易时，新模式暂停，不会默默接管。
2. 更新代码及依赖。当前安装依赖为 `polymarket-client==0.9.0`，服务器已经是 `0.9.0` 时无需降级。适配器也保留已核验的 `0.5.0` 兼容路径；其他未经验证的版本会在新模式启动时明确拒绝运行。此前仅允许 `0.5.0` 的版本检查已修正，不要手动删除检查来绕过适配。
3. 在对应 `.env.botN` 添加：

```dotenv
EXIT_MODE=hedge_merge
HEDGE_TRIGGER=matched
HEDGE_POLL_INTERVAL=5
SELL_MIN_BID_GAP=0.02
SELL_POSITION_GAP=0.02
# MAX_HOLD_ENABLED / MAX_HOLD_HOURS 继续控制超时退出
```

账户还需有效的 `RELAYER_API_KEY` 和 `RELAYER_API_KEY_ADDRESS`（现有 `POLYMARKET_*` 别名仍可用）。原来只配 `PK + PROXY_ADDRESS` 的旧账户，需要补齐 Relayer 凭据才能采用此退出方式。

```bash
cd /root/weather_lp/guardian_v8
python -m pip install -r requirements.txt
python main.py bot1
```

不要同时用另一个机器人、另一台服务器或手动交易控制同一个钱包的退出仓位。SDK `0.5.0` 使用普通 CTF / negative-risk CTF 路由；SDK `0.9.0` 额外支持其市场上下文识别的普通 Polymarket Protocol V2 市场。新协议订单使用 SDK 的 Exchange V3 路由及 EIP-712 version 3，授权对象为 SDK 配置的 Router；旧 CTF 订单仍使用对应 Exchange 及 version 2、合并授权仍给 collateral adapter。调用地址、token 配对和合并编码均由相应 SDK 获取，不硬编码生产合约地址，不自行拼接跨市场或 Combo 组合。

两个触发模式的反向 BUY 都不用等原 token 的 CLOB 余额同步。**合并**只使用确认的真实链上份额；`MATCHED` 可能永久 FAILED，提前买入后可能需要走上述残余卖出恢复。WS 事件唤醒工作线程，REST 默认每 5 秒补查；网络、撤单确认和同市场未核实的成交仍可能带来延迟。

## 3. 订单与恢复保护

- 退出中的市场暂停两边奖励挂单。反向单提交前等待已在途的奖励挂单请求结束，撤掉同市场旧单，并对账撤单期间发生的成交，减少自成交和反复增加敞口。
- 反向 BUY 单独登记所有权，discover / audit 不会收编、追价或按重复奖励买单清理它们。反向买入的确认进入同一净持仓账本，不递归触发另一轮反向买入。
- 每一条自己的 maker/taker 成交按交易 ID、bucket、订单 ID、token 和方向持久化去重；多个 maker leg 分别处理。断线后查询最近成交，并持续跟踪尚未结算的交易 ID。
- 撤单后，必须核实订单终态及其成交结算，才释放未成交份额。部分成交只补净缺口，不再次购买原始总量。
- 反向下单前保存签名、预计算订单 ID 和预留份额。响应超时时查询这个订单；若重发，只重发同一签名，不生成新 salt。拒绝响应仅在首次提交结果明确时释放预留。
- 普通市场与 negative-risk 市场使用 SDK 的市场上下文、token 映射和合并路由；不会把同一事件下不同问题的 YES / NO 错配。合并前查询链上余额和 operator approval，必要时先授权。
- 授权查询和授权写入使用实际 token ledger：普通及负风险 CTF 均在 Conditional Tokens 上授权对应 collateral adapter，Protocol V2 在 PositionManager 上授权 Router。旧负风险 Adapter 可转发 `balanceOfBatch`，但不能用它调用 `isApprovedForAll` / `setApprovalForAll`；SDK 的余额读取目标不能直接当作授权目标。
- 合并数量是明确的六位小数基础单位，绝不使用 `amount="max"`。同一钱包同时只允许一笔授权/合并交易在途，等待 Relayer `STATE_CONFIRMED` 后才从账本扣除配对份额。
- 合并已返回交易 ID 时，重启继续查询同一笔交易；不会重复提交。关闭顺序为停止退出工作线程、等待奖励写请求结束、最后撤单，持久化记录保留用于下次恢复。

## 4. 状态文件和排查

状态文件按钱包隔离，位置为：

```text
guardian_v8/data/hedge_exit/<钱包地址小写>.json
```

使用原子替换、fsync 和进程锁。此目录已排除在 Git 外。文件含可重发的已签订单，**需要保留并妥善备份，不要上传或在聊天中粘贴完整内容**。换机器必须连同状态迁移；同钱包换实例名不应新建另一份状态。

本次自动将旧 version 1 状态升级为 version 2，保留原库存、订单签名、交易 ID 与去重记录，增加暂记成交和残余卖出状态。旧代码会拒绝读取 version 2，避免误忽略提前触发的份额；不要手工改版本号或拿旧状态覆盖恢复。暂记成交未结束时也不能切回 `HEDGE_TRIGGER=confirmed`。状态命令分开展示 `inventory`、`provisional` 和 `recover_sell`，不会把暂记份额显示成可合并的已到账持仓。

下面的命令只读本地状态，不读取私钥、不连接交易所，也不输出签名：

```bash
python hedge_status.py
python hedge_status.py --state data/hedge_exit/0x你的钱包地址.json
```

常用日志：

```bash
grep -E 'HEDGE|成交退出模式' data/bot1/guardian.log
grep -E 'HEDGE FILL|HEDGE MERGED' data/bot1/trades.log
```

文件日志默认 WARNING，正常买入过程主要看控制台；成交和合并记录写入 `trades.log`。

| 状态 / 日志 | 含义与处理 |
| --- | --- |
| `HEDGE RETRY ... missing_orderbook=1/3` | 接口返回 `No orderbook exists for the requested token id`（订单簿不存在）。同一市场、同一 token 在持续不可用期间最多输出 3 条；第 3 条提示后续同类日志静默。后台仍按原周期检查，订单簿恢复后清零计数并输出 `HEDGE BOOK RESTORED`，下单、成交对账和合并可继续；不会清除持仓或把任务标为完成。计数仅影响日志，进程重启后重新计数，其他错误仍照常报告。 |
| `HEDGE FUNDS` | 可用资金不足；市场暂停继续做市，等待资金满足条件。不会偷偷改成卖出或增加份额。 |
| `HEDGE SOURCE FAILED` | 提前触发的原始成交永久失败；进入撤 BUY / 确认结算 / 合并配对 / 卖残余恢复，期间不会继续买反向份额。 |
| `HEDGE RECOVERY WAIT` | 残余 SELL 的可用余额未同步、份额太小或不满足最小量；保留真实持仓等待，不凑量。 |
| `HEDGE DUST` | 净缺口小于订单簿最小份额，或不足 0.01 份的精度；不向上凑量。可配对部分可合并，剩余零头保留，需要后续对账处理。 |
| `HEDGE ORDER UNKNOWN` | 下单结果不明；保留预留，核实同一订单。不会因为一次查不到就新建另一笔。 |
| `Transaction submission unknown` | 授权/合并请求响应丢失，尚无可核对的交易 ID。该市场停止继续买入/合并；先核实 Relayer 历史、链上交易及余额，再修复记录。单纯重启不会解除，不能靠删除状态“重试”。 |
| `HEDGE PAUSED` / `HEDGE JOURNAL` | 数据读取或状态保存失败；暂停新奖励挂单。先修复网络、凭据或磁盘问题，再恢复。 |
| `Approval query failed` | 授权只读查询失败，日志附带 ledger/operator；不会把失败当作“未授权”并盲目发送交易。旧版在负风险 CTF 市场可能把授权查到 Neg Risk Adapter 并反复 `eth_call ... execution reverted`，请更新代码、保留状态文件后重启；不需要降级 SDK。 |
| `Hedge execution exceeds reserved shares` / `inventory conflict` | 实际执行与账本不一致；冻结该市场，不用反向循环买入掩盖差额。需核对外部交易。 |

小于最小下单量的残余、市场关闭、资金不足、接口故障都可能使自动退出停留在等待状态。该版本不会为了强行完成合并而多买另一边。

2026-10-21 负风险市场授权查询修复前的完整代码保存在 Git 提交 `1237400`。修复经过公开 Polygon RPC 的只读调用复现：同一负风险市场的 `balanceOfBatch` 在旧 Adapter 与 CTF 均可返回，但 `isApprovedForAll` 在旧 Adapter 会 revert、在 CTF 正常返回布尔值。修复同时覆盖授权读取与写入，未提交实盘授权或合并交易，恢复时不需要删除或修改 journal。

如需回退，在所有退出任务完成、没有在途交易及残余持仓后设置 `EXIT_MODE=sell` 并重启。若账本仍有未完成任务，程序会拒绝切回旧卖出模式，防止把已预留用于合并的份额卖掉。

## 5. 实现依据与验证

本次通过 Polymarket 文档 MCP 查询，并逐项对照 SDK `0.5.0`、`0.9.0` 的源码、模型、ABI 编解码和签名字段。`0.9.0` 增加协议中立的 `asset_id`，旧的 `token_id`、`market` 别名仍兼容；读取成交时同时支持别名，持久化的签名单按实际 SDK 模型序列化和恢复。兼容性修改前的完整代码已保存在 Git 提交 `c1e9829`。

- [Place Orders](https://docs.polymarket.com/trading/place-orders)：限价单按份额、tick 和数量精度；市价 BUY 按美元预算；费用在买入金额之外计算。
- [Real-Time Order Updates](https://docs.polymarket.com/trading/realtime-order-updates)：MATCHED / RETRYING / CONFIRMED / FAILED 生命周期，断线后需补查订单和成交。
- [Manage Positions](https://docs.polymarket.com/trading/positions/manage)：等量 outcome 合并、明确基础单位、授权及确认要求。
- [Fees](https://docs.polymarket.com/trading/fees)：主动成交费用说明。
- [SDK Changelog](https://docs.polymarket.com/changelog/sdks)：Python `0.9.0` 的 asset/condition 别名、新协议订单和合并支持。
- [Get Order Book](https://docs.polymarket.com/api-reference/market-data/get-order-book)：`No orderbook exists for the requested token id` 是订单簿接口的 404 错误；与请求超时、限流或其他 404 分开处理。

离线验证命令：

```bash
python -m compileall -q config.py guardian.py execution.py ws_router.py hedge_exit.py hedge_sdk.py hedge_status.py
python -m pytest tests -q
```

测试分别在 SDK `0.5.0` 与隔离的 `0.9.0` 环境运行，覆盖阈值反转、全部六种 tick、SDK 真实数量转换与 EIP-712 哈希、链上余额 ABI 解码、CTF / V2 授权对象及 SDK 真实合并调用编码、部分成交、重复/乱序事件、撤单竞态、响应丢失、重启、合并状态、资金不足、最小量、存储故障以及旧模式回归。`0.5.0` 环境跳过只适用于 `0.9.0` 的新协议测试。没有用真实资金验证，不要运行仓库里的 `test_sdk.py` 作为离线测试。
