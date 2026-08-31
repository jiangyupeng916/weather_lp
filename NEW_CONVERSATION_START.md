# 新对话开场白

你好！我是这个 Polymarket Maker Bot 项目的开发者，上一个对话上下文已满，需要在新对话中继续开发。

## 项目概览

这是一个 **Polymarket CLOB 平台的纯 Maker 做市机器人**（Guardian V8），用 Python 3.12+ 开发，通过在订单簿买盘挂限价单提供流动性，赚取平台返利奖励。

**核心特性**：
- 自动筛选市场 + 挂限价买单（Maker）
- 买入成交后自动卖出平仓
- WebSocket 实时监控 best_bid 变化 + 市场成交
- 支持多账号并行运行（bot1/bot2/bot5 等）

## 必读文档（按优先级）

请按以下顺序阅读文档，以快速了解项目：

### 1. 运维手册（最优先）
**文件**：`guardian_v8/USAGE.md`  
**内容**：日常运维、启动停止、参数配置、多账号管理、故障排查  
**为什么先读**：包含最实用的操作命令和配置说明，能快速上手

### 2. 架构文档（核心设计）
**文件**：`guardian_v8/ARCHITECTURE.md`  
**内容**：模块设计、策略逻辑、WebSocket 实现、线程模型、主循环调度  
**关键章节**：
- 第六章「策略逻辑」：撤单触发条件、筛选器、卖出策略
- 第七章「WebSocket 实时监控」：WS 事件处理、成交撤单策略

### 3. README（项目说明）
**文件**：`guardian_v8/README.md`  
**内容**：项目背景、快速开始、目录结构、开发指南

### 4. Git 提交历史（最近改动）
**查看命令**：`git log --oneline -20`  
**关键 commit**（从新到旧）：
- `3bd2b1b` — MAKER_RANK=1 新增「有成交就撤单」策略（2026-08-29）
- `e3c9846` — 筛选器新增到期天数上限 SCREENER_MAX_DAYS_TO_EXPIRY（2026-08-29）
- `1194020` — 修复 MAKER_RANK=1 挂单被 audit 误判超价反复撤单（2026-08-29）
- `d2dc0ec` — 批量撤单分片：规避 cancel burst=120 限流（2026-08-28）

## 当前状态和待办

### 已完成（最近一轮对话）

1. **修复 MAKER_RANK=1 的 audit 超价误判**：
   - 问题：audit 用 `>=` 判断超价，导致 RANK=1（挂买一档）时挂单价 == best_bid 被误判超价，每轮 audit 都撤单
   - 修复：`>=` 改成 `>`（严格大于才是超价）
   - 文件：`guardian.py:1138`

2. **新增「有成交就撤单」策略（CANCEL_ON_TRADE）**：
   - 问题：RANK=1 挂死水市场时 best_bid 几乎不变，现有「bid 变化撤单」失效
   - 解决：WS 监听 `last_trade_price` 事件，市场有成交 → 撤单重挂刷新流动性
   - 限定：只对 `MAKER_RANK=1` + `CANCEL_ON_TRADE=true` 生效，RANK=2+ 不受影响
   - 文件：`market_ws.py`、`guardian.py`、`config.py`
   - 用途：bot5（死水市场 + RANK=1）启用，bot1/bot2（活跃市场 + RANK=2）不启用

3. **筛选器新增到期天数上限（SCREENER_MAX_DAYS_TO_EXPIRY）**：
   - 功能：与 MIN 组成 [min, max] 范围过滤（如 0~100 天到期的市场）
   - 默认 `inf`（不过滤上限，向后兼容）
   - 文件：`config.py`、`screener/markets.py`

4. **持仓超时强平时间可配（MAX_HOLD_HOURS）**：
   - 功能：持仓超过 N 小时未卖出 → FOK 市价全卖
   - 默认 4 小时，bot5 设了 24 小时
   - 文件：`config.py`

5. **撤单分片修复**：
   - 问题：717 条订单整批撤单超过 cancel burst=120 被拒
   - 修复：分片撤单（每片 100 条，间隔 10s）
   - 文件：`guardian.py:_batch_cancel`

### 当前 bot 配置

- **bot1/bot2**：MAKER_RANK=2（买二档），活跃市场（NFL/CFB/Midterms），CANCEL_ON_TRADE=false
- **bot5**：MAKER_RANK=1（买一档），死水市场（SCREENER_MAX_VOLUME_24H=1），CANCEL_ON_TRADE=true，到期 0~100 天

### 已知问题和待办

查看项目记忆文件：`C:\Users\蒋玉鹏\.claude\projects\D--cursor-guardian-guardian-v7\memory\MEMORY.md`

关键待办：
- onchain_balance（GET balance allowance = 200 req/10s）在持仓暴涨时的限流风险（当前持仓少，不用管）
- 考虑给筛选器结果加「N 次确认再移除」的稳定性机制（当前靠调整阈值避免边界波动）

## 开发约定

1. **每轮改动 git commit + push**
2. **编译 + 测试**：`python -m py_compile <文件>` + `pytest tests/`
3. **代码风格**：跟随现有代码（类型标注、注释中文、文档字符串）
4. **日志级别**：文件默认 WARNING（省性能），控制台 INFO，排查时临时 DEBUG
5. **env 文件不进 git**：`.env.*` 本地改后需手动同步到服务器

## 服务器信息

- **路径**：`/root/weather_lp/guardian_v8`
- **管理脚本**：`up.sh`（启动全部）、`down.sh`（停止全部）、`up_bot1.sh` 等单 bot 脚本
- **Screen 会话**：`bot1_guardian`、`bot2_guardian`、`bot5_guardian`
- **日志**：`data/bot1/guardian.log` 等

## 如果你需要修改代码

**典型流程**：
1. 阅读 ARCHITECTURE.md 理解设计
2. 读相关代码文件（用 Read 工具）
3. 编辑修改（用 Edit 工具）
4. 编译测试（`py_compile` + `pytest`）
5. git commit + push
6. 提示我在服务器上 `git pull` + 重启对应 bot

**关键文件**：
- `guardian.py`（1715 行）：主逻辑、状态机、撤单、挂单、持仓
- `config.py`：所有配置参数
- `screener/`：市场筛选器（markets.py、clob.py、gamma.py）
- `wss/market_ws.py`：WebSocket 市场频道
- `execution.py`：执行层（下单、撤单、限流）

---

**开始新对话时，请告诉我：你想了解什么，或者需要修改/排查什么功能？我会先帮你定位到相关文档和代码。**
