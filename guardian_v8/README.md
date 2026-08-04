# Guardian V8

> Polymarket CLOB 平台的纯 Maker 自动化做市机器人，挂限价单赚取返利奖励

**版本**：V8.1  
**最后更新**：2026-08-04

---

## 项目简介

Guardian V8 是一个基于 [Polymarket CLOB](https://docs.polymarket.com) 平台的纯 Maker 做市机器人。通过挂限价单提供流动性赚取平台返利，买入成交后自动平仓保持资金循环。

### 核心目标

- **提供流动性**：在多个市场挂买单，赚取平台 maker 返利奖励
- **快速平仓**：买入成交后立即卖出（两级卖出策略），保持资金循环
- **风险控制**：纯 Maker 模式（Post-Only），不吃单不付 taker 费用

### 技术栈

- **语言**：Python 3.12+
- **SDK**：[polymarket-client](https://github.com/Polymarket/py-clob-client)（官方 CLOB SDK）
- **实时通信**：websocket-client（订单/成交推送 + bid 变化监控）
- **HTTP 客户端**：httpx

### 与 V7 的关键差异

| 特性 | V7（旧版） | V8（当前版） |
|------|-----------|-------------|
| CLOB 交互 | 手搓 REST + WebSocket | 官方 SDK（py-clob-client） |
| 卖出策略 | 纯 taker 立即卖出 | 两级卖出（即时 taker + 兜底 maker） |
| 凭据管理 | 明文传递私钥 | SDK 内置派生机制 |
| 实时监控 | 无 | WebSocket bid 变化秒级撤单 |

---

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/jiangyupeng916/weather_lp.git
cd weather_lp/guardian_v8
```

### 2. 配置环境

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate  # Linux/Mac
# Windows: venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

### 3. 配置凭据

需要 Polymarket 账户私钥。创建 `.env.bot1` 文件：

```bash
cp .env.example .env.bot1
# 编辑 .env.bot1，填入以下必需参数：
# PRIVATE_KEY=0x...        # 账户私钥
# POLY_ADDRESS=0x...       # 钱包地址
# POLY_PASSPHRASE=...      # API passphrase
# POLY_API_KEY=...         # API key
# POLY_API_SECRET=...      # API secret
```

**安全提示**：`.env.*` 文件已加入 `.gitignore`，不会被 git 跟踪。

### 4. 验证连通性

```bash
python test_sdk.py bot1
# 预期输出：测试完成：下单 ✓  查询 ✓  撤单 ✓
```

### 5. 启动

```bash
python main.py
```

启动成功后会看到：
```
==============================
Maker-only Guardian V8.0 启动
地址: 0x...
[HEARTBEAT] 已启动 interval=7.0s
[SCREENER] xxx markets in x.xs
==============================
```

---

## 核心特性

### ✅ 纯 Maker 模式

只挂单不吃单，所有买单都是 **Post-Only**，绝不以 Taker 成交，保证不付 taker 费用，赚取平台 maker 返利。

### ✅ 智能筛选器

内置筛选器按 `reward_per_dollar`（每美元收益）评分自动选择市场，优先选择：
- reward 高、流动性好的市场
- 排除流动性过低、价格极端的市场

### ✅ 两级卖出

**核心设计，刻意为之**：
- **即时卖出**（BUY 成交触发，~1-2s）：taker 跨价吃单，快速平仓落袋
- **兜底卖出**（每 120s 扫描）：maker 挂单耐心等待，既赚价差又不付费

双保险 + 自愈，详见 [ARCHITECTURE.md](./ARCHITECTURE.md) §6.2。

### ✅ 实时监控

WebSocket 推送秒级响应：
- **用户频道 WS**：订单/成交事件实时推送，触发即时卖出
- **市场频道 WS**：best_bid 变化实时推送，1-2s 内触发撤单重挂

### ✅ 自动纠偏

audit 定期检测（每 120s）：
- 超价订单自动撤销
- 丢失订单自动恢复
- 重复订单自动清理
- 孤儿订单自动清理

### ✅ 心跳保活

7s 心跳保持订单存活，防止交易所自动取消订单。

---

## 文档导航

- **[ARCHITECTURE.md](./ARCHITECTURE.md)** - 模块设计、线程模型、状态机、调度逻辑、策略详解
- **[USAGE.md](./USAGE.md)** - 服务器部署、日常运维、故障排查

---

## 项目状态

- **仓库**：https://github.com/jiangyupeng916/weather_lp
- **服务器**：217.60.38.228（Ubuntu 2vCPU/4GB，screen 托管）
- **最新版本**：V8.1（main 分支）
- **运行模式**：`Guardian`（REST + WebSocket 混合，实盘在跑）

---

## 许可

本项目仅供学习和研究使用。

## 免责声明

本软件按"原样"提供，不提供任何明示或暗示的保证。使用本软件进行交易的风险由使用者自行承担。作者不对因使用本软件而导致的任何直接或间接损失负责。

---

**Guardian V8** - 让流动性提供更智能
