# Guardian V8 使用手册

> 适用版本：V8.0（polymarket-client SDK）  
> 服务器：217.60.38.228（Ubuntu，2vCPU / 4GB / 40GB）  
> 最后更新：2026-08-02

---

## 目录

1. [首次部署（仅需执行一次）](#1-首次部署仅需执行一次)
2. [启动 Bot](#2-启动-bot)
3. [挂起到后台](#3-挂起到后台)
4. [查看运行状态](#4-查看运行状态)
5. [查看日志](#5-查看日志)
6. [正常关闭](#6-正常关闭)
7. [重启 Bot](#7-重启-bot)
8. [强制关闭（紧急）](#8-强制关闭紧急)
9. [更新代码](#9-更新代码)
10. [SSH 断线后恢复](#10-ssh-断线后恢复)
11. [常见问题](#11-常见问题)
12. [命令速查表](#12-命令速查表)

---

## 1. 首次部署（仅需执行一次）

### 1.1 从本地传输配置文件

在**本地 Windows PowerShell** 执行：

```powershell
# 传输凭据配置文件
scp D:\cursor\guardian\guardian_v7\.env.bot1 root@217.60.38.228:/root/guardian_v8/.env.bot1
```

### 1.2 在服务器上补充 V8 参数

SSH 登录服务器后执行：

```bash
# 追加心跳容错次数参数
echo "HEARTBEAT_MAX_ERRORS=5" >> /root/guardian_v8/.env.bot1

# 设置文件权限（防止其他用户读取私钥）
chmod 600 /root/guardian_v8/.env.bot1

# 确认文件已就绪
ls -la /root/guardian_v8/.env.bot1
```

### 1.3 安装依赖

```bash
cd /root/guardian_v8

# 创建虚拟环境（如果尚未创建）
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 1.4 验证 SDK 连通性

```bash
python test_sdk.py bot1
```

输出最后一行应为：`测试完成：下单 ✓  查询 ✓  撤单 ✓`

---

## 2. 启动 Bot

每次启动都在 `screen` 会话中运行，确保 SSH 断线后 bot 继续运行。

```bash
# SSH 登录服务器
ssh root@217.60.38.228

# 新建 screen 会话（命名为 bot1）
screen -S bot1

# 进入项目目录并激活虚拟环境
cd /root/guardian_v8
source venv/bin/activate

# 启动
python main.py
```

**启动成功标志**（看到以下日志即正常）：

```
==============================
Maker-only Guardian V8.0 启动
地址: 0x...
[HEARTBEAT] 已启动 interval=8.0s
[SCREENER] xxx markets in x.xs
==============================
```

---

## 3. 挂起到后台

Bot 启动并确认正常后，将其挂入后台（bot 继续运行，可以安全关闭 SSH）：

```
Ctrl+A  然后  D
```

看到 `[detached from 12345.bot1]` 说明挂起成功。

---

## 4. 查看运行状态

```bash
screen -ls
```

**正常输出示例：**
```
There is a screen on:
    12345.bot1    (Detached)
1 Socket in /run/screen/S-root.
```

| 状态 | 含义 |
|------|------|
| `Detached` | ✅ Bot 在后台正常运行 |
| `Attached` | Bot 有人正在查看 |
| 无输出 | ❌ Bot 未运行 |

---

## 5. 查看日志

### 实时日志（推荐日常监控）

```bash
tail -f /root/guardian_v8/data/bot1/guardian.log
```

退出：按 `Ctrl+C`

### 最近 100 行

```bash
tail -n 100 /root/guardian_v8/data/bot1/guardian.log
```

### 只看错误和警告

```bash
grep -E "ERROR|WARNING|CRITICAL" /root/guardian_v8/data/bot1/guardian.log | tail -50
```

### 只看心跳状态

```bash
grep "HEARTBEAT" /root/guardian_v8/data/bot1/guardian.log | tail -20
```

### 只看成交记录

```bash
grep "成交\|TRADE\|FILL" /root/guardian_v8/data/bot1/guardian.log | tail -30
```

### 进入 screen 直接看（最完整，含终端输出）

```bash
screen -r bot1
```

查看完毕后挂回后台：`Ctrl+A` + `D`

---

## 6. 正常关闭

正常关闭会**先撤销所有挂单**，再停止程序。

```bash
# 进入 screen
screen -r bot1

# 发送停止信号
Ctrl+C
```

等待日志出现关闭确认：

```
开始优雅关闭...
[CANCEL ALL] xx 已取消
[HEARTBEAT] 已停止
系统已停止
```

然后退出 screen：

```bash
exit
```

---

## 7. 重启 Bot

```bash
# 进入 screen
screen -r bot1

# 停止（Ctrl+C，等待优雅关闭）
Ctrl+C

# 重新启动
python main.py

# 确认正常后挂入后台
Ctrl+A 然后 D
```

---

## 8. 强制关闭（紧急）

> ⚠️ **警告**：强制关闭不会自动撤单！订单将继续挂在交易所，直到心跳超时（约 10 秒）后被交易所自动取消。  
> 仅在 bot 无响应时使用。

```bash
# 强制终止 screen 会话
screen -S bot1 -X quit
```

或者找到进程手动杀死：

```bash
ps aux | grep "python main.py"
kill -9 <PID>
```

---

## 9. 更新代码

```bash
# 进入项目目录
cd /root/guardian_v8

# 先正常关闭 bot（见第6节）

# 拉取最新代码
git pull origin main

# 如有新依赖
source venv/bin/activate
pip install -r requirements.txt

# 重新启动
screen -r bot1
python main.py
```

---

## 10. SSH 断线后恢复

SSH 断线不影响 bot 运行（screen 保持后台）。重新连接后：

```bash
# 重新 SSH 登录
ssh root@217.60.38.228

# 确认 bot 还在运行
screen -ls

# 恢复查看
screen -r bot1
```

---

## 11. 常见问题

### Q: Bot 启动后立即退出，日志无内容？

检查配置文件是否存在：

```bash
ls -la /root/guardian_v8/.env.bot1
cat /root/guardian_v8/.env.bot1 | grep -v PK | grep -v SECRET | grep -v PASS
```

### Q: 大量日志显示"订单丢失纠偏"？

这是心跳失败后的正常恢复行为，不是 bug。流程：

1. 心跳连续失败 → 交易所自动取消所有订单
2. 120 秒后 Audit 检测到订单消失 → 触发"纠偏"重新下单
3. `HEARTBEAT_MAX_ERRORS=5` 可以提高容错次数，减少误报

检查心跳状态：

```bash
grep "HEARTBEAT" /root/guardian_v8/data/bot1/guardian.log | tail -20
```

### Q: screener 速度从 ~8s 变成 ~35s？

北京时间 18:00-22:00 是网络高峰期，VPN/代理拥塞正常。  
部署在海外服务器（无需 VPN）后此问题自动消失。

### Q: 看到 SSLEOFError？

main.py 里的 HTTP/2 禁用补丁应该已解决此问题。若仍出现，确认 main.py 顶部的补丁代码未被删除。

### Q: 想切换到 bot2 账号？

编辑 `main.py`，将 `INSTANCE = "bot1"` 改为 `INSTANCE = "bot2"`，  
并确保 `/root/guardian_v8/.env.bot2` 文件存在，然后重启。

---

## 12. 命令速查表

| 操作 | 命令 |
|------|------|
| **SSH 登录** | `ssh root@217.60.38.228` |
| **启动（新建会话）** | `screen -S bot1` → `cd /root/guardian_v8 && source venv/bin/activate && python main.py` |
| **挂起后台** | `Ctrl+A` + `D` |
| **查看状态** | `screen -ls` |
| **进入查看** | `screen -r bot1` |
| **退出查看（不停止）** | `Ctrl+A` + `D` |
| **实时日志** | `tail -f /root/guardian_v8/data/bot1/guardian.log` |
| **看错误日志** | `grep -E "ERROR\|CRITICAL" /root/guardian_v8/data/bot1/guardian.log \| tail -30` |
| **正常关闭** | `screen -r bot1` → `Ctrl+C` → 等关闭完成 → `exit` |
| **强制关闭** | `screen -S bot1 -X quit` |
| **更新代码** | 关闭 → `git pull` → `pip install -r requirements.txt` → 重启 |
