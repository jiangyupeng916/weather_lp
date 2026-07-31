#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""wss/main.py — Guardian + 市场频道 WS 完整 bot 入口。

从项目根目录运行：
    python -m wss.main

加载 wss/.env.wss，启动 GuardianWss（继承全部 guardian.py 功能 + WS 市场频道）。

与原 main.py 的差异：
- 加载 wss/.env.wss（而非 .env.bot1）
- 运行 GuardianWss（而非 Guardian）
- _poll_best_bids 降频至 30s，WS 驱动亚秒级撤单
"""

from __future__ import annotations

import io
import logging
import os
import sys
import warnings

# ── 实例配置：wss 独立实例 ─────────────────────────────────────────────────
INSTANCE = "wss"
# ────────────────────────────────────────────────────────────────────────────

# .env.wss 必须在 config 模块 import 之前加载
from dotenv import load_dotenv as _load_env
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)          # 项目根目录（config.py 所在）
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
_ENV_FILE = os.path.join(_HERE, ".env.wss")

if os.path.exists(_ENV_FILE):
    _load_env(_ENV_FILE)
    print(f"[wss/main] 已加载配置: {_ENV_FILE}")
else:
    # 回退到项目根目录 .env.{INSTANCE}（若存在）
    _ROOT_ENV = os.path.join(os.path.dirname(_HERE), f".env.{INSTANCE}")
    if os.path.exists(_ROOT_ENV):
        _load_env(_ROOT_ENV)
        print(f"[wss/main] 回退加载: {_ROOT_ENV}")
    else:
        _load_env()  # 最后回退到默认 .env
        print("[wss/main] 未找到 wss/.env.wss，使用默认 .env")

# Windows 终端 UTF-8 输出
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

warnings.filterwarnings("ignore")

# ── 日志配置 ────────────────────────────────────────────────────────────────
_DATA_DIR = os.path.join(os.path.dirname(_HERE), "data", INSTANCE)
os.makedirs(_DATA_DIR, exist_ok=True)

_LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=_LOG_FMT,
    handlers=[
        logging.FileHandler(os.path.join(_DATA_DIR, "guardian.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# 降低第三方库噪音
for _noisy in ("urllib3", "websocket", "requests", "httpx", "httpcore",
               "py_clob_client_v2", "charset_normalizer"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger("wss.main")

# ── 主逻辑 ───────────────────────────────────────────────────────────────────
from config import Config  # noqa: E402（dotenv 必须先加载）
from wss.guardian_wss import GuardianWss  # noqa: E402


def main() -> None:
    cfg = Config(instance_name=INSTANCE)
    cfg.validate()
    GuardianWss(cfg).run()


if __name__ == "__main__":
    main()
