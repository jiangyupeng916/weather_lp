#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Guardian V7.0 — Maker-only 统一交易守护入口"""

from __future__ import annotations

import io
import os
import sys
import warnings

# ── 实例配置：修改此处切换账号/策略 ──────────────────────────────────────────
# 对应项目根目录下的 .env.<INSTANCE> 文件，例如 .env.bot1
# 所有数据/日志输出到 data/<INSTANCE>/ 目录，多实例互不干扰
INSTANCE = "bot2"
# ────────────────────────────────────────────────────────────────────────────

# .env 必须在 config 模块 import 之前加载，否则 Config 类定义时读到的是空环境变量
from dotenv import load_dotenv as _load_env
_env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f".env.{INSTANCE}")
if os.path.exists(_env_file):
    _load_env(_env_file)
else:
    _load_env()  # 回退到默认 .env

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

# 屏蔽第三方库的版本兼容警告等噪音
warnings.filterwarnings("ignore")

import logging
from config import Config
from guardian import Guardian

LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", INSTANCE)
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FMT,
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, "guardian.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# 降低第三方库日志噪音
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("websocket").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("py_clob_client_v2").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("charset_normalizer").setLevel(logging.WARNING)
logging.getLogger("requests.packages.urllib3").setLevel(logging.WARNING)


def main():
    cfg = Config(instance_name=INSTANCE)
    cfg.validate()
    Guardian(cfg).run()


if __name__ == "__main__":
    main()
