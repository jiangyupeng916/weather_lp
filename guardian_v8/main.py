#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Guardian V8 入口

切换账号：修改 INSTANCE 变量后重启。
支持 bot1 / bot2 多实例，对应加载 .env.bot1 / .env.bot2。
"""

# ── HTTP/2 禁用补丁（必须在 polymarket SDK 导入之前执行）────────────────────
# polymarket-client 的 SyncTransport 默认 http2=True，TLS ClientHello 携带
# ALPN h2 扩展，部分网络环境（含 VPN TUN 模式）会在握手阶段强制 RST → SSLEOFError。
# requests 库（V7 使用）默认 HTTP/1.1 无此扩展，同网络下可正常工作。
# 此补丁令 httpx.Client 始终以 HTTP/1.1 初始化，行为与 requests 对齐。
import httpx as _httpx
_orig_httpx_client_init = _httpx.Client.__init__
def _httpx_client_no_h2(self, *args, **kwargs):
    kwargs["http2"] = False
    _orig_httpx_client_init(self, *args, **kwargs)
_httpx.Client.__init__ = _httpx_client_no_h2
# ─────────────────────────────────────────────────────────────────────────────

import logging
import os
import sys

# ── 实例切换 ──────────────────────────────────────────────────────────────────
# 优先级：命令行参数 > 环境变量 INSTANCE > 默认 bot1。
#   python main.py          → bot1（向后兼容，与旧行为一致）
#   python main.py bot2     → bot2
#   INSTANCE=bot2 python main.py → bot2
# 两个实例读不同 .env.<instance>、写不同 data/<instance>/，无共享状态可同时运行。
INSTANCE = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get("INSTANCE", "bot1"))

# ── 加载环境变量（必须在导入 Config 之前）─────────────────────────────────────
# 注意：Config 类字段中的 os.environ.get() 在类定义（import）时立即求值，
# 因此必须在 import config 之前就把 .env 写入 os.environ。
import os as _os
from dotenv import load_dotenv as _load_dotenv
_env_file = f".env.{INSTANCE}"
if _os.path.exists(_env_file):
    _load_dotenv(_env_file)
elif _os.path.exists(".env"):
    _load_dotenv(".env")

from config import load_config, Config


def _setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # 控制台：INFO+
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)
    # 第三方库：只显示 WARNING+（httpx 每次 HTTP 请求都打 INFO，太噪）
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websocket").setLevel(logging.WARNING)

    # 文件：DEBUG+
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", INSTANCE)
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(log_dir, "guardian.log"), encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)


if __name__ == "__main__":
    _setup_logging()
    cfg = Config(instance_name=INSTANCE)
    try:
        cfg.validate()
    except EnvironmentError as e:
        logging.critical("配置校验失败: %s", e)
        sys.exit(1)

    from guardian import Guardian
    Guardian(cfg).run()
