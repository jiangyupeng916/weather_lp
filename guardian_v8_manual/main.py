"""手动做市入口：python main.py [bot1|bot2|...]。"""

# 在 SDK 导入前禁用 httpx HTTP/2（部分网络的 TLS h2 握手会失败）。
import httpx as _httpx
_original_client_init = _httpx.Client.__init__


def _http1_client_init(self, *args, **kwargs):
    kwargs["http2"] = False
    _original_client_init(self, *args, **kwargs)


_httpx.Client.__init__ = _http1_client_init

import logging
import os
from pathlib import Path
import re
import sys
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
explicit = len(sys.argv) > 1 or bool(os.environ.get("INSTANCE"))
INSTANCE = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("INSTANCE", "bot1")
if not re.fullmatch(r"[A-Za-z0-9_-]+", INSTANCE):
    sys.exit("实例名只能包含字母、数字、下划线和连字符")
env_file = ROOT / f".env.{INSTANCE}"
if env_file.is_file():
    load_dotenv(env_file)
elif explicit:
    sys.exit(f"指定实例 {INSTANCE!r} 缺少凭据文件 {env_file}")
elif (ROOT / ".env").is_file():
    load_dotenv(ROOT / ".env")

from config import Config


def setup_logging():
    log_dir = ROOT / "data" / INSTANCE
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    root.addHandler(console)
    file_handler = RotatingFileHandler(log_dir / "guardian.log", maxBytes=200 * 1024 * 1024,
                                       backupCount=5, encoding="utf-8")
    file_handler.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "WARNING").upper(), logging.WARNING))
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)
    for name in ("httpx", "httpcore", "websocket"):
        logging.getLogger(name).setLevel(logging.WARNING)


if __name__ == "__main__":
    setup_logging()
    cfg = Config(instance_name=INSTANCE)
    cfg.validate()
    from polymarket import RelayerApiKey, SecureClient
    from manual_guardian import ManualGuardian
    api_key = (RelayerApiKey(key=cfg.relayer_api_key, address=cfg.relayer_api_key_address)
               if cfg.relayer_api_key and cfg.relayer_api_key_address else None)
    client = SecureClient.create(private_key=cfg.pk, wallet=cfg.wallet or cfg.proxy or None,
                                 api_key=api_key)
    ManualGuardian(cfg, client=client).run()
