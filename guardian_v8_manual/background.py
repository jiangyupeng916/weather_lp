#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""后台任务调度 — 重 REST「只算不写」+ 单飞 + 结果队列（H1 第二小步）

把阻塞的周期 REST（订单发现、订单簿查询、持仓查询）从主线程剥离：
- 后台线程只做纯查询/计算，产出一个「结果对象」丢进队列。
- 主线程每 1s tick 从队列取出结果，应用到 _markets（唯一写入方）。
- 后台线程绝不直接读写 _markets —— 保住「主线程独占 _markets 无锁」这一核心不变量。

单飞（single-flight）：同名任务同一时刻最多一个在跑。到期时若上一个还没结束，
submit 返回 False 跳过本次（避免慢请求堆叠线程）。in_flight 标记在
「结果入队之后」才清除 —— 宁可多跳一个周期，也不让同名任务并发。
"""

from __future__ import annotations

import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, List, Tuple

logger = logging.getLogger("guardian.background")

# 结果三元组：(任务名, 是否成功, 成功时为返回值 / 失败时为异常对象)
Result = Tuple[str, bool, Any]


class BackgroundDispatcher:
    """后台跑纯计算任务，结果回主线程应用。线程安全。"""

    def __init__(self, max_workers: int = 6):
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="bg"
        )
        self._results: "queue.Queue[Result]" = queue.Queue()
        self._inflight: set[str] = set()
        self._lock = threading.Lock()

    def submit(self, name: str, fn: Callable[..., Any], *args, **kwargs) -> bool:
        """单飞提交：同名任务未结束时返回 False（跳过本次），否则入池并返回 True。"""
        with self._lock:
            if name in self._inflight:
                return False
            self._inflight.add(name)
        self._executor.submit(self._run, name, fn, args, kwargs)
        return True

    def _run(self, name: str, fn: Callable[..., Any], args: tuple, kwargs: dict):
        try:
            result = fn(*args, **kwargs)
            self._results.put((name, True, result))
        except Exception as e:  # noqa: BLE001 — 后台任务异常统一记录，不影响主线程
            logger.error("[BG] 任务 %s 异常: %s", name, e, exc_info=True)
            self._results.put((name, False, e))
        finally:
            # 入队之后再清除 in_flight：保证「结果已可取」与「可再次提交」同序，
            # 且宁可让下一次 submit 多跳一个周期，也不让同名任务并发。
            with self._lock:
                self._inflight.discard(name)

    def drain(self) -> List[Result]:
        """取出当前所有已完成结果（非阻塞）。主线程每 tick 调用一次。"""
        out: List[Result] = []
        while True:
            try:
                out.append(self._results.get_nowait())
            except queue.Empty:
                break
        return out

    def in_flight(self, name: str) -> bool:
        with self._lock:
            return name in self._inflight

    def shutdown(self, wait: bool = False):
        self._executor.shutdown(wait=wait)
