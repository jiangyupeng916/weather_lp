#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""周期任务调度器 — 单一 1s tick + 独立计时器（H1 第一步）

从 Guardian.run() 抽出的纯调度逻辑：每个周期任务有自己的 interval 和
last_run 时间戳，tick() 时逐个判断是否到期。不依赖任何网络或 Guardian
状态，因此可用假时钟（now_fn 注入）做单元测试。

设计要点：
- 到期即触发，触发前先更新 last_run —— 即便回调抛异常也不会热循环重试。
- 每个任务的异常被独立捕获并记录，一个任务失败不影响同一 tick 内其他任务。
- run_immediately=True 的任务（如 screener）首个 tick 立即触发。
"""

from __future__ import annotations

import logging
import time
from typing import Callable, List

logger = logging.getLogger("guardian.scheduler")


class PeriodicScheduler:
    """按各自间隔触发一组回调的轻量调度器（单线程，主循环内驱动）。"""

    def __init__(self, now_fn: Callable[[], float] = time.time):
        self._now = now_fn
        self._tasks: List[dict] = []

    def add(
        self,
        name: str,
        interval: float,
        callback: Callable[[], None],
        run_immediately: bool = False,
    ) -> None:
        """注册一个周期任务。

        run_immediately=True 时，last_run 置 0.0，使首个 tick 立即触发。
        否则从当前时刻起算，等待一个完整 interval 后才首次触发。
        """
        last = 0.0 if run_immediately else self._now()
        self._tasks.append(
            {"name": name, "interval": interval, "cb": callback, "last": last}
        )

    def tick(self) -> List[str]:
        """检查所有任务，触发已到期的。返回本次触发的任务名列表。

        触发前先更新 last_run：回调抛异常时不会在下一 tick 立刻重试（防热循环），
        与到期后正常触发的节奏一致。
        """
        now = self._now()
        fired: List[str] = []
        for t in self._tasks:
            if now - t["last"] >= t["interval"]:
                t["last"] = now
                try:
                    t["cb"]()
                except Exception as e:
                    logger.error("[SCHED] 任务 %s 异常: %s", t["name"], e, exc_info=True)
                fired.append(t["name"])
        return fired
