#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PeriodicScheduler 单元测试 — 纯调度逻辑，用假时钟驱动，不碰网络。

运行：
    cd guardian_v8
    python -m pytest tests/test_scheduler.py -v
"""

from __future__ import annotations

import sys
import os

# 让 `import scheduler` 找到 guardian_v8/scheduler.py（父目录）
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from scheduler import PeriodicScheduler


class FakeClock:
    """可手动推进的假时钟，替代 time.time。"""

    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


# ── run_immediately ────────────────────────────────────────────────────────

def test_run_immediately_fires_on_first_tick():
    clock = FakeClock()
    sched = PeriodicScheduler(now_fn=clock)
    calls = []
    sched.add("screener", 30.0, lambda: calls.append("s"), run_immediately=True)

    fired = sched.tick()

    assert fired == ["screener"]
    assert calls == ["s"]


def test_non_immediate_waits_full_interval():
    clock = FakeClock()
    sched = PeriodicScheduler(now_fn=clock)
    calls = []
    sched.add("discover", 30.0, lambda: calls.append("d"))

    # t=1000：刚注册，不该触发
    assert sched.tick() == []
    assert calls == []

    # t=1029：还差 1s
    clock.advance(29)
    assert sched.tick() == []

    # t=1030：到期
    clock.advance(1)
    assert sched.tick() == ["discover"]
    assert calls == ["d"]


# ── 间隔重复 ────────────────────────────────────────────────────────────────

def test_interval_repeats():
    clock = FakeClock()
    sched = PeriodicScheduler(now_fn=clock)
    calls = []
    sched.add("poll", 3.0, lambda: calls.append(clock.t))

    # 模拟每 1s 一个 tick，跑 10s
    fired_count = 0
    for _ in range(10):
        clock.advance(1)
        if sched.tick():
            fired_count += 1

    # 注册于 t=1000，间隔 3s → 在 1003/1006/1009 触发 3 次
    assert fired_count == 3
    assert calls == [1003, 1006, 1009]


# ── 多任务独立 ──────────────────────────────────────────────────────────────

def test_multiple_tasks_independent():
    clock = FakeClock()
    sched = PeriodicScheduler(now_fn=clock)
    log = []
    sched.add("fast", 3.0, lambda: log.append("fast"))
    sched.add("slow", 120.0, lambda: log.append("slow"))

    # 推进 3s：只有 fast 到期
    clock.advance(3)
    assert set(sched.tick()) == {"fast"}

    # 再推进到 120s 边界：两者都到期同一 tick
    clock.advance(117)  # 总 120s
    fired = sched.tick()
    assert set(fired) == {"fast", "slow"}


# ── 异常隔离 ────────────────────────────────────────────────────────────────

def test_one_task_exception_does_not_block_others():
    clock = FakeClock()
    sched = PeriodicScheduler(now_fn=clock)
    log = []

    def boom():
        raise RuntimeError("任务炸了")

    sched.add("boom", 1.0, boom, run_immediately=True)
    sched.add("ok", 1.0, lambda: log.append("ok"), run_immediately=True)

    fired = sched.tick()

    # boom 抛异常被吞掉，ok 照常执行；两者都记为 fired
    assert set(fired) == {"boom", "ok"}
    assert log == ["ok"]


def test_exception_does_not_hot_loop():
    """抛异常的任务不会在下一 tick 立刻重试（last_run 已在触发前更新）。"""
    clock = FakeClock()
    sched = PeriodicScheduler(now_fn=clock)
    fire_times = []

    def boom():
        fire_times.append(clock.t)
        raise RuntimeError("持续失败")

    sched.add("boom", 5.0, boom, run_immediately=True)

    # 同一时刻连续 tick 两次：只应触发一次
    sched.tick()
    sched.tick()
    assert len(fire_times) == 1

    # 推进不足 interval：仍不触发
    clock.advance(4)
    sched.tick()
    assert len(fire_times) == 1

    # 推进到 interval：再次触发（按正常节奏，非热循环）
    clock.advance(1)
    sched.tick()
    assert len(fire_times) == 2
