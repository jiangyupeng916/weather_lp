#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BackgroundDispatcher 单元测试 —— 纯逻辑，无网络无实盘。"""

import sys
import threading
import time
from pathlib import Path

# 让测试能 import 到 guardian_v8 根目录的模块
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from background import BackgroundDispatcher  # noqa: E402


def _wait_until(pred, timeout=2.0, interval=0.005):
    """轮询等待条件成立，超时抛断言。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    raise AssertionError("等待超时")


def test_submit_runs_and_returns_result():
    """任务跑完，结果按 (name, True, value) 入队，可 drain 取出。"""
    bg = BackgroundDispatcher(max_workers=2)
    try:
        assert bg.submit("job", lambda: 42) is True
        # in_flight 清除后结果必然已入队（_run 先 put 再 discard）
        _wait_until(lambda: not bg.in_flight("job"))
        results = bg.drain()
        assert results == [("job", True, 42)]
    finally:
        bg.shutdown(wait=True)


def test_single_flight_rejects_concurrent():
    """同名任务在跑时，第二次 submit 返回 False（跳过）。"""
    bg = BackgroundDispatcher(max_workers=4)
    release = threading.Event()
    started = threading.Event()

    def slow():
        started.set()
        release.wait(timeout=2.0)
        return "done"

    try:
        assert bg.submit("job", slow) is True
        assert started.wait(timeout=2.0)
        # 上一个还卡在 release.wait，同名 submit 必须被拒
        assert bg.submit("job", lambda: "second") is False
        assert bg.in_flight("job") is True
        release.set()
        _wait_until(lambda: not bg.in_flight("job"))
        results = bg.drain()
        assert results == [("job", True, "done")]
    finally:
        release.set()
        bg.shutdown(wait=True)


def test_resubmit_after_completion():
    """任务完成后，同名可再次提交。"""
    bg = BackgroundDispatcher(max_workers=2)
    try:
        assert bg.submit("job", lambda: 1) is True
        _wait_until(lambda: not bg.in_flight("job"))
        assert bg.submit("job", lambda: 2) is True
        _wait_until(lambda: not bg.in_flight("job"))
        results = bg.drain()
        # 两次结果都应取到（顺序即完成序）
        assert ("job", True, 1) in results
        assert ("job", True, 2) in results
    finally:
        bg.shutdown(wait=True)


def test_exception_captured_as_failed_result():
    """任务抛异常 → (name, False, exc) 入队，不崩线程。"""
    bg = BackgroundDispatcher(max_workers=2)

    def boom():
        raise ValueError("炸了")

    try:
        assert bg.submit("job", boom) is True
        _wait_until(lambda: not bg.in_flight("job"))
        results = bg.drain()
        assert len(results) == 1
        name, ok, payload = results[0]
        assert name == "job"
        assert ok is False
        assert isinstance(payload, ValueError)
        assert str(payload) == "炸了"
        # 异常后同名仍可再提交
        assert bg.submit("job", lambda: "ok") is True
    finally:
        bg.shutdown(wait=True)


def test_drain_empty_returns_empty_list():
    """无结果时 drain 返回空列表，非阻塞。"""
    bg = BackgroundDispatcher(max_workers=2)
    try:
        assert bg.drain() == []
    finally:
        bg.shutdown(wait=True)


def test_multiple_named_tasks_independent():
    """不同名任务互不影响单飞。"""
    bg = BackgroundDispatcher(max_workers=4)
    try:
        assert bg.submit("a", lambda: "ra") is True
        assert bg.submit("b", lambda: "rb") is True
        _wait_until(lambda: not bg.in_flight("a") and not bg.in_flight("b"))
        results = dict((n, v) for n, ok, v in bg.drain() if ok)
        assert results == {"a": "ra", "b": "rb"}
    finally:
        bg.shutdown(wait=True)
