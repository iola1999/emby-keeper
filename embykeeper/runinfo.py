from __future__ import annotations

from asyncio import Event
import asyncio
from datetime import datetime
from enum import IntEnum, auto
from typing import TYPE_CHECKING, Callable, Dict, List
import random
import string

from pydantic import BaseModel, PrivateAttr

from .utils import to_iterable
if TYPE_CHECKING:
    from loguru import Logger

_running_runs: Dict[str, RunContext] = {}
_completed_runs: Dict[str, RunContext] = {}
_children: Dict[str, List[str]] = {}
_MAX_COMPLETED_RUNS = 128


class RunStatus(IntEnum):
    CATAGORY = auto()
    PENDING = auto()
    INITIALIZING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    NONEED = auto()
    FAIL = auto()
    CANCELLED = auto()
    ERROR = auto()
    SKIP = auto()
    IGNORE = auto()
    RESCHEDULE = auto()


class LogRecord(BaseModel):
    level: str
    message: str
    time: datetime


class RunContext(BaseModel):
    _finished: Event = PrivateAttr(default_factory=Event)
    _started: Event = PrivateAttr(default_factory=Event)
    _cancel: Callable = PrivateAttr(default=None)

    id: str
    parent_ids: List[str] = []
    description: str = None
    status: RunStatus = RunStatus.PENDING
    status_info: str = None
    log: List[LogRecord] = []
    duration: float = None
    start_time: datetime = None
    end_time: datetime = None
    next_time: datetime = None
    reschedule: int = None

    def start(self, status: RunStatus = RunStatus.RUNNING):
        """开始任务, 设置开始时间和状态"""
        self.start_time = datetime.now()
        self.set(status)
        self._started.set()

    def set(self, status: RunStatus = None):
        """设置状态"""

        if status:
            self.status = status

    def finish(self, status: RunStatus = None, status_info: str = None):
        """完成任务并记录状态和时间"""

        # 设置结束状态
        self.set(status)
        if status_info:
            self.status_info = status_info
        self.end_time = datetime.now()

        # 计算持续时间
        if self.start_time:
            self.duration = (self.end_time - self.start_time).total_seconds()

        # 从运行中任务列表移除
        if self.id in _running_runs:
            del _running_runs[self.id]

        # 设置完成事件
        self._finished.set()

        # 只在进程内保留少量近期任务状态。
        self.save()

        return self

    def save(self):
        """保留兼容接口；运行记录不再写入 cache.json。"""
        _completed_runs[self.id] = self
        while len(_completed_runs) > _MAX_COMPLETED_RUNS:
            removed_id = next(iter(_completed_runs))
            _completed_runs.pop(removed_id, None)
            for parent_id, child_ids in list(_children.items()):
                if removed_id in child_ids:
                    _children[parent_id] = [child_id for child_id in child_ids if child_id != removed_id]
                if not _children[parent_id]:
                    del _children[parent_id]

    @classmethod
    def cancel_all(cls):
        """取消所有运行中的任务"""
        for run in list(_running_runs.values()):
            run.cancel_tree()
            if run.status != RunStatus.CATAGORY:
                run.finish(RunStatus.CANCELLED, "任务被取消")

    def bind_logger(self, logger: Logger):
        """将 loguru logger 绑定到当前任务"""
        return logger.bind(run_id=self.id)

    @classmethod
    def prepare(
        cls,
        description: str = None,
        parent_ids: List[str] = None,
        run_id: str = None,
    ):
        """生成一个新的任务上下文"""

        if run_id is None:
            chars = string.ascii_uppercase + string.digits
            while True:
                run_id = "".join(random.choices(chars, k=6))
                if RunContext.get(run_id) is None:
                    break
        run = cls(id=run_id, parent_ids=to_iterable(parent_ids))
        run.description = description

        # 添加到运行中任务列表
        _running_runs[run_id] = run

        # 如果有父任务, 记录进程内父子关系
        if parent_ids:
            for parent_id in parent_ids:
                children = _children.setdefault(parent_id, [])
                if run_id not in children:
                    children.append(run_id)

        return run

    @classmethod
    def get(cls, run_id: str) -> "RunContext":
        # 优先从运行中任务获取
        if run_id in _running_runs:
            return _running_runs[run_id]

        return _completed_runs.get(run_id)

    def get_parents(self):
        """获取所有父任务"""
        parents = []
        for parent_id in self.parent_ids:
            parent = RunContext.get(parent_id)
            if parent:
                parents.append(parent)
        return parents

    def get_children(self):
        """获取所有子任务"""
        children = []
        child_ids = _children.get(self.id, [])
        for child_id in child_ids:
            child = RunContext.get(child_id)
            if child:
                children.append(child)
        return children

    def yield_logs(self, reverse: bool = False, include_children: bool = False):
        """按时间顺序产出日志记录"""
        logs = self.log.copy()

        if include_children:
            for child in self.get_children():
                logs.extend(child.log)

        # 确保所有日志都有时间戳
        for log in logs:
            if log.time is None:
                log.time = datetime.now()

        # 按时间排序
        logs.sort(key=lambda x: x.time, reverse=reverse)
        yield from logs

    @classmethod
    def run(cls, func: Callable, description: str = None, parent_ids: List[str] = None):
        async def runner():
            ctx = RunContext.prepare(
                description=description or func.__name__,
                parent_ids=parent_ids,
            )
            task = asyncio.create_task(func(ctx))
            ctx._cancel = task.cancel
            try:
                result = await task
                if ctx.id in _running_runs:
                    ctx.finish(RunStatus.SUCCESS)
                return result
            except asyncio.CancelledError:
                if ctx.id in _running_runs:
                    ctx.finish(RunStatus.CANCELLED, "任务被取消")
                raise
            except Exception:
                if ctx.id in _running_runs:
                    ctx.finish(RunStatus.ERROR, "任务发生错误")
                raise

        return runner()

    def get_running_children(self):
        """获取所有正在运行的子任务"""
        children = []
        child_ids = _children.get(self.id, [])
        for child_id in child_ids:
            if child_id in _running_runs:
                children.append(_running_runs[child_id])
        return children

    def cancel_tree(self):
        """取消当前任务及其所有运行中的子任务"""
        # 先取消所有子任务
        for child in self.get_running_children():
            if child._cancel:
                child._cancel()

        # 取消自身任务
        if self._cancel:
            self._cancel()

    @classmethod
    def get_or_create(
        cls,
        run_id: str = None,
        description: str = None,
        parent_ids: List[str] = None,
        status: RunStatus = RunStatus.CATAGORY,
    ):
        """获取现有任务或创建新任务"""

        if run_id:
            existing = cls.get(run_id)
            if existing:
                return existing
        ctx = cls.prepare(description=description, parent_ids=parent_ids, run_id=run_id)
        if status:
            ctx.set(status)
        return ctx
