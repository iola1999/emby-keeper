import atexit
import json
import os
import stat
import tempfile
import threading
from typing import Any, Dict, List, Optional

from loguru import logger

from .utils import CachedFuncProxy
from .config import config


class Cache:
    """缓存访问层。

    本地 JSON 缓存常驻内存，低频数据合并后再落盘；凭据类数据继续立即落盘。
    运行记录由 runinfo 模块保存在进程内，不再写入 cache.json。
    """

    DEFERRED_WRITE_DELAY = 15.0
    DEFERRED_PREFIXES = (
        "scheduler",
        "monitor.pornfans.answer.qa",
        "emby.env",
    )
    DISABLED_PREFIXES = ("runinfo",)

    def __init__(self):
        self._mongo_client = None
        self._cache_file = None
        self._data: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._flush_timer: Optional[threading.Timer] = None
        self._dirty = False
        self._closed = False

        if hasattr(config, "mongodb") and config.mongodb:
            try:
                from pymongo import MongoClient

                self._mongo_client = MongoClient(config.mongodb)
                self._db = self._mongo_client.embykeeper
                self._collection = self._db.cache
            except ImportError:
                logger.warning("没有安装 pymongo 包, 将使用 JSON 存储缓存.")
                self._setup_json_cache()
        else:
            self._setup_json_cache()

        atexit.register(self.close)

    def _setup_json_cache(self):
        self._cache_file = config.basedir / "cache.json"
        if self._cache_file.exists():
            try:
                with open(self._cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._data = data
                else:
                    logger.warning("缓存文件根节点不是对象, 将使用全新缓存.")
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"缓存文件读取失败, 将使用全新缓存: {e}")

        # 旧版本的运行记录只用于历史展示，当前程序没有依赖它恢复任务状态。
        old_runinfo = self._data.pop("runinfo", None)
        if old_runinfo:
            self._dirty = True
            logger.info("已移除旧运行记录缓存, 运行记录改为仅保存在内存中.")
            self._schedule_flush_locked()

    @staticmethod
    def _key_matches_prefix(key: str, prefix: str) -> bool:
        return key == prefix or key.startswith(prefix + ".")

    @classmethod
    def _is_disabled_key(cls, key: str) -> bool:
        return any(cls._key_matches_prefix(key, prefix) for prefix in cls.DISABLED_PREFIXES)

    @classmethod
    def _is_deferred_key(cls, key: str) -> bool:
        return any(cls._key_matches_prefix(key, prefix) for prefix in cls.DEFERRED_PREFIXES)

    def _schedule_flush_locked(self) -> None:
        if self._closed or self._flush_timer is not None:
            return
        timer = threading.Timer(self.DEFERRED_WRITE_DELAY, self.flush)
        timer.daemon = True
        self._flush_timer = timer
        timer.start()

    def _cancel_flush_timer_locked(self) -> None:
        timer = self._flush_timer
        self._flush_timer = None
        if timer is not None:
            timer.cancel()

    def _mark_dirty_locked(self, key: str) -> bool:
        self._dirty = True
        if self._is_deferred_key(key):
            self._schedule_flush_locked()
            return False
        self._cancel_flush_timer_locked()
        return True

    def _atomic_write_json_locked(self) -> None:
        cache_dir = self._cache_file.parent
        cache_dir.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self._data, ensure_ascii=False, separators=(",", ":"))

        previous_stat = None
        try:
            previous_stat = self._cache_file.stat()
        except FileNotFoundError:
            pass

        fd, temp_name = tempfile.mkstemp(
            prefix=f".{self._cache_file.name}.", suffix=".tmp", dir=str(cache_dir)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                fd = -1
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())

            if previous_stat is not None:
                os.chmod(temp_name, stat.S_IMODE(previous_stat.st_mode))
                chown = getattr(os, "chown", None)
                if chown is not None:
                    try:
                        chown(temp_name, previous_stat.st_uid, previous_stat.st_gid)
                    except PermissionError:
                        pass
            else:
                os.chmod(temp_name, 0o600)

            os.replace(temp_name, self._cache_file)

            directory_flag = getattr(os, "O_DIRECTORY", 0)
            try:
                directory_fd = os.open(str(cache_dir), os.O_RDONLY | directory_flag)
            except OSError:
                directory_fd = None
            if directory_fd is not None:
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass

    def flush(self) -> bool:
        """将待写内容原子地保存到本地 JSON 文件。"""
        if self._mongo_client or self._cache_file is None:
            return False
        with self._lock:
            self._flush_timer = None
            if not self._dirty:
                return False
            try:
                self._atomic_write_json_locked()
            except (OSError, TypeError, ValueError) as e:
                logger.error(f"缓存文件写入失败: {e}")
                self._schedule_flush_locked()
                return False
            self._dirty = False
            return True

    def close(self) -> None:
        client = self._mongo_client
        if client is not None:
            self._mongo_client = None
            client.close()
            return

        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._cancel_flush_timer_locked()
            if self._dirty:
                try:
                    self._atomic_write_json_locked()
                except (OSError, TypeError, ValueError) as e:
                    logger.error(f"缓存文件关闭前写入失败: {e}")
                else:
                    self._dirty = False

    def get(self, key: str, default: Any = None) -> Any:
        if self._mongo_client:
            result = self._collection.find_one({"_id": key})
            return result["value"] if result else default

        if self._is_disabled_key(key):
            return default
        with self._lock:
            value: Any = self._data
            try:
                for part in key.split("."):
                    value = value[part]
                return value
            except (KeyError, TypeError):
                return default

    def set(self, key: str, value: Any) -> None:
        if self._mongo_client:
            self._collection.update_one({"_id": key}, {"$set": {"value": value}}, upsert=True)
            return
        if self._is_disabled_key(key):
            return

        with self._lock:
            parts = key.split(".")
            current = self._data
            for part in parts[:-1]:
                child = current.get(part)
                if not isinstance(child, dict):
                    child = {}
                    current[part] = child
                current = child
            if current.get(parts[-1]) == value:
                return
            current[parts[-1]] = value
            flush_now = self._mark_dirty_locked(key)
        if flush_now:
            self.flush()

    @staticmethod
    def _delete_from_mapping(data: Dict[str, Any], key: str) -> bool:
        parts = key.split(".")
        current = data
        parents = []
        for part in parts[:-1]:
            if not isinstance(current, dict) or part not in current:
                return False
            parents.append((current, part))
            current = current[part]

        if not isinstance(current, dict) or parts[-1] not in current:
            return False
        del current[parts[-1]]
        for parent, part in reversed(parents):
            child = parent.get(part)
            if isinstance(child, dict) and not child:
                del parent[part]
            else:
                break
        return True

    def delete(self, key: str) -> None:
        if self._mongo_client:
            self._collection.delete_one({"_id": key})
            return
        if self._is_disabled_key(key):
            return

        with self._lock:
            if not self._delete_from_mapping(self._data, key):
                return
            flush_now = self._mark_dirty_locked(key)
        if flush_now:
            self.flush()

    def find_by_prefix(self, prefix: str) -> List[str]:
        if self._mongo_client:
            return [
                doc["_id"] for doc in self._collection.find({"_id": {"$regex": f"^{prefix}"}}, {"_id": 1})
            ]

        def get_keys_with_prefix(d, current_path="", keys=None):
            if keys is None:
                keys = []
            for key, value in d.items():
                path = f"{current_path}.{key}" if current_path else key
                if isinstance(value, dict):
                    get_keys_with_prefix(value, path, keys)
                elif path.startswith(prefix):
                    keys.append(path)
            return keys

        with self._lock:
            return get_keys_with_prefix(self._data)

    def delete_by_prefix(self, prefix: str) -> None:
        self.delete_many(self.find_by_prefix(prefix))

    def delete_many(self, keys: List[str]) -> None:
        """批量删除缓存键，并将本地 JSON 只写入一次。"""
        if self._mongo_client:
            self._collection.delete_many({"_id": {"$in": keys}})
            return

        with self._lock:
            changed_keys = []
            for key in keys:
                if self._is_disabled_key(key):
                    continue
                if self._delete_from_mapping(self._data, key):
                    changed_keys.append(key)
            if not changed_keys:
                return
            self._dirty = True
            flush_now = any(not self._is_deferred_key(key) for key in changed_keys)
            if flush_now:
                self._cancel_flush_timer_locked()
            else:
                self._schedule_flush_locked()
        if flush_now:
            self.flush()


cache: Cache = CachedFuncProxy(lambda: Cache())
