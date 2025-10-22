import os
import json
import logging
import asyncio
import time
import threading
import queue
import functools
import hashlib
from typing import Dict, List, Any, Optional, Union, Tuple, Callable, Set
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from collections import deque, OrderedDict
import weakref
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("agentic_cache_logger")

# Memory Management Systems
class LRUCache:
    def __init__(self, capacity: int = 1000):
        self.cache = OrderedDict()
        self.capacity = capacity
        self._lock = threading.RLock()

    def get(self, key: str) -> Any:
        with self._lock:
            if key not in self.cache:
                return None
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self.cache:
                self.cache[key] = value
                self.cache.move_to_end(key)
                return

            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
            
            self.cache[key] = value

    def remove(self, key: str) -> None:
        with self._lock:
            if key in self.cache:
                del self.cache[key]

    def clear(self) -> None:
        with self._lock:
            self.cache.clear()
            
    def __len__(self) -> int:
        with self._lock:
            return len(self.cache)

class MemoryBuffer:
    def __init__(self, max_size: int = 1000):
        self.buffer = deque(maxlen=max_size)
        self._lock = threading.RLock()

    def append(self, item: Any) -> None:
        with self._lock:
            self.buffer.append(item)

    def extend(self, items: List[Any]) -> None:
        with self._lock:
            self.buffer.extend(items)

    def get_all(self) -> List[Any]:
        with self._lock:
            return list(self.buffer)
            
    def get_recent(self, n: int) -> List[Any]:
        with self._lock:
            return list(self.buffer)[-n:] if n > 0 else []

    def clear(self) -> None:
        with self._lock:
            self.buffer.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self.buffer)

class ResultCache:
    def __init__(self, ttl: int = 3600):  # Default TTL: 1 hour
        self.cache = {}
        self.ttl = ttl
        self._lock = threading.RLock()
        self._start_cleanup_thread()

    def _start_cleanup_thread(self):
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()

    def _cleanup_loop(self):
        while True:
            time.sleep(60)  
            self._cleanup_expired()

    def _cleanup_expired(self):
        now = time.time()
        with self._lock:
            expired_keys = [k for k, (_, exp) in self.cache.items() if exp < now]
            for k in expired_keys:
                del self.cache[k]

    def get_result(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self.cache:
                value, expiry = self.cache[key]
                if time.time() < expiry:
                    return value
                else:
                    del self.cache[key]
        return None

    def store_result(self, key: str, value: Any, custom_ttl: Optional[int] = None) -> None:
        ttl = custom_ttl if custom_ttl is not None else self.ttl
        with self._lock:
            self.cache[key] = (value, time.time() + ttl)

    def invalidate(self, key: str) -> None:
        with self._lock:
            if key in self.cache:
                del self.cache[key]
                
    def invalidate_by_prefix(self, prefix: str) -> None:
        with self._lock:
            keys_to_remove = [k for k in self.cache if k.startswith(prefix)]
            for k in keys_to_remove:
                del self.cache[k]

    def clear(self) -> None:
        with self._lock:
            self.cache.clear()
            
    def __len__(self) -> int:
        with self._lock:
            return len(self.cache)

class HierarchicalMemory:
    def __init__(self, config: Dict[str, Any]):
        # Short-term memory (fast, limited capacity)
        self.short_term = LRUCache(capacity=config.get("short_term_capacity", 100))
        
        # Working memory (medium-term, larger capacity)
        self.working = {}
        
        # Long-term memory (persistent)
        self.long_term = config.get("persistent_storage", {})
        
        # Active context (what's currently relevant)
        self.context = {}
        
        # Event/history buffer
        self.history = MemoryBuffer(max_size=config.get("history_size", 1000))
        
        # Result cache for expensive operations
        self.result_cache = ResultCache(ttl=config.get("result_cache_ttl", 3600))
        
        # Memory indexing
        self.indexes = {}
        
        # Task-specific buffers
        self.task_buffers = {}
        
        # Memory locks for thread safety
        self._locks = {
            "working": threading.RLock(),
            "context": threading.RLock(),
            "indexes": threading.RLock(),
            "task_buffers": threading.RLock(),
        }
        
        # Configuration
        self.forget_threshold = config.get("forget_threshold", 0.2)
        self.consolidation_interval = config.get("consolidation_interval", 100)
        self.operation_count = 0
        
        # Prefetch mechanism
        self.prefetch_enabled = config.get("prefetch_enabled", True)
        self.prefetch_patterns = config.get("prefetch_patterns", {})

    def get(self, key: str, namespace: str = "working") -> Any:
        """Retrieve a value from memory with automatic fallback between memory levels"""
        # Try short-term memory first (fastest)
        short_key = f"{namespace}:{key}"
        value = self.short_term.get(short_key)
        if value is not None:
            return value
            
        # Try working memory
        if namespace == "working":
            with self._locks["working"]:
                if key in self.working:
                    value = self.working[key]
                    # Cache in short-term for faster future access
                    self.short_term.put(short_key, value)
                    return value
        
        # Try context
        elif namespace == "context":
            with self._locks["context"]:
                if key in self.context:
                    value = self.context[key]
                    self.short_term.put(short_key, value)
                    return value
        
        # Try long-term memory as last resort
        if key in self.long_term:
            value = self.long_term[key]
            # Cache in short-term and possibly working memory
            self.short_term.put(short_key, value)
            if namespace == "working":
                with self._locks["working"]:
                    self.working[key] = value
            return value
            
        return None

    def put(self, key: str, value: Any, namespace: str = "working", persistent: bool = False) -> None:
        """Store a value in memory with optional persistence"""
        short_key = f"{namespace}:{key}"
        
        # Always update short-term cache for fast access
        self.short_term.put(short_key, value)
        
        # Update the target memory space
        if namespace == "working":
            with self._locks["working"]:
                self.working[key] = value
        elif namespace == "context":
            with self._locks["context"]:
                self.context[key] = value
                
        # Persist to long-term if requested
        if persistent:
            self.long_term[key] = value
            
        # Perform prefetching if enabled
        if self.prefetch_enabled:
            self._prefetch_related(key, namespace)
            
        # Periodically consolidate memory
        self.operation_count += 1
        if self.operation_count % self.consolidation_interval == 0:
            self._consolidate_memory()

    def update_context(self, new_context: Dict[str, Any]) -> None:
        """Update the active context with new information"""
        with self._locks["context"]:
            self.context.update(new_context)
            
        # Update short-term cache with new context items
        for key, value in new_context.items():
            self.short_term.put(f"context:{key}", value)

    def get_full_context(self) -> Dict[str, Any]:
        """Get the complete context including history"""
        with self._locks["context"]:
            context_copy = self.context.copy()
            
        return {
            "context": context_copy,
            "history": self.history.get_recent(20),  # Get only recent history for efficiency
            "working_memory": self.get_snapshot("working")
        }

    def record_event(self, event: Dict[str, Any]) -> None:
        """Record an event in the history buffer"""
        # Add timestamp if not present
        if "timestamp" not in event:
            event["timestamp"] = time.time()
        self.history.append(event)

    def create_task_buffer(self, task_id: str, max_size: int = 100) -> None:
        """Create a dedicated buffer for a specific task"""
        with self._locks["task_buffers"]:
            self.task_buffers[task_id] = MemoryBuffer(max_size=max_size)

    def add_to_task_buffer(self, task_id: str, item: Any) -> None:
        """Add an item to a task-specific buffer"""
        with self._locks["task_buffers"]:
            if task_id not in self.task_buffers:
                self.create_task_buffer(task_id)
            self.task_buffers[task_id].append(item)

    def get_task_buffer(self, task_id: str) -> List[Any]:
        """Get all items from a task-specific buffer"""
        with self._locks["task_buffers"]:
            if task_id not in self.task_buffers:
                return []
            return self.task_buffers[task_id].get_all()

    def cache_result(self, operation: str, params: Dict[str, Any], result: Any, ttl: Optional[int] = None) -> None:
        """Cache the result of an expensive operation"""
        # Create a stable hash of the operation and parameters
        key = self._hash_operation(operation, params)
        self.result_cache.store_result(key, result, ttl)

    def get_cached_result(self, operation: str, params: Dict[str, Any]) -> Optional[Any]:
        """Retrieve a cached operation result if available"""
        key = self._hash_operation(operation, params)
        return self.result_cache.get_result(key)

    def forget(self, key: str, namespace: str = "working") -> None:
        """Explicitly remove an item from memory"""
        short_key = f"{namespace}:{key}"
        self.short_term.remove(short_key)
        
        if namespace == "working":
            with self._locks["working"]:
                if key in self.working:
                    del self.working[key]
        elif namespace == "context":
            with self._locks["context"]:
                if key in self.context:
                    del self.context[key]
        
        # Also remove from long-term if present
        if key in self.long_term:
            del self.long_term[key]

    def get_snapshot(self, namespace: str = "working") -> Dict[str, Any]:
        """Get a snapshot of a memory namespace"""
        if namespace == "working":
            with self._locks["working"]:
                return self.working.copy()
        elif namespace == "context":
            with self._locks["context"]:
                return self.context.copy()
        return {}
        
    def create_index(self, name: str, keys: List[str], namespace: str = "working") -> None:
        """Create an index for faster lookup of related items"""
        with self._locks["indexes"]:
            self.indexes[name] = {"namespace": namespace, "keys": keys, "mapping": {}}
            
            # Build initial index
            if namespace == "working":
                with self._locks["working"]:
                    for k in keys:
                        if k in self.working:
                            if k not in self.indexes[name]["mapping"]:
                                self.indexes[name]["mapping"][k] = set()
                            self.indexes[name]["mapping"][k].add(self.working[k])
            elif namespace == "context":
                with self._locks["context"]:
                    for k in keys:
                        if k in self.context:
                            if k not in self.indexes[name]["mapping"]:
                                self.indexes[name]["mapping"][k] = set()
                            self.indexes[name]["mapping"][k].add(self.context[k])

    def query_index(self, index_name: str, query: Dict[str, Any]) -> Set[Any]:
        """Query an index to find matching items"""
        with self._locks["indexes"]:
            if index_name not in self.indexes:
                return set()
                
            index = self.indexes[index_name]
            results = set()
            first_key = True
            
            for key, value in query.items():
                if key not in index["mapping"]:
                    continue
                    
                key_matches = set()
                for item in index["mapping"][key]:
                    if item == value:
                        key_matches.add(item)
                        
                if first_key:
                    results = key_matches
                    first_key = False
                else:
                    results &= key_matches
                    
            return results

    def _hash_operation(self, operation: str, params: Dict[str, Any]) -> str:
        """Create a stable hash for an operation and its parameters"""
        # Sort dictionary keys for stable serialization
        serialized = json.dumps({"op": operation, "params": params}, sort_keys=True)
        return hashlib.md5(serialized.encode()).hexdigest()

    def _consolidate_memory(self) -> None:
        """Consolidate memory by removing less important items"""
        with self._locks["working"]:
            working_size = len(self.working)
            if working_size > 1000:  
                to_remove = int(working_size * self.forget_threshold)
                keys = list(self.working.keys())[:to_remove]
                for key in keys:
                    del self.working[key]
                    short_key = f"working:{key}"
                    self.short_term.remove(short_key)
                    
                logger.info(f"Memory consolidated: removed {len(keys)} items from working memory")

    def _prefetch_related(self, key: str, namespace: str) -> None:
        """Prefetch related items based on access patterns"""
        # Check if there are any prefetch patterns for this key
        if key in self.prefetch_patterns:
            related_keys = self.prefetch_patterns[key]
            # Load related items into short-term memory
            for related_key in related_keys:
                if namespace == "working":
                    with self._locks["working"]:
                        if related_key in self.working:
                            self.short_term.put(f"{namespace}:{related_key}", self.working[related_key])
                elif namespace == "context":
                    with self._locks["context"]:
                        if related_key in self.context:
                            self.short_term.put(f"{namespace}:{related_key}", self.context[related_key])
                elif related_key in self.long_term:
                    self.short_term.put(f"{namespace}:{related_key}", self.long_term[related_key])

