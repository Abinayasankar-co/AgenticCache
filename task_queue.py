import queue
import threading
import asyncio
import time
from typing import List, Dict, Any
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("agentic_cache_logger")


class TaskQueue:
    def __init__(self, num_workers: int = 5):
        self.task_queue = queue.PriorityQueue()
        self.result_map = {}
        self.workers = []
        self.num_workers = num_workers
        self.running = False
        self._lock = threading.RLock()
        self._task_id_counter = 0
        self._task_events = {}  # Maps task_id to threading.Event for waiting
        self.task_stats = {
            "submitted": 0,
            "completed": 0,
            "failed": 0,
            "avg_processing_time": 0.0,
        }

    def start(self):
        if self.running:
            return
            
        self.running = True
        for _ in range(self.num_workers):
            worker = threading.Thread(target=self._worker_loop, daemon=True)
            worker.start()
            self.workers.append(worker)
            
        logger.info(f"TaskQueue started with {self.num_workers} workers")

    def stop(self):
        self.running = False
        # Add empty tasks to unblock workers
        for _ in range(self.num_workers):
            self.task_queue.put((0, None))
        
        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=1.0)
        self.workers = []
        
        logger.info("TaskQueue stopped")

    async def submit(self, coroutine_func, *args, priority: int = 1, **kwargs) -> Any:
        with self._lock:
            task_id = str(self._task_id_counter)
            self._task_id_counter += 1
            event = threading.Event()
            self._task_events[task_id] = event
            self.task_stats["submitted"] += 1
        
        # Add task to queue
        self.task_queue.put((priority, (task_id, coroutine_func, args, kwargs, time.time())))
        
        # Wait for task completion
        while not event.is_set():
            # Periodically yield control to the event loop
            await asyncio.sleep(0.01)
        
        # Get result
        with self._lock:
            result = self.result_map.pop(task_id)
            del self._task_events[task_id]
        
        # Raise exception if task failed
        if isinstance(result, Exception):
            raise result
            
        return result
        
    def get_stats(self) -> Dict[str, Any]:
        #Get Queue Stats
        with self._lock:
            return self.task_stats.copy()

    def _worker_loop(self):
        #Worker Function to process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            while self.running:
                try:
                    # Get next task
                    priority, task_data = self.task_queue.get(timeout=0.1)
                    if task_data is None:
                        continue
                        
                    task_id, coro_func, args, kwargs, submit_time = task_data
                    
                    # Execute coroutine
                    try:
                        result = loop.run_until_complete(coro_func(*args, **kwargs))
                        with self._lock:
                            self.result_map[task_id] = result
                            self.task_stats["completed"] += 1
                            
                            # Update average processing time
                            processing_time = time.time() - submit_time
                            self.task_stats["avg_processing_time"] = (
                                (self.task_stats["avg_processing_time"] * (self.task_stats["completed"] - 1) + processing_time) / 
                                self.task_stats["completed"]
                            )
                    except Exception as e:
                        with self._lock:
                            self.result_map[task_id] = e
                            self.task_stats["failed"] += 1
                    
                    # Signal completion
                    with self._lock:
                        if task_id in self._task_events:
                            self._task_events[task_id].set()
                    
                except queue.Empty:
                    pass
                except Exception as e:
                    logger.error(f"Error in worker loop: {str(e)}")
        finally:
            loop.close()


