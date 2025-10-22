# AgenticCache
This is a Cache file escpecially for Agentic approaches .

# agentic_cache
A Python package for advanced memory management and task queuing, designed for agentic systems, AI development, and high-concurrency applications. It includes hierarchical memory, caching mechanisms, and a priority-based task queue to handle complex, stateful workflows efficiently.

## Features
- **Hierarchical Memory**: Multi-level storage (short-term, working, long-term, context) with prefetching, indexing, and automatic consolidation.
- **Caching Systems**: LRU cache, FIFO buffers, and TTL-based result caching with background expiration.
- **Task Queuing**: Priority queue for async coroutines with stats tracking and thread safety.
- **Thread-Safe**: All operations use reentrant locks for concurrent access.
- **Configurable**: Extensive config options for capacities, TTLs, thresholds, and more.
- **Extensible**: Easy to integrate into agent-based systems for memory persistence and event handling.

## Installation
Install via pip:
  ``` bash
   pip install agentic_cache
  ```

or from source:
  ``` bash
  git clone https://github.com/Abinayasankar-co/AgenticCache.git
  cd agentic_cache
  python setup.py install
  ```


## Quick Start
```python
import logging
from agent_core import HierarchicalMemory, TaskQueue

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Configure and initialize memory
config = {
    "short_term_capacity": 200,
    "history_size": 500,
    "result_cache_ttl": 7200,
    "prefetch_enabled": True,
    "prefetch_patterns": {"user_data": ["profile", "history"]}
}
memory = HierarchicalMemory(config)

# Store and retrieve data
memory.put("user_id", 12345, namespace="working", persistent=True)
value = memory.get("user_id")  # Returns 12345

# Record an event
memory.record_event({"type": "login", "timestamp": time.time()})

# Initialize task queue
queue = TaskQueue(num_workers=3)
queue.start()

# Submit an async task (example coroutine)
async def example_coro(x):
    await asyncio.sleep(1)
    return x * 2

result = await queue.submit(example_coro, 10)  # Returns 20

# Stop queue when done
queue.stop()

```
## Documentation
See USAGE.md for detailed usage and API reference.

## Contributing
Pull requests welcome! Please follow standard Python conventions.

## License
Apache License
