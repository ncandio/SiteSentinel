"""
Custom scheduler implementation for website monitoring using uvloop event loop.
Handles concurrent execution of monitoring tasks with different intervals.
Supports both a thread-based scheduler and Dask distributed computing.
"""

import asyncio
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional
try:
    import uvloop
    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False
from concurrent.futures import ThreadPoolExecutor

# Import Dask components for distributed computing
try:
    import dask
    from distributed import Client, Future
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Task:
    """Represents a scheduled task with an execution time and callback."""

    def __init__(
        self,
        task_id: int,
        interval: float,
        callback: Callable,
        args: tuple,
    ):
        """Initialize a task.

        Args:
            task_id: Unique task identifier
            interval: Execution interval in seconds
            callback: Function to call
            args: Arguments for the callback function
        """
        if asyncio.iscoroutinefunction(callback):
            raise ValueError("Async functions are not supported. Please provide a synchronous function.")
            
        self.task_id = task_id
        self.interval = interval
        self.callback = callback
        self.args = args
        self.last_run = None
        self.is_running = False
        self.error_count = 0
        self.task = None
        self._stop = False
        self._timeout = 300  # 5 minutes timeout for task execution

    async def run(self, scheduler):
        """Run the task periodically."""
        while not self._stop:
            try:
                await scheduler._execute_task(self)
                # Use wait_for to ensure sleep doesn't block indefinitely
                await asyncio.wait_for(asyncio.sleep(self.interval), timeout=self.interval + 1)
            except asyncio.TimeoutError:
                logger.warning(f"Task {self.task_id} sleep timeout, continuing...")
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in task {self.task_id} loop: {e}")
                # Use wait_for to ensure sleep doesn't block indefinitely
                try:
                    await asyncio.wait_for(asyncio.sleep(self.interval), timeout=self.interval + 1)
                except asyncio.TimeoutError:
                    logger.warning(f"Task {self.task_id} error recovery sleep timeout, continuing...")
                    continue

        self.future = None  # For tracking Dask futures


    def stop(self):
        """Stop the task."""
        self._stop = True
        if self.task and not self.task.done():
            self.task.cancel()


class Scheduler:

    """Handles scheduling and execution of periodic tasks with different intervals using uvloop."""

    """Handles scheduling and execution of periodic tasks with different intervals.
    Supports both thread-based and Dask-based scheduling approaches.
    """


    def __init__(self, max_workers: int = 10, use_dask: bool = False, dask_client=None):
        """Initialize the scheduler.

        Args:
            max_workers: Maximum number of concurrent workers
            use_dask: Whether to use Dask for task execution
            dask_client: An existing Dask Client to use, or None to create a new one
        """
        if UVLOOP_AVAILABLE:
            self.loop = uvloop.new_event_loop()
        else:
            self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.tasks: Dict[int, Task] = {}
        self.task_counter = 0

        self.running = False
        self.max_workers = max_workers
        self.active_workers = 0
        self.worker_semaphore = asyncio.Semaphore(max_workers)
        self._stop_event = asyncio.Event()
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self._timeout = 300  # 5 minutes timeout for task execution
        
        # Dask configuration
        self.use_dask = use_dask and DASK_AVAILABLE
        self.dask_client = None
        self.dashboard_link = None
        self.last_dashboard_log = 0
        
        if self.use_dask:
            try:
                if dask_client:
                    self.dask_client = dask_client
                else:
                    # Create a local dask client with desired number of workers
                    self.dask_client = Client(n_workers=max_workers, threads_per_worker=1)
                    
                    # Log dashboard information for accessing the Dask console
                    self.dashboard_link = self.dask_client.dashboard_link
                    logger.info(f"Dask scheduler initialized with {max_workers} workers")
                    logger.info(f"Access Dask dashboard at: {self.dashboard_link}")
                    print(f"\033[97;45m DASK DASHBOARD \033[0m \033[1;95m{self.dashboard_link}\033[0m")
            except Exception as e:
                logger.error(f"Failed to initialize Dask: {e}")
                self.use_dask = False

    async def _execute_task(self, task: Task):
        """Execute a task asynchronously."""
        try:
            task.is_running = True
            task.last_run = time.time()
            
            # Use wait_for to ensure semaphore doesn't block indefinitely
            await asyncio.wait_for(
                self.worker_semaphore.acquire(),
                timeout=task._timeout
            )
            
            self.active_workers += 1
            
            # Run task with timeout
            await asyncio.wait_for(
                self.loop.run_in_executor(
                    self._thread_pool,
                    task.callback,
                    *task.args
                ),
                timeout=task._timeout
            )
            
            # Force garbage collection after task execution to clean up resources
            import gc
            gc.collect()
                
        except asyncio.TimeoutError:
            logger.error(f"Task {task.task_id} execution timed out after {task._timeout} seconds")
            task.error_count += 1
        except Exception as e:
            logger.error(f"Error executing task {task.task_id}: {e}")
            task.error_count += 1
        finally:
            task.is_running = False
            self.active_workers -= 1
            try:
                self.worker_semaphore.release()
            except ValueError:
                # Ignore if semaphore was already released
                pass

    def add_task(self, interval: float, callback: Callable, *args) -> int:
        """Add a new task to the scheduler.

        Args:
            interval: Execution interval in seconds
            callback: Function to call (must be synchronous)
            args: Arguments for the callback function

        Returns:
            Task ID

        Raises:
            ValueError: If callback is an async function
        """
        if asyncio.iscoroutinefunction(callback):
            raise ValueError("Async functions are not supported. Please provide a synchronous function.")

        self.task_counter += 1
        task_id = self.task_counter

        task = Task(task_id, interval, callback, args)
        self.tasks[task_id] = task
        
        if self.running:
            task.task = self.loop.create_task(task.run(self))
        
        logger.info(f"Added task {task_id} with interval {interval}s")
        return task_id

    def remove_task(self, task_id: int) -> bool:
        """Remove a task from the scheduler.

        Args:
            task_id: ID of the task to remove

        Returns:
            Whether the task was removed
        """
        if task_id in self.tasks:
            task = self.tasks[task_id]
            task.stop()
            del self.tasks[task_id]
            logger.info(f"Removed task {task_id}")
            return True
        return False

    async def _run(self):
        """Run the scheduler."""
        self._stop_event.clear()
        
        # Start all existing tasks
        for task in self.tasks.values():
            task.task = self.loop.create_task(task.run(self))
        
        try:
            # Log that the scheduler is really running
            logger.info("Scheduler event loop is running and processing tasks")
            
            # Wait for stop signal with timeout
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=1.0  # Check every second
                    )
                except asyncio.TimeoutError:
                    # Check if any tasks are stuck
                    for task_id, task in list(self.tasks.items()):
                        if task.is_running and task.last_run and time.time() - task.last_run > task._timeout:
                            logger.warning(f"Task {task_id} appears stuck, removing...")
                            self.remove_task(task_id)
                    continue
        except Exception as e:
            logger.error(f"Error in scheduler main loop: {e}")
        finally:
            # Stop all tasks
            for task in self.tasks.values():
                task.stop()
            
            self.running = False
            logger.info("Scheduler event loop has stopped")



    def start(self):
        """Start the scheduler."""
        if not self.running:
            self.running = True
            threading.Thread(target=self._start_loop, daemon=True).start()
            logger.info("Scheduler started")
            
    def _start_loop(self):
        """Start the event loop in a separate thread."""
        try:
            self.loop.run_until_complete(self._run())
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            logger.error(f"Unexpected error in scheduler: {e}")
            self.stop()
        finally:
            try:
                self._thread_pool.shutdown(wait=True)
            except Exception as e:
                logger.error(f"Error shutting down thread pool: {e}")

    def stop(self):
        """Stop the scheduler."""
        if self.running:
            self._stop_event.set()
            self.loop.stop()
            self.running = False
            
            # Close thread pool executor to release resources
            if hasattr(self, '_thread_pool') and self._thread_pool:
                try:
                    self._thread_pool.shutdown(wait=True)
                    logger.info("Thread pool executor shutdown completed")
                except Exception as e:
                    logger.error(f"Error shutting down thread pool: {e}")
            
            # Clean up Dask resources
            if self.use_dask and self.dask_client:
                try:
                    # Cancel any pending futures
                    for task_id, task in self.tasks.items():
                        if hasattr(task, 'future') and task.future and not task.future.done():
                            task.future.cancel()
                    
                    # Log the final dashboard message with shutdown notice
                    if self.dashboard_link:
                        logger.info(f"Shutting down Dask dashboard at: {self.dashboard_link}")
                    
                    # Close the Dask client
                    self.dask_client.close()
                    logger.info("Dask client closed")
                    self.dashboard_link = None
                except Exception as e:
                    logger.error(f"Error shutting down Dask client: {e}")
            
            # Force garbage collection at shutdown
            import gc
            gc.collect()
                
            logger.info("Scheduler stopped")

    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self.running

    def get_task_info(self, task_id: int) -> Optional[Dict]:
        """Get information about a task.

        Args:
            task_id: ID of the task

        Returns:
            Dictionary with task information or None if task doesn't exist
        """
        if task_id in self.tasks:
            task = self.tasks[task_id]
            return {
                "task_id": task.task_id,
                "interval": task.interval,
                "last_run": task.last_run,
                "last_run_time": (
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(task.last_run))
                    if task.last_run
                    else None
                ),
                "is_running": task.is_running,
                "error_count": task.error_count,
            }
        return None

    def list_tasks(self) -> List[Dict]:
        """List all tasks and their status.

        Returns:
            List of dictionaries with task information
        """
        return [
            {
                "task_id": task.task_id,
                "interval": task.interval,
                "last_run": task.last_run,
                "last_run_time": (
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(task.last_run))
                    if task.last_run
                    else None
                ),
                "is_running": task.is_running,
                "error_count": task.error_count,
            }
            for task in self.tasks.values()
        ]

    def get_dask_status(self) -> Dict:
        """Get status information about the scheduler.

        Returns:
            Dictionary with status information including Dask details if enabled
        """
        status = {
            "status": "running" if self.running else "stopped",
            "available_workers": self.max_workers - self.active_workers,
            "tasks_pending": len([t for t in self.tasks.values() if t.is_running]),
            "tasks_total": len(self.tasks),
            "using_dask": self.use_dask
        }
        
        # Add detailed Dask information if available
        if self.use_dask and self.dask_client:
            try:
                dask_info = self.dask_client.scheduler_info()
                status.update({
                    "dask_workers": len(dask_info.get("workers", {})),
                    "dask_tasks_processing": dask_info.get("processing", 0),
                    "dask_tasks_total": dask_info.get("total", 0),
                    "dask_dashboard": self.dashboard_link,
                    "dask_dashboard_refresh_interval": "10 seconds",
                    "dask_client_active": self.dask_client.status == "running"
                })
            except Exception as e:
                logger.warning(f"Could not get Dask info: {e}")
                status["dask_error"] = str(e)
                
        return status
