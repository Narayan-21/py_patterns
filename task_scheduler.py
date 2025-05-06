import threading
import time
import uuid
import logging, random
from collections import deque
from typing import Callable, Optional, Deque

# Configure logging to include thread names and timestamps
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(threadName)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class RetryableTask:
    def __init__(
        self,
        task_func: Callable,
        retry_count: int = 0,
        max_retries: int = 3,
        task_id: Optional[str] = None
    ):
        self.task_func = task_func
        self.retry_count = retry_count
        self.max_retries = max_retries
        self.task_id = task_id or str(uuid.uuid4())

    def run(self):
        logging.info(f"Task {self.task_id}: attempt {self.retry_count + 1}")
        self.task_func()

    def should_retry(self) -> bool:
        return self.retry_count < self.max_retries

    def increment_retry(self) -> "RetryableTask":
        self.retry_count += 1
        return self

class Worker(threading.Thread):
    def __init__(self, scheduler: 'Scheduler', name: str):
        super().__init__(name=name)
        self.scheduler = scheduler
        self.queue: Deque = deque()
        self.queue_lock = threading.Lock()
        self._stop_event = threading.Event()
        self.daemon = True  # Daemon threads won't block program exit

    def add_task(self, task: RetryableTask):
        with self.queue_lock:
            self.queue.append(task)

    def get_queue_size(self) -> int:
        with self.queue_lock:
            return len(self.queue)

    def shutdown(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.is_set():
            task = None
            with self.queue_lock:
                if self.queue:
                    task = self.queue.popleft()

            if task:
                try:
                    task.run()
                except Exception as e:
                    logging.warning(f"Task {task.task_id} failed: {e}")
                    self.scheduler.handle_failed_task(task)
            else:
                self._stop_event.wait(0.01)

class Scheduler:
    def __init__(self, num_workers: int = 4):
        self.workers = []
        for i in range(num_workers):
            worker = Worker(self, name=f"Worker-{i}")
            self.workers.append(worker)
            worker.start()

    def submit(self, task_func: Callable, max_retries: int = 3):
        if not callable(task_func):
            raise ValueError("Submitted task must be callable")
        task = RetryableTask(task_func, retry_count=0, max_retries=max_retries)
        self._submit_task(task)

    def _submit_task(self, task: RetryableTask):
        selected = min(self.workers, key=lambda w: w.get_queue_size())
        print("selected ***", selected)
        selected.add_task(task)
        logging.info(f"Submitted task {task.task_id} to {selected.name}")

    def handle_failed_task(self, task: RetryableTask):
        if task.should_retry():
            logging.info(f"Retrying task {task.task_id} (retry {task.retry_count + 1})")
            self._submit_task(task.increment_retry())
        else:
            logging.error(f"Task {task.task_id} failed after {task.max_retries} retries")

    def shutdown(self):
        logging.info("Shutting down scheduler...")
        for worker in self.workers:
            worker.shutdown()
        for worker in self.workers:
            worker.join()
        logging.info("Scheduler shut down complete.")

if __name__ == "__main__":
    # Example tasks
    def failing_task():
        failing_task.attempt += 1
        if failing_task.attempt < 4:
            raise Exception(f"Simulated failure at attempt {failing_task.attempt}")
        print(f"=> failing_task succeeded on attempt {failing_task.attempt}")
    failing_task.attempt = 0

    def successful_task():
        print("✅ successful_task is running!")
        time.sleep(0.2)
        print("✅ successful_task completed!")

    scheduler = Scheduler(num_workers=4)
    scheduler.submit(failing_task, max_retries=3)
    scheduler.submit(successful_task, max_retries=2)

    def process_order(order_id: int):
        duration = random.uniform(0.1, 0.5)
        logging.info(f"Order {order_id}: processing for {duration:.2f}s")
        time.sleep(duration)
        logging.info(f"Order {order_id}: completed")

    scheduler = Scheduler(num_workers=4)

    # Submit multiple order processing tasks to demonstrate load balancing
    num_orders = 12
    for order_id in range(1, num_orders + 1):
        scheduler.submit(lambda oid=order_id: process_order(oid), max_retries=0)
        
    # Allow time for tasks to process
    time.sleep(2)

    scheduler.shutdown()
