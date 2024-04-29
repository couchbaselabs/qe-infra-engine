import logging
import time
import enum
import concurrent

class TaskStates(enum.Enum):
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"

class Task:

    def __init__(self, task_name):
        self.task_name = task_name
        self.exception = None
        self.start_time = None
        self.end_time = None
        self.logger = logging.getLogger("tasks")
        self.result = False
        self.result_json = None
        self.state = TaskStates.CREATED

    def set_exception(self, exception):
        self.exception = exception
        self.complete_task()
        raise Exception(self.exception)

    def complete_task(self):
        self.state = TaskStates.COMPLETED
        self.end_time = time.time()

    def execute(self, callbacks:list, max_workers:int):
        if not isinstance(callbacks, list):
            self.set_exception()

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(add_node, node): node for node in node_data}
            for future in futures:
                pass


