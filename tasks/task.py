import logging
import time
import concurrent
import uuid
from constants.task_states import TaskStates
from tasks.sub_task import SubTask
class Task:
    def __init__(self, task_name, max_workers):
        self.task_name = task_name
        self.id = uuid.uuid4()
        self.task_exception = None
        self.start_time = None
        self.end_time = None
        self.logger = logging.getLogger("tasks")
        self.result = False
        self.result_json = None
        self.state = TaskStates.CREATED
        self.subtasks = {}
        self.executor_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def start_task(self):
        self.state = TaskStates.RUNNING

    def set_result(self, result):
        self.result = result

    def set_exception(self, exception):
        if not isinstance(exception, Exception):
            exception = Exception(exception)
        self.exception = exception
        self.logger.error(exception)
        self.complete_task(result=False)
        raise self.exception

    def complete_task(self, result):
        self.state = TaskStates.COMPLETED
        self.end_time = time.time()
        self.set_result(result=result)

    def add_sub_task(self, subtask:SubTask):
        self.logger.debug(f"Sub task {subtask.sub_task_name}_{subtask.id} added for execution")
        future_instance = self.executor_pool.submit(subtask.execute_sub_task)
        self.subtasks[subtask] = future_instance

    def get_sub_task_result(self, subtask:SubTask):
        result = False
        try:
            result = self.subtasks[subtask].result()
            exception = self.subtasks[subtask].exception()
            if exception:
                self.logger.critical(f"Exception in {subtask.sub_task_name}_{subtask.id}: {exception}")
        except Exception as e:
            self.logger.warning(f"{subtask.sub_task_name}_{subtask.id} has not run properly and has ended abruptly : {e}")
        return result

    def generate_json_result(self, timeout=3600):
        self.set_exception(NotImplementedError("The generate json result for the task is not implemented"))

    def execute(self):
        self.set_exception(NotImplementedError("The execute for the task is not implemented"))
