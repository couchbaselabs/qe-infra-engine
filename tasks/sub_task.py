from concurrent.futures import Future
import uuid
import time
import logging
from constants.task_states import SubTaskStates

class SubTask(Future):
    def __init__(self, sub_task_name):
        self.sub_task_name = sub_task_name
        self.id = uuid.uuid4()
        self.exception = None
        self.start_time = None
        self.end_time = None
        self.logger = logging.getLogger("tasks")
        self.result = False
        self.result_json = None
        self.state = SubTaskStates.CREATED

    def start_sub_task(self):
        self.state = SubTaskStates.RUNNING

    def set_exception(self, exception):
        self.exception = exception
        self.complete_sub_task(result=False)
        raise self.exception

    def complete_sub_task(self, result):
        self.state = SubTaskStates.COMPLETED
        self.end_time = time.time()
        self.set_result(result)

    def set_result(self, result):
        self.result = result

    def execute_sub_task(self):
        self.set_exception(NotImplementedError("The execute sub task for the sub task is not implemented"))

    def generate_json_result(self):
        self.set_exception(NotImplementedError("The generate json result for the sub task is not implemented"))
