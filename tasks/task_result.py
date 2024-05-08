import time
from constants.task_states import TaskStates

class TaskResult:
    def __init__(self) -> None:
        self.exception = None
        self.start_time = None
        self.end_time = None
        self.result = False
        self.state = TaskStates.CREATED
        self.subtasks = {}
        self.result_json = None

    def start_task(self):
        self.start_time = time.time()
        self.state = TaskStates.RUNNING

    def set_result(self, result):
        self.result = result

    def set_exception(self, exception):
        if not isinstance(exception, Exception):
            exception = Exception(exception)
        self.exception = exception
        self.complete_task(result=False)

    def complete_task(self, result):
        self.state = TaskStates.COMPLETED
        self.end_time = time.time()
        self.set_result(result=result)

    @staticmethod
    def generate_json_result(task_result, timeout=3600):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if task_result.state != TaskStates.COMPLETED:
                time.sleep(10)
                continue
            if task_result.result:
                if task_result.result_json is not None:
                    return task_result.result_json
                task_result.result_json = {}
                for sub_task in task_result.subtasks:
                    if isinstance(task_result.subtasks[sub_task], TaskResult):
                        task_result.result_json[sub_task] = TaskResult.generate_json_result(task_result.subtasks[sub_task])
                    else:
                        task_result.result_json[sub_task] = task_result.subtasks[sub_task]
                return task_result.result_json
            elif task_result:
                task_result.result_json = str(task_result.exception)
                return task_result.result_json