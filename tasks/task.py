import logging
import concurrent
import uuid
from tasks.task_result import TaskResult
from helper.sdk_helper.testdb_helper.task_pool_helper import TaskPoolSDKHelper
class Task:
    def __init__(self, task_name, max_workers, store_results=False):
        self.task_name = task_name
        self.id = uuid.uuid4()
        self.logger = logging.getLogger("tasks")
        self.task_result = TaskResult()
        self.subtasks = {}
        self.store_results = store_results
        self.executor_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

        try:
            self.task_pool_helper = TaskPoolSDKHelper()
            self.logger.info(f"Connection to Task Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Task Pool using SDK : {e}"
            raise Exception(exception)

        try:
            self.task_pool_helper.create_task_doc(self.id, self.task_name)
        except Exception as e:
            exception = f"Cannot create task document and add to task pool using SDK : {e}"
            raise Exception(exception)

    def start_task(self):
        self.logger.info(f"Starting task {self.task_name}_{self.id}")
        self.task_result.start_task()

        try:
            self.task_pool_helper.update_task_started(self.id, self.task_result.start_time)
        except Exception as e:
            exception = f"Cannot create task document and add to task pool using SDK : {e}"
            raise Exception(exception)

    def complete_task(self, result):
        self.task_result.complete_task(result)

        try:
            self.task_pool_helper.update_task_completed(self.id, self.task_result.end_time, result)
        except Exception as e:
            exception = f"Cannot create task document and add to task pool using SDK : {e}"
            raise Exception(exception)

    def set_exception(self, exception):
        self.task_result.set_exception(exception)
        self.logger.error(exception)
        self.complete_task(result=False)
        raise self.task_result.exception

    def set_subtask_exception(self, exception: str | Exception):
        if not isinstance(exception, Exception):
            exception = Exception(exception)
        self.logger.error(exception)
        raise exception

    def add_sub_task(self, subtask, params):
        self.logger.debug(f"Sub task {subtask.__name__} added for execution")
        subtask_id = f'{subtask.__name__}_{uuid.uuid4()}'
        task_result = TaskResult()
        task_result.start_task()
        future_instance = self.executor_pool.submit(subtask, task_result, params)
        self.subtasks[subtask_id] = (future_instance, task_result)
        return subtask_id

    def get_sub_task_result(self, subtask_id):
        try:
            exception = self.subtasks[subtask_id][0].exception()
            if exception:
                self.logger.critical(f"Exception in {subtask_id}: {exception}")
                self.subtasks[subtask_id][1].set_exception(exception)
            else:
                result = self.subtasks[subtask_id][0].result()
                self.subtasks[subtask_id][1].complete_task(result=True)
        except Exception as e:
            self.logger.warning(f"{subtask_id} has not run properly and has ended abruptly : {e}")
            self.subtasks[subtask_id][1].set_exception(e)

        task_result = self.subtasks[subtask_id][1]
        TaskResult.generate_json_result(task_result)
        self.subtasks.pop(subtask_id, None)
        return task_result

    def generate_json_result(self, timeout=3600):
        TaskResult.generate_json_result(self.task_result, timeout=timeout)
        if self.store_results:
            self.add_task_result_to_db()
        return self.task_result.result_json

    def execute(self):
        raise NotImplementedError("The execute for the task is not implemented")

    def add_task_result_to_db(self):
        try:
            self.task_pool_helper.add_results_to_task(self.id, self.task_result.result_json)
        except Exception as e:
            exception = f"Cannot add task result to task pool : {e}"
            self.set_exception(exception)
