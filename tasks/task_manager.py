import concurrent.futures
import logging
from tasks.task import Task
from tasks.task_result import TaskResult

class TaskManager:
    def __init__(self, max_workers) -> None:
        self.task_executor_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.running_tasks = {}
        self.logger = logging.getLogger("task_manager")

    def add_task(self, task: Task):
        future_instance = self.task_executor_pool.submit(task.execute)
        self.running_tasks[task.id] = [future_instance, task]

    def get_task_result(self, task_id: str = None, task: Task = None):
        if task_id is None and task is None:
            raise ValueError("Both task and task_id are None, cannot fetch result")
        elif task is not None:
            task_id = task.id

        try:
            exception = self.running_tasks[task_id][0].exception()
            if exception:
                self.logger.critical(f"Exception in {task_id}: {exception}")
                self.running_tasks[task_id][1].set_exception(exception)
            else:
                result = self.running_tasks[task_id][0].result()
                self.running_tasks[task_id][1].complete_task(result=True)
        except Exception as e:
            self.logger.warning(f"{task_id} has not run properly and has ended abruptly : {e}")
            self.subtasks[task_id][1].set_exception(e)

        task_result = self.running_tasks[task_id][1].task_result
        TaskResult.generate_json_result(task_result)
        self.running_tasks.pop(task_id, None)
        return task_result

        

        
