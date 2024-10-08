from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from tasks.node_maintenance.node_operations.add_nodes import AddNodesTask
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper

class ChangeNodesTask(Task):

    def change_nodes_sub_task(self, task_result: TaskResult, params: dict) -> None:
        if "node" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        node = params["node"]
        if "old_ipaddr" not in node:
            self.set_subtask_exception(ValueError(f"old_ipaddr key is missing for the node {node}"))

        old_ipaddr = node["old_ipaddr"]

        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            res = server_pool_helper.delete_node(old_ipaddr)
            if res:
                self.logger.info(f"Doc with old ipaddr {old_ipaddr} successfully deleted")
            else:
                exception = f"Cannot delete old ipaddr {old_ipaddr} doc in server-pool : {e}"
                self.set_subtask_exception(exception)
        except Exception as e:
            exception = f"Cannot find old ipaddr {old_ipaddr} doc in server-pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["delete_old_ipaddr"] = True

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a AddNodesTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of nodes which has to be added to server-pool
                        Each node is a dictionary with many fields
        """
        task_name = ChangeNodesTask.__name__
        if max_workers is None:
            max_workers = 100
        super().__init__(task_name, max_workers, store_results=True)

        if "data" not in params or params["data"] is None:
            exception = ValueError(f"Data is not present to add to server-pool")
            self.set_exception(exception)
        elif not isinstance(params["data"], list):
            exception = ValueError(f"data param has to be a list : {params['data']}")
            self.set_exception(exception)
        else:
            self.data = params["data"]

        for count, node in enumerate(self.data):
            if "old_ipaddr" not in node:
                exception = ValueError(f"old ipaddr missing from node : {node}")
                self.set_exception(exception)
            if "new_ipaddr" not in node:
                exception = ValueError(f"new ipaddr missing from node : {node}")
                self.set_exception(exception)
            params["data"][count]["ipaddr"] = node["new_ipaddr"]

        self.add_nodes_task = AddNodesTask(params)

    def generate_json_result(self, timeout=3600):
        self.task_result.result_json = []
        for node in self.data:
            res = {}
            res["old_ipaddr"] = node["old_ipaddr"]
            res["new_ipaddr"] = node["new_ipaddr"]
            res["delete_old_node"] = TaskResult.generate_json_result(self.task_result.subtasks[node["old_ipaddr"]])
            res["add_new_ipaddr"] = TaskResult.generate_json_result(self.add_nodes_task.task_result)[node["new_ipaddr"]]
            self.task_result.result_json.append(res)
        return self.task_result.result_json

    def execute(self):
        self.start_task()

        sub_tasks = []
        for node in self.data:
            params = {"node" : node}
            subtask = self.change_nodes_sub_task
            subtaskid = self.add_sub_task(subtask, params)
            sub_tasks.append([node["old_ipaddr"], subtaskid])
        for old_ipaddr, subtask_id in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            self.task_result.subtasks[old_ipaddr] = task_result

        self.add_nodes_task.execute()

        self.complete_task(result=True)
