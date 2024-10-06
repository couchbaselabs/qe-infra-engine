from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from helper.jenkins_helper.jenkins_helper_factory import JenkinsHelperFactory
from constants.jenkins import JENKINS_URLS

class RemoveNodesTask(Task):

    def remove_node_from_server_pool(self, task_result: TaskResult, params: dict) -> None:
        if "node" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        node = params["node"]
        if "ipaddr" not in node:
            self.set_subtask_exception(ValueError(f"ipaddr key not found in node {node}"))

        ipaddr = node["ipaddr"]

        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            res_server_pool = server_pool_helper.delete_node(ipaddr=ipaddr)
            if res_server_pool:
                self.logger.info(f"Doc with node ipaddr {ipaddr} successfully deleted")
            else:
                exception = f"Cannot delete node with ipaddr {ipaddr} doc in server-pool"
                self.set_subtask_exception(exception)
        except Exception as e:
            exception = f"Cannot find node with ipaddr {ipaddr} doc in server-pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["remove_server_from_server_pool"] = res_server_pool

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a RemoveNodesTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of nodes which has to be removed from server-pool
                        Each node is a dictionary with many fields
        """
        task_name = RemoveNodesTask.__name__
        if max_workers is None:
            max_workers = 100
        super().__init__(task_name, max_workers, store_results=True)

        if "data" not in params or params["data"] is None:
            exception = ValueError(f"Data is not present to delete from server-pool")
            self.set_exception(exception)
        elif not isinstance(params["data"], list):
            exception = ValueError(f"data param has to be a list : {params['data']}")
            self.set_exception(exception)
        else:
            self.data = params["data"]

        for node in self.data:
            if "ipaddr" not in node:
                exception = ValueError(f"ipaddr missing from node : {node}")
                self.set_exception(exception)

    def execute(self):
        self.start_task()
        sub_tasks = []
        for node in self.data:
            params = {"node" : node}
            subtask = self.remove_node_from_server_pool
            subtaskid = self.add_sub_task(subtask, params)
            sub_tasks.append([node["ipaddr"], subtaskid])
        for doc_key, subtask_id in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            if doc_key not in self.task_result.subtasks:
                self.task_result.subtasks[doc_key] = {}
            self.task_result.subtasks[doc_key] = task_result
        self.complete_task(result=True)
