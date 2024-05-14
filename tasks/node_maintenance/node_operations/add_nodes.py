import copy
from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
from constants.doc_templates import NODE_TEMPLATE

class AddNodesTask(Task):

    def add_nodes_sub_task(self, task_result: TaskResult, params: dict) -> None:
        if "node" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        node = params["node"]
        if "ipaddr" not in node:
            self.set_subtask_exception(ValueError(f"ipaddr key is missing for the node {node}"))
        required_fields = ["ssh_username", "ssh_password", "vm_name", "poolId", "origin"]
        for field in required_fields:
            if field not in node:
                exception = f"Field {field} not present for node {node['ipaddr']}"
                self.set_subtask_exception(exception)

        ipaddr = node['ipaddr']
        ssh_username = node['ssh_username']
        ssh_password = node['ssh_password']

        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr=ipaddr,
                                                                                    ssh_username=ssh_username,
                                                                                    ssh_password=ssh_password)
            self.logger.info(f"Connection to node {ipaddr} successful")
        except Exception as e:
            exception = f"Cannot connect to node {ipaddr} : {e}"
            self.set_subtask_exception(exception)

        try:
            mac_address = remote_connection_helper.find_mac_address()
        except Exception as e:
            exception = f"Could not find mac adddress for node {ipaddr} : {e}"
            self.set_subtask_exception(exception)

        try:
            memory = remote_connection_helper.find_memory_total()
        except Exception as e:
            exception = f"Could not find total memory for node {ipaddr} : {e}"
            self.set_subtask_exception(exception)

        try:
            short_os, os_version = remote_connection_helper.find_os_version()
        except Exception as e:
            exception = f"Could not find os version for node {ipaddr} : {e}"
            self.set_subtask_exception(exception)

        try:
            result_init_node = remote_connection_helper.initialize_node()
            self.logger.info(f"Initialization for node {ipaddr} completed successfuly")
        except Exception as e:
            exception = f"Cannot Initialize node {ipaddr}  : {e}"
            self.set_subtask_exception(exception)

        doc = copy.deepcopy(NODE_TEMPLATE)

        # Remove doc key post cleanup
        doc["doc_key"] = node['ipaddr']
        doc['ipaddr'] = node['ipaddr']
        doc["mac_address"] = mac_address
        doc["vm_name"] = node["vm_name"]
        doc["memory"] = memory
        doc["origin"] = node["origin"]
        doc["os"] = short_os
        doc["os_version"] = os_version
        doc["poolId"] = node["poolId"]
        doc["prevUser"] = ""
        doc["username"] = ""
        doc["state"] = "available"
        doc["tags"] = {}

        try:
            res = server_pool_helper.upsert_node_to_server_pool(doc)
            if not res:
                exception = f"Cannot add node {ipaddr} into server pool"
                self.set_subtask_exception(exception)

            self.logger.info(f"Document for node {ipaddr} added to server pool successfuly")

        except Exception as e:
            exception = f"Cannot add node {ipaddr} into server pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["init_node_res"] = result_init_node
        task_result.result_json["node_doc"] = doc
   
    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a AddNodesTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of nodes which has to be added to server-pool
                        Each node is a dictionary with many fields
        """
        task_name = AddNodesTask.__name__
        if max_workers is None:
            max_workers = 100
        super().__init__(task_name, max_workers)

        if "data" not in params or params["data"] is None:
            exception = ValueError(f"Data is not present to add to server-pool")
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
            subtask = self.add_nodes_sub_task
            subtaskid = self.add_sub_task(subtask, params)
            sub_tasks.append([node["ipaddr"], subtaskid])
        for doc_key, subtask_id in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            if doc_key not in self.task_result.subtasks:
                self.task_result.subtasks[doc_key] = {}
            self.task_result.subtasks[doc_key] = task_result
        self.complete_task(result=True)