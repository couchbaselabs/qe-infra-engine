import copy
import time
from tasks.task import Task
from tasks.sub_task import SubTask
from constants.task_states import SubTaskStates, TaskStates
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
from constants.doc_templates import NODE_TEMPLATE

class AddNodesTask(Task):

    class AddNodeSubTask(SubTask):
        def __init__(self, params:dict):
            """
            Initialize a AddNodeSubTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - node (dict) : The node details in dict
            """
            sub_task_name = AddNodesTask.AddNodeSubTask.__name__
            super().__init__(sub_task_name)
            if "node" not in params:
                self.set_exception(ValueError("Invalid arguments passed"))
            self.node = params["node"]
            if "ipaddr" not in self.node:
                self.set_exception(ValueError(f"ipaddr key is missing for the node {self.node}"))
            required_fields = ["ssh_username", "ssh_password", "vm_name", "poolId", "origin"]
            for field in required_fields:
                if field not in self.node:
                    exception = f"Field {field} not present for node {self.node['ipaddr']}"
                    self.set_exception(exception)

        def generate_json_result(self, timeout=3600):
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.state != SubTaskStates.COMPLETED:
                    time.sleep(10)
                    continue
                if self.result:
                    self.result_json = {}
                    self.result_json["init_node_res"] = self.result_init_node
                    self.result_json["node_doc"] = self.doc
                    return self.result_json
                else:
                    return str(self.exception)
        
        def execute_sub_task(self):
            self.start_sub_task()

            ipaddr = self.node['ipaddr']
            ssh_username = self.node['ssh_username']
            ssh_password = self.node['ssh_password']

            try:
                server_pool_helper = ServerPoolSDKHelper()
                self.logger.info(f"Connection to Server Pool successful")
            except Exception as e:
                exception = f"Cannot connect to Server Pool using SDK : {e}"
                self.set_exception(exception)

            try:
                remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr=ipaddr,
                                                                                      ssh_username=ssh_username,
                                                                                      ssh_password=ssh_password)
                self.logger.info(f"Connection to node {self.node['ipaddr']} successful")
            except Exception as e:
                exception = f"Cannot connect to node {self.node['ipaddr']} : {e}"
                self.set_exception(exception)

            try:
                mac_address = remote_connection_helper.find_mac_address()
            except Exception as e:
                exception = f"Could not find mac adddress for node {ipaddr} : {e}"
                self.set_exception(exception)

            try:
                memory = remote_connection_helper.find_memory_total()
            except Exception as e:
                exception = f"Could not find total memory for node {ipaddr} : {e}"
                self.set_exception(exception)

            try:
                short_os, os_version = remote_connection_helper.find_os_version()
            except Exception as e:
                exception = f"Could not find os version for node {ipaddr} : {e}"
                self.set_exception(exception)

            try:
                self.result_init_node = remote_connection_helper.initialize_node()
                self.logger.info(f"Initialization for node {self.node['ipaddr']} completed successfuly")
            except Exception as e:
                exception = f"Cannot Initialize node {self.node['ipaddr']}  : {e}"
                self.set_exception(exception)

            doc = copy.deepcopy(NODE_TEMPLATE)

            doc['ipaddr'] = self.node['ipaddr']
            doc["mac_address"] = mac_address
            doc["vm_name"] = self.node["vm_name"]
            doc["memory"] = memory
            doc["origin"] = self.node["origin"]
            doc["os"] = short_os
            doc["os_version"] = os_version
            doc["poolId"] = self.node["poolId"]
            doc["prevUser"] = ""
            doc["username"] = ""
            doc["state"] = "available"
            doc["tags"] = {}

            self.doc = doc

            try:
                res = server_pool_helper.upsert_node_to_server_pool(self.doc)
                if not res:
                    exception = f"Cannot add node {ipaddr} into server pool"
                    self.set_exception(exception)

                self.logger.info(f"Document for node {ipaddr} added to server pool successfuly")

            except Exception as e:
                exception = f"Cannot add node {ipaddr} into server pool : {e}"
                self.set_exception(exception)

            self.complete_sub_task(result=True)
        
    def __init__(self, params, max_workers=None):
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

    def generate_json_result(self, timeout=3600):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.state != TaskStates.COMPLETED:
                time.sleep(10)
                continue
            if self.result:
                self.result_json = {}
                for sub_task in self.subtasks:
                    if sub_task.doc["ipaddr"] not in self.result_json:
                        self.result_json[sub_task.doc["ipaddr"]] = {}
                    self.result_json[sub_task.doc["ipaddr"]] = sub_task.generate_json_result()
                return self.result_json
            else:
                return str(self.exception)
    
    def execute(self):
        self.start_task()
        sub_tasks = []
        for node in self.data:
            params = {"node" : node}
            subtask = AddNodesTask.AddNodeSubTask(params)
            self.add_sub_task(subtask)
            sub_tasks.append(subtask)
        for sub_task in sub_tasks:
            self.get_sub_task_result(subtask=sub_task)
        self.complete_task(result=True)