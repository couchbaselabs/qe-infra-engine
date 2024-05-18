import copy
from typing import Optional
from constants.doc_templates import SLAVE_TEMPLATE
from tasks.task import Task
from tasks.task_result import TaskResult
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory

class AddSlaves(Task):

    def add_slave_sub_task(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "ipaddr" not in slave:
            self.set_subtask_exception(ValueError(f"ipaddr key is missing for the slave {slave}"))
        required_fields = ["ssh_username", "ssh_password", "name_label", "name", "description",
                           "labels", "origin", "jenkins_host"]
        for field in required_fields:
            if field not in slave:
                exception = f"Field {field} not present for slave {slave['ipaddr']}"
                self.set_subtask_exception(exception)

        ipaddr = slave['ipaddr']
        ssh_username = slave['ssh_username']
        ssh_password = slave['ssh_password']

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Slave Pool successful")
        except:
            exception = f"Cannot connect to Slave Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr=ipaddr,
                                                                                  ssh_username=ssh_username,
                                                                                  ssh_password=ssh_password)
            self.logger.info(f"Connection to slave {slave['ipaddr']} successful")
        except Exception as e:
            exception = f"Cannot connect to IP : {e}"
            self.set_subtask_exception(exception)

        try:
            mac_address = remote_connection_helper.find_mac_address()
            self.logger.info(f"Mac address for slave {slave['ipaddr']} found successfuly")
        except Exception as e:
            exception = f"Cannot find Mac address for slave {slave['ipaddr']} : {e}"
            self.set_subtask_exception(exception)

        try:
            memory = remote_connection_helper.find_memory_total()
            self.logger.info(f"Total Memory for slave {slave['ipaddr']} found successfuly")
        except Exception as e:
            exception = f"Cannot find Total memory for slave {slave['ipaddr']} : {e}"
            self.set_subtask_exception(exception)

        try:
            short_os, os_version = remote_connection_helper.find_os_version()
            self.logger.info(f"Operating System for slave {slave['ipaddr']} found successfuly")
        except Exception as e:
            exception = f"Cannot find OS version for slave {slave['ipaddr']} : {e}"
            self.set_subtask_exception(exception)

        doc = copy.deepcopy(SLAVE_TEMPLATE)

        doc['ipaddr'] = slave['ipaddr']
        doc['name'] = slave['name']
        doc['description'] = slave['description']
        doc['labels'] = slave['labels']
        doc['os'] = short_os
        doc['os_version'] = os_version
        doc['state'] = 'running'
        doc['memory'] = memory
        doc['tags'] = {}
        doc['mac_address'] = mac_address
        doc['origin'] = slave['origin']
        doc['name_label'] = slave['name_label']
        doc['jenkins_host'] = slave['jenkins_host']

        try:
            result_init_slave = remote_connection_helper.initialize_slave()
            self.logger.info(f"Initialization for slave {slave['ipaddr']} completed successfuly")
        except Exception as e:
            exception = f"Cannot Initialize slave {slave['ipaddr']}  : {e}"
            self.set_subtask_exception(exception)

        try:
            res = slave_pool_helper.upsert_slave_to_slave_pool(doc)
            if not res:
                exception = f"Cannot add slave {slave['ipaddr']} to slave pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for slave {slave['ipaddr']} added to slave pool successfuly")
        except Exception as e:
            exception = f"Cannot add slave {slave['ipaddr']} to slave pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["init_slave_res"] = result_init_slave
        task_result.result_json["slave_doc"] = doc

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a AddSlaves with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of slaves which has to be added to slave-pool
                        Each slave is a dictionary with many fields
        """
        task_name = AddSlaves.__name__
        if max_workers is None:
            max_workers = 100
        super().__init__(task_name, max_workers)

        if "data" not in params or params["data"] is None:
            exception = ValueError(f"Data is not present to add to slave-pool")
            self.set_exception(exception)
        elif not isinstance(params["data"], list):
            exception = ValueError(f"data param has to be a list : {params['data']}")
            self.set_exception(exception)
        else:
            self.data = params["data"]

        for slave in self.data:
            if "ipaddr" not in slave:
                exception = ValueError(f"ipaddr missing from slave : {slave}")
                self.set_exception(exception)
    
    def execute(self):
        self.start_task()
        sub_tasks = []
        for slave in self.data:
            params = {"slave" : slave}
            subtask = self.add_slave_sub_task
            subtaskid = self.add_sub_task(subtask, params)
            sub_tasks.append([slave["ipaddr"], subtaskid])
        for doc_key, subtask_id in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            if doc_key not in self.task_result.subtasks:
                self.task_result.subtasks[doc_key] = {}
            self.task_result.subtasks[doc_key] = task_result
        self.complete_task(result=True)
