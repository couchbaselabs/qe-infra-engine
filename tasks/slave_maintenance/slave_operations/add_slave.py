import copy
from typing import Optional
from constants.doc_templates import SLAVE_TEMPLATE
from constants.jenkins import JENKINS_URLS
from tasks.task import Task
from tasks.task_result import TaskResult
from helper.jenkins_helper.jenkins_helper_factory import JenkinsHelperFactory
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory

class AddSlavesTask(Task):

    def initialize_slave_subtask(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "ipaddr" not in slave:
            self.set_subtask_exception(ValueError(f"ipaddr key is missing for the slave {slave}"))
        required_fields = ["ssh_username", "ssh_password"]
        for field in required_fields:
            if field not in slave:
                exception = f"Field {field} not present for slave {slave['ipaddr']}"
                self.set_subtask_exception(exception)

        ipaddr = slave["ipaddr"]
        ssh_username = slave["ssh_username"]
        ssh_password = slave["ssh_password"]

        try:
            remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr=ipaddr,
                                                                                  ssh_username=ssh_username,
                                                                                  ssh_password=ssh_password)
            self.logger.info(f"Connection to slave {slave['ipaddr']} successful")
        except Exception as e:
            exception = f"Cannot connect to IP : {e}"
            self.set_subtask_exception(exception)

        try:
            result_init_slave = remote_connection_helper.initialize_slave()
            self.logger.info(f"Initialization for slave {slave['ipaddr']} completed successfuly")
        except Exception as e:
            exception = f"Cannot Initialize slave {slave['ipaddr']}  : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["init_slave_res"] = result_init_slave

    def add_slave_to_jenkins(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "ipaddr" not in slave:
            self.set_subtask_exception(ValueError(f"ipaddr key is missing for the slave {slave}"))
        required_fields = ["name", "description", "num_executors", "remote_fs", "labels",
                           "usage_mode", "ipaddr", "ssh_username", "ssh_password", "jenkins_host"]
        for field in required_fields:
            if field not in slave:
                exception = f"Field {field} not present for slave {slave['ipaddr']}"
                self.set_subtask_exception(exception)

        usage_modes_allowed = ['EXCLUSIVE', 'NORMAL']
        if slave["usage_mode"] not in usage_modes_allowed:
            exception = ValueError(f"usage_mode {slave['usage_mode']} consists of invalid usage mode, allowed : {usage_modes_allowed}")
            self.set_subtask_exception(exception)

        if slave["jenkins_host"] not in JENKINS_URLS:
            exception = ValueError(f"The value of jenkins host is invalid : {slave['jenkins_host']}")
            self.set_subtask_exception(exception)

        try:
            jenkins_helper = JenkinsHelperFactory.fetch_helper(JENKINS_URLS[slave["jenkins_host"]])
            self.logger.info(f"Connection to Jenkins successful")
        except Exception as e:
            exception = f"Cannot connect to Jenkins : {e}"
            self.set_subtask_exception(exception)

        try:
            status, response = jenkins_helper.add_slave(params=slave)
            if status != 200:
                exception = f"Cannot add slave {slave['name']} to Jenkins : {status}, {response}"
                self.set_subtask_exception(exception)
        except Exception as e:
            exception = f"Cannot add slave {slave['name']} to Jenkins : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["add_to_jenkins"] = str(response)

    def add_slave_to_slave_pool(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "ipaddr" not in slave:
            self.set_subtask_exception(ValueError(f"ipaddr key is missing for the slave {slave}"))
        required_fields = ["ssh_username", "ssh_password", "name_label", "name", "description",
                           "labels", "origin", "jenkins_host", "num_executors", "usage_mode", "remote_fs"]
        for field in required_fields:
            if field not in slave:
                exception = f"Field {field} not present for slave {slave['ipaddr']}"
                self.set_subtask_exception(exception)

        if slave["jenkins_host"] not in JENKINS_URLS:
            exception = ValueError(f"The value of jenkins host is invalid : {slave['jenkins_host']}")
            self.set_subtask_exception(exception)

        ipaddr = slave['ipaddr']
        ssh_username = slave['ssh_username']
        ssh_password = slave['ssh_password']

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Slave Pool successful")
        except Exception as e:
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
        doc["doc_key"] = slave['name']
        doc["num_executors"] = slave["num_executors"]
        doc["remote_fs"] = slave["remote_fs"]
        doc["usage_mode"] = slave["usage_mode"]

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
        task_result.result_json["slave_doc"] = doc

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a AddSlavesTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of slaves which has to be added to slave-pool
                        Each slave is a dictionary with many fields
                    - add_to_jenkins (bool) : True if the slave needs to be added to jenkins.
                        False if the slave need not be added to jenkins, but just the slave-pool
                    - initialize_slave (bool) : True if the slave needs initialized.
                        False if the slave need not be initialized, but just the slave-pool/jenkins
        """
        task_name = AddSlavesTask.__name__
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

        if "add_to_jenkins" not in params or params["add_to_jenkins"] is None:
            exception = ValueError(f"add_to_jenkins is not present in params : {params}")
            self.set_exception(exception)
        elif not isinstance(params["add_to_jenkins"], bool):
            exception = ValueError(f"add_to_jenkins param has to be a bool : {params['data']}")
            self.set_exception(exception)
        else:
            self.add_to_jenkins = params["add_to_jenkins"]

        if "initialize_slave" not in params or params["initialize_slave"] is None:
            exception = ValueError(f"initialize_slave is not present in params : {params}")
            self.set_exception(exception)
        elif not isinstance(params["initialize_slave"], bool):
            exception = ValueError(f"initialize_slave param has to be a bool : {params['data']}")
            self.set_exception(exception)
        else:
            self.initialize_slave = params["initialize_slave"]

        for slave in self.data:
            if "ipaddr" not in slave:
                exception = ValueError(f"ipaddr missing from slave : {slave}")
                self.set_exception(exception)

        self.sub_task_functions = []
        if self.add_to_jenkins:
            self.sub_task_functions.append(self.add_slave_to_jenkins)
        if self.initialize_slave:
            self.sub_task_functions.append(self.initialize_slave_subtask)
        self.sub_task_functions.append(self.add_slave_to_slave_pool)

    def execute(self):
        self.start_task()

        for sub_task_function in self.sub_task_functions:
            sub_tasks = []
            for slave in self.data:
                params = {"slave" : slave}
                subtaskid = self.add_sub_task(sub_task_function, params)
                sub_tasks.append([slave["name"], subtaskid])
            for doc_key, subtask_id in sub_tasks:
                task_result = self.get_sub_task_result(subtask_id=subtask_id)
                if doc_key not in self.task_result.subtasks:
                    self.task_result.subtasks[doc_key] = {}
                self.task_result.subtasks[doc_key][sub_task_function.__name__] = task_result

        self.complete_task(result=True)

    def generate_json_result(self, timeout=3600):
        TaskResult.generate_json_result(self.task_result, timeout=timeout)
        for doc_key in self.task_result.result_json:
           for sub_task_name in self.task_result.result_json[doc_key]:
               res = TaskResult.generate_json_result(self.task_result.subtasks[doc_key][sub_task_name], timeout=timeout)
               self.task_result.result_json[doc_key][sub_task_name] = res
        return self.task_result.result_json