from typing import Optional
from constants.jenkins import JENKINS_URLS
from tasks.task import Task
from tasks.task_result import TaskResult
from tasks.slave_maintenance.slave_operations.remove_slave import RemoveSlavesTask
from tasks.slave_maintenance.slave_operations.add_slave import AddSlavesTask
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper
from helper.jenkins_helper.jenkins_helper_factory import JenkinsHelperFactory

class ChangeSlavesTask(Task):

    def change_slave_properties_in_slave_pool(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "name" not in slave:
            self.set_subtask_exception(ValueError(f"name key is missing for the slave {slave}"))
        name = slave["name"]

        # Can only change labels, name, description, state, origin, name_label
        changeable_fields = ['labels', 'description', 'state', 'origin', 'name_label', 'num_executors', 'remote_fs', 'usage']
        field_present = False
        for field in changeable_fields:
            if field in slave:
                field_present = True
        if not field_present:
            exception = f"None of the fields in {changeable_fields} present for slave {slave['name']}"
            self.set_subtask_exception(exception)

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Slave Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Slave Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            slave_doc = eval(slave_pool_helper.get_slave_pool_doc(name))
            self.logger.info(f"Doc with slave name {name} successfully found in slave-pool")
        except Exception as e:
            exception = f"Cannot find slave name {name} doc in slave-pool : {e}"
            self.set_subtask_exception(exception)

        if "usage_mode" in slave:
            usage_modes_allowed = ['EXCLUSIVE', 'NORMAL']
            if slave["usage_mode"] not in usage_modes_allowed:
                exception = ValueError(f"usage_mode {slave['usage_mode']} consists of invalid usage mode, allowed : {usage_modes_allowed}")
                self.set_subtask_exception(exception)

        if "labels" in slave:
            if not isinstance(slave["labels"], list):
                raise ValueError(f"{slave['labels']} labels is not a list")

            for label in slave["labels"]:
                if " " in label:
                    raise ValueError(f"label {label} consists of a space, not valid")

        for field in changeable_fields:
            if field in slave:
                slave_doc[field] = slave[field] if field in slave else slave_doc[field]

        try:
            res = slave_pool_helper.upsert_slave_to_slave_pool(slave_doc)
            if not res:
                exception = f"Cannot update slave {slave['name']} doc in slave pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for slave {slave['name']} updated in slave pool successfuly")
        except Exception as e:
            exception = f"Cannot update slave {slave['name']} doc in slave pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["new_doc"] = slave_doc

    def change_slave_properties_in_jenkins(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "name" not in slave:
            self.set_subtask_exception(ValueError(f"name key is missing for the slave {slave}"))
        slave_name = slave["name"]

        one_required_fields = ["description", "num_executors", "remote_fs", "labels", "usage_mode"]
        field_present = False
        for field in one_required_fields:
            if field in slave:
                field_present = True
        if not field_present:
            exception = f"None of the fields in {one_required_fields} present for slave {slave['name']}"
            self.set_subtask_exception(exception)

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Slave Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Slave Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            slave_doc = eval(slave_pool_helper.get_slave_pool_doc(slave_name))
            self.logger.info(f"Doc with slave {slave_name} successfully found in slave-pool")
        except Exception as e:
            exception = f"Cannot find slave {slave_name} doc in slave-pool : {e}"
            self.set_subtask_exception(exception)

        jenkins_url = slave_doc["jenkins_host"]

        if jenkins_url not in JENKINS_URLS:
            exception = ValueError(f"The value of jenkins host is invalid : {slave['jenkins_host']}")
            self.set_subtask_exception(exception)

        slave_name = slave["name"]
        description = slave["description"] if "description" in slave else slave_doc["description"]
        num_executors = slave["num_executors"] if "num_executors" in slave else slave_doc["num_executors"]
        remote_fs = slave["remote_fs"] if "remote_fs" in slave else slave_doc["remote_fs"]
        labels = slave["labels"] if "labels" in slave else slave_doc["labels"]
        usage_mode = slave["usage_mode"] if "usage_mode" in slave else slave_doc["usage_mode"]

        if usage_mode:
            usage_modes_allowed = ['EXCLUSIVE', 'NORMAL']
            if usage_mode not in usage_modes_allowed:
                exception = ValueError(f"usage_mode {usage_mode} consists of invalid usage mode, allowed : {usage_modes_allowed}")
                self.set_subtask_exception(exception)

        if labels:
            if not isinstance(labels, list):
                raise ValueError(f"{labels} labels is not a list")

            for label in labels:
                if " " in label:
                    raise ValueError(f"label {label} consists of a space, not valid")

        try:
            jenkins_helper = JenkinsHelperFactory.fetch_helper(JENKINS_URLS[slave_doc["jenkins_host"]])
            self.logger.info(f"Connection to Jenkins successful")
        except Exception as e:
            exception = f"Cannot connect to Jenkins : {e}"
            self.set_subtask_exception(exception)

        try:
            status, response = jenkins_helper.update_slave_properties(slave_name=slave_name,
                                                                      description=description,
                                                                      labels=labels,
                                                                      num_executors=num_executors,
                                                                      remote_fs=remote_fs,
                                                                      usage_mode=usage_mode)
            if status != 200:
                exception = f"Cannot change slave {slave['name']} in Jenkins : {status}, {response}"
                self.set_subtask_exception(exception)
        except Exception as e:
            exception = f"Cannot change slave {slave['name']} in Jenkins : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["change_in_jenkins"] = [str(status), str(response)]

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a ChangeSlavesTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of slaves which has to be changed in slave-pool
                        Each slave is a dictionary with many fields
                    - change_in_jenkins (bool) : True if the slave needs to be changed in jenkins.
                        False if the slave need not be changed in jenkins, but just the slave-pool
        """
        task_name = ChangeSlavesTask.__name__
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

        if "change_in_jenkins" not in params or params["change_in_jenkins"] is None:
            exception = ValueError(f"change_in_jenkins is not present in params : {params}")
            self.set_exception(exception)
        elif not isinstance(params["change_in_jenkins"], bool):
            exception = ValueError(f"change_in_jenkins param has to be a bool : {params['data']}")
            self.set_exception(exception)
        else:
            self.change_in_jenkins = params["change_in_jenkins"]

        for slave in self.data:
            if "name" not in slave:
                exception = ValueError(f"name missing from slave : {slave}")
                self.set_exception(exception)
            if "old_ipaddr" not in slave:
                exception = ValueError(f"old_ipaddr missing from slave : {slave}")
                self.set_exception(exception)
            if "new_ipaddr" not in slave:
                exception = ValueError(f"new_ipaddr missing from slave : {slave}")
                self.set_exception(exception)

        self.remove_slave_task_params = None
        self.add_slave_task_params = None

    def execute(self):
        self.start_task()

        sub_tasks = []
        for slave in self.data:
            params = {"slave" : slave}
            if slave["old_ipaddr"] == slave["new_ipaddr"]:
                subtaskid = self.add_sub_task(self.change_slave_properties_in_slave_pool, params)
                sub_tasks.append([slave["name"], subtaskid, self.change_slave_properties_in_slave_pool.__name__])
                if self.change_in_jenkins:
                    subtaskid = self.add_sub_task(self.change_slave_properties_in_jenkins, params)
                    sub_tasks.append([slave["name"], subtaskid, self.change_slave_properties_in_jenkins.__name__])
            else:
                if "ssh_username" not in slave:
                    exception = ValueError(f"ssh_username missing from slave : {slave}")
                    self.set_exception(exception)
                if "ssh_password" not in slave:
                    exception = ValueError(f"ssh_password missing from slave : {slave}")
                    self.set_exception(exception)

                if self.remove_slave_task_params is None:
                    self.remove_slave_task_params = {}
                self.remove_slave_task_params["delete_from_jenkins"] = self.change_in_jenkins
                if "data" not in self.remove_slave_task_params:
                    self.remove_slave_task_params["data"] = []
                slave_data = {
                    "name" : slave["name"]
                }
                self.remove_slave_task_params["data"].append(slave_data)

                try:
                    slave_pool_helper = SlavePoolSDKHelper()
                    self.logger.info(f"Connection to Slave Pool successful")
                except Exception as e:
                    exception = f"Cannot connect to Slave Pool using SDK : {e}"
                    self.set_exception(exception)

                try:
                    slave_doc = eval(slave_pool_helper.get_slave_pool_doc(slave["name"]))
                    self.logger.info(f"Doc with slave name {slave['name']} successfully found in slave-pool")
                except Exception as e:
                    exception = f"Cannot find slave name {slave['name']} doc in slave-pool : {e}"
                    self.set_exception(exception)

                if self.add_slave_task_params is None:
                    self.add_slave_task_params = {}
                self.add_slave_task_params["add_to_jenkins"] = self.change_in_jenkins
                self.add_slave_task_params["initialize_slave"] = False
                if "data" not in self.add_slave_task_params:
                    self.add_slave_task_params["data"] = []
                slave_data = {
                    "name": slave["name"],
                    "ipaddr": slave["new_ipaddr"],
                    "description": slave_doc["description"],
                    "num_executors": slave_doc["num_executors"],
                    "remote_fs": slave_doc["remote_fs"],
                    "labels": slave_doc["labels"],
                    "usage_mode": slave_doc["usage_mode"],
                    "ssh_username": slave["ssh_username"],
                    "ssh_password": slave["ssh_password"],
                    "jenkins_host": slave_doc["jenkins_host"],
                    "name_label": slave_doc["name_label"],
                    "origin": slave_doc["origin"]
                }
                self.add_slave_task_params["data"].append(slave_data)

        for doc_key, subtask_id, sub_task_name in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            if doc_key not in self.task_result.subtasks:
                self.task_result.subtasks[doc_key] = {}
            self.task_result.subtasks[doc_key][sub_task_name] = task_result

        if self.remove_slave_task_params is not None:
            self.remove_slave_task = RemoveSlavesTask(params=self.remove_slave_task_params)
            self.add_slave_task = AddSlavesTask(params=self.add_slave_task_params)

            self.remove_slave_task.execute()
            self.add_slave_task.execute()

        self.complete_task(result=True)

    def generate_json_result(self, timeout=3600):
        TaskResult.generate_json_result(self.task_result, timeout=timeout)
        for doc_key in self.task_result.result_json:
           for sub_task_name in self.task_result.result_json[doc_key]:
               res = TaskResult.generate_json_result(self.task_result.subtasks[doc_key][sub_task_name], timeout=timeout)
               self.task_result.result_json[doc_key][sub_task_name] = res
        if self.remove_slave_task_params is not None:
            for slave in self.add_slave_task_params["data"]:
                if slave["name"] not in self.task_result.result_json:
                    self.task_result.result_json[slave["name"]] = {}
                self.task_result.result_json[slave["name"]]["add_slave_task"] = self.add_slave_task.generate_json_result(timeout=timeout)[slave["name"]]
            for slave in self.remove_slave_task_params["data"]:
                if slave["name"] not in self.task_result.result_json:
                    self.task_result.result_json[slave["name"]] = {}
                self.task_result.result_json[slave["name"]]["remove_slave_task"] = self.remove_slave_task.generate_json_result(timeout=timeout)[slave["name"]]
        try:
            self.task_pool_helper.add_results_to_task(self.id, self.task_result.result_json)
        except Exception as e:
            exception = ValueError(f"host_tasks param has to be a list : {params['host_tasks']}")
            self.set_exception(exception)
        return self.task_result.result_json