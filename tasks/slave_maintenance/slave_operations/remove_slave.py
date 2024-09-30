from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper
from helper.jenkins_helper.jenkins_helper_factory import JenkinsHelperFactory
from constants.jenkins import JENKINS_URLS

class RemoveSlavesTask(Task):

    def remove_slave_from_jenkins(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "name" not in slave:
            self.set_subtask_exception(ValueError(f"name key not found in slave {slave}"))

        name = slave["name"]

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Slave Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Slave Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            slave_doc = eval(slave_pool_helper.get_slave_pool_doc(name))
            self.logger.info(f"Slave with name {name} successfully found in slave-pool")
        except Exception as e:
            exception = f"Cannot find slave with name {name} in slave-pool : {e}"
            self.set_subtask_exception(exception)

        jenkins_host = slave_doc["jenkins_host"]
        ipaddr = slave_doc["ipaddr"]

        try:
            jenkins_helper = JenkinsHelperFactory.fetch_helper(JENKINS_URLS[jenkins_host])
        except Exception as e:
            exception = f"Cannot fetch helper for slave {name}:{ipaddr} : {e}"
            self.set_subtask_exception(exception)

        try:
            res_jenkins_status, res_jenkins_response = jenkins_helper.remove_slave(name)
            self.logger.info(f"Slave with ipaddr {ipaddr} and name {name} successfully removed from jenkins")
        except Exception as e:
            exception = f"Cannot remove slave {ipaddr}:{name} from jenkins : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["remove_slave_from_jenkins"] = [str(res_jenkins_status), str(res_jenkins_response)]

    def remove_slave_from_slave_pool(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "name" not in slave:
            self.set_subtask_exception(ValueError(f"name key not found in slave {slave}"))

        name = slave["name"]

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Slave Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Slave Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            res_slave_pool = slave_pool_helper.delete_slave_pool_doc(name)
            if res_slave_pool:
                self.logger.info(f"Doc with slave name {name} successfully deleted")
            else:
                exception = f"Cannot delete slave name {name} doc in slave-pool"
                self.set_subtask_exception(exception)
        except Exception as e:
            exception = f"Cannot find name {name} doc in slave-pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["remove_slave_from_slave_pool"] = res_slave_pool

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a RemoveSlavesTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of slaves which has to be removed from slave-pool
                        Each slave is a dictionary with many fields
                    - delete_from_jenkins (bool) : True if the slave needs to be deleted from jenkins.
                        False if the slave need not be deleted from jenkins, but just the slave-pool
        """
        task_name = RemoveSlavesTask.__name__
        if max_workers is None:
            max_workers = 100
        super().__init__(task_name, max_workers)

        if "data" not in params or params["data"] is None:
            exception = ValueError(f"Data is not present to delete from slave-pool")
            self.set_exception(exception)
        elif not isinstance(params["data"], list):
            exception = ValueError(f"data param has to be a list : {params['data']}")
            self.set_exception(exception)
        else:
            self.data = params["data"]

        if "delete_from_jenkins" not in params or params["delete_from_jenkins"] is None:
            exception = ValueError(f"delete_from_jenkins is not present in params : {params}")
            self.set_exception(exception)
        elif not isinstance(params["delete_from_jenkins"], bool):
            exception = ValueError(f"delete_from_jenkins param has to be a bool : {params['data']}")
            self.set_exception(exception)
        else:
            self.delete_from_jenkins = params["delete_from_jenkins"]

        for slave in self.data:
            if "name" not in slave:
                exception = ValueError(f"name missing from slave : {slave}")
                self.set_exception(exception)

        self.sub_task_functions = []
        if self.delete_from_jenkins:
            self.sub_task_functions.append(self.remove_slave_from_jenkins)
        self.sub_task_functions.append(self.remove_slave_from_slave_pool)

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

        try:
            self.task_pool_helper.add_results_to_task(self.id, self.task_result.result_json)
        except Exception as e:
            exception = ValueError(f"host_tasks param has to be a list : {params['host_tasks']}")
            self.set_exception(exception)

        return self.task_result.result_json
