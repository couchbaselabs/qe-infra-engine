from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from constants.jenkins import JENKINS_URLS
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper
from helper.jenkins_helper.jenkins_helper_factory import JenkinsHelperFactory

class ReconnectSlavesTask(Task):

    def reconnect_slave(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "name" not in slave:
            self.set_subtask_exception(ValueError(f"name key is missing for the slave {slave}"))
        slave_name = slave["name"]

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Slave Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Slave Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            slave_doc = eval(slave_pool_helper.get_slave_pool_doc(slave_name))
            self.logger.info(f"Doc with slave name {slave_name} successfully found in slave-pool")
        except Exception as e:
            exception = f"Cannot find slave name {slave_name} doc in slave-pool : {e}"
            self.set_subtask_exception(exception)

        ipaddr = slave_doc["ipaddr"]
        jenkins_host = slave_doc["jenkins_host"]

        try:
            jenkins_helper = JenkinsHelperFactory.fetch_helper(JENKINS_URLS[jenkins_host])
        except Exception as e:
            exception = f"Cannot fetch helper for slave {ipaddr}:{slave_name} : {e}"
            self.set_subtask_exception(exception)

        # Step 1 - Reconnect to jenkins
        try:
            status_jenkins, res_jenkins = jenkins_helper.reconnect_slave(slave_name)
            self.logger.info(f"Slave with ipaddr {ipaddr} and name {slave_name} successfully reconnected to jenkins")
        except Exception as e:
            exception = f"Cannot reconnect slave {ipaddr}:{slave_name} to jenkins : {e}"
            self.set_subtask_exception(exception)

        # Step 2 - Change in slave-pool
        try:
            slave_doc["state"] = "online"
            res_upsert = slave_pool_helper.upsert_slave_to_slave_pool(slave_doc)
            if not res_upsert:
                exception = f"Cannot update slave {slave_name} in slave pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for slave {slave_name} updated in slave pool successfuly")
        except Exception as e:
            exception = f"Cannot update slave {slave['ipaddr']} in slave pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["reconnect_slave_from_jenkins"] = [str(status_jenkins), str(res_jenkins)]
        task_result.result_json["update_slave_in_slave_pool"] = res_upsert

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a ReconnectSlavesTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of slaves which has to be reconnected
                        Each slave is a dictionary with many fields
        """
        task_name = ReconnectSlavesTask.__name__
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
            if "name" not in slave:
                exception = ValueError(f"name missing from slave : {slave}")
                self.set_exception(exception)

    def execute(self):
        self.start_task()

        sub_tasks = []
        for slave in self.data:
            params = {"slave" : slave}
            subtaskid = self.add_sub_task(self.reconnect_slave, params)
            sub_tasks.append([slave["name"], subtaskid])
        for doc_key, subtask_id in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            self.task_result.subtasks[doc_key] = task_result

        self.complete_task(result=True)

