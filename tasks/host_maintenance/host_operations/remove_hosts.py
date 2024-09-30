from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper

class RemoveHostsTask(Task):

    def remove_host_doc(self, task_result: TaskResult, params: dict) -> None:
        if "host" not in params:
            exception = ValueError(f"host field not present for host {params}")
            self.set_subtask_exception(exception)

        required_fields = ["label", "hostname"]
        for field in required_fields:
            if field not in params["host"]:
                exception = ValueError(f"Field {field} not present for host {params}")
                self.set_subtask_exception(exception)

        host = params["host"]

        try:
            host_sdk_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            res = host_sdk_helper.remove_host(host["label"])
            if not res:
                exception = f"Cannot remove host {host['label']} from host pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for host {host['label']} removed from host pool successfuly")
        except Exception as e:
            exception = f"Cannot remove vm {host['label']} from host pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = str(True)

    def remove_vm_docs(self, task_result: TaskResult, params: dict) -> None:
        if "host" not in params:
            exception = ValueError(f"host field not present for host {params}")
            self.set_subtask_exception(exception)

        try:
            host_sdk_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        required_fields = ["label", "hostname"]
        for field in required_fields:
            if field not in params["host"]:
                exception = ValueError(f"Field {field} not present for host {params}")
                self.set_subtask_exception(exception)

        host = params["host"]

        try:
            query_result = host_sdk_helper.fetch_vms_by_host(host["label"])
        except Exception as e:
            exception = f"Cannot fetch all vms from host-pool : {e}"
            self.set_subtask_exception(exception)

        vm_docs = []
        for row in query_result:
            row[host_sdk_helper.vm_collection_name]["doc_key"] = row["id"]
            vm_docs.append(row[host_sdk_helper.vm_collection_name])

        task_result.result_json = {}
        for doc in vm_docs:
            doc_key = doc["doc_key"]
            try:
                res = host_sdk_helper.remove_vm(doc_key)
                if not res:
                    exception = f"Cannot remove vm {doc_key} from host pool"
                    task_result.result_json[doc_key] = exception
                else:
                    self.logger.info(f"Document for vm {doc_key} removed from host pool successfuly")
                    task_result.result_json[doc_key] = str(True)
            except Exception as e:
                exception = f"Cannot remove vm {doc_key} from host pool : {e}"
                task_result.result_json[doc_key] = exception

    def remove_host_vm_docs(self, task_result: TaskResult, params: dict) -> None:

        if "host" not in params:
            exception = ValueError(f"host field not present for host {params}")
            self.set_subtask_exception(exception)

        required_fields = ["label", "hostname"]
        for field in required_fields:
            if field not in params["host"]:
                exception = ValueError(f"Field {field} not present for host {params}")
                self.set_subtask_exception(exception)

        try:
            HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        subtask_remove_vms_id = self.add_sub_task(self.remove_vm_docs, params)
        subtask_remove_host_id = self.add_sub_task(self.remove_host_doc, params)

        task_result_remove_host = self.get_sub_task_result(subtask_id=subtask_remove_host_id)
        task_result_remove_vms = self.get_sub_task_result(subtask_id=subtask_remove_vms_id)

        task_result.subtasks["remove_vms_data"] = task_result_remove_vms
        task_result.subtasks["remove_host_data"] = task_result_remove_host

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a RemoveHostsTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of nodes which has to be added to server-pool
                        Each node is a dictionary with many fields
        """
        task_name = RemoveHostsTask.__name__
        if max_workers is None:
            max_workers = 100
        super().__init__(task_name, max_workers)

        if "data" not in params or params["data"] is None:
            exception = ValueError(f"Data is not present to remove from host-pool")
            self.set_exception(exception)
        elif not isinstance(params["data"], list):
            exception = ValueError(f"data param has to be a list : {params['data']}")
            self.set_exception(exception)
        else:
            self.data = params["data"]

        for count, host in enumerate(self.data):
            if "label" not in host:
                exception = ValueError(f"label missing from host : {host}")
                self.set_exception(exception)

    def execute(self):
        self.start_task()

        sub_tasks = []
        for host in self.data:
            params = {"host" : host}
            subtask = self.remove_host_vm_docs
            subtaskid = self.add_sub_task(subtask, params)
            sub_tasks.append([host["label"], subtaskid])
        for host, subtask_id in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            self.task_result.subtasks[host] = task_result

        self.complete_task(result=True)

    def generate_json_result(self, timeout=3600):
        super().generate_json_result(timeout)

        try:
            self.task_pool_helper.add_results_to_task(self.id, self.task_result.result_json)
        except Exception as e:
            exception = ValueError(f"host_tasks param has to be a list : {params['host_tasks']}")
            self.set_exception(exception)