from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from tasks.host_maintenance.host_operations.add_hosts import AddHostTask
from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from constants.task_states import TaskStates

class UpdateHostsTask(Task):
    def delete_host_vm_docs(self, task_result: TaskResult, params: dict) -> None:
        if self.add_host_task.task_result.state != TaskStates.COMPLETED:
            self.set_subtask_exception("Cannot delete host/vm docs before updation of docs")

        if "host" not in params:
            self.set_subtask_exception(ValueError(f"host not found in params {params}"))

        required_fields = ["label", "hostname"]
        for field in required_fields:
            if field not in params["host"]:
                self.set_subtask_exception(ValueError(f"{field} key is missing for the host {params["host"]}"))

        host = params["host"]

        try:
            host_sdk_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

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
        update_hosts_task_res = TaskResult.generate_json_result(self.add_host_task.task_result)
        for vm in vm_docs:
            update_host_subtask = update_hosts_task_res[host["label"]]
            if vm["name_label"] not in update_host_subtask["adding_vm_data"]:
                try:
                    res = host_sdk_helper.remove_vm(vm["name_label"])
                    if not res:
                        exception = f"Cannot remove vm {vm['name_label']} from host pool"
                        task_result.result_json[vm["name_label"]] = exception
                    else:
                        self.logger.info(f"Document for vm {vm['name_label']} removed from host pool successfuly")
                        task_result.result_json[vm["name_label"]] = str(True)
                except Exception as e:
                    exception = f"Cannot remove vm {vm['name_label']} from host pool : {e}"
                    task_result.result_json[vm["name_label"]] = exception

    def _fetch_host_doc(self, host_label):
        try:
            host_sdk_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_exception(exception)

        try:
            host_doc = eval(host_sdk_helper.fetch_host(host_label))
        except Exception as e:
            exception = f"Cannot fetch host doc for {host_label} from host-pool : {e}"
            self.set_exception(exception)

        return host_doc

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a UpdateHostsTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of nodes which has to be added to server-pool
                        Each node is a dictionary with many fields
        """
        task_name = UpdateHostsTask.__name__
        if max_workers is None:
            max_workers = 100
        super().__init__(task_name, max_workers)

        if "data" not in params or params["data"] is None:
            exception = ValueError(f"Data is not present to update host-pool")
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

        params = {}
        params["data"] = []
        for host in self.data:
            host_doc = self._fetch_host_doc(host["label"])
            host_info = {
                "username" : host_doc["xen_username"],
                "password" : host_doc["xen_password"],
                "group" : host_doc["group"],
                "label" : host_doc["name"]
            }
            params["data"].append(host_info)
        self.add_host_task = AddHostTask(params)

    def execute(self):
        self.start_task()

        self.add_host_task.execute()
        self.task_result.subtasks["update_host_data"] = self.add_host_task.task_result

        sub_tasks = []
        for host in self.data:
            params = {"host" : host}
            subtask = self.delete_host_vm_docs
            subtaskid = self.add_sub_task(subtask, params)
            sub_tasks.append([host["label"], subtaskid])
        for host, subtask_id in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            if "delete_vms_data" not in self.task_result.subtasks:
                self.task_result.subtasks["delete_vms_data"] = {}
            self.task_result.subtasks["delete_vms_data"][host] = task_result

        self.complete_task(result=True)
