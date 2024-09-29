import time
import copy
from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from helper.xen_orchestra_helper.xen_orchestra_factory import XenOrchestraObjectFactory
from constants.doc_templates import VM_TEMPLATE, HOST_TEMPLATE

class AddHostTask(Task):

    def add_host_on_testdb(self, task_result: TaskResult, params: dict) -> None:
        if "label" not in params:
            self.set_subtask_exception(ValueError(f"label key is missing for the host {params}"))

        required_fields = ["host_data", "group", "xen_username", "xen_password"]
        for field in required_fields:
            if field not in params:
                self.set_subtask_exception(ValueError(f"{field} key is missing for the host {params['label']}"))

        host = params["label"]
        host_data = params["host_data"]
        group = params["group"]
        xen_username = params["xen_username"]
        xen_password = params["xen_password"]

        try:
            host_pool_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"

        host_doc = copy.deepcopy(HOST_TEMPLATE)
        host_doc["name"] = host
        host_doc["hostname"] = f'{host}.sc.couchbase.com'

        host_doc["ipaddr"] = ""
        if "address" in host_data:
            host_doc["ipaddr"] = host_data["address"]

        host_doc["cpu"] = -1
        if "CPUs" in host_data:
            if "cpu_count" in host_data["CPUs"]:
                host_doc["cpu"] = int(host_data["CPUs"]["cpu_count"])
        if "name_label" in host_data:
            host_doc["name_label"] = host_data["name_label"]
        host_doc["memory"] = -1
        if "memory" in host_data and "size" in host_data["memory"]:
            host_doc["memory"] = host_data["memory"]["size"]

        host_doc["state"] = ""
        if "power_state" in host_data:
            host_doc["state"] = host_data["power_state"]

        host_doc["poolId"] = ""
        if "$poolId" in host_data:
            host_doc["poolId"] = host_data["$poolId"]
        host_doc["group"] = group
        host_doc["xen_username"] = xen_username
        host_doc["xen_password"] = xen_password
        host_doc["tags"] = {"list" : [], "details" : {}}
        if "rebootRequired" in host_data:
            host_doc["tags"]["details"]["reboot_required"] = host_data["rebootRequired"]
            host_doc["tags"]["list"].append("reboot_required")
        else:
            host_doc["tags"]["details"]["reboot_required"] = False

        try:
            res = host_pool_helper.update_host(doc=host_doc)
            if not res:
                exception = f"Cannot add host {host} to host pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for host {host} added to host pool successfuly")
        except Exception as e:
            exception = f"Cannot add host {host} to host pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["host_doc"] = host_doc

    def add_vms_on_testdb(self, task_result: TaskResult, params: dict) -> None:
        required_fields = ["label", "group", "vms_data"]
        for key in required_fields:
            if key not in params:
                self.set_subtask_exception(ValueError(f"{key} key is missing for the host {params}"))

        group = params["group"]
        host = params["label"]
        vms_data = params["vms_data"]

        if len(vms_data) == 0:
            self.set_subtask_exception(ValueError(f"No VMs found for host {host}"))

        try:
            host_pool_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}

        for vm in vms_data:
            vm_doc = copy.deepcopy(VM_TEMPLATE)

            vm_doc["addresses"] = {}
            if "addresses" in vm:
                vm_doc["addresses"] = vm["addresses"]

            vm_doc["cpu"] = -1
            if "CPUs" in vm and "number" in vm["CPUs"]:
                vm_doc["cpu"] = int(vm["CPUs"]["number"])

            vm_doc["mainIpAddress"] = ""
            if "mainIpAddress" in vm:
                vm_doc["mainIpAddress"] = vm["mainIpAddress"]

            vm_doc["memory"] = -1
            if "memory" in vm and "size" in vm["memory"]:
                vm_doc["memory"] = vm["memory"]["size"]

            vm_doc["name_label"] = ""
            if "name_label" in vm_doc:
                vm_doc["name_label"] = vm["name_label"]

            vm_doc["os_version"] = "unknown"
            if "os_version" in vm:
                if vm["os_version"] and "name" in vm["os_version"]:
                    vm_doc["os_version"] = vm["os_version"]["name"]

            vm_doc["state"] = ""
            if "power_state" in vm:
                vm_doc["state"] = vm["power_state"]

            vm_doc["poolId"] = ""
            if "$poolId" in vm:
                vm_doc["poolId"] = vm["$poolId"]
            vm_doc["group"] = group
            vm_doc["host"] = host
            vm_doc["tags"] = {"list" : [], "details" : {}}
            task_result.result_json[vm_doc["name_label"]] = {}
            try:
                res = host_pool_helper.update_vm(doc=vm_doc)
                if not res:
                    exception = f"Cannot add vm {vm_doc['name_label']} to host pool"
                    self.set_subtask_exception(exception)
                self.logger.info(f"Document for vm {vm_doc['name_label']} added to host pool successfuly")
            except Exception as e:
                exception = f"Cannot add vm {vm_doc['name_label']} to host pool : {e}"
                self.set_subtask_exception(exception)

            task_result.result_json[vm_doc["name_label"]]["vm_doc"] = vm_doc

    def _remove_host_from_xen_orchestra(self, host):
        try:
            xen_orchestra_helper = XenOrchestraObjectFactory.fetch_helper()
            self.logger.info(f"Connection to xen orchestra successful")
        except Exception as e:
            exception = f"Cannot connect to XenOrchestra : {e}"
            self.set_subtask_exception(exception)

        label = host["label"]
        hostname = host["hostname"] if "hostname" in host else f'{label}.sc.couchbase.com'

        try:
            remove_result = xen_orchestra_helper.remove_host(label=label,
                                                             host=hostname)
        except Exception as e:
            exception = f"Cannot remove host {label} from XenOrchestra : {e}"
            self.set_subtask_exception(exception)

        if not remove_result:
            exception = f"Cannot remove host {label} from XenOrchestra"
            self.set_subtask_exception(exception)

    def add_hosts_sub_task(self, task_result: TaskResult, params: dict) -> None:
        if "host" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        host = params["host"]
        if "label" not in host:
            self.set_subtask_exception(ValueError(f"label key is missing for the host {host}"))

        label = host["label"]

        required_fields = ["username", "password", "group"]
        for field in required_fields:
            if field not in host:
                self.set_subtask_exception(ValueError(f"{field} key is missing for the host {label}"))

        try:
            HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            xen_orchestra_helper = XenOrchestraObjectFactory.fetch_helper()
            self.logger.info(f"Connection to xen orchestra successful")
        except Exception as e:
            exception = f"Cannot connect to XenOrchestra : {e}"
            self.set_subtask_exception(exception)

        label = host["label"]
        hostname = host["hostname"] if "hostname" in host else f'{label}.sc.couchbase.com'
        group = host["group"]
        xen_orchestra_username = host["username"]
        xen_orchestra_password = host["password"]

        try:
            server_status = xen_orchestra_helper.get_server_status(label=label,
                                                                host=hostname)
            if server_status:
                self.logger.critical(f"Host with label {label} and hostname {hostname} already exists in Xen orchestra")
                self.logger.critical(f"Deleting host with label {label} and hostname {hostname} from Xen orchestra")
                self._remove_host_from_xen_orchestra(host)
        except Exception as e:
            if str(e) == f"Host with label {label} and host {hostname} not found":
                pass
            else:
                exception = f"Cannot fetch server status from XenOrchestra : {e}"
                self.set_subtask_exception(exception)

        try:
            xen_orchestra_helper.add_host(label=label,
                                          host=hostname,
                                          username=xen_orchestra_username,
                                          password=xen_orchestra_password)
        except Exception as e:
            exception = f"Cannot add server to XenOrchestra : {e}"
            self.set_subtask_exception(exception)

        time.sleep(10)

        try:
            server_status = xen_orchestra_helper.get_server_status(label=label,
                                                                   host=hostname)
        except Exception as e:
            exception = f"Cannot fetch server status from XenOrchestra : {e}"
            self.set_subtask_exception(exception)

        if "error" in server_status:
            exception = f"Cannot fetch server status from XenOrchestra : {server_status}"
            self.set_subtask_exception(exception)

        try:
            hosts_data = xen_orchestra_helper.fetch_list_hosts(label=label,
                                                               host=hostname)
        except Exception as e:
            exception = f"Cannot fetch host list data from XenOrchestra : {e}"
            self.set_subtask_exception(exception)

        try:
            vms_data = xen_orchestra_helper.fetch_list_vms(label=label,
                                                           host=hostname)
        except Exception as e:
            exception = f"Cannot fetch vm list data from XenOrchestra : {e}"
            self.set_subtask_exception(exception)

        self._remove_host_from_xen_orchestra(host)

        params = {
            "label" : label,
            "host_data" : hosts_data,
            "group" : group,
            "xen_username" : xen_orchestra_username,
            "xen_password" : xen_orchestra_password
        }
        subtask_add_host_id = self.add_sub_task(self.add_host_on_testdb, params)

        params = {
            "label" : label,
            "vms_data" : vms_data,
            "group" : group
        }
        subtask_add_vms_id = self.add_sub_task(self.add_vms_on_testdb, params)

        task_result_add_host = self.get_sub_task_result(subtask_id=subtask_add_host_id)
        task_result_add_vms = self.get_sub_task_result(subtask_id=subtask_add_vms_id)

        task_result.subtasks["adding_host_data"] = task_result_add_host
        task_result.subtasks["adding_vm_data"] = task_result_add_vms

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a AddHostTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - data (list) : List of nodes which has to be added to server-pool
                        Each node is a dictionary with many fields
        """
        task_name = AddHostTask.__name__
        if max_workers is None:
            max_workers = 2000
        super().__init__(task_name, max_workers)

        if "data" not in params or params["data"] is None:
            exception = ValueError(f"Data is not present to add to host-pool")
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
            subtask = self.add_hosts_sub_task
            subtaskid = self.add_sub_task(subtask, params)
            sub_tasks.append([host["label"], subtaskid])
        for host, subtask_id in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            self.task_result.subtasks[host] = task_result

        self.complete_task(result=True)