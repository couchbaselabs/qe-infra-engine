import time
from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from tasks.host_maintenance.host_operations.update_hosts import UpdateHostsTask
from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from constants.doc_templates import VM_TEMPLATE

# TODO tasks
'''
1. Check the number of VMs present in server-pool per host
'''

class HostHealthMonitorTask(Task):

    def check_for_vms_state(self, task_result: TaskResult, params: dict) -> None:
        if "host_doc" not in params:
            self.set_subtask_exception(ValueError(f"host_doc key is missing in params {params}"))
        if "vm_docs" not in params:
            self.set_subtask_exception(ValueError(f"vm_docs key is missing in params {params}"))

        host_doc = params["host_doc"]
        host = host_doc["name"]
        vm_docs = params["vm_docs"]

        try:
            host_pool_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        if "tags" not in host_doc:
            host_doc["tags"] = {}

        host_doc["tags"]["vm_states"] = {}
        for vm in vm_docs:
            if vm["state"] not in host_doc["tags"]["vm_states"]:
                host_doc["tags"]["vm_states"][vm["state"]] = 0
            host_doc["tags"]["vm_states"][vm["state"]] += 1

        try:
            res = host_pool_helper.update_host(host_doc)
            if not res:
                exception = f"Cannot upsert host {host} with halted-vm checks to host pool"
            self.logger.info(f"Document for host {host} with halted-vm checks upserted to host pool successfuly")
        except Exception as e:
            exception = f"Cannot upsert host {host} with halted-vm checks to host pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["vm_states"] = host_doc["tags"]["vm_states"]

    def check_for_cpu_usage(self, task_result: TaskResult, params: dict) -> None:
        if "host_doc" not in params:
            self.set_subtask_exception(ValueError(f"host_doc key is missing in params {params}"))
        if "vm_docs" not in params:
            self.set_subtask_exception(ValueError(f"vm_docs key is missing in params {params}"))

        host_doc = params["host_doc"]
        host = host_doc["name"]
        vm_docs = params["vm_docs"]

        try:
            host_pool_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        cpu = 0
        for vm in vm_docs:
            if vm["state"] == "Running":
                cpu += int(vm["cpu"])

        if "tags" not in host_doc:
            host_doc["tags"] = {}

        if int(host_doc["cpu"]) != 0:
            host_doc["tags"]["allocated_cpu_utilization"] = cpu / int(host_doc["cpu"]) * 100
        else:
            host_doc["tags"]["allocated_cpu_utilization"] = 0

        try:
            res = host_pool_helper.update_host(host_doc)
            if not res:
                exception = f"Cannot upsert host {host} with cpu-utilization checks to host pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for host {host} with cpu-utilization checks upserted to host pool successfuly")
        except Exception as e:
            exception = f"Cannot upsert host {host} with cpu-utilization checks to host pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["allocated_cpu_utilization"] = host_doc["tags"]["allocated_cpu_utilization"]

    def check_for_mem_usage(self, task_result: TaskResult, params: dict) -> None:
        if "host_doc" not in params:
            self.set_subtask_exception(ValueError(f"host_doc key is missing in params {params}"))
        if "vm_docs" not in params:
            self.set_subtask_exception(ValueError(f"vm_docs key is missing in params {params}"))

        host_doc = params["host_doc"]
        host = host_doc["name"]
        vm_docs = params["vm_docs"]

        try:
            host_pool_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        memory = 0
        for vm in vm_docs:
            if vm["state"] == "Running":
                memory += int(vm["memory"])

        if "tags" not in host_doc:
            host_doc["tags"] = {}

        if int(host_doc["memory"]) == 0:
            host_doc["tags"]["allocated_memory_utilization"] = 0
        else:
            host_doc["tags"]["allocated_memory_utilization"] = memory / int(host_doc["memory"]) * 100


        try:
            res = host_pool_helper.update_host(host_doc)
            if not res:
                exception = f"Cannot upsert host {host} with memory-utilization checks to host pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for host {host} with memory-utilization checks upserted to host pool successfuly")
        except Exception as e:
            exception = f"Cannot upsert host {host} with memory-utilization checks to host pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["allocated_memory_utilization"] = host_doc["tags"]["allocated_memory_utilization"]

    def check_vm_network(self, task_result: TaskResult, params: dict) -> None:
        if "vm_doc" not in params:
            self.set_subtask_exception(ValueError(f"vm_doc key is missing in params {params}"))

        vm_doc = params["vm_doc"]

        try:
            host_sdk_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        if "tags" not in vm_doc:
            vm_doc["tags"] = {}

        if "addresses" not in vm_doc:
            vm_doc["tags"]["addresses_available"] = False
        else:
            present = False
            for key in vm_doc["addresses"]:
                if "ipv4" in key:
                    present = True
            vm_doc["tags"]["addresses_available"] = present

        if "mainIpAddress" not in vm_doc:
            vm_doc["tags"]["mainIpAddress_available"] = False
        else:
            if len(vm_doc["mainIpAddress"].split(".")) != 4:
                vm_doc["tags"]["mainIpAddress_available"] = False
            else:
                vm_doc["tags"]["mainIpAddress_available"] = True

        try:
            res = host_sdk_helper.update_vm(vm_doc)
            if not res:
                exception = f"Cannot upsert vm {vm_doc['name_label']} with network-consistency checks to host pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for vm {vm_doc['name_label']} with network-consistency checks upserted to host pool successfuly")
        except Exception as e:
            exception = f"Cannot upsert vm {vm_doc['name_label']} with network-consistency checks to host pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["addresses_available"] = vm_doc["tags"]["addresses_available"]
        task_result.result_json["mainIpAddress_available"] = vm_doc["tags"]["mainIpAddress_available"]

    def check_vm_os_version(self, task_result: TaskResult, params: dict) -> None:
        if "vm_doc" not in params:
            self.set_subtask_exception(ValueError(f"vm_doc key is missing in params {params}"))

        vm_doc = params["vm_doc"]

        try:
            host_sdk_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        if "tags" not in vm_doc:
            vm_doc["tags"] = {}

        if "os_version" not in vm_doc:
            vm_doc["tags"]["os_version_available"] = False
        elif vm_doc["os_version"] == "":
            vm_doc["tags"]["os_version_available"] = False
        else:
            vm_doc["tags"]["os_version_available"] = True

        try:
            res = host_sdk_helper.update_vm(vm_doc)
            if not res:
                exception = f"Cannot upsert vm {vm_doc['name_label']} with os-version-consistency checks to host pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for vm {vm_doc['name_label']} with os-version-consistency checks upserted to host pool successfuly")
        except Exception as e:
            exception = f"Cannot upsert vm {vm_doc['name_label']} with os-version-consistency checks to host pool : {e}"

        task_result.result_json = {}
        task_result.result_json["os_version_available"] = vm_doc["tags"]["os_version_available"]

    def check_vms_in_server_pool(self, task_result: TaskResult, params: dict) -> None:
        if "vm_doc" not in params:
            self.set_subtask_exception(ValueError(f"vm_doc key is missing in params {params}"))

        vm_doc = params["vm_doc"]

        try:
            host_sdk_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        addresses = set(vm_doc["addresses"].values())
        addresses.add(vm_doc["mainIpAddress"])

        query_result = server_pool_helper.fetch_node_by_ipaddr(ipaddr=list(addresses))
        nodes_ipaddrs = []
        for row in query_result:
            nodes_ipaddrs.append(row["_default"]["ipaddr"])

        ip_present = True
        if len(nodes_ipaddrs) == 0:
            ip_present = False
        else:
            ip_present = True

        if "tags" not in vm_doc:
            vm_doc["tags"] = {}

        vm_doc["tags"]["vm_in_server_pool"] = ip_present

        try:
            res = host_sdk_helper.update_vm(vm_doc)
            if not res:
                exception = f"Cannot upsert vm {vm_doc['name_label']} with server-pool-consistency checks to host pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for vm {vm_doc['name_label']} with server-pool-consistency checks upserted to host pool successfuly")
        except Exception as e:
            exception = f"Cannot upsert vm {vm_doc['name_label']} with server-pool-consistency checks to host pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json ["vm_in_server_pool"] = vm_doc["tags"]["vm_in_server_pool"]

    def check_vm_field_consistency(self, task_result: TaskResult, params: dict) -> None:
        if "vm_doc" not in params:
            self.set_subtask_exception(ValueError(f"vm_doc key is missing in params {params}"))

        vm_doc = params["vm_doc"]

        try:
            host_sdk_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        fields_required = list(VM_TEMPLATE.keys())

        fields_absent = []
        for field in fields_required:
            if field not in vm_doc:
                fields_absent.append(field)

        fields_extra = []
        for field in vm_doc:
            if field not in fields_required:
                fields_extra.append(field)

        if "tags" not in vm_doc:
            vm_doc["tags"] = {}

        if len(fields_absent) == 0 and len(fields_extra) == 0:
            vm_doc["tags"]["field_consistency"] = {
                "fields_match" : True
            }
        else:
            vm_doc["tags"]["field_consistency"] = {
                "fields_match" : False
            }
            if len(fields_absent) > 0:
                vm_doc["tags"]["field_consistency"]["fields_absent"] = fields_absent
            if len(fields_extra) > 0:
                vm_doc["tags"]["field_consistency"]["fields_extra"] = fields_extra

        try:
            res = host_sdk_helper.update_vm(vm_doc)
            if not res:
                exception = f"Cannot upsert vm {vm_doc['name_label']} with field-consistency checks to host pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for vm {vm_doc['name_label']} with field-consistency checks upserted to host pool successfuly")
        except Exception as e:
            exception = f"Cannot upsert vm {vm_doc['name_label']} with field-consistency checks to host pool : {e}"

        task_result.result_json = {}
        task_result.result_json["field_consistency"] = vm_doc["tags"]["field_consistency"]

    def _get_vm_sub_task_names(self):
        all_tasks_dic = {
            "check vm os version" : self.check_vm_os_version,
            "check vm network" : self.check_vm_network,
            "vm server pool consistency check" : self.check_vms_in_server_pool,
            "vm field consistency check" : self.check_vm_field_consistency,
        }
        return all_tasks_dic

    def _get_host_sub_task_names(self):
        all_tasks_dic = {
            "allocated memory utilization check" : self.check_for_mem_usage,
            "allocated cpu utilization check" : self.check_for_cpu_usage,
            "check vms state" : self.check_for_vms_state
        }
        return all_tasks_dic

    def host_sub_tasks(self, task_result: TaskResult, params: dict) -> None:
        if "host_doc" not in params:
            self.set_subtask_exception(ValueError(f"host_doc key is missing in params {params}"))
        if "vm_docs" not in params:
            self.set_subtask_exception(ValueError(f"vm_docs key is missing in params {params}"))

        for task in self.host_sub_task_names:
            subtask_id = self.add_sub_task(self.host_sub_task_names[task], params)
            sub_task_result = self.get_sub_task_result(subtask_id)
            task_result.subtasks[task] = sub_task_result

    def vm_sub_tasks(self, task_result: TaskResult, params: dict) -> None:
        if "vm_doc" not in params:
            self.set_subtask_exception(ValueError(f"vm_doc key is missing in params {params}"))

        for task in self.vm_sub_task_names:
            subtask_id = self.add_sub_task(self.vm_sub_task_names[task], params)
            sub_task_result = self.get_sub_task_result(subtask_id)
            task_result.subtasks[task] = sub_task_result
    
    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a HostHealthMonitorTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - update_docs (bool) : A boolean value indicating whether the documents in host-pool have to be updated
                    - group (list, optional) : List of group for which the task should be run.
                        If it is not provided, all group are considered
                    - host_tasks (list, optional): List of tasks which have to be run for the host collection
                        If it is not provided all tasks are considered
                    - vm_tasks (list, optional): List of tasks which have to be run for the vms collection
                        If it is not provided all tasks are considered
        """
        task_name = HostHealthMonitorTask.__name__
        if max_workers is None:
            max_workers = 100
        super().__init__(task_name, max_workers)

        if "update_docs" not in params:
            exception = ValueError(f"update_docs is not present in params : {params}")
            self.set_exception(exception)
        elif not isinstance(params["update_docs"], bool):
            exception = ValueError(f"update_docs needs to be a boolean value {params}")
            self.set_exception(exception)
        else:
            self.update_docs = params["update_docs"]

        if "group" not in params:
            self.group = []
        elif params["group"] is None:
            self.group = []
        elif not isinstance(params["group"], list):
            exception = ValueError(f"group param has to be a list : {params['group']}")
            self.set_exception(exception)
        else:
            self.group = params["group"]

        all_host_sub_tasks = self._get_host_sub_task_names()
        if "host_tasks" not in params:
            self.host_sub_task_names = all_host_sub_tasks
        elif params["host_tasks"] is None:
            self.host_sub_task_names = all_host_sub_tasks
        elif not isinstance(params["host_tasks"], list):
            exception = ValueError(f"host_tasks param has to be a list : {params['host_tasks']}")
            self.set_exception(exception)
        else:
            self.host_sub_task_names = {}
            for task in params["host_tasks"]:
                if task not in all_host_sub_tasks:
                    exception = ValueError(f"Invalid Sub task name : {task}")
                    self.set_exception(exception)
                else:
                    self.host_sub_task_names[task] = all_host_sub_tasks[task]

        all_vm_sub_tasks = self._get_vm_sub_task_names()
        if "vm_tasks" not in params:
            self.vm_sub_task_names = all_vm_sub_tasks
        elif params["vm_tasks"] is None:
            self.vm_sub_task_names = all_vm_sub_tasks
        elif not isinstance(params["vm_tasks"], list):
            exception = ValueError(f"vm_tasks param has to be a list : {params['vm_tasks']}")
            self.set_exception(exception)
        else:
            self.vm_sub_task_names = {}
            for task in params["vm_tasks"]:
                if task not in all_vm_sub_tasks:
                    exception = ValueError(f"Invalid Sub task name : {task}")
                    self.set_exception(exception)
                else:
                    self.vm_sub_task_names[task] = all_vm_sub_tasks[task]

    def execute(self):
        self.start_task()

        try:
            host_pool_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_exception(exception)

        try:
            if len(self.group) > 0:
                query_result = host_pool_helper.fetch_hosts_by_group(self.group)
            else:
                query_result = host_pool_helper.fetch_all_host()
        except Exception as e:
            exception = f"Cannot fetch all hosts from host-pool : {e}"
            self.set_exception(exception)

        host_docs = []
        for row in query_result:
            row[host_pool_helper.host_collection_name]["doc_key"] = row["id"]
            host_docs.append(row[host_pool_helper.host_collection_name])

        if self.update_docs:
            params = {}
            params["data"] = []
            for host in host_docs:
                host_info = {
                    "username" : host["xen_username"],
                    "password" : host["xen_password"],
                    "group" : host["group"],
                    "label" : host["name"],
                    "hostname" : host["ipaddr"]
                }
                params["data"].append(host_info)
            self.update_task = UpdateHostsTask(params)
            self.update_task.execute()
            self.update_task.generate_json_result()
            self.task_result.subtasks["update_docs_task"] = self.update_task.task_result

            # Waiting for a while to let the cluster catch up with the latest updates
            time.sleep(60)

            # Need to fetch fresh updated data
            try:
                if len(self.group) > 0:
                    query_result = host_pool_helper.fetch_hosts_by_group(self.group)
                else:
                    query_result = host_pool_helper.fetch_all_host()
            except Exception as e:
                exception = f"Cannot fetch all hosts from host-pool : {e}"
                self.set_exception(exception)

            host_docs = []
            for row in query_result:
                row[host_pool_helper.host_collection_name]["doc_key"] = row["id"]
                host_docs.append(row[host_pool_helper.host_collection_name])

        host_sub_task_ids = []
        vm_sub_tasks_ids = []
        for host_doc in host_docs:
            try:
                query_result = host_pool_helper.fetch_vms_by_host(host_doc["name"])
            except Exception as e:
                exception = f"Cannot fetch vms by host from host-pool : {e}"
                self.set_exception(exception)

            vm_docs = []
            try:
                for row in query_result:
                    row[host_pool_helper.vm_collection_name]["doc_key"] = row["id"]
                    vm_docs.append(row[host_pool_helper.vm_collection_name])
            except Exception as e:
                exception = f"Cannot fetch vms by host from host-pool : {e}"
                self.set_exception(exception)

            params = {
                "host_doc" : host_doc,
                "vm_docs" : vm_docs
            }
            host_sub_task_id = self.add_sub_task(self.host_sub_tasks, params)
            host_sub_task_ids.append([host_doc["name"], host_sub_task_id])
            
            for vm_doc in vm_docs:
                params = {
                    "vm_doc" : vm_doc
                }
                vm_sub_task_id = self.add_sub_task(self.vm_sub_tasks, params)
                vm_sub_tasks_ids.append([host_doc["name"],vm_doc["name_label"],vm_sub_task_id])

        self.task_result.subtasks["host_tasks"] = {}
        for host, subtask_id in host_sub_task_ids:
            self.task_result.subtasks["host_tasks"][host] = self.get_sub_task_result(subtask_id)
        
        self.task_result.subtasks["vm_tasks"] = {}
        for host, vm, subtask_id in vm_sub_tasks_ids:
            if host not in self.task_result.subtasks["vm_tasks"]:
                self.task_result.subtasks["vm_tasks"][host] = {}
            self.task_result.subtasks["vm_tasks"][host][vm] = self.get_sub_task_result(subtask_id)

        self.complete_task(result=True)
    
    def generate_json_result(self, timeout=3600):
        TaskResult.generate_json_result(self.task_result)
        print(self.task_result.result_json)
        result_json = {}
        if self.update_docs:
           result_json["update_docs"] = self.task_result.result_json["update_docs_task"]
        result_json["monitor_task"] = {}
        for host in self.task_result.subtasks["host_tasks"]:
            result_json["monitor_task"][host] = {}
            result_json["monitor_task"][host]["host_tasks"] = TaskResult.generate_json_result(self.task_result.subtasks["host_tasks"][host])
            result_json["monitor_task"][host]["vm_tasks"] = {}
            for vm in self.task_result.subtasks["vm_tasks"][host]:
                result_json["monitor_task"][host]["vm_tasks"][vm] = TaskResult.generate_json_result(self.task_result.subtasks["vm_tasks"][host][vm])
        self.task_result.result_json = result_json
        return self.task_result.result_json
