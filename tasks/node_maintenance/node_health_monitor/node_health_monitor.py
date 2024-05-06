import time
import paramiko, paramiko.ssh_exception
import socket
import inspect
from tasks.task import Task
from tasks.sub_task import SubTask
from constants.task_states import SubTaskStates, TaskStates
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
from constants.doc_templates import NODE_TEMPLATE


# TODO tasks
'''
1. Checking state and move from booked to available if its in booked state for more than 48hrs
2. Checking status of ntp
3. Checking status of directories and permissions on the nodes
4. Checking status of reserved nodes
'''

class NodeHealthMonitorTask(Task):

    class CheckConnectivitySubTask(SubTask):
        def __init__(self, params:dict):
            """
            Initialize a CheckConnectivitySubTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - node (dict) : The json node document in dict from the testdb
            """
            sub_task_name = NodeHealthMonitorTask.CheckConnectivitySubTask.__name__
            super().__init__(sub_task_name)
            if "node" not in params:
                self.set_exception(ValueError("Invalid arguments passed"))
            self.node_doc = params["node"]

        def generate_json_result(self, timeout=3600):
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.state != SubTaskStates.COMPLETED:
                    time.sleep(10)
                    continue
                if self.result:
                    self.result_json = {}
                    self.result_json["connection_check"] = self.node_doc["tags"]["connection_check"]
                    return self.result_json
                else:
                    return self.exception

        def execute_sub_task(self):
            self.start_sub_task()

            ipaddr = self.node_doc["ipaddr"]

            try:
                server_pool_helper = ServerPoolSDKHelper()
                self.logger.info(f"Connection to Server Pool successful")
            except Exception as e:
                exception = f"Cannot connect to Server Pool using SDK : {e}"
                self.set_exception(exception)

            if "tags" not in self.node_doc:
                self.node_doc["tags"] = {}

            try:
                RemoteConnectionObjectFactory.fetch_helper(ipaddr,"root","couchbase")
                self.node_doc["tags"]["connection_check"] = True
                # self.node_doc["state"] = "available" \
                #     if self.node_doc["state"] == "unreachable"\
                #         else self.node_doc["state"]
            except Exception as e:
                self.node_doc["tags"]["connection_check"] = False
                # self.node_doc["state"] = "unreachable"

            try:
                res = server_pool_helper.upsert_node_to_server_pool(self.node_doc)
                if not res:
                    exception = f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool"
                    self.set_exception(exception)

                self.logger.info(f"Document for node {ipaddr} with node-connectivity checks upserted to server pool successfuly")

            except Exception as e:
                exception = f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool : {e}"
                self.set_exception(exception)

            self.complete_sub_task(result=True)

    class CheckConnectivity2SubTask(SubTask):
        def __init__(self, params:dict):
            """
            Initialize a CheckConnectivity2SubTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - node (dict) : The json node document in dict from the testdb
            """
            sub_task_name = NodeHealthMonitorTask.CheckConnectivity2SubTask.__name__
            super().__init__(sub_task_name)
            if "node" not in params:
                self.set_exception(ValueError("Invalid arguments passed"))
            self.node_doc = params["node"]

        def generate_json_result(self, timeout=3600):
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.state != SubTaskStates.COMPLETED:
                    time.sleep(10)
                    continue
                if self.result:
                    self.result_json = {}
                    self.result_json["connection_check"] = self.node_doc["tags"]["connection_check"]
                    if "connection_check_err" in self.node_doc["tags"]:
                        self.result_json["connection_check_err"] = self.node_doc["tags"]["connection_check_err"]
                    return self.result_json
                else:
                    return self.exception

        def execute_sub_task(self):
            self.start_sub_task()

            ipaddr = self.node_doc["ipaddr"]

            try:
                server_pool_helper = ServerPoolSDKHelper()
                self.logger.info(f"Connection to Server Pool successful")
            except Exception as e:
                exception = f"Cannot connect to Server Pool using SDK : {e}"
                self.set_exception(exception)

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            retries = 5
            connection = False
            connection_errors = set()
            for retry in range(retries):
                self.logger.info(f'Checking ssh connectivity for node {ipaddr} retry {retry + 1} / {retries}')
                try:
                    ssh.connect(ipaddr,
                                username="root",
                                password="couchbase")
                    ssh.close()
                    connection = True
                    connection_errors.add(None)
                    break
                except paramiko.PasswordRequiredException as e:
                    connection = False
                    connection_errors.add("paramiko.PasswordRequiredException")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except paramiko.BadAuthenticationType as e:
                    connection = False
                    connection_errors.add("paramiko.BadAuthenticationType")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except paramiko.AuthenticationException as e:
                    connection = False
                    connection_errors.add("paramiko.AuthenticationException")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except paramiko.BadHostKeyException as e:
                    connection = False
                    connection_errors.add("paramiko.BadHostKeyException")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except paramiko.ChannelException as e:
                    connection = False
                    connection_errors.add("paramiko.ChannelException")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except paramiko.ProxyCommandFailure as e:
                    connection = False
                    connection_errors.add("paramiko.ProxyCommandFailure")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except paramiko.ConfigParseError as e:
                    connection = False
                    connection_errors.add("paramiko.ConfigParseError")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except paramiko.CouldNotCanonicalize as e:
                    connection = False
                    connection_errors.add("paramiko.CouldNotCanonicalize")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except paramiko.ssh_exception.NoValidConnectionsError as e:
                    connection = False
                    connection_errors.add("paramiko.NoValidConnectionsError")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except socket.timeout as e:
                    connection = False
                    connection_errors.add("socket.timeout")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except paramiko.SSHException as e:
                    connection = False
                    connection_errors.add("paramiko.SSHException")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except socket.error as e:
                    connection = False
                    connection_errors.add("socket.error")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')
                except Exception as e:
                    connection = False
                    connection_errors.add("generic.Exception")
                    self.logger.error(f'Unable to connect to {ipaddr} : {e}')

            if "tags" not in self.node_doc:
                self.node_doc["tags"] = {}

            self.node_doc["tags"]["connection_check"] = connection

            if len(connection_errors) > 1:
                self.node_doc["tags"]["connection_check_err"] = ' '.join(str(item) for item in connection_errors)

            try:
                res = server_pool_helper.upsert_node_to_server_pool(self.node_doc)
                if not res:
                    exception = f"Cannot upsert node {ipaddr} with node-connectivity-2 checks to server pool"
                    self.set_exception(exception)

                self.logger.info(f"Document for node {ipaddr} with node-connectivity-2 checks upserted to server pool successfuly")

            except Exception as e:
                exception = f"Cannot upsert node {ipaddr} with node-connectivity-2 checks to server pool : {e}"
                self.set_exception(exception)

            self.complete_sub_task(result=True)

    class FieldConsistencySubTask(SubTask):
        def __init__(self, params:dict):
            """
            Initialize a FieldConsistencySubTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - node (dict) : The json node document in dict from the testdb
            """
            sub_task_name = NodeHealthMonitorTask.FieldConsistencySubTask.__name__
            super().__init__(sub_task_name)
            if "node" not in params:
                self.set_exception(ValueError("Invalid arguments passed"))
            self.node_doc = params["node"]

        def generate_json_result(self, timeout=3600):
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.state != SubTaskStates.COMPLETED:
                    time.sleep(10)
                    continue
                if self.result:
                    self.result_json = {}
                    self.result_json["field_consistency"] = self.node_doc["tags"]["field_consistency"]
                    return self.result_json
                else:
                    return self.exception

        def execute_sub_task(self):
            self.start_sub_task()

            ipaddr = self.node_doc["ipaddr"]

            try:
                server_pool_helper = ServerPoolSDKHelper()
                self.logger.info(f"Connection to Server Pool successful")
            except Exception as e:
                exception = f"Cannot connect to Server Pool using SDK : {e}"
                self.set_exception(exception)

            if "tags" not in self.node_doc:
                self.node_doc["tags"] = {}

            fields_required = list(NODE_TEMPLATE.keys())
            # TODO - Remove the following line after server-pool cleanup
            fields_required.append("doc_key")

            fields_absent = []
            for field in fields_required:
                if field not in self.node_doc:
                    fields_absent.append(field)

            fields_extra = []
            for field in self.node_doc:
                if field not in fields_required:
                    fields_extra.append(field)

            if len(fields_absent) == 0 and len(fields_extra) == 0:
                self.node_doc["tags"]["field_consistency"] = {
                    "fields_match" : True
                }
            else:
                self.node_doc["tags"]["field_consistency"] = {
                    "fields_match" : False
                }
                if len(fields_absent) > 0:
                    self.node_doc["tags"]["field_consistency"]["fields_absent"] = fields_absent
                if len(fields_extra) > 0:
                    self.node_doc["tags"]["field_consistency"]["fields_extra"] = fields_extra

            try:
                res = server_pool_helper.upsert_node_to_server_pool(self.node_doc)
                if not res:
                    exception = f"Cannot upsert node {ipaddr} with field_consistency checks to server pool"
                    self.set_exception(exception)

                self.logger.info(f"Document for node {ipaddr} with field_consistency checks upserted to server pool successfuly")

            except Exception as e:
                exception = f"Cannot upsert node {ipaddr} with field_consistency checks to server pool : {e}"
                self.set_exception(exception)

            self.complete_sub_task(result=True)

    class NodeStatsMatchSubTask(SubTask):
        def __init__(self, params:dict):
            """
            Initialize a NodeStatsMatchSubTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - node (dict) : The json node document in dict from the testdb
            """
            sub_task_name = NodeHealthMonitorTask.NodeStatsMatchSubTask.__name__
            super().__init__(sub_task_name)
            if "node" not in params:
                self.set_exception(ValueError("Invalid arguments passed"))
            self.node_doc = params["node"]

        def generate_json_result(self, timeout=3600):
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.state != SubTaskStates.COMPLETED:
                    time.sleep(10)
                    continue
                if self.result:
                    self.result_json = {}
                    self.result_json["mac_address_node_match"] = self.node_doc["tags"]["mac_address_node_check"]
                    self.result_json["memory_node_match"] = self.node_doc["tags"]["memory_node_check"]
                    self.result_json["os_node_match"] = self.node_doc["tags"]["os_node_check"]
                    return self.result_json
                else:
                    return self.exception

        def execute_sub_task(self):
            self.start_sub_task()

            ipaddr = self.node_doc["ipaddr"]

            # TODO - Remove post cleaning up of server pool
            if "tags" not in self.node_doc and "connection_check" not in self.node_doc["tags"]:
                exception = f"OS version check was run before connection checks for {ipaddr}"
                self.set_exception(exception)
            elif not self.node_doc["tags"]["connection_check"]:
                exception = f"The node is unreachable, cannot perform os version checks for {ipaddr}"
                self.set_exception(exception)


            try:
                server_pool_helper = ServerPoolSDKHelper()
                self.logger.info(f"Connection to Server Pool successful")
            except Exception as e:
                exception = f"Cannot connect to Server Pool using SDK : {e}"
                self.set_exception(exception)

            if "tags" not in self.node_doc:
                self.node_doc["tags"] = {}

            try:
                remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr,"root","couchbase")
            except Exception as e:
                exception = f"The node {ipaddr} is unreachable, cannot perform os version checks : {e}"
                self.set_exception(exception)

            try:
                mac_address_in_node = remote_connection_helper.find_mac_address()
            except Exception as e:
                exception = f"Could not find mac adddress for node {ipaddr} : {e}"
                self.set_exception(exception)

            if "mac_address" in self.node_doc and mac_address_in_node == self.node_doc["mac_address"]:
                self.node_doc["tags"]["mac_address_node_check"] = {
                    "mac_address_node_match" : True
                }
            else:
                self.node_doc["tags"]["mac_address_node_check"] = {
                    "mac_address_node_match" : False,
                    "mac_address_in_node" : mac_address_in_node
                }

            try:
                memory_in_node = remote_connection_helper.find_memory_total()
            except Exception as e:
                exception = f"Could not find total memory for node {ipaddr} : {e}"
                self.set_exception(exception)

            if "memory" in self.node_doc and memory_in_node == self.node_doc["memory"]:
                self.node_doc["tags"]["memory_node_check"] = {
                    "memory_node_match" : True
                }
            else:
                self.node_doc["tags"]["memory_node_check"] = {
                    "memory_node_match" : False,
                    "memory_in_node" : memory_in_node
                }

            try:
                os_node = remote_connection_helper.find_os_version()
            except Exception as e:
                exception = f"Could not find os version for node {ipaddr} : {e}"
                self.set_exception(exception)

            if "os_version" in self.node_doc and os_node == self.node_doc["os_version"]:
                self.node_doc["tags"]["os_node_check"] = {
                    "os_node_match" : True
                }
            else:
                self.node_doc["tags"]["os_node_check"] = {
                    "os_node_match" : False,
                    "os_in_node" : os_node
                }


            try:
                res = server_pool_helper.upsert_node_to_server_pool(self.node_doc)
                if not res:
                    exception = f"Cannot upsert node {ipaddr} with node-stats-consistency checks to server pool"
                    self.set_exception(exception)

                self.logger.info(f"Document for node {ipaddr} with node-stats-consistency checks upserted to server pool successfuly")

            except Exception as e:
                exception = f"Cannot upsert node {ipaddr} with node-stats-consistency checks to server pool : {e}"
                self.set_exception(exception)

            self.complete_sub_task(result=True)

    class HostPoolCheckSubTask(SubTask):
        def __init__(self, params:dict):
            """
            Initialize a HostPoolCheckSubTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - node (dict) : The json node document in dict from the testdb
            """
            sub_task_name = NodeHealthMonitorTask.HostPoolCheckSubTask.__name__
            super().__init__(sub_task_name)
            if "node" not in params:
                self.set_exception(ValueError("Invalid arguments passed"))
            self.node_doc = params["node"]

        def generate_json_result(self, timeout=3600):
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.state != SubTaskStates.COMPLETED:
                    time.sleep(10)
                    continue
                if self.result:
                    self.result_json = {}
                    self.result_json["ip_in_host_pool"] = self.node_doc["tags"]["ip_in_host_pool"]
                    if self.result_json["ip_in_host_pool"]:
                        self.result_json["origin_host_pool"] = self.node_doc["tags"]["origin_host_pool"]
                        self.result_json["vm_name_host_pool"] = self.node_doc["tags"]["vm_name_host_pool"]
                        self.result_json["os_version_host_pool"] = self.node_doc["tags"]["os_version_host_pool"]
                    return self.result_json
                else:
                    return self.exception

        def execute_sub_task(self):
            self.start_sub_task()

            ipaddr = self.node_doc["ipaddr"]

            try:
                host_sdk_helper = HostSDKHelper()
                self.logger.info(f"Connection to Host Pool successful")
            except Exception as e:
                exception = f"Cannot connect to Host Pool using SDK : {e}"
                self.set_exception(exception)

            try:
                server_pool_helper = ServerPoolSDKHelper()
                self.logger.info(f"Connection to Server Pool successful")
            except Exception as e:
                exception = f"Cannot connect to Server Pool using SDK : {e}"
                self.set_exception(exception)

            if "tags" not in self.node_doc:
                self.node_doc["tags"] = {}

            try:
                vms = host_sdk_helper.fetch_vm(ipaddr=ipaddr)
            except Exception as e:
                exception = f"Cannot fetch vm {ipaddr} from Host Pool using SDK : {e}"
                self.set_exception(exception)

            try:
                vms = [vm[host_sdk_helper.vm_collection_name] for vm in vms]
            except Exception as e:
                exception = f"Unable to parse query result from Host Pool using SDK : {e}"
                self.set_exception(exception)

            if len(vms) == 0:
                self.node_doc["tags"]["ip_in_host_pool"] = False
            else:
                self.node_doc["tags"]["ip_in_host_pool"] = True

                vm = vms[0]
                if "origin" in self.node_doc and vm["host"] == self.node_doc["origin"]:
                        self.node_doc["tags"]["origin_host_pool"] = {
                            "origin_match" : True
                        }
                else:
                    self.node_doc["tags"]["origin_host_pool"] = {
                        "origin_match" : False,
                        "origin_host_pool" : vm["host"]
                    }

                if "vm_name" in self.node_doc and vm["name_label"] == self.node_doc["vm_name"]:
                    self.node_doc["tags"]["vm_name_host_pool"] = {
                        "vm_name_match" : True
                    }
                else:
                    self.node_doc["tags"]["vm_name_host_pool"] = {
                        "vm_name_match" : False,
                        "vm_name_host_pool" : vm["name_label"]
                    }

                if "os_version" not in vm:
                    vm["os_version"] = "unknown"

                if "os_version" in self.node_doc and vm["os_version"] == self.node_doc["os_version"]:
                    self.node_doc["tags"]["os_version_host_pool"] = {
                        "os_version_match" : True
                    }
                else:
                    self.node_doc["tags"]["os_version_host_pool"] = {
                        "os_version_match" : False,
                        "os_version_host_pool" : vm["os_version"]
                    }

            try:
                res = server_pool_helper.upsert_node_to_server_pool(self.node_doc)
                if not res:
                    exception = f"Cannot upsert node {ipaddr} with host-pool-consistency checks to server pool"
                    self.set_exception(exception)

                self.logger.info(f"Document for node {ipaddr} with host-pool-consistency checks upserted to server pool successfuly")

            except Exception as e:
                exception = f"Cannot upsert node {ipaddr} with host-pool-consistency checks to server pool : {e}"
                self.set_exception(exception)

            self.complete_sub_task(result=True)

    def __init__(self, params, max_workers=None):
        """
            Initialize a NodeHealthMonitorTask with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - poolId (list, optional) : List of poolIds for which the task should be run.
                        If it is not provided, all poolIds are considered
                    - tasks (list, optional): List of tasks which have to be run
                        If it is not provided all tasks are considered
        """
        task_name = NodeHealthMonitorTask.__name__
        if max_workers is None:
            max_workers = 100
        super().__init__(task_name, max_workers)

        if "poolId" not in params:
            self.poolId = []
        elif params["poolId"] is None:
            self.poolId = []
        elif not isinstance(params["poolId"], list):
            exception = ValueError(f"poolId param has to be a list : {params['poolId']}")
            self.set_exception(exception)
        else:
            self.poolId = params["poolId"]

        all_sub_tasks = self._get_sub_task_names()
        if "tasks" not in params:
            self.sub_task_names = all_sub_tasks
        elif params["tasks"] is None:
            self.sub_task_names = all_sub_tasks
        elif not isinstance(params["tasks"], list):
            exception = ValueError(f"tasks param has to be a list : {params['tasks']}")
            self.set_exception(exception)
        else:
            for task in params["tasks"]:
                if task not in all_sub_tasks:
                    exception = ValueError(f"Invalid Sub task name : {task}")
                    self.set_exception(exception)
            self.sub_task_names = params["tasks"]

    def _get_sub_task_names(self):
        sub_task_names = []
        for name, obj in inspect.getmembers(self):
            if inspect.isclass(obj) and\
                obj != self.__class__ and\
                    obj.__module__ == self.__module__:
                if obj.__qualname__.startswith(self.__class__.__name__):
                    sub_task_names.append(obj.__name__)
        return sub_task_names

    def execute(self):
        self.start_task()
        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_exception(exception)
        try:
            if len(self.poolId) > 0:
                query_result = server_pool_helper.fetch_nodes_by_poolId(self.poolId)
            else:
                query_result = server_pool_helper.fetch_all_nodes()
        except Exception as e:
            exception = f"Cannot fetch docs from server-pool : {e}"
            self.set_exception(exception)

        docs = []
        for row in query_result:
            row[server_pool_helper.server_pool_collection]["doc_key"] = row["id"]
            docs.append(row["_default"])

        for sub_task_name in self.sub_task_names:
            sub_tasks = []
            sub_task_class = getattr(self.__class__, sub_task_name)
            for doc in docs:
                params = {"node" : doc}
                sub_task_instance = sub_task_class(params)
                self.add_sub_task(sub_task_instance)
                sub_tasks.append(sub_task_instance)
            for sub_task in sub_tasks:
                self.get_sub_task_result(subtask=sub_task)

        self.complete_task(result=True)

    def generate_json_result(self, timeout=3600):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.state != TaskStates.COMPLETED:
                time.sleep(10)
                continue
            if self.result:
                self.result_json = {}
                for sub_task in self.subtasks:
                    if sub_task.node_doc["ipaddr"] not in self.result_json:
                        self.result_json[sub_task.node_doc["ipaddr"]] = {}
                    if sub_task.sub_task_name not in self.result_json[sub_task.node_doc["ipaddr"]]:
                        self.result_json[sub_task.node_doc["ipaddr"]][sub_task.sub_task_name] = {}
                    self.result_json[sub_task.node_doc["ipaddr"]][sub_task.sub_task_name] = sub_task.generate_json_result()
                return self.result_json
            else:
                return self.exception