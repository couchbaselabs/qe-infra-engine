import paramiko, paramiko.ssh_exception
import socket
from tasks.task import Task
from tasks.task_result import TaskResult
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

    def _initialize_tags(self, doc: dict):
        if "tags" not in doc:
            doc["tags"] = {}
        if "list" not in doc["tags"]:
            doc["tags"]["list"] = []
        if "details" not in doc["tags"]:
            doc["tags"]["details"] = {}

    def _flush_tags_list(self, doc: dict, tags: list):
        for tag in tags:
            if tag in doc["tags"]["list"]:
                doc["tags"]["list"] = list(filter(lambda x: x !=tag, doc["tags"]["list"]))

    def check_connectivity_sub_task(self, task_result, params):
        if "node" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        node_doc = params["node"]
        ipaddr = node_doc["ipaddr"]
        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        self._initialize_tags(node_doc)
        tags = ["unreachable"]
        self._flush_tags_list(node_doc, tags)

        try:
            RemoteConnectionObjectFactory.fetch_helper(ipaddr,"root","couchbase")
            node_doc["tags"]["details"]["connection_check"] = True
            # node_doc["state"] = "available" \
            #     if node_doc["state"] == "unreachable"\
            #         else node_doc["state"]
        except Exception as e:
            node_doc["tags"]["details"]["connection_check"] = False
            node_doc["tags"]["list"].append("unreachable")
            # node_doc["state"] = "unreachable"

        try:
            res = server_pool_helper.upsert_node_to_server_pool(node_doc)
            if not res:
                exception = f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool"
                self.set_subtask_exception(exception)
            self.logger.info(f"Document for node {ipaddr} with node-connectivity checks upserted to server pool successfuly")
        except Exception as e:
            exception = f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["connection_check"] = node_doc["tags"]["details"]["connection_check"]

    def check_connectivity2_sub_task(self, task_result, params):
        if "node" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        node_doc = params["node"]

        ipaddr = node_doc["ipaddr"]

        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

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

        self._initialize_tags(node_doc)
        tags = ["unreachable"]
        self._flush_tags_list(node_doc, tags)


        node_doc["tags"]["details"]["connection_check"] = connection
        if not connection:
            node_doc["tags"]["list"].append("unreachable")

        if len(connection_errors) > 1:
            node_doc["tags"]["details"]["connection_check_err"] = ' '.join(str(item) for item in connection_errors)

        try:
            res = server_pool_helper.upsert_node_to_server_pool(node_doc)
            if not res:
                exception = f"Cannot upsert node {ipaddr} with node-connectivity-2 checks to server pool"
                self.set_subtask_exception(exception)

            self.logger.info(f"Document for node {ipaddr} with node-connectivity-2 checks upserted to server pool successfuly")

        except Exception as e:
            exception = f"Cannot upsert node {ipaddr} with node-connectivity-2 checks to server pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["connection_check"] = node_doc["tags"]["details"]["connection_check"]
        if "connection_check_err" in node_doc["tags"]["details"]:
            task_result.result_json["connection_check_err"] = node_doc["tags"]["details"]["connection_check_err"]

    def field_consistency_sub_task(self, task_result, params):
        if "node" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        node_doc = params["node"]

        ipaddr = node_doc["ipaddr"]

        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        self._initialize_tags(node_doc)
        tags = ["no_fields_consistency"]
        self._flush_tags_list(node_doc, tags)

        fields_required = list(NODE_TEMPLATE.keys())
        # TODO - Remove the following line after server-pool cleanup
        fields_required.append("doc_key")

        fields_absent = []
        for field in fields_required:
            if field not in node_doc:
                fields_absent.append(field)

        fields_extra = []
        for field in node_doc:
            if field not in fields_required:
                fields_extra.append(field)

        if len(fields_absent) == 0 and len(fields_extra) == 0:
            node_doc["tags"]["details"]["field_consistency"] = {
                "fields_match" : True
            }
        else:
            node_doc["tags"]["list"].append("no_fields_consistency")
            node_doc["tags"]["details"]["field_consistency"] = {
                "fields_match" : False
            }
            if len(fields_absent) > 0:
                node_doc["tags"]["details"]["field_consistency"]["fields_absent"] = fields_absent
            if len(fields_extra) > 0:
                node_doc["tags"]["details"]["field_consistency"]["fields_extra"] = fields_extra

        try:
            res = server_pool_helper.upsert_node_to_server_pool(node_doc)
            if not res:
                exception = f"Cannot upsert node {ipaddr} with field_consistency checks to server pool"
                self.set_subtask_exception(exception)

            self.logger.info(f"Document for node {ipaddr} with field_consistency checks upserted to server pool successfuly")

        except Exception as e:
            exception = f"Cannot upsert node {ipaddr} with field_consistency checks to server pool : {e}"
            self.set_subtask_exception(exception)


        task_result.result_json = {}
        task_result.result_json["field_consistency"] = node_doc["tags"]["details"]["field_consistency"]

    def node_stats_match_sub_task(self, task_result, params):
        if "node" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        node_doc = params["node"]

        ipaddr = node_doc["ipaddr"]

        # TODO - Remove post cleaning up of server pool
        if "tags" not in node_doc and "connection_check" not in node_doc["tags"]["details"]:
            exception = f"OS version check was run before connection checks for {ipaddr}"
            self.set_subtask_exception(exception)
        elif not node_doc["tags"]["details"]["connection_check"]:
            exception = f"The node is unreachable, cannot perform os version checks for {ipaddr}"
            self.set_subtask_exception(exception)

        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        self._initialize_tags(node_doc)
        tags = ["mac_address_node_mismatch", "memory_node_mismatch", "os_node_mismatch"]
        self._flush_tags_list(node_doc, tags)

        try:
            remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr,"root","couchbase")
        except Exception as e:
            exception = f"The node {ipaddr} is unreachable, cannot perform os version checks : {e}"
            self.set_subtask_exception(exception)

        try:
            mac_address_in_node = remote_connection_helper.find_mac_address()
        except Exception as e:
            exception = f"Could not find mac adddress for node {ipaddr} : {e}"
            self.set_subtask_exception(exception)

        if "mac_address" in node_doc and mac_address_in_node == node_doc["mac_address"]:
            node_doc["tags"]["details"]["mac_address_node_check"] = {
                "mac_address_node_match" : True
            }
        else:
            node_doc["tags"]["details"]["mac_address_node_check"] = {
                "mac_address_node_match" : False,
                "mac_address_in_node" : mac_address_in_node
            }
            node_doc["tags"]["list"].append("mac_address_node_mismatch")

        try:
            memory_in_node = remote_connection_helper.find_memory_total()
        except Exception as e:
            exception = f"Could not find total memory for node {ipaddr} : {e}"
            self.set_subtask_exception(exception)

        if "memory" in node_doc and memory_in_node == node_doc["memory"]:
            node_doc["tags"]["details"]["memory_node_check"] = {
                "memory_node_match" : True
            }
        else:
            node_doc["tags"]["details"]["memory_node_check"] = {
                "memory_node_match" : False,
                "memory_in_node" : memory_in_node
            }
            node_doc["tags"]["list"].append("memory_node_mismatch")

        try:
            os_node = remote_connection_helper.find_os_version()
        except Exception as e:
            exception = f"Could not find os version for node {ipaddr} : {e}"
            self.set_subtask_exception(exception)

        if "os_version" in node_doc and os_node == node_doc["os_version"]:
            node_doc["tags"]["details"]["os_node_check"] = {
                "os_node_match" : True
            }
        else:
            node_doc["tags"]["details"]["os_node_check"] = {
                "os_node_match" : False,
                "os_in_node" : os_node
            }
            node_doc["tags"]["list"].append("os_node_mismatch")

        try:
            res = server_pool_helper.upsert_node_to_server_pool(node_doc)
            if not res:
                exception = f"Cannot upsert node {ipaddr} with node-stats-consistency checks to server pool"
                self.set_subtask_exception(exception)

            self.logger.info(f"Document for node {ipaddr} with node-stats-consistency checks upserted to server pool successfuly")

        except Exception as e:
            exception = f"Cannot upsert node {ipaddr} with node-stats-consistency checks to server pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["mac_address_node_match"] = node_doc["tags"]["details"]["mac_address_node_check"]
        task_result.result_json["memory_node_match"] = node_doc["tags"]["details"]["memory_node_check"]
        task_result.result_json["os_node_match"] = node_doc["tags"]["details"]["os_node_check"]

    def host_pool_check_sub_task(self, task_result, params):
        if "node" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        node_doc = params["node"]

        ipaddr = node_doc["ipaddr"]

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

        self._initialize_tags(node_doc)
        tags = ["ip_not_in_host_pool", "origin_host_pool_mismatch", "vm_name_host_pool_mismatch", "os_host_pool_mismatch"]
        self._flush_tags_list(node_doc, tags)

        try:
            vms = host_sdk_helper.fetch_vm(ipaddr=ipaddr)
        except Exception as e:
            exception = f"Cannot fetch vm {ipaddr} from Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            vms = [vm[host_sdk_helper.vm_collection_name] for vm in vms]
        except Exception as e:
            exception = f"Unable to parse query result from Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        if len(vms) == 0:
            node_doc["tags"]["details"]["ip_in_host_pool"] = False
            node_doc["tags"]["list"].append("ip_not_in_host_pool")
        else:
            node_doc["tags"]["details"]["ip_in_host_pool"] = True
            vm = vms[0]
            if "origin" in node_doc and vm["host"] == node_doc["origin"]:
                    node_doc["tags"]["details"]["origin_host_pool"] = {
                        "origin_match" : True
                    }
            else:
                node_doc["tags"]["list"].append("origin_host_pool_mismatch")
                node_doc["tags"]["details"]["origin_host_pool"] = {
                    "origin_match" : False,
                    "origin_host_pool" : vm["host"]
                }

            if "vm_name" in node_doc and vm["name_label"] == node_doc["vm_name"]:
                node_doc["tags"]["details"]["vm_name_host_pool"] = {
                    "vm_name_match" : True
                }
            else:
                node_doc["tags"]["list"].append("vm_name_host_pool_mismatch")
                node_doc["tags"]["details"]["vm_name_host_pool"] = {
                    "vm_name_match" : False,
                    "vm_name_host_pool" : vm["name_label"]
                }

            if "os_version" not in vm:
                vm["os_version"] = "unknown"

            if "os_version" in node_doc and vm["os_version"] == node_doc["os_version"]:
                node_doc["tags"]["details"]["os_version_host_pool"] = {
                    "os_version_match" : True
                }
            else:
                node_doc["tags"]["list"].append("os_host_pool_mismatch")
                node_doc["tags"]["details"]["os_version_host_pool"] = {
                    "os_version_match" : False,
                    "os_version_host_pool" : vm["os_version"]
                }

        try:
            res = server_pool_helper.upsert_node_to_server_pool(node_doc)
            if not res:
                exception = f"Cannot upsert node {ipaddr} with host-pool-consistency checks to server pool"
                self.set_subtask_exception(exception)

            self.logger.info(f"Document for node {ipaddr} with host-pool-consistency checks upserted to server pool successfuly")

        except Exception as e:
            exception = f"Cannot upsert node {ipaddr} with host-pool-consistency checks to server pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["ip_in_host_pool"] = node_doc["tags"]["details"]["ip_in_host_pool"]
        if task_result.result_json["ip_in_host_pool"]:
            task_result.result_json["origin_host_pool"] = node_doc["tags"]["details"]["origin_host_pool"]
            task_result.result_json["vm_name_host_pool"] = node_doc["tags"]["details"]["vm_name_host_pool"]
            task_result.result_json["os_version_host_pool"] = node_doc["tags"]["details"]["os_version_host_pool"]

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
        sub_task_names = [
            "check_connectivity_sub_task",
            "check_connectivity2_sub_task",
            "field_consistency_sub_task",
            "node_stats_match_sub_task",
            "host_pool_check_sub_task"
        ]
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
            for doc in docs:
                params = {"node" : doc}
                sub_task = getattr(self, sub_task_name)
                subtaskid = self.add_sub_task(sub_task, params)
                sub_tasks.append([doc["doc_key"], subtaskid])
            for doc_key, subtask_id in sub_tasks:
                task_result = self.get_sub_task_result(subtask_id=subtask_id)
                if doc_key not in self.task_result.subtasks:
                    self.task_result.subtasks[doc_key] = {}
                self.task_result.subtasks[doc_key][sub_task_name] = task_result

        self.complete_task(result=True)

    def generate_json_result(self, timeout=3600):
        TaskResult.generate_json_result(self.task_result)
        for doc_key in self.task_result.result_json:
           for sub_task_name in self.task_result.result_json[doc_key]:
               res = TaskResult.generate_json_result(self.task_result.subtasks[doc_key][sub_task_name])
               self.task_result.result_json[doc_key][sub_task_name] = res
        return self.task_result.result_json
