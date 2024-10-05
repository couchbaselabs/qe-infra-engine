from typing import Optional
from tasks.task import Task
from tasks.task_result import TaskResult
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper
from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper

import json
import os

class GetJSONTask(Task):

    def _convert_query_result_to_list(query_result, collection_name):
        results_list = []
        for row in query_result:

            doc = row[collection_name]
            doc["doc_key"] = row["id"]

            results_list.append(doc)
        return results_list

    def get_hosts_json(self, task_result: TaskResult, params: dict) -> None:
        if "results_dir" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))

        results_dir = params["results_dir"]

        if not (os.path.exists(results_dir) and os.path.isdir(results_dir)):
            exception = f"Path {results_dir} does not exist : {e}"
            self.set_subtask_exception(exception)

        try:
            host_pool_helper = HostSDKHelper()
            self.logger.info(f"Connection to Host Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Host Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            query_result = host_pool_helper.fetch_all_host()
            self.logger.info(f"Successfully fetched all hosts from host pool")
        except Exception as e:
            exception = f"Cannot fetch all hosts from host-pool : {e}"
            self.set_subtask_exception(exception)

        host_docs = GetJSONTask._convert_query_result_to_list(query_result, host_pool_helper.host_collection_name)
        path_to_hosts_json = os.path.join(results_dir, "all_hosts.json")

        try:
            query_result = host_pool_helper.fetch_all_vms()
            self.logger.info(f"Successfully fetched all vms from host pool")
        except Exception as e:
            exception = f"Cannot fetch all vms from host-pool : {e}"
            self.set_subtask_exception(exception)

        vm_docs = GetJSONTask._convert_query_result_to_list(query_result, host_pool_helper.vm_collection_name)
        path_to_vms_json = os.path.join(results_dir, "all_vms.json")

        try:
            with open(path_to_hosts_json, 'w') as json_file:
                json.dump(host_docs, json_file, indent=4) 
            
            with open(path_to_vms_json, 'w') as json_file:
                json.dump(vm_docs, json_file, indent=4) 

            self.logger.info(f"Successfully created json file with all documents from host pool")
        except:
            exception = f"Cannot create json file with all documents from host-pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["get_hosts_json"] = True
        task_result.result_json["hosts_json"] = path_to_hosts_json
        task_result.result_json["vms_json"] = path_to_vms_json

    def get_nodes_json(self, task_result: TaskResult, params: dict) -> None:
        if "results_dir" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))

        results_dir = params["results_dir"]

        if not (os.path.exists(results_dir) and os.path.isdir(results_dir)):
            exception = f"Path {results_dir} does not exist : {e}"
            self.set_subtask_exception(exception)

        try:
            server_pool_helper = ServerPoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            query_result = server_pool_helper.fetch_all_nodes()
            self.logger.info(f"Successfully fetched all nodes from server pool")
        except Exception as e:
            exception = f"Cannot fetch docs from server-pool : {e}"
            self.set_subtask_exception(exception)

        docs = GetJSONTask._convert_query_result_to_list(query_result, server_pool_helper.server_pool_collection)
        path_to_json = os.path.join(results_dir, "all_nodes.json")

        try:
            with open(path_to_json, 'w') as json_file:
                json.dump(docs, json_file, indent=4)
            self.logger.info(f"Successfully created json file with all documents from server pool")
        except:
            exception = f"Cannot create json file with all documents from server-pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["get_nodes_json"] = True
        task_result.result_json["nodes_json"] = path_to_json

    def get_slaves_json(self, task_result: TaskResult, params: dict) -> None:
        if "results_dir" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))

        results_dir = params["results_dir"]

        if not (os.path.exists(results_dir) and os.path.isdir(results_dir)):
            exception = f"Path {results_dir} does not exist : {e}"
            self.set_subtask_exception(exception)

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Server Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Server Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            query_result = slave_pool_helper.fetch_all_slaves()
            self.logger.info(f"Successfully fetched all slaves from slave pool")
        except Exception as e:
            exception = f"Cannot fetch docs from slave-pool : {e}"
            self.set_subtask_exception(exception)

        slave_docs = GetJSONTask._convert_query_result_to_list(query_result, slave_pool_helper.slave_doc_collection_name)
        path_to_slaves_json = os.path.join(results_dir, "all_slaves.json")

        try:
            query_result = slave_pool_helper.fetch_all_jenkins_slaves()
            self.logger.info(f"Successfully fetched all jenkins slaves data from slave pool")
        except Exception as e:
            exception = f"Cannot fetch docs from slave-pool : {e}"
            self.set_subtask_exception(exception)

        jenkins_docs = GetJSONTask._convert_query_result_to_list(query_result, slave_pool_helper.jenkins_doc_collection_name)
        path_to_jenkins_json = os.path.join(results_dir, "all_jenkins_slaves.json")

        try:
            with open(path_to_slaves_json, 'w') as json_file:
                json.dump(slave_docs, json_file, indent=4)

            with open(path_to_jenkins_json, 'w') as json_file:
                json.dump(jenkins_docs, json_file, indent=4)

            self.logger.info(f"Successfully created json file with all documents from slave pool")
        except:
            exception = f"Cannot create json file with all documents from slave-pool : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["get_slaves_json"] = True
        task_result.result_json["slaves_json"] = path_to_slaves_json
        task_result.result_json["jenkins_json"] = path_to_jenkins_json

    def __init__(self, params:dict, max_workers: Optional[int]=None):
        """
            Initialize a GetJSON with the given params.
            Args:
            params (dict): The dictionary with the following fields
                Valid keys:
                    - get_node_pool (bool) : True if all documents of QE-server-pool has to be converted to json.
                        False if the QE-server-pool json need not be fetched
                    - get_host_pool (bool) : True if all documents of QE-host-pool has to be converted to json.
                        False if the QE-host-pool json need not be fetched
                    - get_slave_pool (bool) : True if all documents of QE-slave-pool has to be converted to json.
                        False if the QE-slave-pool json need not be fetched
                    - results_dir (string) : The results directory into which the json has to be stored
        """
        task_name = GetJSONTask.__name__
        if max_workers is None:
            max_workers = 3
        super().__init__(task_name, max_workers, store_results=True)


        if "get_node_pool" not in params or params["get_node_pool"] is None:
            exception = ValueError(f"get_node_pool is not present in params : {params}")
            self.set_exception(exception)
        elif not isinstance(params["get_node_pool"], bool):
            exception = ValueError(f"get_node_pool param has to be a bool : {params['get_node_pool']}")
            self.set_exception(exception)
        else:
            self.get_node_pool = params["get_node_pool"]

        if "get_host_pool" not in params or params["get_host_pool"] is None:
            exception = ValueError(f"get_host_pool is not present in params : {params}")
            self.set_exception(exception)
        elif not isinstance(params["get_host_pool"], bool):
            exception = ValueError(f"get_host_pool param has to be a bool : {params['get_host_pool']}")
            self.set_exception(exception)
        else:
            self.get_host_pool = params["get_host_pool"]

        if "get_slave_pool" not in params or params["get_slave_pool"] is None:
            exception = ValueError(f"get_slave_pool is not present in params : {params}")
            self.set_exception(exception)
        elif not isinstance(params["get_slave_pool"], bool):
            exception = ValueError(f"get_slave_pool param has to be a bool : {params['get_slave_pool']}")
            self.set_exception(exception)
        else:
            self.get_slave_pool = params["get_slave_pool"]

        if "results_dir" not in params or params["get_slave_pool"] is None:
            exception = ValueError(f"get_slave_pool is not present in params : {params}")
            self.set_exception(exception)
        elif not isinstance(params["results_dir"], str):
            exception = ValueError(f"get_slave_pool is not present in params : {params}")
            self.set_exception(exception)
        elif not(os.path.exists(params["results_dir"]) and os.path.isdir(params["results_dir"])):
            exception = ValueError(f"get_slave_pool is not present in params : {params}")
            self.set_exception(exception)
        else:
            self.results_dir = params["results_dir"]

        self.sub_task_functions = []
        if self.get_node_pool:
            self.sub_task_functions.append(self.get_nodes_json)
        if self.get_host_pool:
            self.sub_task_functions.append(self.get_hosts_json)
        if self.get_slave_pool:
            self.sub_task_functions.append(self.get_slaves_json)

    def execute(self):
        self.start_task()

        sub_tasks = []
        for sub_task_function in self.sub_task_functions:
            params = {"results_dir" : self.results_dir}
            subtaskid = self.add_sub_task(sub_task_function, params)
            sub_tasks.append([subtaskid, sub_task_function])

        for subtask_id, sub_task_function in sub_tasks:
            task_result = self.get_sub_task_result(subtask_id=subtask_id)
            self.task_result.subtasks[sub_task_function.__name__] = task_result

        self.complete_task(result=True)
