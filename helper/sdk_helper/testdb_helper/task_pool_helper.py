import os
import threading
import copy
from util.sdk_util.sdk_client import SDKClient
from helper.sdk_helper.testdb_helper.test_db_helper import TestDBSDKHelper
from helper.sdk_helper.sdk_helper import SingeltonMetaClass
from constants.doc_templates import TASK_TEMPLATE, TASK_RESULT_TEMPLATE
from constants.task_states import TaskStates

class TaskPoolSDKHelper(TestDBSDKHelper, metaclass=SingeltonMetaClass):

    _initialized = threading.Event()
    _lock = threading.Lock()

    def __init__(self):
        if not self._initialized.is_set():
            with self._lock:
                if not self._initialized.is_set():
                    super().__init__()
                    self.task_pool_bucket_name =  os.environ.get("TESTDB_TASK_POOL_BUCKET")
                    self.tasks_doc_scope_name = os.environ.get("TESTDB_TASK_POOL_TASKS_SCOPE")
                    self.tasks_doc_collection_name = os.environ.get("TESTDB_TASK_POOL_TASKS_COLLECTION")
                    self.results_doc_scope_name = os.environ.get("TESTDB_TASK_POOL_RESULTS_SCOPE")
                    self.results_doc_collection_name = os.environ.get("TESTDB_TASK_POOL_RESULTS_COLLECTION")

                    self.tasks_doc_connection = SDKClient(ip_addr=self.cluster_ipaddr,
                                                          username=self.cluster_username,
                                                          password=self.cluster_password,
                                                          bucket=self.task_pool_bucket_name,
                                                          scope=self.tasks_doc_scope_name,
                                                          collection=self.tasks_doc_collection_name)

                    self.logger.info(f"SDK Client created for {self.task_pool_bucket_name}.{self.tasks_doc_scope_name}.{self.tasks_doc_collection_name}")

                    self.results_doc_connection = SDKClient(ip_addr=self.cluster_ipaddr,
                                                            username=self.cluster_username,
                                                            password=self.cluster_password,
                                                            bucket=self.task_pool_bucket_name,
                                                            scope=self.results_doc_scope_name,
                                                            collection=self.results_doc_collection_name)

                    self.logger.info(f"SDK Client created for {self.task_pool_bucket_name}.{self.results_doc_scope_name}.{self.results_doc_collection_name}")

                    self._initialized.set()

    def create_task_doc(self, task_id, task_name):
        task_doc = copy.deepcopy(TASK_TEMPLATE)
        task_doc["task_id"] = task_id
        task_doc["task_name"] = task_name
        task_doc["start_time"] = ""
        task_doc["end_time"] = ""
        task_doc["state"] = TaskStates.CREATED
        task_doc["result"] = False

        key = task_doc["task_id"]
        return self.upsert_doc(client=self.tasks_doc_connection,
                               key=key,
                               doc=task_doc,
                               bucket_name=self.task_pool_bucket_name,
                               scope=self.tasks_doc_scope_name,
                               collection=self.tasks_doc_collection_name)

    def update_task_started(self, task_id, start_time):
        try:
            task_doc = eval(self.get_doc(task_id))
        except Exception as e:
            msg = f"Error fetching document from task with id {task_id} : {e}"
            self.logger.error(msg)
            raise Exception(msg)

        task_doc["start_time"] = start_time
        task_doc["state"] = TaskStates.RUNNING

        return self.upsert_doc(client=self.tasks_doc_connection,
                               key=task_id,
                               doc=task_doc,
                               bucket_name=self.task_pool_bucket_name,
                               scope=self.tasks_doc_scope_name,
                               collection=self.tasks_doc_collection_name)

    def update_task_completed(self, task_id, end_time, result):
        try:
            task_doc = eval(self.get_doc(task_id))
        except Exception as e:
            msg = f"Error fetching document from task with id {task_id} : {e}"
            self.logger.error(msg)
            raise Exception(msg)

        task_doc["end_time"] = end_time
        task_doc["state"] = TaskStates.COMPLETED
        task_doc["result"] = result

        return self.upsert_doc(client=self.tasks_doc_connection,
                               key=task_id,
                               doc=task_doc,
                               bucket_name=self.task_pool_bucket_name,
                               scope=self.tasks_doc_scope_name,
                               collection=self.tasks_doc_collection_name)

    def add_results_to_task(self, task_id, result):
        result_doc = copy.deepcopy(TASK_RESULT_TEMPLATE)
        result_doc["task_id"] = task_id
        result_doc["result"] = result

        key = task_id
        return self.upsert_doc(client=self.results_doc_connection,
                               key=key,
                               doc=result_doc,
                               bucket_name=self.task_pool_bucket_name,
                               scope=self.results_doc_scope_name,
                               collection=self.results_doc_collection_name)








