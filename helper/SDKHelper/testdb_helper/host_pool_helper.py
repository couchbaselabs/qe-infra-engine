import os
from util.sdk_util.sdk_client import SDKClient
import logging
import threading

class HostSDKHelper:

    _instance = None
    _lock = threading.Lock()
    _initialized = threading.Event()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(HostSDKHelper, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not self._initialized.is_set():
            with self._lock:
                if not self._initialized.is_set():

                    self.logger = logging.getLogger("helper")

                    self.cluster_ipaddr = os.environ.get("TESTDB_CLUSTER_IPADDR")
                    self.cluster_username = os.environ.get("TESTDB_CLUSTER_USERNAME")
                    self.cluster_password = os.environ.get("TESTDB_CLUSTER_PASSWORD")

                    self.host_pool_bucket_name =  os.environ.get("TESTDB_HOST_POOL_BUCKET")
                    self.host_scope_name = os.environ.get("TESTDB_HOST_POOL_HOST_SCOPE")
                    self.host_collection_name = os.environ.get("TESTDB_HOST_POOL_HOST_COLLECTION")
                    self.vm_scope_name = os.environ.get("TESTDB_HOST_POOL_VM_SCOPE")
                    self.vm_collection_name = os.environ.get("TESTDB_HOST_POOL_VM_COLLECTION")

                    self.host_connection = SDKClient(ip_addr=self.cluster_ipaddr,
                                                     username=self.cluster_username,
                                                     password=self.cluster_password,
                                                     bucket=self.host_pool_bucket_name,
                                                     scope=self.host_scope_name,
                                                     collection=self.host_collection_name)

                    self.logger.info(f"SDK Client created for {self.host_pool_bucket_name}.{self.host_scope_name}.{self.host_collection_name}")

                    self.vm_connection = SDKClient(ip_addr=self.cluster_ipaddr,
                                                   username=self.cluster_username,
                                                   password=self.cluster_password,
                                                   bucket=self.host_pool_bucket_name,
                                                   scope=self.vm_scope_name,
                                                   collection=self.vm_collection_name)

                    self.logger.info(f"SDK Client created for {self.host_pool_bucket_name}.{self.vm_scope_name}.{self.vm_collection_name}")

                    self._initialized.set()

    def add_host(self, doc):
        key = doc["name"]
        res = self.host_connection.upsert(key, doc, retries=5)
        if res:
            self.logger.info(f"Document with key {key} successfully upserted into {self.host_pool_bucket_name}.{self.host_scope_name}.{self.host_collection_name}")
        else:
            self.logger.error(f"Upsert into {self.host_pool_bucket_name}.{self.host_scope_name}.{self.host_collection_name} failed for document with key {key}")
        return res

    def add_vm(self, doc):
        key = doc["name_label"]
        res = self.vm_connection.upsert(key, doc, retries=5)
        if res:
            self.logger.info(f"Document with key {key} successfully upserted into {self.host_pool_bucket_name}.{self.vm_scope_name}.{self.vm_collection_name}")
        else:
            self.logger.error(f"Upsert into {self.host_pool_bucket_name}.{self.vm_scope_name}.{self.vm_collection_name} failed for document with key {key}")
        return res

    def fetch_all_vms(self):
        query = f"SELECT * FROM `{self.host_pool_bucket_name}`.`{self.vm_scope_name}`.`{self.vm_collection_name}`"
        self.logger.info(f"Running query {query}")
        return self.vm_connection.query(query, retries=5)

    def fetch_all_host(self):
        query = f"SELECT * FROM `{self.host_pool_bucket_name}`.`{self.host_scope_name}`.`{self.host_collection_name}`"
        self.logger.info(f"Running query {query}")
        return self.host_connection.query(query, retries=5)
