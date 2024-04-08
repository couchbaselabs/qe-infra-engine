import os
import threading
from util.sdk_util.sdk_client import SDKClient
from helper.SDKHelper.testdb_helper.test_db_helper import TestDBSDKHelper
from helper.SDKHelper.sdk_helper import SingeltonMetaClass

class HostSDKHelper(TestDBSDKHelper, metaclass=SingeltonMetaClass):

    _initialized = threading.Event()
    _lock = threading.Lock()
    
    def __init__(self) -> None:
        if not self._initialized.is_set():
            with self._lock:
                if not self._initialized.is_set():
                    super().__init__()
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
        return self.add_doc(client=self.host_connection,
                            key=key,
                            doc=doc,
                            bucket_name=self.host_pool_bucket_name,
                            scope=self.host_scope_name,
                            collection=self.host_collection_name)

    def add_vm(self, doc):
        key = doc["name_label"]
        return self.add_doc(client=self.vm_connection,
                            key=key,
                            doc=doc,
                            bucket_name=self.host_pool_bucket_name,
                            scope=self.vm_scope_name,
                            collection=self.vm_collection_name)

    def fetch_all_vms(self):
        return self.fetch_all_docs(client=self.vm_connection,
                                   bucket_name=self.host_pool_bucket_name,
                                   scope=self.vm_scope_name,
                                   collection=self.vm_collection_name)

    def fetch_all_host(self):
        return self.fetch_all_docs(client=self.host_connection,
                                   bucket_name=self.host_pool_bucket_name,
                                   scope=self.host_scope_name,
                                   collection=self.host_collection_name)
