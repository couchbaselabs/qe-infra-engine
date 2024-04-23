import os
import threading
from util.sdk_util.sdk_client import SDKClient
from helper.sdk_helper.testdb_helper.test_db_helper import TestDBSDKHelper
from helper.sdk_helper.sdk_helper import SingeltonMetaClass

class ServerPoolSDKHelper(TestDBSDKHelper, metaclass=SingeltonMetaClass):

    _initialized = threading.Event()
    _lock = threading.Lock()

    def __init__(self):
        if not self._initialized.is_set():
            with self._lock:
                if not self._initialized.is_set():
                    super().__init__()
                    self.server_pool_bucket_name =  os.environ.get("TESTDB_SERVER_POOL_BUCKET")
                    self.server_pool_scope = os.environ.get("TESTDB_SERVER_POOL_SCOPE")
                    self.server_pool_collection = os.environ.get("TESTDB_SERVER_POOL_COLLECTION")

                    self.server_pool_client = SDKClient(ip_addr=self.cluster_ipaddr,
                                                        username=self.cluster_username,
                                                        password=self.cluster_password,
                                                        bucket=self.server_pool_bucket_name)
                    self.logger.info(f"SDK Client created for {self.server_pool_bucket_name}.{self.server_pool_scope}.{self.server_pool_collection}")
                    self._initialized.set()

    def upsert_node_to_server_pool(self, doc):
        key = doc["ipaddr"]
        return self.upsert_doc(client=self.server_pool_client,
                               key=key,
                               doc=doc,
                               bucket_name=self.server_pool_bucket_name,
                               scope=self.server_pool_scope,
                               collection=self.server_pool_collection)

    def fetch_all_nodes(self):
        return self.fetch_all_docs(client=self.server_pool_client,
                                   bucket_name=self.server_pool_bucket_name,
                                   scope=self.server_pool_scope,
                                   collection=self.server_pool_collection)

    def get_node(self, ipaddr):
        return self.get_doc(client=self.server_pool_client,
                            key=ipaddr)

    def delete_node(self, ipaddr):
        key = ipaddr
        return self.delete_doc(client=self.server_pool_client,
                               key=key,
                               bucket_name=self.server_pool_bucket_name,
                               scope=self.server_pool_scope,
                               collection=self.server_pool_collection)
    
    def fetch_nodes_by_poolId(self, poolId : list):
        query = f"SELECT META().id,* FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}` WHERE ANY v IN poolId SATISFIES v IN {poolId} END;"
        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)







