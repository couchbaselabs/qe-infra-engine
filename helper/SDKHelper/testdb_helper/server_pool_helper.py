import os
import logging
import threading
from util.sdk_util.sdk_client import SDKClient

class ServerPoolSDKHelper:

    _instance = None
    _lock = threading.Lock()
    _initialized = threading.Event()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ServerPoolSDKHelper, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized.is_set():
            with self._lock:
                if not self._initialized.is_set():
                    self.logger = logging.getLogger("helper")
                    self.cluster_ipaddr = os.environ.get("TESTDB_CLUSTER_IPADDR")
                    self.cluster_username = os.environ.get("TESTDB_CLUSTER_USERNAME")
                    self.cluster_password = os.environ.get("TESTDB_CLUSTER_PASSWORD")

                    self.server_pool_bucket_name =  os.environ.get("TESTDB_SERVER_POOL_BUCKET")
                    self.server_pool_scope = os.environ.get("TESTDB_SERVER_POOL_SCOPE")
                    self.server_pool_collection = os.environ.get("TESTDB_SERVER_POOL_COLLECTION")

                    self.server_pool_client = SDKClient(ip_addr=self.cluster_ipaddr,
                                                        username=self.cluster_username,
                                                        password=self.cluster_password,
                                                        bucket=self.server_pool_bucket_name)
                    self.logger.info(f"SDK Client created for {self.server_pool_bucket_name}.{self.server_pool_scope}.{self.server_pool_collection}")
                    self._initialized.set()

    def add_node_to_server_pool(self, doc):
        # key = doc["ipaddr"]
        # TODO - Remove the doc_key part after cleanup of server-pool
        key = doc["doc_key"]
        res = self.server_pool_client.upsert(key, doc, retries=5)
        if res:
            self.logger.info(f"Document with key {key} successfully upserted into {self.server_pool_bucket_name}.{self.server_pool_scope}.{self.server_pool_collection}")
        else:
            self.logger.error(f"Upsert into {self.server_pool_bucket_name}.{self.server_pool_scope}.{self.server_pool_collection} failed for document with key {key}")
        return res

    def fetch_all_nodes(self):
        query = f"SELECT META().id,* FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}`"
        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)
    
    def fetch_nodes_by_poolId(self, poolId=[]):
        query = f"SELECT META().id,* FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}` WHERE ANY v IN poolId SATISFIES v IN {poolId} END;"
        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)
    
    def get_node(self, ipaddr):
        self.logger.info(f"Fetching doc with key {ipaddr}")
        return self.server_pool_client.get(ipaddr, retries=5)
    
    def delete_node(self, ipaddr):
        key = ipaddr
        self.logger.info(f"Deleting doc with key {key}")
        res = self.server_pool_client.delete_doc(key, retries=5)
        if res:
            self.logger.info(f"Document with key {key} successfully deleted from {self.server_pool_bucket_name}.{self.server_pool_scope}.{self.server_pool_collection}")
        else:
            self.logger.error(f"Delete from {self.server_pool_bucket_name}.{self.server_pool_scope}.{self.server_pool_collection} failed for document with key {key}")
        return res






