import sys
sys.path.append('../qe-infra-engine')
sys.path.append('../qe-infra-engine/util')
sys.path.append('../qe-infra-engine/util/ssh_util')
sys.path.append('../qe-infra-engine/util/sdk_util')

import os
from util.sdk_util.sdk_client import SDKClient

class SDKHelper:
    def __init__(self) -> None:
        self.cluster_ipaddr = os.environ.get("TESTDB_CLUSTER_IPADDR")
        self.cluster_username = os.environ.get("TESTDB_CLUSTER_USERNAME")
        self.cluster_password = os.environ.get("TESTDB_CLUSTER_PASSWORD")

        self.server_pool_bucket_name =  "QE-server-pool"

        self.server_pool_client = SDKClient(ip_addr=self.cluster_ipaddr,
                                            username=self.cluster_username,
                                            password=self.cluster_password,
                                            bucket=self.server_pool_bucket_name)
        
    def add_node_to_server_pool(self, doc):
        key = doc["ipaddr"]
        res = self.server_pool_client.upsert(key, doc)
        return res

    



