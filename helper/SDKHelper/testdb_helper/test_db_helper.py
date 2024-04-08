import os
from helper.SDKHelper.sdk_helper import SDKHelper

class TestDBSDKHelper(SDKHelper):

    def __init__(self):
        super().__init__()
        self.cluster_ipaddr = os.environ.get("TESTDB_CLUSTER_IPADDR")
        self.cluster_username = os.environ.get("TESTDB_CLUSTER_USERNAME")
        self.cluster_password = os.environ.get("TESTDB_CLUSTER_PASSWORD")

