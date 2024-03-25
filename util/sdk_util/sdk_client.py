from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, UpsertOptions, GetOptions, RemoveOptions

from datetime import timedelta

class SDKClient:
    def __init__(self, ip_addr, username, password, bucket, scope=None, collection=None, tls_enabled=False) -> None:
        self.ip_addr = ip_addr
        self.username = username
        self.password = password
        self.bucket = bucket
        self.password = password
        self.scope = scope
        self.collection = collection

        auth = PasswordAuthenticator(
            self.username,
            self.password,
        )
        if tls_enabled:
            self.cluster = Cluster(f'couchbases://{self.ip_addr}', ClusterOptions(auth))
        else:
            self.cluster = Cluster(f'couchbase://{self.ip_addr}', ClusterOptions(auth))

        self.cluster.wait_until_ready(timedelta(seconds=60))

        self.bucket_connection = self.cluster.bucket(self.bucket)

        if not scope:
            self.scope = "_default"
        if not collection:
            self.collection = "_default"

        self.collection_connection = self.bucket_connection.scope(self.scope).collection(self.collection)

    def upsert(self, key, doc):
        res = self.collection_connection.upsert(key, doc, UpsertOptions(timeout=timedelta(seconds=60)))
        return res.success

    def get(self, key):
        result = self.collection_connection.get(key, GetOptions(timeout=timedelta(seconds=60)))
        return result.content_as[str]

    def query(self, query):
        query_result = self.cluster.query(query)
        return query_result.rows()

    def delete_doc(self, key):
        res = self.collection_connection.remove(key, RemoveOptions(timeout=timedelta(seconds=60)))
        return res.success
