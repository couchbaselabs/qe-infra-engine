from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, UpsertOptions, GetOptions, RemoveOptions

from datetime import timedelta
import logging
class SDKClient:
    def __init__(self, ip_addr, username, password, bucket, scope=None, collection=None, tls_enabled=False) -> None:
        self.ip_addr = ip_addr
        self.username = username
        self.password = password
        self.bucket = bucket
        self.password = password
        self.scope = scope
        self.collection = collection

        self.logger = logging.getLogger("util")

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

    def upsert(self, key, doc, retries=0):
        while retries >= 0:
            try:
                res = self.collection_connection.upsert(key, doc, UpsertOptions(timeout=timedelta(seconds=60)))
                return res.success
            except Exception as e:
                self.logger.error("Upsert failed, retrying ...")
                retries -= 1
                if retries < 0 :
                    self.logger.error(f"Upsert failed even after all retries with error {e}")
                    raise e


    def get(self, key, retries=0):
        while retries >= 0:
            try:
                result = self.collection_connection.get(key, GetOptions(timeout=timedelta(seconds=60)))
                return result.content_as[str]
            except Exception as e:
                retries -= 1
                self.logger.error("Get failed, retrying ...")
                if retries < 0:
                    self.logger.error(f"Get failed even after all retries with error {e}")
                    raise e


    def query(self, query, retries=0):
        while retries >= 0:
            try:
                query_result = self.cluster.query(query)
                return query_result.rows()
            except Exception as e:
                retries -= 1
                self.logger.error("Query failed, retrying ...")
                if retries < 0:
                    self.logger.error(f"Query failed even after all retries with error {e}")
                    raise e

    def delete_doc(self, key, retries=0):
        while retries >= 0:
            try:
                res = self.collection_connection.remove(key, RemoveOptions(timeout=timedelta(seconds=60)))
                return res.success
            except Exception as e:
                retries -= 1
                self.logger.error("Delete doc failed, retrying ...")
                if retries < 0:
                    self.logger.error(f"Delete doc failed even after all retries with error {e}")
                    return e
