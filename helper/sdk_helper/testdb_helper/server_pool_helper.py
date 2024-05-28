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
        key = doc["doc_key"]
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

    def fetch_node_by_ipaddr(self, ipaddr : list):
        query = f"SELECT META().id,* FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}` WHERE ipaddr in {ipaddr}"
        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)
    
    def fetch_distinct_values_array(self, field):
        query = f"SELECT DISTINCT unnested_element FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}` UNNEST {field} AS unnested_element;"
        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)
    
    def fetch_distinct_values(self, field):
        query = f"SELECT DISTINCT {field} FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}`"
        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)
    
    def fetch_count_nodes_by_filters(self, filters:dict = None):
        where_clause = ""
        if filters is not None and len(filters) > 0:
            where_clause = "WHERE"
            for count, filter in enumerate(filters):
                if count > 0:
                    where_clause += f"OR "
                where_clause += f"(ANY v IN {filter} SATISFIES v IN {filters[filter]} END)"
                where_clause += f"OR {filter} IN {filters[filter]}"
        query = f"SELECT COUNT(*) AS count FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}` {where_clause}"
        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)
    
    def fetch_nodes_by_filters(self, fields:list, page:int,
                               offset:int, filters:dict = None):
        where_clause = ""
        if filters is not None and len(filters) > 0:
            where_clause = "WHERE"
            for count, filter in enumerate(filters):
                if count > 0:
                    where_clause += f"OR "
                where_clause += f"(ANY v IN {filter} SATISFIES v IN {filters[filter]} END)"
                where_clause += f"OR {filter} IN {filters[filter]}"
        query = f"SELECT {','.join(fields)} FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}` {where_clause} LIMIT {page} OFFSET {offset};"
        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)
    
    def fetch_aggregate_state(self, filters:dict = None):
        where_clause = ""
        if filters is not None and len(filters) > 0:
            where_clause = "WHERE"
            for count, filter in enumerate(filters):
                if count > 0:
                    where_clause += f"OR "
                where_clause += f"(ANY v IN {filter} SATISFIES v IN {filters[filter]} END)"
                where_clause += f"OR {filter} IN {filters[filter]}"
        query = f"SELECT state, COUNT(*) as count FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}` {where_clause} GROUP BY state;"
        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)
    
    def fetch_aggregate_tags(self, filters:dict = None):
        where_clause = ""
        if filters is not None and len(filters) > 0:
            where_clause = "WHERE"
            for count, filter in enumerate(filters):
                if count > 0:
                    where_clause += f"OR "
                where_clause += f"(ANY v IN {filter} SATISFIES v IN {filters[filter]} END)"
                where_clause += f"OR {filter} IN {filters[filter]}"

        query = f"""SELECT RAW OBJECT obj.name:obj.val FOR obj IN (SELECT final.*
        FROM (select tag_value.name, count(*) as val
        FROM (select object_pairs(`tags`) AS tag FROM `{self.server_pool_bucket_name}`.`{self.server_pool_scope}`.`{self.server_pool_collection}` {where_clause}) AS pair
        unnest pair.tag as tag_value 
        group by tag_value.name) as final)
        END as result"""

        self.logger.info(f"Running query {query}")
        return self.server_pool_client.query(query, retries=5)
