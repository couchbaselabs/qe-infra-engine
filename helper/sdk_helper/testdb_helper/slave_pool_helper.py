import os
import threading
from util.sdk_util.sdk_client import SDKClient
from helper.sdk_helper.testdb_helper.test_db_helper import TestDBSDKHelper
from helper.sdk_helper.sdk_helper import SingeltonMetaClass

class SlavePoolSDKHelper(TestDBSDKHelper, metaclass=SingeltonMetaClass):

    _initialized = threading.Event()
    _lock = threading.Lock()

    def __init__(self):
        if not self._initialized.is_set():
            with self._lock:
                if not self._initialized.is_set():
                    super().__init__()
                    self.slave_pool_bucket_name =  os.environ.get("TESTDB_SLAVE_POOL_BUCKET")
                    self.slave_doc_scope_name = os.environ.get("TESTDB_SLAVE_POOL_SLAVE_DOC_SCOPE")
                    self.slave_doc_collection_name = os.environ.get("TESTDB_SLAVE_POOL_SLAVE_DOC_COLLECTION")
                    self.jenkins_doc_scope_name = os.environ.get("TESTDB_SLAVE_POOL_JENKINS_DOC_SCOPE")
                    self.jenkins_doc_collection_name = os.environ.get("TESTDB_SLAVE_POOL_JENKINS_DOC_COLLECTION")

                    self.slave_doc_connection = SDKClient(ip_addr=self.cluster_ipaddr,
                                                          username=self.cluster_username,
                                                          password=self.cluster_password,
                                                          bucket=self.slave_pool_bucket_name,
                                                          scope=self.slave_doc_scope_name,
                                                          collection=self.slave_doc_collection_name)

                    self.logger.info(f"SDK Client created for {self.slave_pool_bucket_name}.{self.slave_doc_scope_name}.{self.slave_doc_collection_name}")

                    self.jenkins_doc_connection = SDKClient(ip_addr=self.cluster_ipaddr,
                                                   username=self.cluster_username,
                                                   password=self.cluster_password,
                                                   bucket=self.slave_pool_bucket_name,
                                                   scope=self.jenkins_doc_scope_name,
                                                   collection=self.jenkins_doc_collection_name)

                    self.logger.info(f"SDK Client created for {self.slave_pool_bucket_name}.{self.jenkins_doc_scope_name}.{self.jenkins_doc_collection_name}")

                    self._initialized.set()

    def upsert_slave_to_slave_pool(self, doc):
        key = doc["doc_key"]
        return self.upsert_doc(client=self.slave_doc_connection,
                               key=key,
                               doc=doc,
                               bucket_name=self.slave_pool_bucket_name,
                               scope=self.slave_doc_scope_name,
                               collection=self.slave_doc_collection_name)

    def get_slave_pool_doc(self, name):
        return self.get_doc(client=self.slave_doc_connection,
                            key=name)
    
    def delete_slave_pool_doc(self, name):
        key = name
        return self.delete_doc(client=self.slave_doc_connection,
                               key=key,
                               bucket_name=self.slave_pool_bucket_name,
                               scope=self.slave_doc_scope_name,
                               collection=self.slave_doc_collection_name)
    
    def fetch_aggregate_state(self, filters:dict = None):
        where_clause = ""
        if filters is not None and len(filters) > 0:
            where_clause = "WHERE"
            for count, filter in enumerate(filters):
                if count > 0:
                    where_clause += f"OR "
                where_clause += f"(ANY v IN {filter} SATISFIES v IN {filters[filter]} END)"
                where_clause += f"OR {filter} IN {filters[filter]}"
        query = f"SELECT state, COUNT(*) as count FROM `{self.slave_pool_bucket_name}`.`{self.slave_doc_scope_name}`.`{self.slave_doc_collection_name}` {where_clause} GROUP BY state;"
        self.logger.info(f"Running query {query}")
        return self.slave_doc_connection.query(query, retries=5)
    
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
        FROM (select object_pairs(`tags`) AS tag FROM `{self.slave_pool_bucket_name}`.`{self.slave_doc_scope_name}`.`{self.slave_doc_collection_name}` {where_clause}) AS pair
        unnest pair.tag as tag_value 
        group by tag_value.name) as final)
        END as result"""

        self.logger.info(f"Running query {query}")
        return self.slave_doc_connection.query(query, retries=5)
    
    def fetch_count_slaves_by_filters(self, filters:dict = None):
        where_clause = ""
        if filters is not None and len(filters) > 0:
            where_clause = "WHERE"
            for count, filter in enumerate(filters):
                if count > 0:
                    where_clause += f"OR "
                where_clause += f"(ANY v IN {filter} SATISFIES v IN {filters[filter]} END)"
                where_clause += f"OR {filter} IN {filters[filter]}"
        query = f"SELECT COUNT(*) AS count FROM `{self.slave_pool_bucket_name}`.`{self.slave_doc_scope_name}`.`{self.slave_doc_collection_name}` {where_clause}"
        self.logger.info(f"Running query {query}")
        return self.slave_doc_connection.query(query, retries=5)
    
    def fetch_slaves_by_filters(self, fields:list, page:int,
                               offset:int, filters:dict = None):
        where_clause = ""
        if filters is not None and len(filters) > 0:
            where_clause = "WHERE"
            for count, filter in enumerate(filters):
                if count > 0:
                    where_clause += f"OR "
                where_clause += f"(ANY v IN {filter} SATISFIES v IN {filters[filter]} END)"
                where_clause += f"OR {filter} IN {filters[filter]}"
        query = f"SELECT {','.join(fields)} FROM `{self.slave_pool_bucket_name}`.`{self.slave_doc_scope_name}`.`{self.slave_doc_collection_name}` {where_clause} LIMIT {page} OFFSET {offset};"
        self.logger.info(f"Running query {query}")
        return self.slave_doc_connection.query(query, retries=5)
    
    def fetch_distinct_values_array(self, field):
        query = f"SELECT DISTINCT unnested_element FROM `{self.slave_pool_bucket_name}`.`{self.slave_doc_scope_name}`.`{self.slave_doc_collection_name}` UNNEST {field} AS unnested_element;"
        self.logger.info(f"Running query {query}")
        return self.slave_doc_connection.query(query, retries=5)
    
    def fetch_distinct_values(self, field):
        query = f"SELECT DISTINCT {field} FROM `{self.slave_pool_bucket_name}`.`{self.slave_doc_scope_name}`.`{self.slave_doc_collection_name}`"
        self.logger.info(f"Running query {query}")
        return self.slave_doc_connection.query(query, retries=5)






