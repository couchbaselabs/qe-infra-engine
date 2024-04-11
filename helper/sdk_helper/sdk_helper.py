import logging
import threading

class SDKHelper:

    def __init__(self):
        self.logger = logging.getLogger("helper")

    def fetch_all_docs(self, client, bucket_name, scope, collection):
        query = f"SELECT META().id,* FROM `{bucket_name}`.`{scope}`.`{collection}`"
        self.logger.info(f"Running query {query}")
        return client.query(query, retries=5)

    def upsert_doc(self, client, key, doc, bucket_name, scope, collection):
        res = client.upsert(key, doc, retries=5)
        if res:
            self.logger.info(f"Document with key {key} successfully upserted into {bucket_name}.{scope}.{collection}")
        else:
            self.logger.error(f"Upsert into {bucket_name}.{scope}.{collection} failed for document with key {key}")
        return res

    def get_doc(self, client, key):
        self.logger.info(f"Fetching doc with key {key}")
        return client.get(key, retries=5)
    
    def delete_doc(self, client, key, bucket_name, scope, collection):
        self.logger.info(f"Deleting doc with key {key}")
        res = client.delete_doc(key, retries=5)
        if res:
            self.logger.info(f"Document with key {key} successfully deleted from {bucket_name}.{scope}.{collection}")
        else:
            self.logger.error(f"Delete from {bucket_name}.{scope}.{collection} failed for document with key {key}")
        return res

class SingeltonMetaClass(type):
    _instances = {}
    _cls_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._cls_lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(SingeltonMetaClass, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
