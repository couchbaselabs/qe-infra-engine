import threading
import logging
from util.rest_util.rest_client import RestClient, RestMethods

class JenkinsHelper:

    def __init__(self, url, username, password):
        self.logger = logging.getLogger("helper")
        self.url = url
        self.password = password
        self.username = username
        self.rest_client = RestClient(base_url=url,
                                      username=username,
                                      password=password)
    
    def get_all_slaves_info(self):
        endpoint = "/computer/api/json"
        return self.rest_client.request(endpoint)

class SingeltonMetaClass(type):
    _instances = {}
    _cls_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._cls_lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(SingeltonMetaClass, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
    