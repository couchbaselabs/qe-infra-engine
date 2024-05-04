import threading
import logging
import re
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
        status, response = self.rest_client.request(endpoint)
        if status != 200:
            raise Exception(f"Request to {self.url+endpoint} failed with status {status} : {response}")
        return status, response
    
    def get_slave_info(self, slave_name):
        endpoint = f"/computer/{slave_name}/api/json"
        status, response = self.rest_client.request(endpoint)
        if status != 200:
            raise Exception(f"Request to {self.url+endpoint} failed with status {status} : {response}")
        return status, response
    
    def get_slave_ipaddr(self, slave_name):
        endpoint = f"computer/{slave_name}/scriptText"
        scripts = ['println "ifconfig eth0".execute().text',
                   'println "ip addr show enX0".execute().text',
                   'println "ip addr show eth0".execute().text',
                   'println "ip addr show enX2".execute().text',
                   'println "ip addr show enX3".execute().text',
                   'println "ip addr show ens5".execute().text',
                   'println "ip addr show enp130s0".execute().text']
        for script in scripts:
            params = f'script={script}'
            status, output = self.rest_client.request(endpoint=endpoint,
                                                      method=RestMethods.POST,
                                                      params=params,
                                                      content_type="application/x-www-form-urlencoded")
            output = str(output)
            ip_pattern = r'inet (\d+\.\d+\.\d+\.\d+)'
            match = re.search(ip_pattern, output)
            if match:
                ipaddr = match.group(1)
                return ipaddr
        raise Exception(f"Could not find ipaddr of slave {slave_name}")
    
    def get_slave_usage(self, slave_name):
        endpoint = f"/computer/{slave_name}/loadStatistics/api/json?depth=2"
        status, response = self.rest_client.request(endpoint)
        if status != 200:
            raise Exception(f"Request to {self.url+endpoint} failed with status {status} : {response}")
        return status, response
    
    def remove_slave(self, slave_name):
        endpoint = f"/computer/{slave_name}/doDelete"
        status, response = self.rest_client.request(endpoint, method=RestMethods.DELETE)
        if status != 200:
            raise Exception(f"Request to {self.url+endpoint} failed with status {status} : {response}")
        return status, response
    
    def disconnect_slave(self, slave_name, message):
        endpoint = f"/computer/{slave_name}/doDisconnect?offlineMessage={message}"
        status, response = self.rest_client.request(endpoint, method=RestMethods.DELETE)
        if status != 200:
            raise Exception(f"Request to {self.url+endpoint} failed with status {status} : {response}")
        return status, response
    
    def reconnect_slave(self, slave_name):
        endpoint = f"/computer/{slave_name}/launchSlaveAgent"
        status, response = self.rest_client.request(endpoint)
        if status != 200:
            raise Exception(f"Request to {self.url+endpoint} failed with status {status} : {response}")
        return status, response

class SingeltonMetaClass(type):
    _instances = {}
    _cls_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._cls_lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(SingeltonMetaClass, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
    