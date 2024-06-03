import threading
import logging
import re
import json
from constants.jenkins import JENKINS_SSH_LAUNCHER_CREDS
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
    
    def fetch_jenkins_crumb(self):
        """
        Fetch Jenkins-Crumb for CSRF protection
        """
        endpoint = '/crumbIssuer/api/json'
        status, response = self.rest_client.request(endpoint)
        if status != 200:
            raise Exception(f"Request to {self.url+endpoint} failed with status {status} : {response}")
        return status, response
    
    def add_slave(self, params: dict):
        required_fields = ["name", "description", "num_executors", "remote_fs", "labels",
                           "usage_mode", "ipaddr", "ssh_username", "ssh_password"]
        for field in required_fields:
            if field not in params:
                raise ValueError(f"{field} not present in params {params}")

        if not isinstance(params["labels"], list):
            raise ValueError(f"{params['labels']} labels is not a list")

        for label in params["labels"]:
            if " " in label:
                raise ValueError(f"label {label} consists of a space, not valid")
            
        usage_modes_allowed = ['EXCLUSIVE', 'NORMAL']
        if params["usage_mode"] not in usage_modes_allowed:
            raise ValueError(f"usage_mode {params['usage_mode']} consists of invalid usage mode, allowed : {usage_modes_allowed}")
        
        always_online_strategy = {"stapler-class": "hudson.slaves.RetentionStrategy$Always"}

        # Choose the retention strategy
        retention_strategy = always_online_strategy  # or scheduled_strategy, or demand_strategy

        slave_name = params["name"]
        credentials_id = JENKINS_SSH_LAUNCHER_CREDS[self.url][f'{params["ssh_username"]}:{params["ssh_password"]}']

        slave_config_form_data = {
            'name': slave_name,
            'type': 'hudson.slaves.DumbSlave',
            'json': json.dumps({
                'nodeDescription': params["description"],
                'numExecutors': params["num_executors"],
                'remoteFS': params["remote_fs"],
                'labelString': " ".join(params["labels"]),
                'mode': params["usage_mode"],
                'type': 'hudson.slaves.DumbSlave$DescriptorImpl',
                'retentionStrategy': retention_strategy,
                'launcher': {
                    "stapler-class": "hudson.plugins.sshslaves.SSHLauncher",
                    "host": params["ipaddr"],
                    "credentialsId": credentials_id,
                    "hostKeyVerificationStrategy": {
                    "stapler-class": "hudson.plugins.sshslaves.verifiers.ManuallyTrustedKeyVerificationStrategy"
                    }      
                }
            })
        }

        status, crumb_data = self.fetch_jenkins_crumb()    
        crumb = crumb_data['crumb']
        header_params = {
            'Jenkins-Crumb': crumb
        }
        
        endpoint = f"/computer/doCreateItem"
        status, response = self.rest_client.request(endpoint, method=RestMethods.POST,
                                                    params=slave_config_form_data,
                                                    header_params=header_params,
                                                    content_type="application/x-www-form-urlencoded")
        if status != 200:
            raise Exception(f"Request to {self.url+endpoint} failed with status {status} : {response}")
        return status, response
    
    def remove_slave(self, slave_name):
        endpoint = f"/computer/{slave_name}/doDelete"
        status, response = self.rest_client.request(endpoint, method=RestMethods.POST)
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
    