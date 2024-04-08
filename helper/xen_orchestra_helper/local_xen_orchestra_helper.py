from helper.XenOrchestraHelper.xen_orchestra_helper import XenOrchestraHelper
import subprocess
import json
import datetime
import os
import logging
import threading

class LocalXenOrchestraHelper(XenOrchestraHelper):

    _instance = None
    _lock = threading.Lock()
    _initialized = threading.Event()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(LocalXenOrchestraHelper, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not self._initialized.is_set():
            with self._lock:
                if not self._initialized.is_set():
                    super().__init__()
                    self.logger = logging.getLogger("helper")
                    self._initialized.set()

    def add_host(self, label, host, username, password):
        with self._lock:
            command = self._get_add_host_command(label=label,
                                            host=host,
                                            username=username,
                                            password=password)
            command =  command.split()
            self.logger.info(f"Executing command on XenServer : {' '.join(command)}")
            process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if process.returncode != 0:
            msg = f"Command {' '.join(command)} failed with error {process.stderr.strip()}"
            self.logger.error(msg)
            raise Exception(msg)
        self.logger.info(f"Server {label} was successfully added to XenServer")
        id = process.stdout.strip()
        return id

    def remove_host(self, label, host):
        server_info = self.get_server_status(label, host)
        with self._lock:
            command = self._get_remove_host_command(id=server_info['id'])
            command =  command.split()
            self.logger.info(f"Executing command on XenServer : {' '.join(command)}")
            process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if process.returncode != 0:
            msg = f"Command {' '.join(command)} failed with error {process.stderr.strip()}"
            self.logger.error(msg)
            raise Exception(msg)
        self.logger.info(f"Server {label} was successfully removed from XenServer")
        status = process.stdout.strip()
        return bool(status)

    def get_server_status(self, label, host):
        command = self._get_servers_status_command()
        command = command.split()

        with self._lock:
            current_time = datetime.datetime.now()
            timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')
            output_file_path = f"/tmp/server_info_{timestamp_string}.json"
            output_file = open(output_file_path, "w")
            self.logger.info(f"Running command {' '.join(command)} and output is piped to {output_file_path}")
            process = subprocess.run(command, stdout=output_file, stderr=subprocess.PIPE, universal_newlines=True)
            output_file.close()

        if process.returncode != 0:
            if os.path.exists(output_file_path):
                os.remove(output_file_path)
                self.logger.info(f"File '{output_file_path}' deleted successfully.")
            msg = f"Command {' '.join(command)} failed with error {process.stderr.strip()}"
            self.logger.error(msg)
            raise Exception(msg)

        server_info = {}
        with open(output_file_path) as json_file:
            server_info = json.load(json_file)

        if os.path.exists(output_file_path):
            try:
                os.remove(output_file_path)
                self.logger.info(f"File '{output_file_path}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Unable to delete file {output_file_path} : {e}")
        else:
            self.logger.error(f"File '{output_file_path}' does not exist.")

        for server in server_info:
            if server["host"] == host and server["label"] == label:
                return server

        raise Exception(f"Host with label {label} and host {host} not found")

    def fetch_list_vms(self, label, host):

        poolId = self.get_server_status(label, host)["poolId"]

        command = self._get_fetch_list_vms_command()
        command = command.split()
        with self._lock:
            current_time = datetime.datetime.now()
            timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')
            output_file_path = f"/tmp/list_vms_{timestamp_string}.json"
            output_file = open(output_file_path, "w")
            self.logger.info(f"Running command {' '.join(command)} and output is piped to {output_file_path}")
            process = subprocess.run(command, stdout=output_file, stderr=subprocess.PIPE, universal_newlines=True)
            output_file.close()

        if process.returncode != 0:
            if os.path.exists(output_file_path):
                os.remove(output_file_path)
                self.logger.info(f"File '{output_file_path}' deleted successfully.")
            msg = f"Command {' '.join(command)} failed with error {process.stderr.strip()}"
            self.logger.error(msg)
            raise Exception(msg)

        list_vms = {}
        with open(output_file_path) as json_file:
            list_vms = json.load(json_file)

        if os.path.exists(output_file_path):
            try:
                os.remove(output_file_path)
                self.logger.info(f"File '{output_file_path}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Unable to delete file {output_file_path} : {e}")
        else:
            self.logger.error(f"File '{output_file_path}' does not exist.")

        target_vms = []
        for vm in list_vms:
            if vm["$poolId"] == poolId:
                target_vms.append(vm)

        return target_vms

    def fetch_list_hosts(self, label, host):
        poolId = self.get_server_status(label, host)["poolId"]

        command = self._get_fetch_list_hosts_command()
        command = command.split()
        with self._lock:
            current_time = datetime.datetime.now()
            timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')

            output_file_path = f"/tmp/list_vms_{timestamp_string}.json"
            output_file = open(output_file_path, "w")
            self.logger.info(f"Running command {' '.join(command)} and output is piped to {output_file_path}")
            process = subprocess.run(command, stdout=output_file, stderr=subprocess.PIPE, universal_newlines=True)
            output_file.close()

        if process.returncode != 0:
            if os.path.exists(output_file_path):
                os.remove(output_file_path)
                self.logger.info(f"File '{output_file_path}' deleted successfully.")
            msg = f"Command {' '.join(command)} failed with error {process.stderr.strip()}"
            self.logger.error(msg)
            raise Exception(msg)

        list_hosts = {}
        with open(output_file_path) as json_file:
            list_hosts = json.load(json_file)

        if os.path.exists(output_file_path):
            try:
                os.remove(output_file_path)
                self.logger.info(f"File '{output_file_path}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Unable to delete file {output_file_path} : {e}")
        else:
            self.logger.error(f"File '{output_file_path}' does not exist.")

        for host in list_hosts:
            if host["$poolId"] == poolId:
                return host

        raise Exception(f"Host with label {label} and host {host} not found")
