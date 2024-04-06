from helper.XenServerHelper.xen_server_helper import XenServerHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
import os
import datetime
import json
import logging
import threading

class RemoteXenServerHelper(XenServerHelper):

    _instance = None
    _lock = threading.Lock()
    _initialized = threading.Event()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(RemoteXenServerHelper, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not self._initialized.is_set():
            with self._lock:
                if not self._initialized.is_set():
                    super().__init__()
                    self.logger = logging.getLogger("helper")

                    ipaddr = os.environ.get("XEN_SERVER_IP")
                    ssh_username = os.environ.get("XEN_SERVER_USERNAME")
                    ssh_password = os.environ.get("XEN_SERVER_PASSWORD")

                    self.remote_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr=ipaddr,
                                                                                    ssh_username=ssh_username,
                                                                                    ssh_password=ssh_password)
                    self.logger.info("A remote shell connection was successfully established to XenServer")
                    self._initialized.set()

    def add_host(self, label, host, username, password):
        with self._lock:
            command = self.ADD_HOST_COMMAND(label=label,
                                            host=host,
                                            username=username,
                                            password=password)
            self.logger.info(f"Executing command on XenServer : {command}")
            output, error = self.remote_helper.execute_command(command)
        if len(error) != 0:
            msg = f"Command {' '.join(command)} failed with error {error}"
            self.logger.error(msg)
            raise Exception(msg)
        self.logger.info(f"Server {label} was successfully added to XenServer")
        return output[0]

    def remove_host(self, label, host):
        server_info = self.get_server_status(label, host)
        with self._lock:
            command = self.REMOVE_HOST_COMMAND(server_info['id'])
            self.logger.info(f"Executing command on XenServer : {command}")
            output, error = self.remote_helper.execute_command(command)
        if len(error) > 0:
            msg = f"Command {' '.join(command)} failed with error {error}"
            self.logger.error(msg)
            raise Exception(msg)
        self.logger.info(f"Server {label} was successfully removed from XenServer")
        return bool(output[0])

    def get_server_status(self, label, host):
        with self._lock:
            current_time = datetime.datetime.now()
            timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')
            output_file_path = f"/tmp/server_info_{timestamp_string}.json"
            command = f'{self.GET_SERVERS_STATUS_COMMAND()} > {output_file_path}'
            self.logger.info(f"Running command {command}")
            _, error = self.remote_helper.execute_command(command)
            if len(error) > 0:
                self.remote_helper.execute_command(f"rm {output_file_path}")
                msg = f"Command {' '.join(command)} failed with error {error}"
                self.logger.error(msg)
                raise Exception(msg)

            local_dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "..", f"tmp_{timestamp_string}")
            if not os.path.exists(local_dir_path):
                self.logger.info(f"Creating directory {local_dir_path}")
                try:
                    os.makedirs(local_dir_path)
                except Exception as e:
                    self.logger.error(f"Error creating directory {local_dir_path} : {e}")
                    raise e

            local_file_path = os.path.join(local_dir_path, f"{output_file_path.split('/')[-1]}")

            res = self.remote_helper.copy_file_remote_to_local(output_file_path, local_file_path)
            if res:
                self.logger.info(f"Successfuly copied file {output_file_path} from remote to local {local_file_path}")
            else:
                msg = f"Unable to copy file from remote to local : source : {output_file_path}, destination : {local_file_path}"
                self.logger.error(msg)
                raise Exception(msg)

            self.remote_helper.execute_command(f"rm {output_file_path}")

        server_info = {}
        with open(local_file_path) as json_file:
            server_info = json.load(json_file)

        if os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                self.logger.info(f"File '{local_file_path}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Unable to delete file {local_file_path} : {e}")
        else:
            self.logger.error(f"File '{output_file_path}' does not exist.")

        if os.path.exists(local_dir_path):
            try:
                os.rmdir(local_dir_path)
                self.logger.info(f"Directory '{local_dir_path}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Unable to delete directory {local_dir_path} : {e}")
        else:
            self.logger.error(f"Directory '{local_dir_path}' does not exist.")

        for server in server_info:
            if server["host"] == host and server["label"] == label:
                return server

        raise Exception(f"Host with label {label} and host {host} not found")

    def fetch_list_vms(self, label, host):

        poolId = self.get_server_status(label, host)["poolId"]

        with self._lock:
            current_time = datetime.datetime.now()
            timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')
            output_file_path = f"/tmp/list_vms_{timestamp_string}.json"
            command = f'{self.FETCH_LIST_VMS_COMMAND()} > {output_file_path}'
            self.logger.info(f"Running command {command}")

            _, error = self.remote_helper.execute_command(command)
            if len(error) > 0:
                self.remote_helper.execute_command(f"rm {output_file_path}")
                msg = f"Command {' '.join(command)} failed with error {error}"
                self.logger.error(msg)
                raise Exception(msg)

            local_dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "..", f"tmp_{timestamp_string}")
            if not os.path.exists(local_dir_path):
                self.logger.info(f"Creating directory {local_dir_path}")
                try:
                    os.makedirs(local_dir_path)
                except Exception as e:
                    self.logger.error(f"Error creating directory {local_dir_path} : {e}")
                    raise e

            local_file_path = os.path.join(local_dir_path, f"{output_file_path.split('/')[-1]}")

            res = self.remote_helper.copy_file_remote_to_local(output_file_path, local_file_path)
            if res:
                self.logger.info(f"Successfuly copied file {output_file_path} from remote to local {local_file_path}")
            else:
                msg = f"Unable to copy file from remote to local : source : {output_file_path}, destination : {local_file_path}"
                self.logger.error(msg)
                raise Exception(msg)

            self.remote_helper.execute_command(f"rm {output_file_path}")

        list_vms = {}
        with open(local_file_path) as json_file:
            list_vms = json.load(json_file)

        if os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                self.logger.info(f"File '{local_file_path}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Unable to delete file {local_file_path} : {e}")
        else:
            self.logger.error(f"File '{output_file_path}' does not exist.")

        if os.path.exists(local_dir_path):
            try:
                os.rmdir(local_dir_path)
                self.logger.info(f"Directory '{local_dir_path}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Unable to delete directory {local_dir_path} : {e}")
        else:
            self.logger.error(f"Directory '{local_dir_path}' does not exist.")

        target_vms = []
        for vm in list_vms:
            if vm["$poolId"] == poolId:
                target_vms.append(vm)

        return target_vms

    def fetch_list_hosts(self, label, host):
        poolId = self.get_server_status(label, host)["poolId"]

        with self.lock:
            current_time = datetime.datetime.now()
            timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')
            output_file_path = f"/tmp/list_hosts_{timestamp_string}.json"
            command = f'{self.FETCH_LIST_HOSTS_COMMAND()} > {output_file_path}'

            _, error = self.remote_helper.execute_command(command)
            if len(error) > 0:
                self.remote_helper.execute_command(f"rm {output_file_path}")
                msg = f"Command {' '.join(command)} failed with error {error}"
                self.logger.error(msg)
                raise Exception(msg)

            local_dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "..", f"tmp_{timestamp_string}")
            if not os.path.exists(local_dir_path):
                self.logger.info(f"Creating directory {local_dir_path}")
                try:
                    os.makedirs(local_dir_path)
                except Exception as e:
                    self.logger.error(f"Error creating directory {local_dir_path} : {e}")
                    raise e

            local_file_path = os.path.join(local_dir_path, f"{output_file_path.split('/')[-1]}")

            res = self.remote_helper.copy_file_remote_to_local(output_file_path, local_file_path)
            if res:
                self.logger.info(f"Successfuly copied file {output_file_path} from remote to local {local_file_path}")
            else:
                msg = f"Unable to copy file from remote to local : source : {output_file_path}, destination : {local_file_path}"
                self.logger.error(msg)
                raise Exception(msg)

            self.remote_helper.execute_command(f"rm {output_file_path}")

        list_hosts = {}
        with open(local_file_path) as json_file:
            list_hosts = json.load(json_file)

        if os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                self.logger.info(f"File '{local_file_path}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Unable to delete file {local_file_path} : {e}")
        else:
            self.logger.error(f"File '{output_file_path}' does not exist.")

        if os.path.exists(local_dir_path):
            try:
                os.rmdir(local_dir_path)
                self.logger.info(f"Directory '{local_dir_path}' deleted successfully.")
            except Exception as e:
                self.logger.error(f"Unable to delete directory {local_dir_path} : {e}")
        else:
            self.logger.error(f"Directory '{local_dir_path}' does not exist.")

        for host in list_hosts:
            if host["$poolId"] == poolId:
                return host

        raise Exception(f"Host with label {label} and host {host} not found")
