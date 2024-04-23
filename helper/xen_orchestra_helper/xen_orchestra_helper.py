from abc import ABC, abstractmethod
class XenOrchestraHelper(ABC):

    def get_add_host_command(self, label, host, username, password):
        add_host_command = f"xo-cli server.add label={label} host={host} username={username} password={password} allowUnauthorized=true"
        return add_host_command

    def get_remove_host_command(self, id):
        remove_host_command = f"xo-cli server.remove id={id}"
        return remove_host_command

    def get_servers_status_command(self):
        get_servers_status_command = "xo-cli server.getAll --json"
        return get_servers_status_command

    def get_fetch_list_vms_command(self):
        fetch_list_vms_command = "xo-cli --list-objects type=VM"
        return fetch_list_vms_command

    def get_fetch_list_hosts_command(self):
        fetch_list_hosts_command = "xo-cli --list-objects type=host"
        return fetch_list_hosts_command

    @abstractmethod
    def add_host(label, host, username, password):
        pass

    @abstractmethod
    def remove_host(label, host):
        pass

    @abstractmethod
    def get_server_status(label, host):
        pass

    @abstractmethod
    def fetch_list_vms(label, host):
        pass

    @abstractmethod
    def fetch_list_hosts(label, host):
        pass

