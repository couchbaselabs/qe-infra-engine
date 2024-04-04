from abc import ABC, abstractmethod
class XenServerHelper(ABC):

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
