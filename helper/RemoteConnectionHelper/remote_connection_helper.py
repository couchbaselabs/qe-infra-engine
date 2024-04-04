from util.ssh_util.shell_util.remote_connection import RemoteMachineShellConnection
from util.ssh_util.install_util.test_input import TestInputServer
import logging
import threading

class RemoteConnectionHelper:
    def __init__(self, ipaddr, ssh_username, ssh_password) -> None:
        server = TestInputServer()
        server.ip = ipaddr
        server.ssh_username = ssh_username
        server.ssh_password = ssh_password

        self.shell = RemoteMachineShellConnection(server)

        self.logger = logging.getLogger("helper")
        self.lock = threading.Lock()

    def __del__(self):
        self.shell.disconnect()
        self.logger.critical("Disconnected shell connection")