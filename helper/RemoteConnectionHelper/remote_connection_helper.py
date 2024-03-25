import sys
sys.path.append('../qe-infra-engine')
sys.path.append('../qe-infra-engine/util/ssh_util')
sys.path.append('../qe-infra-engine/util/ssh_util/shell_util')
sys.path.append('../qe-infra-engine/util/ssh_util/install_util')

from util.ssh_util.shell_util.remote_connection import RemoteMachineShellConnection
from util.ssh_util.install_util.test_input import TestInputServer

class RemoteConnectionHelper:
    def __init__(self, ipaddr, ssh_username, ssh_password) -> None:
        server = TestInputServer()
        server.ip = ipaddr
        server.ssh_username = ssh_username
        server.ssh_password = ssh_password

        self.shell = RemoteMachineShellConnection(server)

    def __del__(self):
        self.shell.disconnect()

    def find_os_version(self):
        os = ""
        os_version = ""

        command = "cat /etc/os-release"
        output, error = self.shell.execute_command(command)

        for l in output:
            if "PRETTY_NAME" in l:
                os_version = l.split("=")[1]
                os_version = os_version.strip("\n").strip("\"")
            if "ID" in l and "VERSION_ID" not in l and "ID_LIKE" not \
                    in l:
                os = l.split("=")[1]
                os = os.strip("\n").strip("\"")

        return os, os_version
    
    def find_mac_address(self):
        mac_addr = ""

        command = "ip -o link show |cut -d ' ' -f 2,20 | grep eth0"
        output, error = self.shell.execute_command(command)

        for l in output:
            if "eth0" in l:
                mac_addr = l.split()[1]
        
        return mac_addr
    
    def find_memory(self):
        memory = 0
        command = "grep MemTotal /proc/meminfo"
        output, error = self.shell.execute_command(command)
        for l in output:
            if "MemTotal" in l:
                memory = l.split()[1]
                memory = int(memory)
        return memory

    def install_wget(self):
        command = "apt-get install -y wget"
        output, error = self.shell.execute_command(command)

    def install_curl(self):
        command = "apt-get install -y curl"
        output, error = self.shell.execute_command(command)

    def install_libtinfo(self):
        command = "apt-get install -y libtinfo5"
        output, error = self.shell.execute_command(command)

    def apt_update(self):
        command = "apt-get update"
        output, error = self.shell.execute_command(command)

    def install_timesyncd(self):
        command =  "systemctl unmask systemd-timesyncd; apt-get remove -y systemd-timesyncd; apt-get install -y systemd-timesyncd; systemctl start systemd-timesyncd;"
        output, error = self.shell.execute_command(command)
