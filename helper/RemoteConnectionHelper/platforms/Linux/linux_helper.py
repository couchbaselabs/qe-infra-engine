from helper.RemoteConnectionHelper.remote_connection_helper import RemoteConnectionHelper
class LinuxHelper(RemoteConnectionHelper):
    def __init__(self, ipaddr, ssh_username, ssh_password) -> None:
        super().__init__(ipaddr, ssh_username, ssh_password)

    def __del__(self):
        super().__del__()

    def find_os_version(self):
        os = ""
        os_version = ""

        with self.lock:
            command = "cat /etc/os-release"
            output, error = self.shell.execute_command(command)

        if len(error) > 0:
            msg = f"Command {command} failed with error {error}"
            self.logger.error(msg)
            raise Exception(msg)

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

        with self.lock:
            command = "ip -o link show |cut -d ' ' -f 2,20 | grep eth0"
            output, error = self.shell.execute_command(command)

        if len(error) > 0:
            msg = f"Command {command} failed with error {error}"
            self.logger.error(msg)
            raise Exception(msg)

        for l in output:
            if "eth0" in l:
                mac_addr = l.split()[1]

        return mac_addr

    def find_memory_total(self):
        memory = 0

        with self.lock:
            command = "grep MemTotal /proc/meminfo"
            output, error = self.shell.execute_command(command)

        if len(error) > 0:
            msg = f"Command {command} failed with error {error}"
            self.logger.error(msg)
            raise Exception(msg)

        for l in output:
            if "MemTotal" in l:
                memory = l.split()[1]
                memory = int(memory)
        return memory