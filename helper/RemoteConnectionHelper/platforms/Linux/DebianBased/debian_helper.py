from helper.RemoteConnectionHelper.platforms.Linux.linux_helper import LinuxHelper
class DebianHelper(LinuxHelper):
    def __init__(self, ipaddr, ssh_username, ssh_password) -> None:
        super().__init__(ipaddr, ssh_username, ssh_password)

    def __del__(self):
        super().__del__()

    def install_package(self, package):
        with self.lock:
            command = f"apt-get install -y {package}"
            output, error = self.shell.execute_command(command)
        if len(error) > 0:
            msg = f"Command {command} failed with error {error}"
            self.logger.error(msg)
            raise Exception(msg)
        return output

    def apt_update(self):
        with self.lock:
            command = "apt-get update"
            output, error = self.shell.execute_command(command)
        if len(error) > 0:
            msg = f"Command {command} failed with error {error}"
            self.logger.error(msg)
            raise Exception(msg)
        return output

    def install_timesyncd(self):
        with self.lock:
            command =  "systemctl unmask systemd-timesyncd; apt-get remove -y systemd-timesyncd; apt-get install -y systemd-timesyncd; systemctl start systemd-timesyncd;"
            output, error = self.shell.execute_command(command)
        if len(error) > 0:
            msg = f"Command {command} failed with error {error}"
            self.logger.error(msg)
            raise Exception(msg)
        return output

    def set_journalctl_config(self, vacuum_size="100M", vacuum_time="10d"):
        with self.lock:
            command =  f"journalctl --vacuum-size={vacuum_size};journalctl --vacuum-time={vacuum_time}"
            output, error = self.shell.execute_command(command)
        if len(error) > 0:
            msg = f"Command {command} failed with error {error}"
            self.logger.error(msg)
            raise Exception(msg)
        return output


    def initialize_node(self):
        packages_needed = ["wget", "curl", "libtinfo5"]
        res = {}
        try:
            self.apt_update()
            res["apt-update"] = True
        except Exception as e:
            res["apt-update"] = [False, str(e)]

        try:
            self.set_journalctl_config()
            res["init_journal_logs"] = True
        except Exception as e:
            res["init_journal_logs"] = [False, str(e)]

        for package in packages_needed:
            try:
                self.install_package(package)
                res[f"install_pacakge-{package}"] = True
            except Exception as e:
                res[f"install_pacakge-{package}"] = [False, str(e)]
        return res
