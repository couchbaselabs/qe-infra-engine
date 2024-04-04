from util.ssh_util.install_util.test_input import TestInputServer
from util.ssh_util.shell_util.remote_connection import RemoteMachineShellConnection
from helper.RemoteConnectionHelper.remote_connection_helper import RemoteConnectionHelper
from helper.RemoteConnectionHelper.platforms.Windows.windows_helper import WindowsHelper
from helper.RemoteConnectionHelper.platforms.Mac.mac_helper import MacHelper
from helper.RemoteConnectionHelper.platforms.Linux.linux_helper import LinuxHelper
from helper.RemoteConnectionHelper.platforms.Linux.DebianBased.debian_helper import DebianHelper
from helper.RemoteConnectionHelper.platforms.Linux.RPMBased.rpm_helper import RPMHelper
from helper.RemoteConnectionHelper.platforms.Linux.RPMBased.SUSELinux.suse_helper import SUSEHelper

import threading


class RemoteConnectionObjectFactory:
    _objs = {}
    _lock = threading.Lock()

    @staticmethod
    def fetch_helper(ipaddr, ssh_username, ssh_password):
        if ipaddr in RemoteConnectionObjectFactory._objs:
            return RemoteConnectionObjectFactory._objs[ipaddr]
        with RemoteConnectionObjectFactory._lock:
            if ipaddr not in RemoteConnectionObjectFactory._objs:
                target_object = None
                server = TestInputServer()
                server.ip = ipaddr
                server.ssh_username = ssh_username
                server.ssh_password = ssh_password

                shell = RemoteMachineShellConnection(server)
                os_info = RemoteMachineShellConnection.get_info_for_server(server)

                if os_info.type.lower() == "linux":
                    if os_info.deliverable_type.lower() == "deb":
                        target_object = DebianHelper(ipaddr, ssh_username, ssh_password)
                    elif os_info.deliverable_type.lower() == "rpm":
                        if "suse" not in os_info.distribution_version.lower():
                            target_object = RPMHelper(ipaddr, ssh_username, ssh_password)
                        else:
                            target_object = SUSEHelper(ipaddr, ssh_username, ssh_password)
                    else:
                        target_object = LinuxHelper(ipaddr, ssh_username, ssh_password)
                elif os_info.type.lower() == "mac":
                    target_object = MacHelper(ipaddr, ssh_username, ssh_password)
                elif os_info.type.lower() == "windows":
                    target_object = WindowsHelper(ipaddr, ssh_username, ssh_password)
                else:
                    target_object = RemoteConnectionHelper(ipaddr, ssh_username, ssh_password)

                shell.disconnect()

                RemoteConnectionObjectFactory._objs[ipaddr] = target_object
            return RemoteConnectionObjectFactory._objs[ipaddr]
    
    @staticmethod
    def delete_helper(ipaddr):
        if ipaddr in RemoteConnectionObjectFactory._objs:
            if RemoteConnectionObjectFactory._objs[ipaddr] is not None:
                del RemoteConnectionObjectFactory._objs[ipaddr]
        RemoteConnectionObjectFactory._objs.pop(ipaddr, None)
        # TODO - Implement in submodule
        # RemoteMachineShellConnection.delete_info_for_server(None, ipaddr)


