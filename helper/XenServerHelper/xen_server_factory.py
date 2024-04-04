import socket
import os
import logging
from helper.XenServerHelper.LocalXenServerHelper.local_xen_server_helper import LocalXenServerHelper
from helper.XenServerHelper.RemoteXenServerHelper.remote_xen_server_helper import RemoteXenServerHelper

logger = logging.getLogger("helper")

class XenServerObjectFactory:
    @staticmethod
    def _get_local_ipaddr():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            logger.error(f"IP address of local system found successfully {local_ip}")
            return local_ip
        except Exception as e:
            logger.error(f"Error trying to find IP address of local system {e}")
            return None

    @staticmethod
    def fetch_helper():

        local_ipaddr = XenServerObjectFactory._get_local_ipaddr()
        xen_server_ipaddr = os.environ.get("XEN_SERVER_IP")

        target_object = None
        if local_ipaddr == xen_server_ipaddr:
            logger.info(f"XenServer is running on the same local ip")
            target_object = LocalXenServerHelper()
        else:
            logger.info(f"XenServer is not running locally and is on a remote host")
            target_object = RemoteXenServerHelper()

        return target_object