# Copyright 2019 British Broadcasting Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, absolute_import
from gevent import monkey
monkey.patch_all()

import gevent # noqa E402
import time # noqa E402
import signal # noqa E402
import os  # noqa E402
import sys # noqa E402
import json # noqa E402
import socket # noqa E402

from six import itervalues # noqa E402
from socket import gethostname, getfqdn # noqa E402
from os import getpid, environ # noqa E402
from subprocess import check_output # noqa E402
# Handle if systemd is installed instead of newer cysystemd
try:
    from cysystemd import daemon # noqa E402
    SYSTEMD_READY = daemon.Notification.READY
except ImportError:
    from systemd import daemon # noqa E402
    SYSTEMD_READY = "READY=1"

from nmoscommon.mdns import MDNSEngine # noqa E402
from nmoscommon.logger import Logger # noqa E402
from nmoscommon import ptptime # noqa E402
from nmoscommon.httpserver import HttpServer # noqa E402
from nmoscommon.utils import get_node_id, translate_api_version, getLocalIP # noqa E402
from nmoscommon.nmoscommonconfig import config as _config # noqa E402

from .api import NODE_APIVERSIONS, NODE_REGVERSION, PROTOCOL, FacadeAPI # noqa E402
from .registry import FacadeRegistry, FacadeRegistryCleaner # noqa E402
from .aggregator import Aggregator, MDNSUpdater, ALLOWED_SCOPE, OAUTH_MODE, FQDN # noqa E402
from .authclient import AuthRegistry # noqa E402
from .serviceinterface import FacadeInterface # noqa E402

NS = 'urn:x-bbcrd:ips:ns:0.1'
PORT = 12345
HOSTNAME = gethostname().split(".", 1)[0]

# HTTPS under test only at present
# enabled = Use HTTPS only in all URLs and mDNS adverts
# disabled = Use HTTP only in all URLs and mDNS adverts
# mixed = Use HTTP in all URLs, but additionally advertise an HTTPS endpoint for discovery of this API only
ENABLE_P2P = _config.get('node_p2p_enable', True)

# BYPASS AUTHLIB SECURITY CHECK DUE TO REVERSE PROXY
environ["AUTHLIB_INSECURE_TRANSPORT"] = "1"


def updateHost():
    if _config.get('node_hostname') is not None:
        return _config.get('node_hostname')
    elif _config.get('prefer_hostnames', False) is True:
        return FQDN
    elif _config.get('prefer_ipv6', False) is False:
        return getLocalIP()
    else:
        return "[" + getLocalIP(None, socket.AF_INET6) + "]"


HOST = updateHost()
DNS_SD_HTTP_PORT = 80
DNS_SD_HTTPS_PORT = 443
DNS_SD_NAME = 'node_' + str(HOSTNAME) + "_" + str(getpid())
DNS_SD_TYPE = '_nmos-node._tcp'


class NodeFacadeService:
    def __init__(self, interactive=False):
        self.logger = Logger("facade", None)
        if HOST == "":
            self.logger.writeFatal("Unable to start facade due to lack of connectivity")
            sys.exit(1)
        self.running = False
        self.httpServer = None
        self.interface = None
        self.interactive = interactive
        self.registry = None
        self.registry_cleaner = None
        self.node_id = None
        self.mdns = MDNSEngine()
        self.mappings = {
            "device": "ver_dvc",
            "flow": "ver_flw",
            "source": "ver_src",
            "sender": "ver_snd",
            "receiver": "ver_rcv",
            "self": "ver_slf"
        }
        self.mdns_updater = None
        self.auth_registry = AuthRegistry(app=None, scope=ALLOWED_SCOPE)

        self.protocol = PROTOCOL
        if PROTOCOL == "https":
            self.dns_sd_port = DNS_SD_HTTPS_PORT
        else:
            self.dns_sd_port = DNS_SD_HTTP_PORT
        if ENABLE_P2P:
            self.mdns_updater = MDNSUpdater(
                self.mdns, DNS_SD_TYPE, DNS_SD_NAME, self.mappings, self.dns_sd_port, self.logger,
                txt_recs=self._mdns_txt(NODE_APIVERSIONS, self.protocol, OAUTH_MODE)
            )

        self.aggregator = Aggregator(self.logger, self.mdns_updater, self.auth_registry)

    def _mdns_txt(self, versions, protocol, oauth_mode):
        return {
            "api_ver": ",".join(versions),
            "api_proto": protocol,
            "api_auth": str(oauth_mode).lower()
        }

    def sig_handler(self):
        print('Pressed ctrl+c')
        self.stop()

    def sig_hup_handler(self):
        if getLocalIP() != "":
            global HOST
            HOST = updateHost()
            self.registry.modify_node(
                href=self.generate_href(),
                host=HOST,
                api={"versions": NODE_APIVERSIONS, "endpoints": self.generate_endpoints()},
                interfaces=self.list_interfaces()
            )

    def generate_endpoints(self):
        endpoints = []
        endpoints.append({
            "host": HOST,
            "port": self.dns_sd_port,  # Everything should go via apache proxy
            "protocol": self.protocol,
            "authorization": OAUTH_MODE
        })
        return endpoints

    def generate_href(self):
        return "{}://{}/".format(PROTOCOL, HOST)

    def list_interfaces(self):
        interfaces = {}
        # Initially populate interfaces from known-good location
        net_path = "/sys/class/net/"
        if os.path.exists(net_path):
            for interface_name in os.listdir(net_path):
                address_path = net_path + interface_name + "/address"
                if os.path.exists(address_path) and interface_name != 'lo':
                    with open(address_path, 'r') as address_file:
                        address = address_file.readline().strip('\n')
                        if address:
                            interfaces[interface_name] = {
                                "name": interface_name,
                                "chassis_id": None,
                                "port_id": address.lower().replace(":", "-")
                            }

        # Attempt to source proper LLDP data for interfaces
        if os.path.exists("/usr/sbin/lldpcli"):
            try:
                chassis_data = json.loads(check_output(["/usr/sbin/lldpcli", "show", "chassis", "-f", "json"]))
                chassis_id = chassis_data["local-chassis"]['chassis'].values()[0]["id"]["value"]
                if chassis_data["local-chassis"]['chassis'].values()[0]["id"]["type"] == "mac":
                    chassis_id = chassis_id.lower().replace(":", "-")
                interface_data = json.loads(check_output(["/usr/sbin/lldpcli", "show", "statistics", "-f", "json"]))
                if isinstance(interface_data["lldp"]["interface"], dict):
                    for interface_name in interface_data["lldp"]["interface"].keys():
                        if interface_name in interfaces:
                            # Only correct the Chassis ID. Port ID MUST be a MAC address
                            interfaces[interface_name]["chassis_id"] = chassis_id
                else:
                    for interface_block in interface_data["lldp"]["interface"]:
                        interface_name = interface_block.keys()[0]
                        if interface_name in interfaces:
                            # Only correct the Chassis ID. Port ID MUST be a MAC address
                            interfaces[interface_name]["chassis_id"] = chassis_id
            except Exception:
                pass

        return list(itervalues(interfaces))

    def start(self):
        if self.running:
            gevent.signal_handler(signal.SIGINT, self.sig_handler)
            gevent.signal_handler(signal.SIGTERM, self.sig_handler)
            gevent.signal_handler(signal.SIGHUP, self.sig_hup_handler)

        self.mdns.start()
        self.node_id = get_node_id()
        node_version = str(ptptime.ptp_detail()[0]) + ":" + str(ptptime.ptp_detail()[1])
        node_data = {
            "id": self.node_id,
            "label": _config.get('node_label', FQDN),
            "description": _config.get('node_description', "Node on {}".format(FQDN)),
            "tags": _config.get('node_tags', {}),
            "href": self.generate_href(),
            "host": HOST,
            "services": [],
            "hostname": HOSTNAME,
            "caps": {},
            "version": node_version,
            "api": {
                "versions": NODE_APIVERSIONS,
                "endpoints": self.generate_endpoints(),
            },
            "clocks": [],
            "interfaces": self.list_interfaces()
        }
        self.registry = FacadeRegistry(
            self.mappings.keys(),
            self.aggregator,
            self.mdns_updater,
            self.node_id,
            node_data,
            self.logger
        )
        self.registry_cleaner = FacadeRegistryCleaner(self.registry)
        self.registry_cleaner.start()
        self.httpServer = HttpServer(
            FacadeAPI, PORT, '0.0.0.0', api_args=[self.registry, self.auth_registry])
        self.httpServer.start()
        while not self.httpServer.started.is_set():
            self.logger.writeInfo('Waiting for httpserver to start...')
            self.httpServer.started.wait()

        if self.httpServer.failed is not None:
            raise self.httpServer.failed

        self.logger.writeInfo("Running on port: {}".format(self.httpServer.port))

        try:
            self.logger.writeInfo("Registering as {}...".format(self.node_id))
            self.aggregator.register('node', self.node_id, **translate_api_version(node_data, "node", NODE_REGVERSION))
        except Exception as e:
            self.logger.writeWarning("Could not register: {}".format(e.__repr__()))

        self.interface = FacadeInterface(self.registry, self.logger)
        self.interface.start()

    def run(self):
        self.running = True
        pidfile = "/tmp/ips-nodefacade.pid"
        with open(pidfile, 'w') as f:
            f.write(str(getpid()))
        self.start()
        daemon.notify(SYSTEMD_READY)
        while self.running:
            self.registry.update_ptp()
            time.sleep(1)
        os.unlink(pidfile)

    def _cleanup(self):
        try:
            self.logger.writeDebug("cleanup: unregister facade " + self.node_id)
            self.aggregator.unregister('node', self.node_id)
        except Exception as e:
            self.logger.writeWarning("Could not unregister: {}".format(e))

        if self.mdns:
            try:
                self.mdns.stop()
            except Exception as e:
                self.logger.writeWarning("Could not stop mdns: {}".format(e))

        self.registry_cleaner.stop()
        self.interface.stop()
        self.httpServer.stop()
        self.aggregator.stop()
        self.mdns_updater.stop()
        self.logger.writeInfo("Stopped main()")

    def stop(self):
        self._cleanup()
        self.running = False


if __name__ == '__main__':
    Service = NodeFacadeService()
    Service.run()
