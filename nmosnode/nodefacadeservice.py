# Copyright 2017 British Broadcasting Corporation
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

import gevent
from gevent import monkey
monkey.patch_all()

import time
import signal
import os
import sys
import json

from nmoscommon.httpserver import HttpServer
from nmoscommon.utils import get_node_id
from socket import gethostname, getfqdn
from .api import FacadeAPI
from .registry import FacadeRegistry, FacadeRegistryCleaner, legalise_resource
from .serviceinterface import FacadeInterface
from os import getpid
from subprocess import check_output
from systemd import daemon

from .api import NODE_APIVERSIONS
from .api import NODE_REGVERSION

from nmoscommon.utils import getLocalIP
from nmoscommon.aggregator import Aggregator
from nmoscommon.aggregator import MDNSUpdater
from nmoscommon.mdns   import MDNSEngine
from nmoscommon.logger import Logger
from nmoscommon import ptptime
from nmoscommon import nmoscommonconfig
import socket

NS = 'urn:x-bbcrd:ips:ns:0.1'
PORT = 12345
HOSTNAME = gethostname().split(".", 1)[0]
FQDN = getfqdn()

# HTTPS under test only at present
# enabled = Use HTTPS only in all URLs and mDNS adverts
# disabled = Use HTTP only in all URLs and mDNS adverts
# mixed = Use HTTP in all URLs, but additionally advertise an HTTPS endpoint for discovery of this API only
HTTPS_MODE = nmoscommonconfig.config.get('https_mode', 'disabled')

def updateHost () :
    if nmoscommonconfig.config.get('node_hostname') is not None:
        return nmoscommonconfig.config.get('node_hostname')
    elif nmoscommonconfig.config.get('prefer_ipv6',False) == False:
        return getLocalIP()
    else :
        return "[" + getLocalIP(None, socket.AF_INET6) + "]"

HOST = updateHost()

class NodeFacadeService:
    def __init__(self, interactive=False):
        self.logger           = Logger("facade", None)
        if HOST == "":
            self.logger.writeFatal("Unable to start facade due to lack of connectivity")
            sys.exit(1)
        self.running          = False
        self.httpServer       = None
        self.interface        = None
        self.interactive      = interactive
        self.registry         = None
        self.registry_cleaner = None
        self.node_id          = None
        self.mdns             = MDNSEngine()
        self.mdnsname_suffix  = '_' + str(HOSTNAME) + "_" + str(getpid())
        self.mappings         = {"device": "ver_dvc", "flow": "ver_flw", "source": "ver_src", "sender":"ver_snd", "receiver":"ver_rcv", "self":"ver_slf"}
        self.mdns_updater     = MDNSUpdater(self.mdns, "_nmos-node._tcp", "node" + self.mdnsname_suffix, self.mappings, PORT, self.logger, txt_recs={"api_ver": "v1.0,v1.1,v1.2", "api_proto": "http"})
        self.aggregator       = Aggregator(self.logger, self.mdns_updater)

    def sig_handler(self):
        print('Pressed ctrl+c')
        self.stop()

    def sig_hup_handler(self):
        if getLocalIP() != "":
            global HOST
            HOST = updateHost()
            self.registry.modify_node(href=self.generate_href(),
                                      host=HOST,
                                      api={"versions": NODE_APIVERSIONS, "endpoints": self.generate_endpoints()},
                                      interfaces=self.list_interfaces())

    def generate_endpoints(self):
        endpoints = []
        if HTTPS_MODE != "enabled":
            endpoints.append({
                "host" : HOST,
                "port" : 80, #Everything should go via apache proxy
                "protocol" : "http"
            })
        if HTTPS_MODE != "disabled":
            endpoints.append({
                "host" : HOST,
                "port" : 443, #Everything should go via apache proxy
                "protocol" : "https"
            })
        return endpoints

    def generate_href(self):
        if HTTPS_MODE == "enabled":
            return "https://{}/".format(HOST)
        else:
            return "http://{}/".format(HOST)

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

        return interfaces.values()

    def start(self):
        if self.running:
            gevent.signal(signal.SIGINT,  self.sig_handler)
            gevent.signal(signal.SIGTERM, self.sig_handler)
            gevent.signal(signal.SIGHUP, self.sig_hup_handler)

        self.mdns.start()
        self.node_id = get_node_id()
        node_version = str(ptptime.ptp_detail()[0]) + ":" + str(ptptime.ptp_detail()[1])
        node_data = { "id": self.node_id,
                      "label": nmoscommonconfig.config.get('node_label', FQDN),
                      "description" : nmoscommonconfig.config.get('node_description', "Node on {}".format(FQDN)),
                      "tags" : nmoscommonconfig.config.get('node_tags', {}),
                      "href": self.generate_href(),
                      "host": HOST,
                      "services": [],
                      "hostname": HOSTNAME,
                      "caps": {},
                      "version": node_version,
                      "api" : {
                          "versions" : NODE_APIVERSIONS,
                          "endpoints" : self.generate_endpoints(),
                      },
                      "clocks" : [],
                      "interfaces": self.list_interfaces()
        }
        self.registry = FacadeRegistry(self.mappings.keys(), self.aggregator, self.mdns_updater, self.node_id, node_data, self.logger)
        self.registry_cleaner = FacadeRegistryCleaner(self.registry)
        self.registry_cleaner.start()
        self.httpServer = HttpServer(FacadeAPI, PORT, '0.0.0.0', api_args=[self.registry])
        self.httpServer.start()
        while not self.httpServer.started.is_set():
            self.logger.writeInfo('Waiting for httpserver to start...')
            self.httpServer.started.wait()

        if self.httpServer.failed is not None:
            raise self.httpServer.failed

        self.logger.writeInfo("Running on port: {}".format(self.httpServer.port))

        try:
            self.logger.writeInfo("Registering as {}...".format(self.node_id))
            self.aggregator.register('node', self.node_id, **legalise_resource(node_data, "node", NODE_REGVERSION))
        except Exception as e:
            self.logger.writeWarning("Could not register: {}".format(e.__repr__()))

        self.interface = FacadeInterface(self.registry, self.logger)
        self.interface.start()

    def run(self):
        self.running = True
        pidfile = "/tmp/ips-nodefacade.pid"
        file(pidfile, 'w').write(str(getpid()))
        self.start()
        daemon.notify("READY=1")
        while self.running:
            self.registry.update_ptp()
            time.sleep(1)
        os.unlink(pidfile)
        self._cleanup()

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
        self.logger.writeInfo("Stopped main()")

    def stop(self):
        self.running = False
