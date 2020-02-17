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

import json
import time
import threading
import copy
from six.moves.urllib.parse import urlparse, urlunparse
from six import itervalues

from nmoscommon.logger import Logger
from nmoscommon import ptptime
from nmoscommon.mdns.mdnsExceptions import ServiceAlreadyExistsException
from nmoscommon.utils import translate_api_version, api_ver_compare

from .api import NODE_REGVERSION, PROTOCOL

try:
    # Use internal BBC RD ipputils to get PTP if available
    from pyipputils.ippclock import IppClock
    IPP_UTILS_CLOCK_AVAILABLE = True
except ImportError:
    # Library not available, use fallback
    IPP_UTILS_CLOCK_AVAILABLE = False

HEARTBEAT_TIMEOUT = 12  # Seconds
CLEANUP_INTERVAL = 5  # Seconds

# TODO: Enumerate return codes better?

RES_SUCCESS = 0
RES_EXISTS = 1
RES_NOEXISTS = 2
RES_UNAUTHORISED = 3
RES_UNSUPPORTED = 4
RES_OTHERERROR = 5


class FacadeRegistryCleaner(threading.Thread):
    def __init__(self, registry):
        self.stopping = False
        self.registry = registry
        super(FacadeRegistryCleaner, self).__init__()
        self.daemon = True

    def run(self):
        loopcount = 0
        while not self.stopping:
            time.sleep(1)
            loopcount += 1
            if loopcount >= CLEANUP_INTERVAL:
                self.registry.cleanup_services()
                loopcount = 0

    def stop(self):
        self.stopping = True
        self.join()


class FacadeRegistry(object):
    def __init__(self, resources, aggregator, mdns_updater, node_id, node_data, logger=None):
        # `node_data` must be correctly structured
        self.permitted_resources = resources
        self.services = {}
        self.clocks = {"clk0": {"name": "clk0", "ref_type": "internal"}}
        self.aggregator = aggregator
        self.mdns_updater = mdns_updater
        self.node_id = node_id
        assert "interfaces" in node_data  # Check data conforms to latest supported API version
        self.node_data = node_data
        self.logger = Logger("facade_registry", logger)

    def modify_node(self, **kwargs):
        for key in kwargs.keys():
            if key in self.node_data:
                self.node_data[key] = kwargs[key]
        self.update_node()

    def update_node(self):
        self.node_data["services"] = []
        for service_name in self.services:
            href = None
            if self.services[service_name]["href"]:
                if self.services[service_name]["proxy_path"]:
                    href = self.node_data["href"] + self.services[service_name]["proxy_path"]
            self.node_data["services"].append({
                "href": href,
                "type": self.services[service_name]["type"],
                "authorization": self.services[service_name]["authorization"]
            })
        self.node_data["clocks"] = list(itervalues(self.clocks))
        self.node_data["version"] = str(ptptime.ptp_detail()[0]) + ":" + str(ptptime.ptp_detail()[1])
        try:
            self.aggregator.register("node", self.node_id, **self.preprocess_resource("node", self.node_data["id"],
                                     self.node_data, NODE_REGVERSION))
        except Exception as e:
            self.logger.writeError("Exception re-registering node: {}".format(e))

    def register_service(self, name, srv_type, pid, href=None, proxy_path=None, authorization=False):
        if name in self.services:
            return RES_EXISTS

        self.services[name] = {
            "heartbeat": time.time(),
            "resource": {},                     # Registered resources live under here
            "control": {},                      # Registered device controls live under here
            "pid": pid,
            "href": href,
            "proxy_path": proxy_path,
            "type": srv_type,
            "authorization": authorization
        }

        for resource_name in self.permitted_resources:
            self.services[name]["resource"][resource_name] = {}

        self.update_node()
        return RES_SUCCESS

    def update_service(self, name, pid, href=None, proxy_path=None):
        if name not in self.services:
            return RES_NOEXISTS
        if self.services[name]["pid"] != pid:
            return RES_UNAUTHORISED
        self.services[name]["heartbeat"] = time.time()
        self.services[name]["href"] = href
        self.services[name]["proxy_path"] = proxy_path
        self.update_node()
        return RES_SUCCESS

    def unregister_service(self, name, pid):
        if name not in self.services:
            return RES_NOEXISTS
        if self.services[name]["pid"] != pid:
            return RES_UNAUTHORISED
        for namespace in ["resource", "control"]:
            for type in self.services[name][namespace].keys():
                for key in self.services[name][namespace][type].keys():
                    if namespace == "control":
                        self._register(name, "control", pid, type, "remove", self.services[name][namespace][type][key])
                    else:
                        self._unregister(name, namespace, pid, type, key)
        self.services.pop(name, None)
        self.update_node()
        return RES_SUCCESS

    def heartbeat_service(self, name, pid):
        if name not in self.services:
            return RES_NOEXISTS
        if self.services[name]["pid"] != pid:
            return RES_UNAUTHORISED
        self.services[name]["heartbeat"] = time.time()
        return RES_SUCCESS

    def cleanup_services(self):
        timed_out = time.time() - HEARTBEAT_TIMEOUT
        for name in list(self.services.keys()):
            if self.services[name]["heartbeat"] < timed_out:
                self.unregister_service(name, self.services[name]["pid"])

    def register_resource(self, service_name, pid, type, key, value):
        if type not in self.permitted_resources:
            return RES_UNSUPPORTED
        return self._register(service_name, "resource", pid, type, key, value)

    def register_control(self, service_name, pid, device_id, control_data):
        return self._register(
            service_name=service_name,
            namespace="control",
            pid=pid,
            type=device_id,
            key="add",
            value=control_data
        )

    def _register(self, service_name, namespace, pid, type, key, value):
        if namespace != "control":
            if "max_api_version" not in value:
                self.logger.writeWarning(
                    "Service {}: Registration without valid api version specified".format(service_name)
                )
                value["max_api_version"] = "v1.0"
            elif api_ver_compare(value["max_api_version"], NODE_REGVERSION) < 0:
                self.logger.writeWarning(
                    "Trying to register resource with api version too low: '{}' : {}".format(key, json.dumps(value))
                )
        if service_name not in self.services:
            return RES_NOEXISTS
        if not self.services[service_name]["pid"] == pid:
            return RES_UNAUTHORISED
        if key == "00000000-0000-0000-0000-000000000000":
            return RES_OTHERERROR

        # Add a node_id to those resources which need one
        if type == 'device':
            value['node_id'] = self.node_id

        if namespace == "control":
            if type not in self.services[service_name][namespace]:
                # 'type' is the Device ID in this case
                self.services[service_name][namespace][type] = {}

            if key == "add":
                # Register
                self.services[service_name][namespace][type][value["href"]] = value
            else:
                # Unregister
                self.services[service_name][namespace][type].pop(value["href"], None)

            # Reset the parameters below to force re-registration of the corresponding Device
            namespace = "resource"
            key = type  # Device ID
            type = "device"
            value = None

            for name in self.services:  # Find the service which registered the Device in question
                if key in self.services[name]["resource"][type]:
                    value = self.services[name]["resource"][type][key]
                    break

            if not value:  # Device isn't actually registered at present
                return RES_SUCCESS
        else:
            self.services[service_name][namespace][type][key] = value

        # Don't pass non-registration exceptions to clients
        try:
            if namespace == "resource":
                self._update_mdns(type)
        except Exception as e:
            self.logger.writeError("Exception registering with mDNS: {}".format(e))

        try:
            self.aggregator.register_into(namespace, type, key, **self.preprocess_resource(type, key, value,
                                                                                           NODE_REGVERSION))
            self.logger.writeDebug("registering {} {}".format(type, key))
        except Exception as e:
            self.logger.writeError("Exception registering {}: {}".format(namespace, e))
            return RES_OTHERERROR
        return RES_SUCCESS

    def update_resource(self, service_name, pid, type, key, value):
        return self.register_resource(service_name, pid, type, key, value)

    def find_service(self, type, key):
        for service_name in self.services.keys():
            if key in self.services[service_name]["resource"][type]:
                return service_name
        return None

    def unregister_resource(self, service_name, pid, type, key):
        if type not in self.permitted_resources:
            return RES_UNSUPPORTED
        return self._unregister(service_name, "resource", pid, type, key)

    def unregister_control(self, service_name, pid, device_id, control_data):
        # Note use of register here, as we're updating an existing Device
        return self._register(service_name, "control", pid, device_id, "remove", control_data)

    def _unregister(self, service_name, namespace, pid, type, key):
        if service_name not in self.services:
            return RES_NOEXISTS
        if self.services[service_name]["pid"] != pid:
            return RES_UNAUTHORISED
        if key == "00000000-0000-0000-0000-000000000000":
            return RES_OTHERERROR

        self.services[service_name][namespace][type].pop(key, None)

        # Don't pass non-registration exceptions to clients
        try:
            self.aggregator.unregister_from(namespace, type, key)
        except Exception as e:
            self.logger.writeError("Exception unregistering {}: {}".format(namespace, e))
            return RES_OTHERERROR
        try:
            if namespace == "resource":
                self._update_mdns(type)
        except ServiceAlreadyExistsException as e:
            # We can't do anything about this, so just return success
            self.logger.writeError("Exception unregistering from mDNS: {}".format(e))
        except Exception as e:
            self.logger.writeError("Exception unregistering from mDNS: {}".format(e))
            return RES_OTHERERROR
        return RES_SUCCESS

    def list_services(self, api_version="v1.0"):
        return list(self.services.keys())

    def get_service_href(self, name, api_version="v1.0"):
        if name not in self.services:
            return RES_NOEXISTS
        href = self.services[name]["href"]
        if self.services[name]["proxy_path"]:
            href += "/" + self.services[name]["proxy_path"]
        return href

    def get_service_type(self, name, api_version="v1.0"):
        if name not in self.services:
            return RES_NOEXISTS
        return self.services[name]["type"]

    def preprocess_url(self, url):
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        if PROTOCOL == "https":
            if scheme == "http":
                scheme = "https"
            elif scheme == "ws":
                scheme = "wss"
        netloc = self.node_data["host"]
        if parsed_url.port:
            netloc += ":{}".format(parsed_url.port)
        parsed_url = parsed_url._replace(netloc=netloc, scheme=scheme)
        return urlunparse(parsed_url)

    def preprocess_resource(self, type, key, value, api_version="v1.0"):
        if type == "device":
            value_copy = copy.deepcopy(value)
            for name in self.services:
                if key in self.services[name]["control"] and "controls" in value_copy:
                    value_copy["controls"] = value_copy["controls"] + list(self.services[name]["control"][key].values())
            if "controls" in value_copy:
                for control in value_copy["controls"]:
                    control["href"] = self.preprocess_url(control["href"])
            return translate_api_version(value_copy, type, api_version)
        elif type == "sender":
            value_copy = copy.deepcopy(value)
            if "manifest_href" in value_copy:
                value_copy["manifest_href"] = self.preprocess_url(value_copy["manifest_href"])
            return translate_api_version(value_copy, type, api_version)
        else:
            return translate_api_version(value, type, api_version)

    def list_resource(self, type, api_version="v1.0"):
        if type not in self.permitted_resources:
            return RES_UNSUPPORTED
        response = {}
        for name in self.services:
            response = (dict(list(response.items()) + [
                (k, self.preprocess_resource(type, k, x, api_version))
                for (k, x) in self.services[name]["resource"][type].items()
                if (api_version == "v1.0" or (
                    "max_api_version" in x and api_ver_compare(x["max_api_version"], api_version) >= 0
                ))
            ]))
        return response

    def _len_resource(self, type):
        response = 0
        for name in self.services:
            response += len(self.services[name]["resource"][type])
        return response

    def _update_mdns(self, type):
        if type not in self.permitted_resources:
            return RES_UNSUPPORTED
        if not self.mdns_updater:
            return
        num_items = self._len_resource(type)
        if num_items == 1:
            try:
                self.mdns_updater.update_mdns(type, "register")
            except Exception:
                self.mdns_updater.update_mdns(type, "update")
        elif num_items == 0:
            self.mdns_updater.update_mdns(type, "unregister")
        else:
            self.mdns_updater.update_mdns(type, "update")

    def list_self(self, api_version="v1.0"):
        return self.preprocess_resource("node", self.node_data["id"], self.node_data, api_version)

    def _ptp_clock(self):
        clk = {
            "name": "clk1",
            "ref_type": "ptp",
            "version": "IEEE1588-2008",
            "traceable": False,
            "gmid": "00-00-00-00-00-00-00-00",
            "locked": False,
        }
        sts = IppClock().PTPStatus()
        if len(sts.keys()) > 0:
            clk['traceable'] = sts['timeTraceable']
            clk['gmid'] = sts['grandmasterClockIdentity'].lower()
            clk['locked'] = (sts['ofm'][0] == 0)
        return clk

    def update_ptp(self):
        if IPP_UTILS_CLOCK_AVAILABLE:
            old_clk = None
            if "clk1" in self.clocks:
                old_clk = copy.copy(self.clocks["clk1"])
            clk = self._ptp_clock()
            if old_clk is None:
                self.register_clock(clk)
            elif clk != old_clk:
                self.update_clock(clk)

    def register_clock(self, clk_data):
        if "name" not in clk_data:
            return RES_OTHERERROR
        if clk_data["name"] in self.clocks:
            return RES_EXISTS
        self.clocks[clk_data["name"]] = clk_data
        self.update_node()
        return RES_SUCCESS

    def update_clock(self, clk_data):
        if "name" not in clk_data:
            return RES_OTHERERROR
        if clk_data["name"] in self.clocks:
            self.clocks[clk_data["name"]] = clk_data
            self.update_node()
            return RES_SUCCESS
        return RES_NOEXISTS

    def unregister_clock(self, clk_name):
        if clk_name in self.clocks:
            del self.clocks[clk_name]
            self.update_node()
            return RES_SUCCESS
        return RES_NOEXISTS


if __name__ == "__main__":
    import uuid
    registry = FacadeRegistry()
    print("Registering service and flow")
    registry.register_service("pipelinemanager", 100, "http://127.0.0.1:12345")
    test_key = str(uuid.uuid4())
    registry.register_resource("pipelinemanager", "flow", test_key, {"label": "test"})
    registry.cleanup_services()
    print("Find Service:", registry.find_service("flow", test_key))
    print("Self:", registry.list_self())
    print("Flows:", registry.list_resource("flow"))
    print("Sources:", registry.list_resource("source"))
    print("Sleeping for", HEARTBEAT_TIMEOUT + 1, "seconds")
    time.sleep(HEARTBEAT_TIMEOUT + 1)
    registry.cleanup_services()
    # registry.unregister_service("pipelinemanager", 100)
    print("Self:", registry.list_self())
    print("Flows:", registry.list_resource("flow"))
    print("Soures:", registry.list_resource("source"))
