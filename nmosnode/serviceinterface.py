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

from nmoscommon.ipc import Host
from nmoscommon.logger import Logger

ADDRESS="ipc:///tmp/ips-nodefacade"

def ipcmethod(name=None):
    def decorator(function):
        function.ipc_method = True
        function.ipc_name   = name
        return function
    if callable(name):
        return decorator(name)
    return decorator

class FacadeInterface(object):
    def __init__(self, registry, logger):
        self.host = Host(ADDRESS)
        self.registry = registry
        self.logger = Logger("facade_interface", logger)

        def getbases(cl):
            bases = list(cl.__bases__)
            for x in cl.__bases__:
                bases += getbases(x)
            return bases

        for cl in [self.__class__,] + getbases(self.__class__):
            for name in cl.__dict__.keys():
                value = getattr(self, name)
                if callable(value):
                    if hasattr(value, "ipc_method"):
                        self.host.ipcmethod(name)(value)

    def start(self):
        self.host.start()

    def stop(self):
        self.host.stop()

    @ipcmethod
    def srv_register(self, name, srv_type, pid, href, proxy_path):
        self.logger.writeInfo("Service Register {}, {}, {}, {}, {}".format(name, srv_type, pid, href, proxy_path))
        return self.registry.register_service(name, srv_type, pid, href, proxy_path)

    #TODO: =None should be removed once proxying removed from node facade
    @ipcmethod
    def srv_update(self, name, pid, href, proxy_path):
        self.logger.writeInfo("Service Update {}, {}, {}, {}".format(name, pid, href, proxy_path))
        return self.registry.update_service(name, pid, href, proxy_path)

    @ipcmethod
    def srv_unregister(self, name, pid):
        self.logger.writeInfo("Service Unregister {}, {}".format(name, pid))
        return self.registry.unregister_service(name, pid)

    @ipcmethod
    def srv_heartbeat(self, name, pid):
        self.logger.writeDebug("Service Heartbeat {}, {}".format(name, pid))
        return self.registry.heartbeat_service(name, pid)

    @ipcmethod
    def res_register(self, name, pid, type, key, value):
        self.logger.writeInfo("Resource Register {} {} {} {} {}".format(name, pid, type, key, value))
        return self.registry.register_resource(name, pid, type, key, value)

    @ipcmethod
    def res_update(self, name, pid, type, key, value):
        self.logger.writeInfo("Resource Update {} {} {} {} {}".format(name, pid, type, key, value))
        return self.registry.update_resource(name, pid, type, key, value)

    @ipcmethod
    def res_unregister(self, name, pid, type, key):
        self.logger.writeInfo("Resource Unregister {} {} {} {}".format(name, pid, type, key))
        return self.registry.unregister_resource(name, pid, type, key)

    @ipcmethod
    def control_register(self, name, pid, device_id, control_data):
        self.logger.writeInfo("Control Register {} {} {} {}".format(name, pid, device_id, control_data))
        return self.registry.register_control(name, pid, device_id, control_data)

    @ipcmethod
    def control_unregister(self, name, pid, device_id, control_data):
        self.logger.writeInfo("Control Unregister {} {} {} {}".format(name, pid, device_id, control_data))
        return self.registry.unregister_control(name, pid, device_id, control_data)

    @ipcmethod
    def self_get(self, name, pid, api_version):
        return self.registry.list_self(api_version)

    @ipcmethod
    def status_get(self, name, pid):
        return self.registry.aggregator.status()

    @ipcmethod
    def clock_register(self, name, pid, clk_data):
        self.logger.writeInfo("Clock Register {} {}".format(name, pid))
        return self.registry.register_clock(clk_data)

    @ipcmethod
    def clock_update(self, name, pid, clk_data):
        self.logger.writeInfo("Clock Update {} {}".format(name, pid))
        return self.registry.update_clock(clk_data)

    @ipcmethod
    def clock_unregister(self, name, pid, clk_name):
        self.logger.writeInfo("Clock Unregister {} {}".format(name, pid))
        return self.registry.unregister_clock(clk_name)
