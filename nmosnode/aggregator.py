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

import gevent
from gevent import monkey
monkey.patch_all()

from six import itervalues # noqa E402
from six.moves.urllib.parse import urljoin # noqa E402

import requests # noqa E402
import json # noqa E402
import time # noqa E402

import gevent.queue # noqa E402

from nmoscommon.logger import Logger # noqa E402
from mdnsbridge.mdnsbridgeclient import IppmDNSBridge, NoService, EndOfServiceList # noqa E402
from nmoscommon.mdns.mdnsExceptions import ServiceNotFoundException # noqa E402

from nmoscommon.nmoscommonconfig import config as _config # noqa E402
import traceback # noqa E402

AGGREGATOR_APIVERSION = _config.get('nodefacade').get('NODE_REGVERSION')
AGGREGATOR_APINAMESPACE = "x-nmos"
AGGREGATOR_APINAME = "registration"

LEGACY_REG_MDNSTYPE = "nmos-registration"
REGISTRATION_MDNSTYPE = "nmos-register"

BACKOFF_INITIAL_TIMOUT_SECONDS = 5
BACKOFF_MAX_TIMEOUT_SECONDS = 120


class NoAggregator(Exception):
    def __init__(self, mdns_updater=None):
        if mdns_updater is not None:
            mdns_updater.inc_P2P_enable_count()
        super(NoAggregator, self).__init__("No Registration API found")


class RequestTimeout(Exception):
    pass


class InvalidRequest(Exception):
    """Client Side Error during request, HTTP 4xx"""
    def __init__(self, status_code=400):
        super(InvalidRequest, self).__init__("Invalid Request, code {}".format(status_code))
        self.status_code = status_code


class ServerSideError(Exception):
    """Exception raised when a HTTP 5xx, timeout or inability to connect returned during request.
    This indicates a server side or connectivity issue"""
    pass


class TooManyRetries(Exception):
    def __init__(self, mdns_updater=None):
        super(TooManyRetries, self).__init__("Too many retries.")


class Aggregator(object):
    """This class serves as a proxy for the distant aggregation service running elsewhere on the network.
    It will search out aggregators and locate them, falling back to other ones if the one it is connected to
    disappears, and resending data as needed."""
    def __init__(self, logger=None, mdns_updater=None):
        self.logger = Logger("aggregator_proxy", logger)
        self.mdnsbridge = IppmDNSBridge(logger=self.logger)
        self.aggregator = None
        self.registration_order = ["device", "source", "flow", "sender", "receiver"]
        self._mdns_updater = mdns_updater
        # 'registered' is a local mirror of aggregated items. There are helper methods
        # for manipulating this below.
        self._node_data = {
            'node': None,
            'registered': False,
            'entities': {
                'resource': {
                }
            }
        }
        self._running = True
        self._aggregator_list_stale = True
        self._backoff_active = False
        self._backoff_period = 0
        self._reg_queue = gevent.queue.Queue()
        self.heartbeat_thread = gevent.spawn(self._heartbeat_thread)
        self.queue_thread = gevent.spawn(self._process_queue)

    def _get_service_type(self):
        if AGGREGATOR_APIVERSION in ['v1.0', 'v1.1', 'v1.2']:
            return LEGACY_REG_MDNSTYPE
        else:
            return REGISTRATION_MDNSTYPE

    def _discovery(self):
        self.logger.writeDebug("Entering Discovery Mode")
        while not self._node_data['registered'] or not self.aggregator:
            # Wait backoff period
            self._back_off_timer()

            # Update cached list of aggregators
            if self._aggregator_list_stale:
                self._flush_cached_aggregators()

            while True:
                self.aggregator = self._get_aggregator()
                self.logger.writeDebug("Aggregator set to: {}".format(self.aggregator))

                if self.aggregator is None:
                    break

                if self._heartbeat():
                    # Successfully registered Node
                    break

    def _register_node(self):
        if self._node_data.get("node", None) is None:
            self.logger.writeDebug("No node data to register, register returning")
            return False

        # Drain the queue
        while not self._reg_queue.empty():
            try:
                self._reg_queue.get(block=False)
            except gevent.queue.Queue.Empty:
                break

        try:
            # Try register the Node 3 times with aggregator before failing back to next aggregator
            for i in range(0, 3):
                R = self._send("POST", self.aggregator, AGGREGATOR_APIVERSION, "/resource", self._node_data["node"])

                if R.status_code == 201:
                    # Continue to registered operation
                    self._registered()

                    # Trigger registration of Nodes resources
                    self._register_node_resources()

                    return True

                elif R.status_code in [200, 409]:
                    # Delete node from registry & re-register
                    if self._unregister_node(R.headers['Location']):
                        continue
                    else:
                        # Try next registry
                        return False
        except Exception as ex:
            self.writeError("Failed to register node {}".format(ex))
            return False

    def _register_node_resources(self):
        # Re-register items that must be ordered
        # Re-register things we have in the local cache.
        # "namespace" is e.g. "resource"
        # "entities" are the things associated under that namespace.
        for res_type in self.registration_order:
            for namespace, entities in self._node_data["entities"].items():
                if res_type in entities:
                    self.logger.writeInfo("Ordered re-registration for type: '{}' in namespace '{}'"
                                          .format(res_type, namespace))
                    for key in entities[res_type]:
                        self._queue_request("POST", namespace, res_type, key)

        # Re-register everything else
        # Re-register things we have in the local cache.
        # "namespace" is e.g. "resource"
        # "entities" are the things associated under that namespace.
        for namespace, entities in self._node_data["entities"].items():
            for res_type in entities:
                if res_type not in self.registration_order:
                    self.logger.writeInfo("Unordered re-registration for type: '{}' in namespace '{}'"
                                          .format(res_type, namespace))
                    for key in entities[res_type]:
                        self._queue_request("POST", namespace, res_type, key)

    def _registered(self):
        """Mark Node as registered and reset counters"""
        if(self._mdns_updater is not None):
            self._mdns_updater.P2P_disable()

        self._node_data['registered'] = True
        self._aggregator_list_stale = True

        self._reset_backoff_period()

    def _reset_backoff_period(self):
        self.logger.writeDebug("Resetting backoff period")
        self._backoff_period = 0

    def _increase_backoff_period(self):
        """Exponentially increase the backoff period, until set maximum reached"""
        self.logger.writeDebug("Increasing backoff period")
        self._aggregator_list_stale = True

        if self._backoff_period == 0:
            self._backoff_period = BACKOFF_INITIAL_TIMOUT_SECONDS
            return

        self._backoff_period *= 2
        if self._backoff_period > BACKOFF_MAX_TIMEOUT_SECONDS:
            self._backoff_period = BACKOFF_MAX_TIMEOUT_SECONDS

    def _back_off_timer(self):
        self.logger.writeDebug("Backoff timer enabled for {} seconds".format(self._backoff_period))
        self._backoff_active = True
        gevent.sleep(self._backoff_period)
        self._backoff_active = False

    def _flush_cached_aggregators(self):
        self.logger.writeDebug("Flushing cached list of aggregators")
        self._aggregator_list_stale = False
        self.mdnsbridge.updateServices(self._get_service_type)

    def _get_aggregator(self):
        protocol = "http"
        if _config.get('https_mode') == "enabled":
            protocol = "https"

        try:
            return self.mdnsbridge.getHrefWithException(self._get_service_type(), None, AGGREGATOR_APIVERSION, protocol)
        except NoService:
            self.logger.writeDebug("No Registration services found: {} {} {}".format(self._get_service_type(), AGGREGATOR_APIVERSION, protocol))
            if self._mdns_updater is not None:
                self._mdns_updater.inc_P2P_enable_count()
            self._increase_backoff_period()
            return None
        except EndOfServiceList:
            self.logger.writeDebug("End of Registration services list: {} {} {}".format(self._get_service_type(), AGGREGATOR_APIVERSION, protocol))
            self._increase_backoff_period()
            return None

    def _unregister_node(self, url_path):
        """Delete node from registry"""
        try:
            self._node_data['registered'] = False
            R = self._send_request('DELETE', self.aggregator, url_path)

            if R.status_code == 204:
                # Successfully deleted node from Registry
                return True
            else:
                return False
        except Exception:
            return False

    def _heartbeat(self):
        """Performs a heartbeat to registered aggregator
        If heartbeat fails it will take actions to correct the error, by re-registering the Node
        If successfull will return True, else will return False"""
        try:
            self.logger.writeDebug("Sending heartbeat for Node {}"
                                   .format(self._node_data["node"]["data"]["id"]))
            R = self._send("POST", self.aggregator, AGGREGATOR_APIVERSION, "/health/nodes/" + self._node_data["node"]["data"]["id"])

            if R.status_code == 200 and self._node_data["registered"]:
                # Continue to registered operation
                self._registered()
                return True

            elif R.status_code in [200, 409]:
                # Delete node from registry
                if self._unregister_node(R.headers['Location']):
                    return self._register_node()
                else:
                    # Try next registry
                    return False

        except InvalidRequest as e:
            if e.status_code == 404:
                # Re-register
                self.logger.writeWarning("404 error on heartbeat. Marking Node for re-registration")
                self._node_data["registered"] = False
                return self._register_node()
            else:
                # Other error, try next registry
                return False
        except ServerSideError:
            self.logger.writeWarning("Server Side Error on heartbeat. Trying another registry")
            return False
        except Exception:
            # Re-register
            self.logger.writeWarning("Unexpected error on heartbeat. Marking Node for re-registration")
            self._node_data["registered"] = False
            return False

    def _heartbeat_thread(self):
        """The heartbeat thread runs in the background every five seconds.
        If when it runs the Node is believed to be registered it will perform a heartbeat"""

        self.logger.writeDebug("Starting heartbeat thread")
        while self._running:
            print('looping...')
            heartbeat_wait = 5
            if not self._node_data["registered"] or not self.aggregator:
                self._discovery()
            elif self._node_data["node"]:
                # Do heartbeat
                while True:
                    if not self.aggregator:
                        print('breaking1')
                        break
                    if self._heartbeat():
                        break
                    heartbeat_wait = 0
                    if self._node_data["registered"]:
                        self.aggregator = None # Failed to send heartbeat
                        break
                    else:
                        # Heartbet returns 404 and couldnt re-register, try next aggregator
                        # Update cached list of aggregators
                        if self._aggregator_list_stale:
                            self._flush_cached_aggregators()

                        self.aggregator = self._get_aggregator()
                        self.logger.writeDebug("Aggregator changed to: {}".format(self.aggregator))
            else:
                self._node_data["registered"] = False
                if(self._mdns_updater is not None):
                    self._mdns_updater.inc_P2P_enable_count()

            while heartbeat_wait > 0 and self._running:
                gevent.sleep(1)
                heartbeat_wait -= 1
        self.logger.writeDebug("Stopping heartbeat thread")

    def _backoff_timer_thread(self):
        backoff_timeout = BACKOFF_INITIAL_TIMOUT_SECONDS
        self._backoff_active = True
        self.logger.writeDebug("Backoff thread started")

        while self._backoff_active:
            self.logger.writeDebug("Backoff timer enabled for {} seconds".format(backoff_timeout))
            gevent.sleep(backoff_timeout)

            self._process_reregister()

            if self._node_data["registered"]:
                self._backoff_active = False
                return
            else:
                backoff_timeout = backoff_timeout * 2
                if backoff_timeout > BACKOFF_MAX_TIMEOUT_SECONDS:
                    backoff_timeout = BACKOFF_MAX_TIMEOUT_SECONDS

    # Provided the Node is believed to be correctly registered, hand off a single request to the SEND method
    # On client error, clear the resource from the local mirror
    # On other error, mark Node as unregistered and trigger re-registration
    def _process_queue(self):
        self.logger.writeDebug("Starting HTTP queue processing thread")
        # Checks queue not empty before quitting to make sure unregister node gets done
        while self._running or (self._node_data["registered"] and not self._reg_queue.empty()):
            if (not self._node_data["registered"] or
                    self._reg_queue.empty() or
                    self._backoff_active or
                    not self.aggregator):
                gevent.sleep(1)
            else:
                try:
                    queue_item = self._reg_queue.get()
                    namespace = queue_item["namespace"]
                    res_type = queue_item["res_type"]
                    res_key = queue_item["key"]
                    if queue_item["method"] == "POST":
                        if res_type == "node":
                            self._register_node()
                        elif res_key in self._node_data["entities"][namespace][res_type]:
                            data = self._node_data["entities"][namespace][res_type][res_key]
                            try:
                                self._SEND("POST", "/{}".format(namespace), data)
                            except InvalidRequest as e:
                                self.logger.writeWarning("Error registering {} {}: {}".format(res_type, res_key, e))
                                self.logger.writeWarning("Request data: {}".format(data))
                                del self._node_data["entities"][namespace][res_type][res_key]

                    elif queue_item["method"] == "DELETE":
                        translated_type = res_type + 's'
                        try:
                            self._SEND("DELETE", "/{}/{}/{}".format(namespace, translated_type, res_key))
                        except InvalidRequest as e:
                            self.logger.writeWarning("Error deleting resource {} {}: {}"
                                                     .format(translated_type, res_key, e))
                    else:
                        self.logger.writeWarning("Method {} not supported for Registration API interactions"
                                                 .format(queue_item["method"]))
                except Exception:
                    self._node_data["registered"] = False
                    if(self._mdns_updater is not None):
                        self._mdns_updater.P2P_disable()
        self.logger.writeDebug("Stopping HTTP queue processing thread")

    # Queue a request to be processed.
    # Handles all requests except initial Node POST which is done in _process_reregister
    def _queue_request(self, method, namespace, res_type, key):
        self._reg_queue.put({"method": method, "namespace": namespace, "res_type": res_type, "key": key})

    # Register 'resource' type data including the Node
    # NB: Node registration is managed by heartbeat thread so may take up to 5 seconds!
    def register(self, res_type, key, **kwargs):
        self.register_into("resource", res_type, key, **kwargs)

    # Unregister 'resource' type data including the Node
    def unregister(self, res_type, key):
        self.unregister_from("resource", res_type, key)

    # General register method for 'resource' types
    def register_into(self, namespace, res_type, key, **kwargs):
        data = kwargs
        send_obj = {"type": res_type, "data": data}
        if 'id' not in send_obj["data"]:
            self.logger.writeWarning("No 'id' present in data, using key='{}': {}".format(key, data))
            send_obj["data"]["id"] = key

        if namespace == "resource" and res_type == "node":
            # Handle special Node type
            self._node_data["node"] = send_obj
        else:
            self._add_mirror_keys(namespace, res_type)
            self._node_data["entities"][namespace][res_type][key] = send_obj
        self._queue_request("POST", namespace, res_type, key)

    # General unregister method for 'resource' types
    def unregister_from(self, namespace, res_type, key):
        if namespace == "resource" and res_type == "node":
            # Handle special Node type
            self._node_data["node"] = None
        elif res_type in self._node_data["entities"][namespace]:
            self._add_mirror_keys(namespace, res_type)
            if key in self._node_data["entities"][namespace][res_type]:
                del self._node_data["entities"][namespace][res_type][key]
        self._queue_request("DELETE", namespace, res_type, key)

    # Deal with missing keys in local mirror
    def _add_mirror_keys(self, namespace, res_type):
        if namespace not in self._node_data["entities"]:
            self._node_data["entities"][namespace] = {}
        if res_type not in self._node_data["entities"][namespace]:
            self._node_data["entities"][namespace][res_type] = {}

    # Stop the Aggregator object running
    def stop(self):
        self.logger.writeDebug("Stopping aggregator proxy")
        self._running = False
        self.heartbeat_thread.join()
        self.queue_thread.join()

    def status(self):
        return {"api_href": self.aggregator,
                "api_version": AGGREGATOR_APIVERSION,
                "registered": self._node_data["registered"]}

    def _send(self, method, aggregator, api_ver, url, data=None):
        """Handle sending request to the registration API, with error handelling"""

        url = AGGREGATOR_APINAMESPACE + "/" + AGGREGATOR_APINAME + "/" + api_ver + url

        try:
            R = self._send_request(method, aggregator, url, data)
            if R is None:
                # Try another aggregator
                self.logger.writeWarning("No response from aggregator {}".format(aggregator))
                raise ServerSideError

            elif R.status_code in [200, 201, 204, 409]:
                return R

            elif (R.status_code//100) == 4:
                self.logger.writeWarning("{} response from aggregator: {} {}"
                                         .format(R.status_code, method, urljoin(aggregator, url)))
                raise InvalidRequest(R.status_code)

            else:
                self.logger.writeWarning("Unexpected status from aggregator {}: {}, {}"
                                         .format(aggregator, R.status_code, R.content))
                raise ServerSideError

        except requests.exceptions.RequestException as ex:
            # Log a warning, then let another aggregator be chosen
            self.logger.writeWarning("{} from aggregator {}".format(ex, aggregator))
            raise ServerSideError

    def _send_request(self, method, aggregator, url, data=None):
        """Low level method to send a HTTP request"""
        headers = None
        if data is not None:
            data = json.dumps(data)
            headers = {"Content-Type": "application/json"}

        self.logger.writeDebug("{} {}".format(method, urljoin(aggregator, url)))

        # We give a long(ish) timeout below, as the async request may succeed after the timeout period
        # has expired, causing the node to be registered twice (potentially at different aggregators).
        # Whilst this isn't a problem in practice, it may cause excessive churn in websocket traffic
        # to web clients - so, sacrifice a little timeliness for things working as designed the
        # majority of the time...
        if _config.get('prefer_ipv6') is False:
            return requests.request(method, urljoin(aggregator, url), data=data, timeout=1.0, headers=headers)
        else:
            return requests.request(method, urljoin(aggregator, url), data=data, timeout=1.0,
                                    headers=headers, proxies={'http': ''})


class MDNSUpdater(object):
    def __init__(self, mdns_engine, mdns_type, mdns_name, mappings, port, logger, p2p_enable=False, p2p_cut_in_count=5,
                 txt_recs=None):
        self.mdns = mdns_engine
        self.mdns_type = mdns_type
        self.mdns_name = mdns_name
        self.mappings = mappings
        self.port = port
        self.service_versions = {}
        self.txt_rec_base = {}
        if txt_recs:
            self.txt_rec_base = txt_recs
        self.logger = logger
        self.p2p_enable = p2p_enable
        self.p2p_enable_count = 0
        self.p2p_cut_in_count = p2p_cut_in_count

        for mapValue in itervalues(self.mappings):
            self.service_versions[mapValue] = 0

        self.mdns.register(self.mdns_name, self.mdns_type, self.port, self.txt_rec_base)

        self._running = True
        self._mdns_update_queue = gevent.queue.Queue()
        self.mdns_thread = gevent.spawn(self._modify_mdns)

    def _modify_mdns(self):
        while self._running:
            if self._mdns_update_queue.empty():
                gevent.sleep(0.2)
            else:
                try:
                    txt_recs = self._mdns_update_queue.get()
                    self.mdns.update(self.mdns_name, self.mdns_type, txt_recs)
                except ServiceNotFoundException:
                    self.logger.writeError("Unable to update mDNS record of type {} and name {}"
                                           .format(self.mdns_name, self.mdns_type))

    def stop(self):
        self._running = False
        self.mdns_thread.join()

    def _p2p_txt_recs(self):
        txt_recs = self.txt_rec_base.copy()
        txt_recs.update(self.service_versions)
        return txt_recs

    def update_mdns(self, type, action):
        if self.p2p_enable:
            if (action == "register") or (action == "update") or (action == "unregister"):
                self.logger.writeDebug("mDNS action: {} {}".format(action, type))
                self._increment_service_version(type)
                self._mdns_update_queue.put(self._p2p_txt_recs())

    def _increment_service_version(self, type):
        self.service_versions[self.mappings[type]] = self.service_versions[self.mappings[type]]+1
        if self.service_versions[self.mappings[type]] > 255:
            self.service_versions[self.mappings[type]] = 0

    # Counts up a number of times, and then enables P2P
    def inc_P2P_enable_count(self):
        if not self.p2p_enable:
            self.p2p_enable_count += 1
            if self.p2p_enable_count >= self.p2p_cut_in_count:
                self.P2P_enable()

    def _reset_P2P_enable_count(self):
        self.p2p_enable_count = 0

    def P2P_enable(self):
        if not self.p2p_enable:
            self.logger.writeInfo("Enabling P2P Discovery")
            self.p2p_enable = True
            self._mdns_update_queue.put(self._p2p_txt_recs())

    def P2P_disable(self):
        if self.p2p_enable:
            self.logger.writeInfo("Disabling P2P Discovery")
            self.p2p_enable = False
            self._reset_P2P_enable_count()
            self._mdns_update_queue.put(self.txt_rec_base)
        else:
            self._reset_P2P_enable_count()


if __name__ == "__main__":  # pragma: no cover
    from uuid import uuid4

    agg = Aggregator()
    ID = str(uuid4())

    agg.register("node", ID, id=ID, label="A Test Service", href="http://127.0.0.1:12345/", services=[], caps={},
                 version="0:0", hostname="apiTest")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        agg.unregister("node", ID)
        agg.stop()
