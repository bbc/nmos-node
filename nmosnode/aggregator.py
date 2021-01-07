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

from gevent import monkey
monkey.patch_all()

import gevent # noqa E402
import gevent.queue # noqa E402
import requests # noqa E402
import traceback # noqa E402
import json # noqa E402
import time # noqa E402
import webbrowser  # noqa E402

from six import itervalues # noqa E402
from six.moves.urllib.parse import urljoin, urlparse # noqa E402
from collections import deque # noqa E402
from socket import getfqdn  # noqa E402
from authlib.oauth2.rfc6750 import InvalidTokenError # noqa E402
from authlib.oauth2 import OAuth2Error # noqa E402

from nmoscommon.nmoscommonconfig import config as _config # noqa E402
from nmoscommon.logger import Logger # noqa E402
from nmoscommon.mdns.mdnsExceptions import ServiceNotFoundException # noqa E402
from mdnsbridge.mdnsbridgeclient import IppmDNSBridge, NoService, EndOfServiceList # noqa E402

from .api import NODE_APIROOT, PROTOCOL # noqa E402
from .authclient import AuthRegistrar, ALLOWED_SCOPE, ALLOWED_GRANTS, ALLOWED_RESPONSE  # noqa E402

# MDNS Service Names
LEGACY_REG_MDNSTYPE = "nmos-registration"
REGISTRATION_MDNSTYPE = "nmos-register"

# Registry path
AGGREGATOR_APINAMESPACE = "x-nmos"
AGGREGATOR_APINAME = "registration"
AGGREGATOR_APIROOT = AGGREGATOR_APINAMESPACE + '/' + AGGREGATOR_APINAME

# Exponential back off global vars
BACKOFF_INITIAL_TIMOUT_SECONDS = 5
BACKOFF_MAX_TIMEOUT_SECONDS = 40

# OAuth client global vars
FQDN = getfqdn()
OAUTH_MODE = _config.get("oauth_mode", False)


class InvalidRequest(Exception):
    """Client Side Error during request, HTTP 4xx"""
    def __init__(self, status_code=400):
        super(InvalidRequest, self).__init__("Invalid Request, code {}".format(status_code))
        self.status_code = status_code


class ServerSideError(Exception):
    """Exception raised when a HTTP 5xx, timeout or inability to connect returned during request.
    This indicates a server side or connectivity issue"""
    pass


class Aggregator(object):
    """This class serves as a proxy for the distant aggregation service running elsewhere on the network.
    It will search out aggregators and locate them, falling back to other ones if the one it is connected to
    disappears, and resending data as needed."""
    def __init__(self, logger=None, mdns_updater=None, auth_registry=None):
        self.logger = Logger("aggregator_proxy", logger)
        self.mdnsbridge = IppmDNSBridge(logger=self.logger)
        self.aggregator_apiversion = None
        self.service_type = None
        self._set_api_version_and_srv_type(_config.get('nodefacade').get('NODE_REGVERSION'))
        self.aggregator = None
        self.registration_order = ["device", "source", "flow", "sender", "receiver"]
        self._mdns_updater = mdns_updater
        # '_node_data' is a local mirror of aggregated items.
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
        self._aggregator_failure = False  # Variable to flag when aggregator has returned and unexpected error
        self._backoff_active = False
        self._backoff_period = 0

        self.auth_registrar = None  # Class responsible for registering with Auth Server
        self.auth_registry = auth_registry  # Top level class that tracks locally registered OAuth clients
        self.auth_client = None  # Instance of Oauth client responsible for performing token requests

        self._reg_queue = gevent.queue.Queue()
        self.main_thread = gevent.spawn(self._main_thread)
        self.queue_thread = gevent.spawn(self._process_queue)

    def _set_api_version_and_srv_type(self, api_ver):
        """Set the aggregator api version equal to parameter and DNS-SD service type based on api version"""
        self.aggregator_apiversion = api_ver
        self._set_service_type(api_ver)

    def _set_service_type(self, api_ver):
        """Set DNS-SD service type based on current api version in use"""
        if api_ver in ['v1.0', 'v1.1', 'v1.2']:
            self.service_type = LEGACY_REG_MDNSTYPE
        else:
            self.service_type = REGISTRATION_MDNSTYPE

    def _main_thread(self):
        """The main thread runs in the background.
        If, when it runs, the Node is believed to be registered it will perform a heartbeat every 5 seconds.
        If the Node is not registered it will try to register the Node"""
        self.logger.writeDebug("Starting main thread")

        while self._running:
            if self._node_data["node"] and self.aggregator is None:
                self._discovery_operation()
            elif self._node_data["node"] and self._node_data["registered"]:
                self._registered_operation()
            else:
                self._node_data["registered"] = False
                self.aggregator = None
                gevent.sleep(0.2)

        self.logger.writeDebug("Stopping heartbeat thread")

    def _discovery_operation(self):
        """In Discovery operation the Node will wait a backoff period if defined to allow aggregators to recover when in
        a state of error. Selecting the most appropriate aggregator and try to register with it.
        If a registration fails then another aggregator will be tried."""
        self.logger.writeDebug("Entering Discovery Mode")

        # Wait backoff period
        # Do not wait backoff period if aggregator failed, a new aggregator should be tried immediately
        if not self._aggregator_failure:
            self._back_off_timer()

        self._aggregator_failure = False

        # Update cached list of aggregators
        if self._aggregator_list_stale:
            self._flush_cached_aggregators()

        while True:
            self.aggregator = self._get_aggregator()
            if self.aggregator is None:
                self.logger.writeDebug("Failed to find registration API")
                break
            self.logger.writeDebug("Aggregator set to: {}".format(self.aggregator))

            # Perform initial heartbeat, which will attempt to register Node if not already registered
            if self._heartbeat():
                # Successfully registered Node with aggregator andproceed to registered operation
                # Else will try next aggregator
                break

    def _registered_operation(self):
        """In Registered operation, the Node is registered so a heartbeat will be performed,
        if the heartbeat is successful the Node will wait 5 seconds before attempting another heartbeat.
        Else another aggregator will be selected"""
        if not self._heartbeat():
            # Heartbeat failed
            # Flag to update cached list of aggregators and immediately try new aggregator
            self.aggregator = None
            self._aggregator_failure = True

    def _heartbeat(self):
        """Performs a heartbeat to registered aggregator
        If heartbeat fails it will take actions to correct the error, by re-registering the Node
        If successfull will return True, else will return False"""
        if not self.aggregator:
            return False
        try:
            R = self._send("POST", self.aggregator, self.aggregator_apiversion,
                           "health/nodes/{}".format(self._node_data["node"]["data"]["id"]))

            if R.status_code == 200 and self._node_data["registered"]:
                # Continue to registered operation
                self.logger.writeDebug("Successful heartbeat for Node {}"
                                       .format(self._node_data["node"]["data"]["id"]))
                self._registered()
                heartbeat_wait = 5
                while heartbeat_wait > 0 and self._running:
                    gevent.sleep(1)
                    heartbeat_wait -= 1
                return True

            elif R.status_code in [200, 409]:
                # Delete node from registry
                if self._unregister_node(R.headers.get('Location')):
                    return self._register_node(self._node_data["node"])
                else:
                    # Try next registry
                    return False

        except InvalidRequest as e:
            if e.status_code == 404:
                # Re-register
                self.logger.writeWarning("404 error on heartbeat. Marking Node for re-registration")
                self._node_data["registered"] = False
                return self._register_node(self._node_data["node"])
            else:
                # Other error, try next registry
                return False
        except ServerSideError:
            self.logger.writeWarning("Server Side Error on heartbeat. Trying another registry")
            return False
        except Exception as e:
            # Re-register
            self.logger.writeWarning("Unexpected error on heartbeat: {}. Marking Node for re-registration".format(e))
            self._node_data["registered"] = False
            return False

    def _register_auth(self, client_name, client_uri):
        """Register OAuth client with Authorization Server"""
        self.logger.writeInfo("Attempting to register dynamically with Auth Server")
        auth_registrar = AuthRegistrar(
            client_name=client_name,
            client_uri=client_uri,
            redirect_uris=[PROTOCOL + '://' + FQDN + NODE_APIROOT + 'authorize'],
            allowed_scope=ALLOWED_SCOPE,
            allowed_grants=ALLOWED_GRANTS,
            allowed_response=ALLOWED_RESPONSE
        )
        if auth_registrar.registered is True:
            return auth_registrar
        else:
            self.logger.writeWarning("Unable to successfully register with Authorization Server")

    def _register_node(self, node_obj):
        """Attempt to register Node with aggregator
        Returns True is node was successfully registered with aggregator
        Returns False if registration failed
        If registration failed with 200 or 409, will attempt to delete and re-register"""
        if node_obj is None:
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
                R = self._send("POST", self.aggregator, self.aggregator_apiversion,
                               "resource", node_obj)

                if R.status_code == 201:
                    # Continue to registered operation
                    self.logger.writeInfo("Node Registered with {} at version {}"
                                          .format(self.aggregator, self.aggregator_apiversion))
                    self._registered()

                    # Trigger registration of Nodes resources
                    self._register_node_resources()

                    return True

                elif R.status_code in [200, 409]:
                    # Delete node from aggregator & re-register
                    if self._unregister_node(R.headers.get('Location')):
                        continue
                    else:
                        # Try next aggregator
                        return False
        except Exception as e:
            self.logger.writeError("Failed to register node {}".format(e))
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
        """Sleep for defined backoff period"""
        self.logger.writeDebug("Backoff timer enabled for {} seconds".format(self._backoff_period))
        self._backoff_active = True
        gevent.sleep(self._backoff_period)
        self._backoff_active = False

    def _flush_cached_aggregators(self):
        """Flush the list of cached aggregators in the mdns bridge client,
        preventing the use of out of date aggregators"""
        self.logger.writeDebug("Flushing cached list of aggregators")
        self._aggregator_list_stale = False
        self.mdnsbridge.updateServices(self.service_type)

    def _get_aggregator(self):
        """Get the most appropriate aggregator from the mdns bridge client.
        If no aggregator found increment P2P counter, update cache and increase backoff
        If reached the end of available aggregators update cache and increase backoff"""

        try:
            return self.mdnsbridge.getHrefWithException(
                self.service_type, None, self.aggregator_apiversion, PROTOCOL, OAUTH_MODE)
        except NoService:
            self.logger.writeDebug("No Registration services found: {} {} {}".format(
                self.service_type, self.aggregator_apiversion, PROTOCOL))
            if self._mdns_updater is not None:
                self._mdns_updater.inc_P2P_enable_count()
            self._increase_backoff_period()
            return None
        except EndOfServiceList:
            self.logger.writeDebug("End of Registration services list: {} {} {}".format(
                self.service_type, self.aggregator_apiversion, PROTOCOL))
            self._increase_backoff_period()
            return None

    def _unregister_node(self, url_path=None):
        """Delete node from registry, using url_path if specified"""
        if self.aggregator is None:
            self.logger.writeWarning('Could not un-register as no aggregator set')
            return False
        try:
            self._node_data['registered'] = False
            if url_path is None:
                R = self._send('DELETE', self.aggregator, self.aggregator_apiversion,
                               'resource/nodes/{}'.format(self._node_data['node']["data"]["id"]))
            else:
                parsed_url = urlparse(url_path)
                R = self._send_request('DELETE', self.aggregator, parsed_url.path)

            if R.status_code == 204:
                # Successfully deleted node from Registry
                self.logger.writeInfo("Node unregistered from {} at version {}"
                                      .format(self.aggregator, self.aggregator_apiversion))
                return True
            else:
                return False
        except Exception as e:
            self.logger.writeDebug('Exception raised while un-registering {}'.format(e))
            return False

    def _process_queue(self):
        """Provided the Node is believed to be correctly registered, hand off a single request to the SEND method
           On client error, clear the resource from the local mirror
           On other error, mark Node as unregistered and trigger re-registration"""
        self.logger.writeDebug("Starting HTTP queue processing thread")
        # Checks queue not empty before quitting to make sure unregister node gets done
        while self._running:
            if (not self._node_data["registered"] or self._reg_queue.empty()
                    or self._backoff_active or not self.aggregator):
                gevent.sleep(1)
            else:
                try:
                    queue_item = self._reg_queue.get()
                    namespace = queue_item["namespace"]
                    res_type = queue_item["res_type"]
                    res_key = queue_item["key"]
                    if queue_item["method"] == "POST":
                        if res_type == "node":
                            send_obj = self._node_data.get("node")
                        else:
                            send_obj = self._node_data["entities"][namespace][res_type].get(res_key)

                        if send_obj is None:
                            self.logger.writeError("No data to send for resource {}".format(res_type))
                            continue
                        try:
                            self._send("POST", self.aggregator, self.aggregator_apiversion,
                                       "{}".format(namespace), send_obj)
                            self.logger.writeInfo("Registered {} {} {}".format(namespace, res_type, res_key))
                        except InvalidRequest as e:
                            self.logger.writeWarning("Error registering {} {}: {}".format(res_type, res_key, e))
                            self.logger.writeWarning("Request data: {}".format(send_obj))
                            del self._node_data["entities"][namespace][res_type][res_key]

                    elif queue_item["method"] == "DELETE":
                        translated_type = res_type + 's'
                        if namespace == "resource" and res_type == "node":
                            # Handle special Node type
                            self._node_data["node"] = None
                            self._node_data["registered"] = False
                        try:
                            self._send("DELETE", self.aggregator, self.aggregator_apiversion,
                                       "{}/{}/{}".format(namespace, translated_type, res_key))
                            self.logger.writeInfo("Un-registered {} {} {}".format(namespace, translated_type, res_key))
                        except InvalidRequest as e:
                            self.logger.writeWarning("Error deleting resource {} {}: {}"
                                                     .format(translated_type, res_key, e))
                    else:
                        self.logger.writeWarning("Method {} not supported for Registration API interactions"
                                                 .format(queue_item["method"]))
                except ServerSideError:
                    self.aggregator = None
                    self._aggregator_failure = True
                    self._add_request_to_front_of_queue(queue_item)
                except Exception as e:
                    self.logger.writeError("Unexpected Error while processing queue, marking Node for re-registration\n"
                                           "{}".format(e))
                    self._node_data["registered"] = False
                    self.aggregator = None
                    if(self._mdns_updater is not None):
                        self._mdns_updater.P2P_disable()
        self.logger.writeDebug("Stopping HTTP queue processing thread")

    def _queue_request(self, method, namespace, res_type, key):
        """Queue a request to be processed.
           Handles all requests except initial Node POST which is done in _process_reregister"""
        self._reg_queue.put({"method": method, "namespace": namespace, "res_type": res_type, "key": key})

    def _add_request_to_front_of_queue(self, request):
        """Adds item to the front of the queue"""

        new_queue = deque()
        new_queue.append(request)

        # Drain the queue
        while not self._reg_queue.empty():
            try:
                new_queue.append(self._reg_queue.get())
            except gevent.queue.Queue.Empty:
                break

        # Add items back to the queue
        while True:
            try:
                self._reg_queue.put(new_queue.popleft())
            except IndexError:
                break

    def register_auth_client(self, client_name, client_uri):
        """Function for Registering OAuth client with Auth Server and instantiating OAuth Client class"""

        if OAUTH_MODE is True:
            if self.auth_registrar is None:
                self.auth_registrar = self._register_auth(
                    client_name=client_name,
                    client_uri=client_uri
                )
            if self.auth_registrar and self.auth_client is None:
                try:
                    # Register Node Client
                    self.auth_registry.register_client(
                        client_name=client_name, client_uri=client_uri, **self.auth_registrar.server_metadata)
                    self.logger.writeInfo("Successfully registered Auth Client")
                except (OSError, IOError):
                    self.logger.writeError(
                        "Exception accessing OAuth credentials. This may be a file permissions issue.")
                    return
                # Extract the 'RemoteApp' class created when registering
                self.auth_client = getattr(self.auth_registry, client_name)
                # Fetch Token
                self.get_auth_token()

    def get_auth_token(self):
        """Fetch Access Token either using redirection grant flow or using auth_client"""
        if self.auth_client is not None and self.auth_registrar is not None:
            try:
                registered_grants = self.auth_registrar.client_metadata.get("grant_types", {})
                if "authorization_code" in registered_grants:
                    self.logger.writeInfo(
                        "Endpoint '/oauth' on Node API will provide redirect to authorization endpoint on Auth Server."
                    )
                if "client_credentials" in registered_grants:
                    # Fetch Token using Client Credentials Grant
                    token = self.auth_client.fetch_access_token()
                    self.auth_registry.update_local_token(token)
                else:
                    raise OAuth2Error("Client not registered with supported Grant Type. Must be one of: {}".format(
                        registered_grants))
            except OAuth2Error as e:
                self.logger.writeError("Failure fetching access token: {}".format(e))

    def register(self, res_type, key, **kwargs):
        """Register 'resource' type data including the Node
           NB: Node registration is managed by heartbeat thread so may take up to 5 seconds! """
        self.register_into("resource", res_type, key, **kwargs)

    def unregister(self, res_type, key):
        """Unregister 'resource' type data including the Node"""
        self.unregister_from("resource", res_type, key)

    def register_into(self, namespace, res_type, key, **kwargs):
        """General register method for 'resource' types"""
        data = kwargs
        send_obj = {"type": res_type, "data": data}
        if 'id' not in send_obj["data"]:
            self.logger.writeWarning("No 'id' present in data, using key='{}': {}".format(key, data))
            send_obj["data"]["id"] = key

        if namespace == "resource" and res_type == "node":
            # Ensure Registered with Auth Server (is there a better place for this)
            if OAUTH_MODE is True:
                self.register_auth_client(
                    client_name="nmos-node-{}".format(data["id"]),
                    client_uri="{}://{}".format(PROTOCOL, FQDN)
                )
            # Handle special Node type when Node is not registered, by immediately registering
            if self._node_data["node"] is None:
                # Will trigger registration in main thread
                self._node_data["node"] = send_obj
                return
            # Update Node Data
            self._node_data["node"] = send_obj
        else:
            self._add_mirror_keys(namespace, res_type)
            self._node_data["entities"][namespace][res_type][key] = send_obj
        self._queue_request("POST", namespace, res_type, key)

    def unregister_from(self, namespace, res_type, key):
        """General unregister method for 'resource' types"""
        if namespace == "resource" and res_type == "node":
            # Handle special Node type
            self._unregister_node()
            self._node_data["node"] = None
            return
        elif res_type in self._node_data["entities"][namespace]:
            self._add_mirror_keys(namespace, res_type)
            if key in self._node_data["entities"][namespace][res_type]:
                del self._node_data["entities"][namespace][res_type][key]
        self._queue_request("DELETE", namespace, res_type, key)

    def _add_mirror_keys(self, namespace, res_type):
        """Deal with missing keys in local mirror"""
        if namespace not in self._node_data["entities"]:
            self._node_data["entities"][namespace] = {}
        if res_type not in self._node_data["entities"][namespace]:
            self._node_data["entities"][namespace][res_type] = {}

    def stop(self):
        """Stop the Aggregator object running"""
        self.logger.writeDebug("Stopping aggregator proxy")
        self._running = False
        self.main_thread.join()
        self.queue_thread.join()

    def status(self):
        """Return the current status of node in the aggregator"""
        return {"api_href": self.aggregator,
                "api_version": self.aggregator_apiversion,
                "registered": self._node_data["registered"]}

    def _send(self, method, aggregator, api_ver, url, data=None):
        """Handle sending request to the registration API, with error handling
        HTTP 200, 201, 204, 409 - Success, return response
        Timeout, HTTP 5xx, Connection Error - Raise ServerSideError Exception
        HTTP 4xx - Raise InvalidRequest Exception"""

        url = "{}/{}/{}".format(AGGREGATOR_APIROOT, api_ver, url)

        try:
            resp = self._send_request(method, aggregator, url, data)
            if resp is None:
                self.logger.writeWarning("No response from aggregator {}".format(aggregator))
                raise ServerSideError

            elif resp.status_code in [200, 201, 204, 409]:
                return resp

            elif (resp.status_code // 100) == 4:
                self.logger.writeWarning("{} response from aggregator: {} {}"
                                         .format(resp.status_code, method, urljoin(aggregator, url)))
                self.logger.writeDebug("\nResponse: {}".format(resp.content))
                raise InvalidRequest(resp.status_code)

            else:
                self.logger.writeWarning("Unexpected status from aggregator {}: {}, {}"
                                         .format(aggregator, resp.status_code, resp.content))
                raise ServerSideError

        except requests.exceptions.RequestException as e:
            # Log a warning, then let another aggregator be chosen
            self.logger.writeWarning("{} from aggregator {}".format(e, aggregator))
            raise ServerSideError

    def _send_request(self, method, aggregator, url_path, data=None):
        """Low level method to send a HTTP request"""

        url = urljoin(aggregator, url_path)
        self.logger.writeDebug("{} {}".format(method, url))

        # We give a long(ish) timeout below, as the async request may succeed after the timeout period
        # has expired, causing the node to be registered twice (potentially at different aggregators).
        # Whilst this isn't a problem in practice, it may cause excessive churn in websocket traffic
        # to web clients - so, sacrifice a little timeliness for things working as designed the
        # majority of the time...
        kwargs = {
            "method": method, "url": url, "json": data, "timeout": 1.0
        }
        if _config.get('prefer_ipv6') is True:
            kwargs["proxies"] = {'http': ''}

        # If not in OAuth mode, perform standard request
        if OAUTH_MODE is False or self.auth_client is None:
            return requests.request(**kwargs)
        else:
            # If in OAuth Mode, use OAuth client to automatically fetch token / refresh token if expired
            with self.auth_registry.app.app_context():
                try:
                    return self.auth_client.request(**kwargs)
                # General OAuth Error (e.g. incorrect request details, invalid client, etc.)
                except OAuth2Error as e:
                    self.logger.writeError(
                        "Failed to fetch token before making API call to {}. {}".format(url, e))
                    self.auth_registrar = self.auth_client = None


class MDNSUpdater(object):
    def __init__(self, mdns_engine, mdns_type, mdns_name, mappings, port, logger, p2p_enable=False, p2p_cut_in_count=2,
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
        self.service_versions[self.mappings[type]] = self.service_versions[self.mappings[type]] + 1
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
