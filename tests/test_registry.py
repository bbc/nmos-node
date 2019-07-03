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
import six

import unittest
from nmosnode import registry
import time

# to run: python test_registry.py
# or, better: python -m unittest discover


# Because we don't have unittest.mock in 2.7, roll mock classes to cover only what we need.
class MockAggregator:

    def __init__(self):
        self.register_invocations = []
        self.unregister_invocations = []

    def register(self, *args, **kwargs):
        self.register_invocations.append([args, kwargs])

    def register_into(self, *args, **kwargs):
        self.register_invocations.append([args, kwargs])

    def unregister(self, *args, **kwargs):
        self.unregister_invocations.append([args, kwargs])

    def unregister_from(self, *args, **kwargs):
        self.unregister_invocations.append([args, kwargs])


class MockMDNSUpdater:

    def __init__(self):
        self.update_mdns_invocations = []

    def update_mdns(self, type, msg):
        self.update_mdns_invocations.append((type, msg))


class TestRegistryServices(unittest.TestCase):
    """Test registration and maintenance of services in the registry"""

    def setUp(self):
        """Runs before each test"""
        self.res_types = ["flow", "device"]
        self.node_data = {
            "label": "test_node", "href": 'http://127.0.0.1:88/', "host": "127.0.0.1", "services": [], "interfaces": []
        }
        self.registry = registry.FacadeRegistry(self.res_types, MockAggregator(),
                                                MockMDNSUpdater(), "test_node_id", self.node_data)

    def test_register_service(self):
        r = self.registry.register_service("test_srv", srv_type="test", pid=100, href="http://127.0.0.1:12345")
        self.assertEqual(registry.RES_SUCCESS, r)
        self.assertIn("test_srv", self.registry.list_services())
        self.assertEqual("http://127.0.0.1:12345", self.registry.get_service_href("test_srv"))
        service = self.registry.services["test_srv"]  # TODO: Does this expose too much guts?
        self.assertEqual(100, service["pid"])        # TODO: Need for "get_service_pid"?
        self.assertIn("resource", service)
        for rtype in self.res_types:
            self.assertEqual({}, service["resource"][rtype])

    def test_list_services(self):
        self.registry.register_service("test_srv_1", "test", 100, "http://127.0.0.1:12345")
        self.registry.register_service("test_srv_2", "test", 100, "http://127.0.0.1:12345")
        self.assertIn("test_srv_1", self.registry.list_services())
        self.assertIn("test_srv_2", self.registry.list_services())

    def test_get_service_href(self):
        self.registry.register_service("a", "test", 1, "a_href")
        self.assertEqual(registry.RES_NOEXISTS, self.registry.get_service_href("b"))
        self.assertEqual("a_href", self.registry.get_service_href("a"))

    def test_unregister_service(self):
        self.registry.register_service("test_srv_1", "test", 100, "http://127.0.0.1:12345")
        self.registry.register_service("test_srv_2", "test", 100, "http://127.0.0.1:12345")

        # do not allow non-existent services to be removed
        self.assertEqual(registry.RES_NOEXISTS, self.registry.unregister_service("not_there", 1))
        six.assertCountEqual(self, ["test_srv_1", "test_srv_2"], self.registry.list_services())

        # attempt to remove with wrong pid
        self.assertEqual(registry.RES_UNAUTHORISED, self.registry.unregister_service("test_srv_1", 1))
        six.assertCountEqual(self, ["test_srv_1", "test_srv_2"], self.registry.list_services())

        # remove one
        self.assertEqual(registry.RES_SUCCESS, self.registry.unregister_service("test_srv_1", 100))
        self.assertNotIn("test_srv_1", self.registry.services)
        six.assertCountEqual(self, ["test_srv_2"], self.registry.list_services())

    def test_update_service(self):
        self.registry.register_service("a", "test", 1, "http://a")
        self.assertEqual(registry.RES_NOEXISTS, self.registry.update_service("b", 1))
        self.assertEqual(["a"], self.registry.list_services())
        self.assertEqual(registry.RES_UNAUTHORISED, self.registry.update_service("a", 100, "blah"))
        self.assertEqual("http://a", self.registry.get_service_href("a"))
        self.assertEqual(registry.RES_SUCCESS, self.registry.update_service("a", 1, "updated"))
        self.assertAlmostEqual(time.time(), self.registry.services["a"]["heartbeat"], delta=0.04)
        self.assertEqual("updated", self.registry.get_service_href("a"))

    def test_heartbeat_service(self):
        self.registry.register_service("a", "test", 1, "http://a")
        self.assertEqual(registry.RES_NOEXISTS, self.registry.heartbeat_service("b", 1))
        self.assertEqual(registry.RES_UNAUTHORISED, self.registry.heartbeat_service("a", 100))
        self.assertEqual(registry.RES_SUCCESS, self.registry.heartbeat_service("a", 1))
        self.assertAlmostEqual(time.time(), self.registry.services["a"]["heartbeat"], delta=0.02)
        self.assertEqual("http://a", self.registry.get_service_href("a"))

    def test_cleanup_services(self):
        """Services with a heartbeat older than HEARTBEAT_TIMEOUT are removed"""
        self.registry.register_service("a", "test", 1, "a_href")
        self.registry.register_service("b", "test", 2, "b_href")
        self.registry.services["a"]["heartbeat"] = time.time() - registry.HEARTBEAT_TIMEOUT - 1
        self.registry.cleanup_services()
        self.assertEqual(["b"], self.registry.list_services())


class TestRegistry(unittest.TestCase):

    def setUp(self):
        """Runs before each test"""
        self.res_types = ["flow", "device", "sender"]
        self.mock_aggregator = MockAggregator()
        self.mock_mdns_updater = MockMDNSUpdater()
        self.node_data = {"label": "test", "href": "http://abcd", "host": "abcd", "services": [], "interfaces": []}
        self.registry = registry.FacadeRegistry(self.res_types, self.mock_aggregator,
                                                self.mock_mdns_updater, "test_node_id",
                                                self.node_data)

        # pre-populate with some services
        self.registry.register_service("a", srv_type="srv_a", pid=1)
        self.registry.register_service("b", srv_type="srv_b", pid=2)

        # ensure mock aggregator is clean
        self.mock_aggregator.register_invocations = []

    def test_register_device_adds_parent_facade(self):
        """Registering a resource adds a 'node_id' property to the resource"""
        self.registry.register_resource("a", 1, "device", "device_a_key", {"label": "device_a"})
        service_resources = self.registry.list_resource("device")
        self.assertEqual("test_node_id", service_resources["device_a_key"]["node_id"])

    def test_register_calls_aggregator(self):
        """When a resource is registered, the aggregator is informed"""
        self.registry.register_resource("a", 1, "flow", "flow_a_key", {"label": "flow_a"})
        expected_args = [('resource', 'flow', 'flow_a_key'), {'label': 'flow_a'}]
        self.assertEqual(self.mock_aggregator.register_invocations, [expected_args])

    def test_register_updates_mdns(self):
        """When a resource is registered, it is advertised vis mDNS"""
        self.registry.register_resource("a", 1, "flow", "flow_a_key", {"label": "flow_a"})
        expected_args = ('flow', 'register')
        self.assertEqual(self.mock_mdns_updater.update_mdns_invocations, [expected_args])

    def test_sender_manifest_returns_http(self):
        """Check that Sender manifest_href is not modified in HTTP mode"""
        registry.HTTPS_MODE = "disabled"
        self.registry.register_resource("a", 1, "sender", "sender_a_key", {"manifest_href": "http://some-url.com"})
        sender_resources = self.registry.list_resource("sender")
        scheme = sender_resources["sender_a_key"]["manifest_href"].split("://")[0]
        self.assertEqual(scheme, "http")

    def test_sender_manifest_returns_https(self):
        """Check that Sender manifest_href is modified in HTTPS mode"""
        registry.HTTPS_MODE = "enabled"
        self.registry.register_resource("a", 1, "sender", "sender_a_key", {"manifest_href": "http://some-url.com"})
        sender_resources = self.registry.list_resource("sender")
        scheme = sender_resources["sender_a_key"]["manifest_href"].split("://")[0]
        self.assertEqual(scheme, "https")

    def test_device_controls_return_http(self):
        """Check that Device control hrefs are unmodified in HTTP mode"""
        controls = [{"type": "some-type", "href": "http://some-url.com"},
                    {"type": "some-type", "href": "ws://some-url.com"}]
        registry.HTTPS_MODE = "disabled"
        self.registry.register_resource("a", 1, "device", "device_a_key", {"controls": controls,
                                                                           "max_api_version": "v1.2"})
        device_resources = self.registry.list_resource("device", "v1.2")
        self.assertEqual(len(controls), len(device_resources["device_a_key"]["controls"]))
        for control in device_resources["device_a_key"]["controls"]:
            self.assertIn(control["href"].split("://")[0], ["http", "ws"])

    def test_device_controls_return_https(self):
        """Check that Device control hrefs are modified in HTTPS mode"""
        controls = [{"type": "some-type", "href": "http://some-url.com"},
                    {"type": "some-type", "href": "ws://some-url.com"}]
        registry.HTTPS_MODE = "enabled"
        self.registry.register_resource("a", 1, "device", "device_a_key", {"controls": controls,
                                                                           "max_api_version": "v1.2"})
        device_resources = self.registry.list_resource("device", "v1.2")
        self.assertEqual(len(controls), len(device_resources["device_a_key"]["controls"]))
        for control in device_resources["device_a_key"]["controls"]:
            self.assertIn(control["href"].split("://")[0], ["https", "wss"])


if __name__ == '__main__':
    unittest.main()
