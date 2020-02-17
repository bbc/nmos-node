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

from __future__ import print_function

from six import iteritems
from six.moves.urllib.parse import urljoin
from six import PY2

import unittest
import mock
import requests
import gevent
from copy import deepcopy
from nmosnode.aggregator import Aggregator, InvalidRequest, REGISTRATION_MDNSTYPE
from nmosnode.aggregator import AGGREGATOR_APINAMESPACE, LEGACY_REG_MDNSTYPE, AGGREGATOR_APINAME
from nmosnode.aggregator import ServerSideError
from nmosnode.aggregator import BACKOFF_INITIAL_TIMOUT_SECONDS, BACKOFF_MAX_TIMEOUT_SECONDS
from mdnsbridge.mdnsbridgeclient import NoService, EndOfServiceList
import nmosnode

MAX_ITERATIONS = 10


class TestAggregator(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestAggregator, self).__init__(*args, **kwargs)
        if PY2:
            self.assertCountEqual = self.assertItemsEqual

    def setUp(self):
        paths = ['nmosnode.aggregator.Logger',
                 'nmosnode.aggregator.IppmDNSBridge',
                 'gevent.queue.Queue',
                 'gevent.spawn']
        patchers = {name: mock.patch(name) for name in paths}
        self.mocks = {name: patcher.start() for (name, patcher) in iteritems(patchers)}

        self.addCleanup(mock.patch.stopall)

        def printmsg(t):
            def _inner(msg):
                print(t + ": " + msg)
            return _inner

        # self.mocks['nmosnode.aggregator.Logger'].return_value.writeInfo.side_effect = printmsg("INFO")
        # self.mocks['nmosnode.aggregator.Logger'].return_value.writeWarning.side_effect = printmsg("WARNING")
        # self.mocks['nmosnode.aggregator.Logger'].return_value.writeDebug.side_effect = printmsg("DEBUG")
        # self.mocks['nmosnode.aggregator.Logger'].return_value.writeError.side_effect = printmsg("ERROR")
        # self.mocks['nmosnode.aggregator.Logger'].return_value.writeFatal.side_effect = printmsg("FATAL")

    def construct_url(self, aggregator, api_ver, path):
        """Constructs URL from aggregator and path, along with API version and namespace"""
        return "{}/{}/{}/{}/{}".format(aggregator, AGGREGATOR_APINAMESPACE, AGGREGATOR_APINAME, api_ver, path)

    def test_init(self):
        """Test a call to Aggregator()"""
        self.mocks['gevent.spawn'].side_effect = lambda f: mock.MagicMock(thread_function=f)

        a = Aggregator()

        self.mocks['nmosnode.aggregator.Logger'].assert_called_once_with('aggregator_proxy', None)
        self.assertEqual(a.logger, self.mocks['nmosnode.aggregator.Logger'].return_value)
        self.mocks['nmosnode.aggregator.IppmDNSBridge'].assert_called_once_with(logger=a.logger)
        self.assertEqual(a._reg_queue, self.mocks['gevent.queue.Queue'].return_value)
        self.assertEqual(a.main_thread.thread_function, a._main_thread)
        self.assertEqual(a.queue_thread.thread_function, a._process_queue)

    def test_set_api_version(self):
        """Test that setting the api version also sets the correct DNS-SD service type"""
        a = Aggregator()
        versions_service_type = [
            {'api_ver': 'v1.0', 'srv_type': LEGACY_REG_MDNSTYPE},
            {'api_ver': 'v1.1', 'srv_type': LEGACY_REG_MDNSTYPE},
            {'api_ver': 'v1.2', 'srv_type': LEGACY_REG_MDNSTYPE},
            {'api_ver': 'v1.3', 'srv_type': REGISTRATION_MDNSTYPE},
        ]

        for x in versions_service_type:
            a._set_api_version_and_srv_type(x['api_ver'])

            self.assertEqual(a.aggregator_apiversion, x['api_ver'])
            self.assertEqual(a.service_type, x['srv_type'])

    def test_register_into(self):
        """register_into() should register an object into a namespace, adding a scheduled call to the registration
        queue to that effect."""
        a = Aggregator()

        namespace = "potato"
        objects = [("dummy", "testkey", {"test_param": "test_value", "test_param1": "test_value1"})]

        for o in objects:
            a.register_into(namespace, o[0], o[1], **o[2])
            a._reg_queue.put.assert_called_with({
                "method": "POST",
                "namespace": namespace,
                "res_type": o[0],
                "key": o[1]})
            send_obj = {"type": o[0], "data": {k: v for (k, v) in iteritems(o[2])}}
            if 'id' not in send_obj['data']:
                send_obj['data']['id'] = o[1]
            self.assertEqual(a._node_data["entities"][namespace][o[0]][o[1]], send_obj)

    def test_register(self):
        """register() should register an object into namespace "resource", adding a scheduled call to the registration
        queue to that effect. There is special behaviour when registering a node, since the object can only ever have
        one node registration at a time."""

        a = Aggregator()

        namespace = "resource"
        objects = [
            ("dummy", "testkey", {"test_param": "test_value", "test_param1": "test_value1"}),
            ("node", "testnode", {"test_param": "test_value", "test_param1": "test_value1"})
        ]
        with mock.patch("nmosnode.aggregator.OAUTH_MODE", False):
            for o in objects:
                a.register(o[0], o[1], **o[2])

                send_obj = {"type": o[0], "data": {k: v for (k, v) in iteritems(o[2])}}
                if 'id' not in send_obj['data']:
                    send_obj['data']['id'] = o[1]
                if o[0] == "node":
                    self.assertEqual(a._node_data["node"], send_obj)
                else:
                    a._reg_queue.put.assert_called_with({
                        "method": "POST",
                        "namespace": namespace,
                        "res_type": o[0],
                        "key": o[1]})

                    self.assertEqual(a._node_data["entities"][namespace][o[0]][o[1]], send_obj)

    def test_unregister(self):
        """unregister() should schedule a call to unregister the specified devices.
        Special behaviour is expected when unregistering a node, where the request should be made immediately."""
        a = Aggregator()

        namespace = "resource"
        objects = [
            ("dummy", "testkey", {"test_param": "test_value", "test_param1": "test_value1"}),
            ("node", "testnode", {"test_param": "test_value", "test_param1": "test_value1"})
        ]
        with mock.patch("nmosnode.aggregator.OAUTH_MODE", False):
            for o in objects:
                with mock.patch.object(a, '_unregister_node') as un_register:
                    a.register(o[0], o[1], **o[2])

                    a.unregister(o[0], o[1])

                    if o[0] == "node":
                        self.assertIsNone(a._node_data["node"])
                    else:
                        un_register.assert_not_called()
                        a._reg_queue.put.assert_called_with({
                            "method": "DELETE",
                            "namespace": namespace,
                            "res_type": o[0],
                            "key": o[1]})
                        self.assertNotIn(o[1], a._node_data["entities"][namespace][o[0]])

    def test_stop(self):
        """A call to stop should set _running to false and then join the heartbeat thread."""
        self.mocks['gevent.spawn'].side_effect = lambda f: mock.MagicMock(thread_function=f)
        a = Aggregator()

        self.assertTrue(a._running)

        a.stop()

        self.assertFalse(a._running)
        a.main_thread.join.assert_called_with()
        a.queue_thread.join.assert_called_with()

    def test_get_aggregator_returns_services_in_order(self):
        """Test that each aggregator is returned in order"""
        a = Aggregator(mdns_updater=mock.MagicMock())
        test_aggregators = ['http://example0.com/aggregator/',
                            'http://example1.com/aggregator/',
                            'http://example2.com/aggregator/',
                            'http://example3.com/aggregator/']

        a.mdnsbridge.getHrefWithException.side_effect = test_aggregators

        for agg in test_aggregators:
            self.assertEqual(a._get_aggregator(), agg)

    def test_get_aggregator_returns_none_when_end_of_list(self):
        """Test that None is returned and backoff period is increased when EndofServiceList Exception raised"""
        a = Aggregator(mdns_updater=mock.MagicMock())

        a.mdnsbridge.getHrefWithException.side_effect = EndOfServiceList()

        self.assertEqual(a._get_aggregator(), None)
        self.assertEqual(a._backoff_period, BACKOFF_INITIAL_TIMOUT_SECONDS)

    def test_get_aggregator_uses_correct_srv_type(self):
        """Test that _get_aggregator uses the correct service type for API version and protocol type"""
        a = Aggregator(mdns_updater=mock.MagicMock())

        aggregator = "http://example0.com/aggregator/"
        versions = ['v1.0', 'v1.1', 'v1.2', 'v1.3']
        expected_calls = [
            mock.call(LEGACY_REG_MDNSTYPE, None, versions[0], 'http', False),
            mock.call(LEGACY_REG_MDNSTYPE, None, versions[1], 'http', False),
            mock.call(LEGACY_REG_MDNSTYPE, None, versions[2], 'http', False),
            mock.call(REGISTRATION_MDNSTYPE, None, versions[3], 'http', False),
            mock.call(LEGACY_REG_MDNSTYPE, None, versions[0], 'https', False),
            mock.call(LEGACY_REG_MDNSTYPE, None, versions[1], 'https', False),
            mock.call(LEGACY_REG_MDNSTYPE, None, versions[2], 'https', False),
            mock.call(REGISTRATION_MDNSTYPE, None, versions[3], 'https', False),
        ]
        a.mdnsbridge.getHrefWithException.return_value = aggregator
        with mock.patch("nmosnode.aggregator.OAUTH_MODE", False):

            for v in versions:
                a._set_api_version_and_srv_type(v)
                self.assertEqual(a._get_aggregator(), aggregator)

            with mock.patch("nmosnode.aggregator.PROTOCOL", "https"):
                for v in versions:
                    a._set_api_version_and_srv_type(v)
                    self.assertEqual(a._get_aggregator(), aggregator)

                a.mdnsbridge.getHrefWithException.assert_has_calls(expected_calls)

    def test_get_aggregator_returns_none_when_no_aggregators(self):
        """Test that None is returned, increment P2P counter and backoff period is increased
        when NoService Exception raised"""
        a = Aggregator(mdns_updater=mock.MagicMock())

        a.mdnsbridge.getHrefWithException.side_effect = NoService()

        self.assertEqual(a._get_aggregator(), None)
        self.assertEqual(a._backoff_period, BACKOFF_INITIAL_TIMOUT_SECONDS)
        a._mdns_updater.inc_P2P_enable_count.assert_called_with()

    def test_reset_backoff(self):
        """Test backoff period is reset to zero"""
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._backoff_period = 100

        a._reset_backoff_period()
        self.assertEqual(a._backoff_period, 0)

    def test_backoff_thread_timout(self):
        """Check that the backoff thread, sets and un-sets the backoff flag after backoff period"""
        a = Aggregator(mdns_updater=mock.MagicMock())

        with mock.patch('gevent.sleep') as timer:
            a._backoff_period = BACKOFF_INITIAL_TIMOUT_SECONDS
            a._back_off_timer()
            timer.assert_called_with(BACKOFF_INITIAL_TIMOUT_SECONDS)
            self.assertFalse(a._backoff_active)

    def test_increase_backoff_period(self):
        """Check that the back off period is increased exponentially from its minimum to its maximum"""
        a = Aggregator(mdns_updater=mock.MagicMock())

        expected_values = []
        for x in range(0, 3):
            expected_values.append((BACKOFF_INITIAL_TIMOUT_SECONDS) * 2**x)
        expected_values.append(BACKOFF_MAX_TIMEOUT_SECONDS)
        expected_values.append(BACKOFF_MAX_TIMEOUT_SECONDS)

        for x in expected_values:
            a._aggregator_list_stale = False
            a._increase_backoff_period()
            self.assertEqual(a._backoff_period, x)
            self.assertEqual(a._aggregator_list_stale, True)

    # # ================================================================================================================
    # # Test heartbeat operation
    # # ================================================================================================================

    def test_heartbeat_200_when_registered(self):
        """Test heartbeat operation when heartbeat request returns HTTP 200 when registered
        After HTTP 200 heartbeat, Node should still be registered and should wait 5 seconds"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        def killloop(*args, **kwargs):
            a._running = False

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=200)

        with mock.patch('gevent.sleep', side_effect=killloop) as sleep:
            with mock.patch.object(a, '_send', side_effect=request) as send:
                return_val = a._heartbeat()

                self.assertTrue(return_val)
                send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                        "health/nodes/{}".format(DUMMYNODEID))
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()
                self.assertTrue(a._node_data["registered"])
                self.assertTrue(a._aggregator_list_stale)
                self.assertEqual(a._backoff_period, 0)
                sleep.assert_called_with(1)

    def test_heartbeat_200_when_not_registered(self):
        """Test heartbeat operation when heartbeat request returns HTTP 200 when not registered
        After HTTP 200 heartbeat, Node should unregister Node then perform re-registration with same aggregator"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=200, headers={'Location': 'path/xxx'})

        with mock.patch.object(a, '_send', side_effect=request) as send:
            with mock.patch.object(a, '_unregister_node', return_value=True) as un_reg:
                with mock.patch.object(a, '_register_node', return_value=True) as register:
                    return_val = a._heartbeat()

                    self.assertTrue(return_val)
                    send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                            "health/nodes/{}".format(DUMMYNODEID))
                    a._mdns_updater.inc_P2P_enable_count.assert_not_called()
                    un_reg.assert_called_once_with('path/xxx')
                    register.assert_called_once_with(a._node_data["node"])

    def test_heartbeat_200_when_not_registered_failure(self):
        """Test heartbeat operation when heartbeat request returns HTTP 200 when not registered
        and aggregator in failed state
        After HTTP 200 heartbeat, Node should attempt to unregister node then return"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=200, headers={'Location': 'path/xxx'})

        with mock.patch.object(a, '_send', side_effect=request) as send:
            with mock.patch.object(a, '_unregister_node', return_value=False) as un_reg:
                return_val = a._heartbeat()

                self.assertFalse(return_val)
                send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                        "health/nodes/{}".format(DUMMYNODEID))
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()
                un_reg.assert_called_once_with('path/xxx')

    def test_heartbeat_409(self):
        """Test heartbeat operation when heartbeat request returns HTTP 409
        After HTTP 409 heartbeat, Node should unregister Node then perform re-registration with same aggregator"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=409, headers={'Location': 'path/xxx'})

        with mock.patch.object(a, '_send', side_effect=request) as send:
            with mock.patch.object(a, '_unregister_node', return_value=True) as un_reg:
                with mock.patch.object(a, '_register_node', return_value=True) as register:
                    return_val = a._heartbeat()

                    self.assertTrue(return_val)
                    send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                            "health/nodes/{}".format(DUMMYNODEID))
                    a._mdns_updater.inc_P2P_enable_count.assert_not_called()
                    un_reg.assert_called_once_with('path/xxx')
                    register.assert_called_once_with(a._node_data["node"])

    def test_heartbeat_409_failure(self):
        """Test heartbeat operation when heartbeat request returns HTTP 409 and aggregator in failed state
        After HTTP 409 heartbeat, Node should attempt to unregister Node and return"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=409, headers={'Location': 'path/xxx'})

        with mock.patch.object(a, '_send', side_effect=request) as send:
            with mock.patch.object(a, '_unregister_node', return_value=False) as un_reg:
                return_val = a._heartbeat()

                self.assertFalse(return_val)
                send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                        "health/nodes/{}".format(DUMMYNODEID))
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()
                un_reg.assert_called_once_with('path/xxx')

    def test_heartbeat_404(self):
        """Test heartbeat operation when heartbeat request returns HTTP 404
        After HTTP 404, Node should mark itself as not registered and attempt re-registration"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        with mock.patch.object(a, '_send', side_effect=InvalidRequest(status_code=404)) as send:
            with mock.patch.object(a, '_register_node', return_value=True) as register:
                return_val = a._heartbeat()

                self.assertTrue(return_val)
                send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                        "health/nodes/{}".format(DUMMYNODEID))
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()
                register.assert_called_once_with(a._node_data["node"])
                self.assertFalse(a._node_data["registered"])

    def test_heartbeat_404_failure(self):
        """Test heartbeat operation when heartbeat request returns HTTP 404 and aggregator in failed state
        after HTTP 404, Node should mark itself as not registered and attempt re-registration"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        with mock.patch.object(a, '_send', side_effect=InvalidRequest(status_code=404)) as send:
            with mock.patch.object(a, '_register_node', return_value=False) as register:
                return_val = a._heartbeat()

                self.assertFalse(return_val)
                send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                        "health/nodes/{}".format(DUMMYNODEID))
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()
                register.assert_called_once_with(a._node_data["node"])
                self.assertFalse(a._node_data["registered"])

    def test_heartbeat_4xx_failure(self):
        """Test heartbeat operation when heartbeat request returns HTTP 4xx and aggregator in failed state
        Indicates aggregator has encountered an error, heartbeat should fail and next aggregator should be used"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        with mock.patch.object(a, '_send', side_effect=InvalidRequest(status_code=401)) as send:
            return_val = a._heartbeat()

            self.assertFalse(return_val)
            send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                    "health/nodes/{}".format(DUMMYNODEID))
            a._mdns_updater.inc_P2P_enable_count.assert_not_called()
            self.assertTrue(a._node_data["registered"])

    def test_heartbeat_5xx_failure(self):
        """Test heartbeat operation when heartbeat request returns HTTP 5xx and aggregator in failed state
        Indicates aggregator has encountered an error, heartbeat should fail and next aggregator should be used"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        with mock.patch.object(a, '_send', side_effect=ServerSideError) as send:
            return_val = a._heartbeat()

            self.assertFalse(return_val)
            send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                    "health/nodes/{}".format(DUMMYNODEID))
            a._mdns_updater.inc_P2P_enable_count.assert_not_called()
            self.assertTrue(a._node_data["registered"])

    def test_heartbeat_exception_failure(self):
        """Test heartbeat operation when heartbeat request returns an Exception
        Indicates aggregator has encountered an error, heartbeat should fail and next aggregator should be used"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = "http://example.com"

        with mock.patch.object(a, '_send', side_effect=Exception) as send:
            return_val = a._heartbeat()

            self.assertFalse(return_val)
            send.assert_called_with("POST", "http://example.com", a.aggregator_apiversion,
                                    "health/nodes/{}".format(DUMMYNODEID))
            a._mdns_updater.inc_P2P_enable_count.assert_not_called()
            self.assertFalse(a._node_data["registered"])

    def test_heartbeat_if_no_aggregator_set(self):
        """Test heartbeat operation when no aggregator set
        No heartbeat should be performed and should return False"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a.aggregator = None

        with mock.patch.object(a, '_send', side_effect=Exception) as send:
            return_val = a._heartbeat()

            self.assertFalse(return_val)
            send.assert_not_called()
            a._mdns_updater.inc_P2P_enable_count.assert_not_called()
            self.assertTrue(a._node_data["registered"])

    # # ================================================================================================================
    # # Test discovery operation
    # # ================================================================================================================

    def test_discovery_under_normal_operation(self):
        """Test the discovery correctly registers node with aggregator and heartbeats"""
        BACKOFF_PERIOD = 4
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = None
        a._aggregator_list_stale = True
        a._backoff_period = BACKOFF_PERIOD

        a.mdnsbridge.getHrefWithException.return_value = AGGREGATOR_1

        with mock.patch('gevent.sleep') as sleep:
            with mock.patch.object(a, '_heartbeat', return_value=True) as heartbeat:
                a._discovery_operation()

                sleep.assert_called_once_with(BACKOFF_PERIOD)
                a.mdnsbridge.updateServices.assert_called_once_with(LEGACY_REG_MDNSTYPE)
                heartbeat.assert_called_once_with()
                self.assertFalse(a._backoff_active)
                self.assertFalse(a._aggregator_list_stale)
                self.assertEqual(a.aggregator, AGGREGATOR_1)
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    def test_discovery_when_aggregator_failure_flag_set(self):
        """Test the discovery correctly registers node with aggregator and heartbeats,
        but does not when for backoff period"""
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = None
        a._aggregator_list_stale = True
        a._aggregator_failure = True

        a.mdnsbridge.getHrefWithException.return_value = AGGREGATOR_1

        with mock.patch('gevent.sleep') as sleep:
            with mock.patch.object(a, '_heartbeat', return_value=True) as heartbeat:
                a._discovery_operation()

                sleep.assert_not_called()
                a.mdnsbridge.updateServices.assert_called_once_with(LEGACY_REG_MDNSTYPE)
                heartbeat.assert_called_once_with()
                self.assertFalse(a._backoff_active)
                self.assertFalse(a._aggregator_list_stale)
                self.assertEqual(a.aggregator, AGGREGATOR_1)
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    def test_discovery_when_no_aggregator_returned(self):
        """Test the discovery returns when no aggregators found and does not attempt registration"""

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = None
        a._aggregator_list_stale = True
        a._aggregator_failure = True

        a.mdnsbridge.getHrefWithException.side_effect = NoService

        with mock.patch('gevent.sleep') as sleep:
            with mock.patch.object(a, '_heartbeat', return_value=True) as heartbeat:
                a._discovery_operation()

                sleep.assert_not_called()
                a.mdnsbridge.updateServices.assert_called_once_with(LEGACY_REG_MDNSTYPE)
                heartbeat.assert_not_called()
                self.assertFalse(a._backoff_active)
                self.assertTrue(a._aggregator_list_stale)
                self.assertEqual(a.aggregator, None)
                a._mdns_updater.inc_P2P_enable_count.assert_called_once_with()
                self.assertEqual(a._backoff_period, BACKOFF_INITIAL_TIMOUT_SECONDS)

    def test_discovery_when_end_of_list_returned(self):
        """Test the discovery returns when end of aggregator list reached and does not attempt registration"""

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = None
        a._aggregator_list_stale = True
        a._aggregator_failure = True

        a.mdnsbridge.getHrefWithException.side_effect = EndOfServiceList

        with mock.patch('gevent.sleep') as sleep:
            with mock.patch.object(a, '_heartbeat', return_value=True) as heartbeat:
                a._discovery_operation()

                sleep.assert_not_called()
                a.mdnsbridge.updateServices.assert_called_once_with(LEGACY_REG_MDNSTYPE)
                heartbeat.assert_not_called()
                self.assertFalse(a._backoff_active)
                self.assertTrue(a._aggregator_list_stale)
                self.assertEqual(a.aggregator, None)
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()
                self.assertEqual(a._backoff_period, BACKOFF_INITIAL_TIMOUT_SECONDS)

    def test_discovery_when_heartbeat_fails(self):
        """Test that discovery uses tries multiple aggregators before returning"""
        AGGREGATOR_1 = "http://example1.com"
        AGGREGATOR_2 = "http://example2.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = None
        a._aggregator_list_stale = True

        a.mdnsbridge.getHrefWithException.side_effect = [AGGREGATOR_1, AGGREGATOR_2]

        with mock.patch('gevent.sleep'):
            with mock.patch.object(a, '_heartbeat', side_effect=[False, True]) as heartbeat:
                a._discovery_operation()

                a.mdnsbridge.updateServices.assert_called_once_with(LEGACY_REG_MDNSTYPE)
                heartbeat.assert_called_with()
                self.assertFalse(a._backoff_active)
                self.assertFalse(a._aggregator_list_stale)
                self.assertEqual(a.aggregator, AGGREGATOR_2)
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    # # ================================================================================================================
    # # Test registered operation
    # # ================================================================================================================

    def test_registered_operation_under_normal_operation(self):
        """Test the registered operation correctly heartbeats"""
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = AGGREGATOR_1

        with mock.patch.object(a, '_heartbeat', return_value=True) as heartbeat:
            a._registered_operation()

            heartbeat.assert_called_once_with()
            self.assertFalse(a._backoff_active)
            self.assertFalse(a._aggregator_failure)
            self.assertEqual(a.aggregator, AGGREGATOR_1)
            a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    def test_registered_operation_when_heartbeat_fails(self):
        """Test the registered operation when the heartbeat fails"""
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = AGGREGATOR_1

        with mock.patch.object(a, '_heartbeat', return_value=False) as heartbeat:
            a._registered_operation()

            heartbeat.assert_called_once_with()
            self.assertFalse(a._backoff_active)
            self.assertTrue(a._aggregator_failure)
            self.assertEqual(a.aggregator, None)
            a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    # # ================================================================================================================
    # # Test _register_node
    # # ================================================================================================================

    def test_register_node_under_normal_operation(self):
        """Test that the Node correctly registers with aggregator"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=201)

        with mock.patch.object(a, '_send', side_effect=request) as send:
            with mock.patch.object(a, '_register_node_resources') as reg_resources:
                return_val = a._register_node(a._node_data["node"])

                self.assertTrue(return_val)
                send.assert_called_once_with("POST", AGGREGATOR_1, a.aggregator_apiversion,
                                             "resource", a._node_data["node"])
                reg_resources.assert_called_once_with()
                self.assertFalse(a._backoff_active)
                self.assertTrue(a._aggregator_list_stale)
                self.assertTrue(a._node_data['registered'])
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    def test_register_node_when_no_node_data(self):
        """Test that is there is no data the registration fails"""
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False

        with mock.patch.object(a, '_send') as send:
            with mock.patch.object(a, '_register_node_resources') as reg_resources:
                return_val = a._register_node(None)

                self.assertFalse(return_val)
                send.assert_not_called()
                reg_resources.assert_not_called()
                self.assertFalse(a._backoff_active)
                self.assertTrue(a._aggregator_list_stale)
                self.assertFalse(a._node_data['registered'])
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    def test_register_node_response_200(self):
        """Test that the Node correctly un-registers with aggregator when HTTP 200 response received"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        request_list = [
            mock.MagicMock(status_code=200, headers={'Location': 'path/xxx'}),
            mock.MagicMock(status_code=201),
        ]

        with mock.patch.object(a, '_send', side_effect=request_list) as send:
            with mock.patch.object(a, '_unregister_node', return_value=True) as un_reg:
                with mock.patch.object(a, '_register_node_resources') as reg_resources:
                    return_val = a._register_node(a._node_data["node"])

                    self.assertTrue(return_val)
                    send.assert_called_with("POST", AGGREGATOR_1, a.aggregator_apiversion,
                                            "resource", a._node_data["node"])
                    un_reg.assert_called_once_with('path/xxx')
                    reg_resources.assert_called_once_with()
                    self.assertFalse(a._backoff_active)
                    self.assertTrue(a._aggregator_list_stale)
                    self.assertTrue(a._node_data['registered'])
                    a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    def test_register_node_response_409(self):
        """Test that the Node correctly un-registers with aggregator when HTTP 409 response received"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        request_list = [
            mock.MagicMock(status_code=409, headers={'Location': 'path/xxx'}),
            mock.MagicMock(status_code=201),
        ]

        with mock.patch.object(a, '_send', side_effect=request_list) as send:
            with mock.patch.object(a, '_unregister_node', return_value=True) as un_reg:
                with mock.patch.object(a, '_register_node_resources') as reg_resources:
                    return_val = a._register_node(a._node_data["node"])

                    self.assertTrue(return_val)
                    send.assert_called_with("POST", AGGREGATOR_1, a.aggregator_apiversion,
                                            "resource", a._node_data["node"])
                    un_reg.assert_called_once_with('path/xxx')
                    reg_resources.assert_called_once_with()
                    self.assertFalse(a._backoff_active)
                    self.assertTrue(a._aggregator_list_stale)
                    self.assertTrue(a._node_data['registered'])
                    a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    def test_register_node_response_Exception(self):
        """Test that the Node correctly handles Exception during registration"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        with mock.patch.object(a, '_send', side_effect=Exception) as send:
            return_val = a._register_node(a._node_data["node"])

            self.assertFalse(return_val)
            send.assert_called_with("POST", AGGREGATOR_1, a.aggregator_apiversion,
                                    "resource", a._node_data["node"])
            self.assertFalse(a._backoff_active)
            self.assertFalse(a._node_data['registered'])
            a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    def test_register_node_response_if_un_registration_fails(self):
        """Test that the Node correctly handles a failure during un-registration"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        request_response = [
            mock.MagicMock(status_code=409, headers={'Location': 'path/xxx'})
        ]

        with mock.patch.object(a, '_send', side_effect=request_response) as send:
            with mock.patch.object(a, '_unregister_node', return_value=False) as un_reg:

                return_val = a._register_node(a._node_data["node"])

                self.assertFalse(return_val)
                send.assert_called_with("POST", AGGREGATOR_1, a.aggregator_apiversion,
                                        "resource", a._node_data["node"])
                un_reg.assert_called_once_with('path/xxx')
                self.assertFalse(a._backoff_active)
                self.assertFalse(a._node_data['registered'])
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    def test_register_node_drains_queue(self):
        """Test that the register Node drains the queue before registration"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        request_response = [
            mock.MagicMock(status_code=201)
        ]

        queue = [
            {"method": "POST", "namespace": "resource", "res_type": "dummy", "key": DUMMYNODEID},
            {"method": "DELETE", "namespace": "resource", "res_type": "dummy", "key": DUMMYNODEID}
        ]

        a._reg_queue.empty.side_effect = lambda: (len(queue) == 0)

        def _get(block=True):
            if len(queue) == 0:
                raise gevent.queue.Queue.Empty
            return queue.pop(0)
        a._reg_queue.get.side_effect = _get

        with mock.patch.object(a, '_send', side_effect=request_response) as send:
            with mock.patch.object(a, '_register_node_resources') as reg_resources:
                return_val = a._register_node(a._node_data["node"])

                self.assertTrue(return_val)
                send.assert_called_with("POST", AGGREGATOR_1, a.aggregator_apiversion,
                                        "resource", a._node_data["node"])
                reg_resources.assert_called_once_with()
                self.assertFalse(a._backoff_active)
                self.assertTrue(a._node_data['registered'])
                a._mdns_updater.inc_P2P_enable_count.assert_not_called()

    # # ================================================================================================================
    # # Test _unregister_node
    # # ================================================================================================================

    def test_unregister_node_under_normal_operation(self):
        """Test that the Node correctly unregisters with aggregator"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com:234"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=204)

        # No URL Path
        with mock.patch.object(a, '_send', side_effect=request) as send:
            return_val = a._unregister_node(None)

            self.assertTrue(return_val)
            send.assert_called_once_with("DELETE", AGGREGATOR_1, a.aggregator_apiversion,
                                         "resource/nodes/{}".format(DUMMYNODEID))
            self.assertFalse(a._node_data['registered'])

        # Relative URL
        a._node_data["registered"] = True
        with mock.patch.object(a, '_send_request', side_effect=request) as send:
            return_val = a._unregister_node('/path/xxx')

            self.assertTrue(return_val)
            send.assert_called_once_with("DELETE", AGGREGATOR_1, '/path/xxx')
            self.assertFalse(a._node_data['registered'])

        # Absolute URL
        a._node_data["registered"] = True
        with mock.patch.object(a, '_send_request', side_effect=request) as send:
            return_val = a._unregister_node(AGGREGATOR_1 + '/path/xxx')

            self.assertTrue(return_val)
            send.assert_called_once_with("DELETE", AGGREGATOR_1, '/path/xxx')
            self.assertFalse(a._node_data['registered'])

    def test_unregister_node_unexpected_response(self):
        """Test that the Node returns False when DELETE returns an unexpected response"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=200)

        with mock.patch.object(a, '_send', side_effect=request) as send:
            return_val = a._unregister_node(None)

            self.assertFalse(return_val)
            send.assert_called_once_with("DELETE", AGGREGATOR_1, a.aggregator_apiversion,
                                         "resource/nodes/{}".format(DUMMYNODEID))
            self.assertFalse(a._node_data['registered'])

    def test_unregister_node_Exception(self):
        """Test that the Node returns False when Exception raise during DELETE"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        with mock.patch.object(a, '_send', side_effect=Exception) as send:
            return_val = a._unregister_node(None)

            self.assertFalse(return_val)
            send.assert_called_once_with("DELETE", AGGREGATOR_1, a.aggregator_apiversion,
                                         "resource/nodes/{}".format(DUMMYNODEID))
            self.assertFalse(a._node_data['registered'])

    # # ================================================================================================================
    # # Test _register_node_resources
    # # ================================================================================================================

    def test_register_node_resources_under_normal_operation(self):
        """Test that the Node correctly queues resources to be registered with aggregator"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        DUMMYKEY = "dummykey"
        DUMMYPARAMKEY = "dummyparamkey"
        DUMMYPARAMVAL = "dummyparamval"
        DUMMYFLOW = "dummyflow"
        DUMMYDEVICE = "dummydevice"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a.aggregator = AGGREGATOR_1
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        if "entities" not in a._node_data:
            a._node_data["entities"] = {}
        if "resource" not in a._node_data["entities"]:
            a._node_data["entities"]["resource"] = {}
        if "dummy" not in a._node_data["entities"]["resource"]:
            a._node_data["entities"]["resource"]["dummy"] = {}
        if "device" not in a._node_data["entities"]["resource"]:
            a._node_data["entities"]["resource"]["device"] = {}
        if "flow" not in a._node_data["entities"]["resource"]:
            a._node_data["entities"]["resource"]["flow"] = {}
        a._node_data["entities"]["resource"]["dummy"][DUMMYKEY] = {DUMMYPARAMKEY: DUMMYPARAMVAL}
        a._node_data["entities"]["resource"]["device"][DUMMYDEVICE] = {DUMMYPARAMKEY: DUMMYPARAMVAL}
        a._node_data["entities"]["resource"]["flow"][DUMMYFLOW] = {DUMMYPARAMKEY: DUMMYPARAMVAL}

        expected_put_calls = []
        # The re-registration of the other resources should be queued for the next run loop, and arranged in order
        expected_put_calls = (
            sum([
                [mock.call({"method": "POST", "namespace": "resource", "res_type": res_type, "key": key})
                    for key in a._node_data["entities"]["resource"][res_type]
                 ] for res_type in a.registration_order if res_type in a._node_data["entities"]["resource"]
            ], []) + sum([
                [mock.call({"method": "POST", "namespace": "resource", "res_type": res_type, "key": key})
                    for key in a._node_data["entities"]["resource"][res_type]
                 ] for res_type in a._node_data["entities"]["resource"] if res_type not in a.registration_order
            ], [])
        )

        a._register_node_resources()
        self.assertListEqual(a._reg_queue.put.mock_calls, expected_put_calls)

    # # ================================================================================================================
    # # Test queue handelling
    # # ================================================================================================================

    def test_process_queue_does_nothing_when_not_registered(self):
        """The queue processing thread should not send any messages when the node is not registered."""
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a._reg_queue.empty.return_value = False

        def killloop(*args, **kwargs):
            a._running = False

        with mock.patch('gevent.sleep', side_effect=killloop) as sleep:
            with mock.patch.object(a, '_send') as send:
                a._process_queue()

                send.assert_not_called()
                a._mdns_updater.P2P_disable.assert_not_called()
                sleep.assert_called_with(mock.ANY)

    def test_process_queue_does_nothing_when_queue_empty(self):
        """The queue processing thread should not send any messages when the queue is empty."""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        a._reg_queue.empty.return_value = True

        def killloop(*args, **kwargs):
            a._running = False

        with mock.patch('gevent.sleep', side_effect=killloop) as sleep:
            with mock.patch.object(a, '_send') as send:
                a._process_queue()

                send.assert_not_called()
                a._mdns_updater.P2P_disable.assert_not_called()
                sleep.assert_called_with(mock.ANY)

    def test_process_queue_processes_queue_when_running(self):
        """The queue processing thread should check the queue and send a registration/deregistration request
        to the remote aggregator when required."""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        DUMMYKEY = "dummykey"
        DUMMYPARAMKEY = "dummyparamkey"
        DUMMYPARAMVAL = "dummyparamval"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a.aggregator = "www.example.com"
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        if "entities" not in a._node_data:
            a._node_data["entities"] = {}
        if "resource" not in a._node_data["entities"]:
            a._node_data["entities"]["resource"] = {}
        if "dummy" not in a._node_data["entities"]["resource"]:
            a._node_data["entities"]["resource"]["dummy"] = {}
        a._node_data["entities"]["resource"]["dummy"][DUMMYKEY] = {DUMMYPARAMKEY: DUMMYPARAMVAL}

        queue = [
            {"method": "POST", "namespace": "resource", "res_type": "node", "key": DUMMYNODEID},
            {"method": "POST", "namespace": "resource", "res_type": "dummy", "key": DUMMYKEY},
            {"method": "DELETE", "namespace": "resource", "res_type": "dummy", "key": DUMMYKEY}
        ]

        a._reg_queue.empty.side_effect = lambda: (len(queue) == 0)
        a._reg_queue.get.side_effect = lambda: queue.pop(0)

        expected_calls = [
            mock.call('POST', "www.example.com", "v1.2", 'resource',
                      a._node_data["node"]),
            mock.call('POST', "www.example.com", "v1.2", 'resource',
                      a._node_data["entities"]["resource"]["dummy"][DUMMYKEY]),
            mock.call('DELETE', "www.example.com", "v1.2", 'resource/dummys/' + DUMMYKEY)
        ]

        def killloop(*args, **kwargs):
            a._running = False

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch.object(a, '_send') as send:
                a._process_queue()
                send.assert_has_calls(expected_calls)

    def test_process_queue_stops_when_not_running(self):
        """The process queue method should stop as soon as running set to false"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        DUMMYKEY = "dummykey"
        DUMMYPARAMKEY = "dummyparamkey"
        DUMMYPARAMVAL = "dummyparamval"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._running = False
        a.aggregator = "www.example.com"
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        if "entities" not in a._node_data:
            a._node_data["entities"] = {}
        if "resource" not in a._node_data["entities"]:
            a._node_data["entities"]["resource"] = {}
        if "dummy" not in a._node_data["entities"]["resource"]:
            a._node_data["entities"]["resource"]["dummy"] = {}
        a._node_data["entities"]["resource"]["dummy"][DUMMYKEY] = {DUMMYPARAMKEY: DUMMYPARAMVAL}

        queue = [
            {"method": "POST", "namespace": "resource", "res_type": "node", "key": DUMMYNODEID},
        ]

        a._reg_queue.empty.side_effect = lambda: (len(queue) == 0)
        a._reg_queue.get.side_effect = lambda: queue.pop(0)

        with mock.patch('gevent.sleep', side_effect=Exception) as sleep:
            with mock.patch.object(a, '_send') as send:
                try:
                    a._process_queue()
                except Exception:
                    self.fail(msg="process_queue kept running")

                send.assert_not_called
                sleep.assert_not_called()

    def test_process_queue_processes_queue_when_running_and_aborts_on_exception_in_general_register(self):
        """If a non-node register performed by the queue processing thread throws an exception then the loop
        should abort."""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        DUMMYKEY = "dummykey"
        DUMMYPARAMKEY = "dummyparamkey"
        DUMMYPARAMVAL = "dummyparamval"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a.aggregator = "www.example.com"
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        if "entities" not in a._node_data:
            a._node_data["entities"] = {}
        if "resource" not in a._node_data["entities"]:
            a._node_data["entities"]["resource"] = {}
        if "dummy" not in a._node_data["entities"]["resource"]:
            a._node_data["entities"]["resource"]["dummy"] = {}
        a._node_data["entities"]["resource"]["dummy"][DUMMYKEY] = {DUMMYPARAMKEY: DUMMYPARAMVAL}

        queue = [
            {"method": "POST", "namespace": "resource", "res_type": "dummy", "key": DUMMYKEY},
        ]

        a._reg_queue.empty.side_effect = lambda: (len(queue) == 0)
        a._reg_queue.get.side_effect = lambda: queue.pop(0)

        expected_calls = [
            mock.call('POST', "www.example.com", "v1.2", 'resource',
                      a._node_data["entities"]["resource"]["dummy"][DUMMYKEY]),
        ]

        def killloop(*args, **kwargs):
            a._running = False

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch.object(a, '_send', side_effect=InvalidRequest) as send:
                a._process_queue()

                send.assert_has_calls(expected_calls)
                a._mdns_updater.P2P_disable.assert_not_called()
                self.assertNotIn(DUMMYKEY, a._node_data["entities"]["resource"]["dummy"])

    def test_process_queue_processes_queue_when_running_and_aborts_on_exception_in_general_unregister(self):
        """If an unregister performed by the queue processing thread throws an exception then the loop should abort."""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        DUMMYKEY = "dummykey"
        DUMMYPARAMKEY = "dummyparamkey"
        DUMMYPARAMVAL = "dummyparamval"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a.aggregator = "www.example.com"
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        if "entities" not in a._node_data:
            a._node_data["entities"] = {}
        if "resource" not in a._node_data["entities"]:
            a._node_data["entities"]["resource"] = {}
        if "dummy" not in a._node_data["entities"]["resource"]:
            a._node_data["entities"]["resource"]["dummy"] = {}
        a._node_data["entities"]["resource"]["dummy"][DUMMYKEY] = {DUMMYPARAMKEY: DUMMYPARAMVAL}

        queue = [
            {"method": "DELETE", "namespace": "resource", "res_type": "dummy", "key": DUMMYKEY}
        ]

        a._reg_queue.empty.side_effect = lambda: (len(queue) == 0)
        a._reg_queue.get.side_effect = lambda: queue.pop(0)

        expected_calls = [
            mock.call('DELETE', "www.example.com", "v1.2", 'resource/dummys/' + DUMMYKEY)
        ]

        def killloop(*args, **kwargs):
            a._running = False

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch.object(a, '_send', side_effect=InvalidRequest) as send:
                a._process_queue()

                send.assert_has_calls(expected_calls)
                a._mdns_updater.P2P_disable.assert_not_called()

    def test_process_queue_processes_queue_when_running_and_aborts_on_server_side_exception(self):
        """If a non-node register performed by the queue processing thread throws a server side exception,
        new aggregator should be chosen and failed request should be queued added to the front of the queue."""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        DUMMYKEY = "dummykey"
        DUMMYPARAMKEY = "dummyparamkey"
        DUMMYPARAMVAL = "dummyparamval"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a.aggregator = "www.example.com"
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        if "entities" not in a._node_data:
            a._node_data["entities"] = {}
        if "resource" not in a._node_data["entities"]:
            a._node_data["entities"]["resource"] = {}
        if "dummy" not in a._node_data["entities"]["resource"]:
            a._node_data["entities"]["resource"]["dummy"] = {}
        a._node_data["entities"]["resource"]["dummy"][DUMMYKEY] = {DUMMYPARAMKEY: DUMMYPARAMVAL}

        expected_queue = [
            {"method": "POST", "namespace": "resource", "res_type": "dummy", "key": DUMMYKEY},
            {"method": "POST", "namespace": "resource", "res_type": "left", "key": DUMMYKEY},
        ]
        queue = deepcopy(expected_queue)
        updated_queue = []

        a._reg_queue.empty.side_effect = lambda: (len(queue) == 0)
        a._reg_queue.get.side_effect = lambda: queue.pop(0)
        a._reg_queue.put.side_effect = lambda data: updated_queue.append(data)

        expected_calls = [
            mock.call('POST', "www.example.com", "v1.2", 'resource',
                      a._node_data["entities"]["resource"]["dummy"][DUMMYKEY]),
        ]

        def killloop(*args, **kwargs):
            a._running = False

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch.object(a, '_send', side_effect=ServerSideError) as send:
                a._process_queue()

                send.assert_has_calls(expected_calls)
                a._mdns_updater.P2P_disable.assert_not_called()
                self.assertIsNone(a.aggregator)
                self.assertListEqual(updated_queue, expected_queue)

    def test_process_queue_processes_queue_when_running_and_ignores_unknown_methods(self):
        """Unknown verbs in the queue should be ignored."""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        DUMMYKEY = "dummykey"
        DUMMYPARAMKEY = "dummyparamkey"
        DUMMYPARAMVAL = "dummyparamval"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a.aggregator = "www.example.com"
        a._node_data["registered"] = True
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}
        if "entities" not in a._node_data:
            a._node_data["entities"] = {}
        if "resource" not in a._node_data["entities"]:
            a._node_data["entities"]["resource"] = {}
        if "dummy" not in a._node_data["entities"]["resource"]:
            a._node_data["entities"]["resource"]["dummy"] = {}
        a._node_data["entities"]["resource"]["dummy"][DUMMYKEY] = {DUMMYPARAMKEY: DUMMYPARAMVAL}

        queue = [
            {"method": "DANCE", "namespace": "resource", "res_type": "dummy", "key": DUMMYKEY}
        ]

        a._reg_queue.empty.side_effect = lambda: (len(queue) == 0)
        a._reg_queue.get.side_effect = lambda: queue.pop(0)

        def killloop(*args, **kwargs):
            a._running = False

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch.object(a, '_send', side_effect=InvalidRequest) as send:
                a._process_queue()

                send.assert_not_called()
                a._mdns_updater.P2P_disable.assert_not_called()

    def test_process_queue_handles_exception_in_unqueueing(self):
        """An exception in unqueing an item should reset the object state to unregistered."""

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = True
        a.aggregator = "www.example.com"

        a._reg_queue.empty.return_value = False
        a._reg_queue.get.side_effect = Exception

        def killloop(*args, **kwargs):
            a._running = False

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch.object(a, '_send') as send:
                a._process_queue()

                send.assert_not_called()
                a._mdns_updater.P2P_disable.assert_called_with()
                self.assertFalse(a._node_data["registered"])

    # # ================================================================================================================
    # # Test _send method
    # # ================================================================================================================
    def assert_send_runs_correctly(
        self, method,
        aggregator, api_ver,
        url, data=None,
        request=None,
        expected_return=None,
        expected_exception=None,
        prefer_ipv6=False
    ):
        """Function to test that the _send() method runs correctly"""

        def create_mock_request(method, aggregator_url, api_ver, url, expected_data, prefer_ipv6=False):
            if not prefer_ipv6:
                return (mock.call(
                    method=method,
                    url=urljoin(
                        aggregator_url,
                        AGGREGATOR_APINAMESPACE + "/" + AGGREGATOR_APINAME + "/" + a.aggregator_apiversion + '/' + url
                    ),
                    json=expected_data,
                    timeout=1.0))
            else:
                return (mock.call(
                    method=method,
                    url=urljoin(
                        aggregator_url,
                        AGGREGATOR_APINAMESPACE + "/" + AGGREGATOR_APINAME + "/" + a.aggregator_apiversion + '/' + url
                    ),
                    json=expected_data,
                    timeout=1.0,
                    proxies={'http': ''}))

        a = Aggregator(mdns_updater=mock.MagicMock())

        expected_request_calls = []
        expected_request_calls.append(
            create_mock_request(method, aggregator, api_ver, url, data, prefer_ipv6)
        )

        with mock.patch.dict(nmosnode.aggregator._config, {'prefer_ipv6': prefer_ipv6}):
            with mock.patch("requests.request", side_effect=request) as _request:
                R = None
                if expected_exception is not None:
                    with self.assertRaises(expected_exception):
                        R = a._send(method, aggregator, api_ver, url, data)
                else:
                    try:
                        R = a._send(method, aggregator, api_ver, url, data)
                    except Exception as e:
                        self.fail(msg="_send threw an unexpected exception, {}".format(e))

                self.assertListEqual(_request.mock_calls, expected_request_calls)
                if R:
                    self.assertEqual(R.content, expected_return)

    def test_send_200_response(self):
        TEST_CONTENT = "kasjhdlkhnjgsn"

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=200, content=TEST_CONTENT)
        self.assert_send_runs_correctly("POST", "http://www.example.com:80", "v1.2", "test",
                                        request=request, expected_return=TEST_CONTENT)

    def test_send_200_response_ipv6(self):
        TEST_CONTENT = "kasjhdlkhnjgsn"

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=200, content=TEST_CONTENT)
        self.assert_send_runs_correctly("POST", "http://www.example.com:80", "v1.2", "test",
                                        request=request, expected_return=TEST_CONTENT, prefer_ipv6=True)

    def test_send_201_response(self):
        TEST_CONTENT = "kasjhdlkhnjgsn"

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=201, content=TEST_CONTENT)
        self.assert_send_runs_correctly("POST", "http://www.example.com:80", "v1.2", "test",
                                        request=request, expected_return=TEST_CONTENT)

    def test_send_204_response(self):
        TEST_CONTENT = "kasjhdlkhnjgsn"

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=204, content=TEST_CONTENT)
        self.assert_send_runs_correctly("POST", "http://www.example.com:80", "v1.2", "test",
                                        request=request, expected_return=TEST_CONTENT)

    def test_send_409_response(self):
        TEST_CONTENT = "kasjhdlkhnjgsn"

        def request(*args, **kwargs):
            return mock.MagicMock(status_code=409, content=TEST_CONTENT)
        self.assert_send_runs_correctly("POST", "http://www.example.com:80", "v1.2", "test",
                                        request=request, expected_return=TEST_CONTENT)

    def test_send_4xx_response(self):
        """On a 4xx response an InvalidRequest Exception should be raised
        apart from 409, which is handled separately"""
        def request(*args, **kwargs):
            return mock.MagicMock(status_code=401)
        self.assert_send_runs_correctly("POST", "http://www.example.com:80", "v1.2", "test",
                                        request=request, expected_exception=InvalidRequest)

    def test_send_5xx_response(self):
        """On a 5xx response a ServerSideError Exception should be raised"""
        def request(*args, **kwargs):
            return mock.MagicMock(status_code=500)
        self.assert_send_runs_correctly("POST", "http://www.example.com:80", "v1.2", "test",
                                        request=request, expected_exception=ServerSideError)

    def test_send_no_response(self):
        """On a no response a ServerSideError Exception should be raised"""
        def request(*args, **kwargs):
            return None
        self.assert_send_runs_correctly("POST", "http://www.example.com:80", "v1.2", "test",
                                        request=request, expected_exception=ServerSideError)

    def test_send_request_exception(self):
        """On a request exception a ServerSideError Exception should be raised"""
        def request(*args, **kwargs):
            raise requests.exceptions.RequestException
        self.assert_send_runs_correctly("POST", "http://www.example.com:80", "v1.2", "test",
                                        request=request, expected_exception=ServerSideError)

    # ==================================================================================================================
    # Test main thread
    # ==================================================================================================================

    def test_main_thread_under_normal_operation(self):
        """Test the main thread under normal operation"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        def killloop(*args, **kwargs):
            if a._node_data['registered']:
                a._running = False

        request_mocks = [
            mock.MagicMock(name="request1()", status_code=404),
            mock.MagicMock(name="request2()", status_code=201),
            mock.MagicMock(name="request3()", status_code=200),
        ]

        expected_request_calls = [
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion, "resource"),
                      json={"data": {"id": DUMMYNODEID}, "type": "node"}, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
        ]

        a.mdnsbridge.getHrefWithException.return_value = AGGREGATOR_1

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch('requests.request', side_effect=request_mocks) as request:

                a._main_thread()
                self.assertTrue(a._node_data["registered"])
                self.assertListEqual(request.mock_calls, expected_request_calls)

    def test_main_thread_when_heartbeat_200(self):
        """Test the main thread when heartbeat returns 200 response"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        def killloop(*args, **kwargs):
            if a._node_data['registered']:
                a._running = False

        request_mocks = [
            mock.MagicMock(name="request1()", status_code=200, headers={}),
            mock.MagicMock(name="request2()", status_code=204),
            mock.MagicMock(name="request3()", status_code=201),
            mock.MagicMock(name="request4()", status_code=200),
        ]

        expected_request_calls = [
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
            mock.call(method='DELETE', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "resource/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion, "resource"),
                      json={"data": {"id": DUMMYNODEID}, "type": "node"}, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
        ]

        a.mdnsbridge.getHrefWithException.return_value = AGGREGATOR_1

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch('requests.request', side_effect=request_mocks) as request:

                a._main_thread()
                self.assertTrue(a._node_data["registered"])
                self.assertListEqual(request.mock_calls, expected_request_calls)

    def test_main_thread_when_heartbeat_5xx(self):
        """Test the main thread when heartbeat returns 200 response"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"
        AGGREGATOR_2 = "http://example2.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        def killloop(*args, **kwargs):
            if a._node_data['registered']:
                a._running = False

        request_mocks = [
            mock.MagicMock(name="request1()", status_code=500),
            mock.MagicMock(name="request2()", status_code=404),
            mock.MagicMock(name="request3()", status_code=201),
            mock.MagicMock(name="request4()", status_code=200),
        ]

        expected_request_calls = [
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_2, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_2, a.aggregator_apiversion, "resource"),
                      json={"data": {"id": DUMMYNODEID}, "type": "node"}, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_2, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
        ]

        a.mdnsbridge.getHrefWithException.side_effect = [AGGREGATOR_1, AGGREGATOR_2]

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch('requests.request', side_effect=request_mocks) as request:

                a._main_thread()
                self.assertTrue(a._node_data["registered"])
                self.assertListEqual(request.mock_calls, expected_request_calls)

    def test_main_thread_when_register_200(self):
        """Test the main thread when register returns 200 response"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        def killloop(*args, **kwargs):
            if a._node_data['registered']:
                a._running = False

        request_mocks = [
            mock.MagicMock(name="request1()", status_code=404),
            mock.MagicMock(name="request2()", status_code=200, headers={}),
            mock.MagicMock(name="request3()", status_code=204),
            mock.MagicMock(name="request4()", status_code=201),
            mock.MagicMock(name="request5()", status_code=200),
        ]

        expected_request_calls = [
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion, "resource"),
                      json={"data": {"id": DUMMYNODEID}, "type": "node"}, timeout=1.0),
            mock.call(method='DELETE', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "resource/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion, "resource"),
                      json={"data": {"id": DUMMYNODEID}, "type": "node"}, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
        ]

        a.mdnsbridge.getHrefWithException.return_value = AGGREGATOR_1

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch('requests.request', side_effect=request_mocks) as request:

                a._main_thread()
                self.assertTrue(a._node_data["registered"])
                self.assertListEqual(request.mock_calls, expected_request_calls)

    def test_main_thread_when_register_5xx(self):
        """Test the main thread when heartbeat returns 200 response"""
        DUMMYNODEID = "90f7c2c0-cfa9-11e7-9b9d-2fe338e1e7ce"
        AGGREGATOR_1 = "http://example1.com"
        AGGREGATOR_2 = "http://example2.com"

        a = Aggregator(mdns_updater=mock.MagicMock())
        a._node_data["registered"] = False
        a._node_data["node"] = {"type": "node", "data": {"id": DUMMYNODEID}}

        def killloop(*args, **kwargs):
            if a._node_data['registered']:
                a._running = False

        request_mocks = [
            mock.MagicMock(name="request1()", status_code=404),
            mock.MagicMock(name="request2()", status_code=500),
            mock.MagicMock(name="request3()", status_code=404),
            mock.MagicMock(name="request4()", status_code=201),
            mock.MagicMock(name="request5()", status_code=200),
        ]

        expected_request_calls = [
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_1, a.aggregator_apiversion, "resource"),
                      json={"data": {"id": DUMMYNODEID}, "type": "node"}, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_2, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_2, a.aggregator_apiversion, "resource"),
                      json={"data": {"id": DUMMYNODEID}, "type": "node"}, timeout=1.0),
            mock.call(method='POST', url=self.construct_url(AGGREGATOR_2, a.aggregator_apiversion,
                      "health/nodes/{}".format(DUMMYNODEID)), json=None, timeout=1.0),
        ]

        a.mdnsbridge.getHrefWithException.side_effect = [AGGREGATOR_1, AGGREGATOR_2]

        with mock.patch('gevent.sleep', side_effect=killloop):
            with mock.patch('requests.request', side_effect=request_mocks) as request:

                a._main_thread()
                self.assertTrue(a._node_data["registered"])
                self.assertListEqual(request.mock_calls, expected_request_calls)
