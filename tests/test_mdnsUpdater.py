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

from six import itervalues
from six import PY2

import unittest
import mock
import time

from nmosnode.aggregator import MDNSUpdater

MAX_ITERATIONS = 10


class TestMDNSUpdater(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestMDNSUpdater, self).__init__(*args, **kwargs)
        if PY2:
            self.assertCountEqual = self.assertItemsEqual

    def setUp(self):
        self.mappings = {
            "device": "ver_dvc",
            "flow": "ver_flw",
            "source": "ver_src",
            "sender": "ver_snd",
            "receiver": "ver_rcv",
            "self": "ver_slf"}
        self.mdnstype = "_nmos-node._tcp"
        self.txt_recs = {"api_ver": "v1.0,v1.1,v1.2", "api_proto": "http"}
        self.mdnsname = "node_dummy_for_testing"
        self.port = 12345
        self.mdnsengine = mock.MagicMock()
        self.logger = mock.MagicMock()

        self.UUT = MDNSUpdater(
            self.mdnsengine,
            self.mdnstype,
            self.mdnsname,
            self.mappings,
            self.port,
            self.logger,
            txt_recs=self.txt_recs)

    def test_init(self):
        """Test of initialisation of an MDNSUpdater"""

        self.assertEqual(self.UUT.mdns_type, self.mdnstype)
        self.assertEqual(self.UUT.mdns_name, self.mdnsname)
        self.assertEqual(self.UUT.mappings, self.mappings)
        self.assertEqual(self.UUT.port, self.port)
        self.assertEqual(self.UUT.txt_rec_base, self.txt_recs)
        self.assertEqual(self.UUT.logger, self.logger)

        for key in itervalues(self.mappings):
            self.assertIn(key, self.UUT.service_versions)
            self.assertEqual(self.UUT.service_versions[key], 0)

        self.UUT.mdns.register.assert_called_once_with(self.mdnsname, self.mdnstype, self.port, self.txt_recs)

    def test_p2p_enable(self):
        """Test of MDNSUpdater.p2p_enable, should trigger an mdns update"""
        self.UUT.P2P_enable()

        self.assertTrue(self.UUT.p2p_enable)
        self.txt_recs.update(self.UUT.service_versions)
        counter = 0
        while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
            time.sleep(0.1)
            counter += 1
        self.UUT.mdns.update.assert_called_once_with(self.mdnsname, self.mdnstype, self.txt_recs)

    def test_inc_P2P_enable_count(self):
        """Test of of MDNSUpdater.inc_P2P_enable_count, should do nothing the first p2p_cut_in_count - 1 times,
        and then activate P2P mode"""

        for i in range(0, self.UUT.p2p_cut_in_count-1):
            self.UUT.inc_P2P_enable_count()

            self.assertFalse(self.UUT.p2p_enable)
            counter = 0
            while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
                time.sleep(0.1)
                counter += 1
            self.UUT.mdns.update.assert_not_called()

        self.UUT.inc_P2P_enable_count()
        self.assertTrue(self.UUT.p2p_enable)
        self.txt_recs.update(self.UUT.service_versions)
        counter = 0
        while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
            time.sleep(0.1)
            counter += 1
        self.UUT.mdns.update.assert_called_once_with(self.mdnsname, self.mdnstype, self.txt_recs)

    def test_P2P_disable_when_enabled(self):
        """When an MDNSUpdater is already enabled for P2P calling P2P_disable should disable P2P"""

        self.UUT.P2P_enable()
        counter = 0
        while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
            time.sleep(0.1)
            counter += 1
        self.UUT.mdns.update.reset_mock()
        self.UUT.P2P_disable()

        self.assertFalse(self.UUT.p2p_enable)
        counter = 0
        while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
            time.sleep(0.1)
            counter += 1
        self.UUT.mdns.update.assert_called_once_with(self.mdnsname, self.mdnstype, self.txt_recs)

    def test_P2P_disable_resets_enable_count(self):
        """If an MDNSUpdater is not yet enabled for P2P but has had inc_P2P_enable_count called on it then
        calling P2P_disable should reset this counter."""

        for i in range(0, self.UUT.p2p_cut_in_count-1):
            self.UUT.inc_P2P_enable_count()

            self.assertFalse(self.UUT.p2p_enable)
            counter = 0
            while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
                time.sleep(0.1)
                counter += 1
            self.UUT.mdns.update.assert_not_called()

        self.UUT.P2P_disable()

        for i in range(0, self.UUT.p2p_cut_in_count-1):
            self.UUT.inc_P2P_enable_count()

            self.assertFalse(self.UUT.p2p_enable)
            counter = 0
            while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
                time.sleep(0.1)
                counter += 1
            self.UUT.mdns.update.assert_not_called()

        self.UUT.inc_P2P_enable_count()
        self.assertTrue(self.UUT.p2p_enable)
        self.txt_recs.update(self.UUT.service_versions)
        counter = 0
        while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
            time.sleep(0.1)
            counter += 1
        self.UUT.mdns.update.assert_called_once_with(self.mdnsname, self.mdnstype, self.txt_recs)

    def test_update_mdns_does_nothing_when_not_enabled(self):
        """A call to MDNSUpdater.update_mdns should not do anything if P2P is disabled"""

        self.UUT.update_mdns("device", "register")
        self.assertEqual(self.UUT.service_versions[self.mappings["device"]], 0)
        counter = 0
        while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
            time.sleep(0.1)
            counter += 1
        self.UUT.mdns.update.assert_not_called()

    def test_update_mdns(self):
        """A call to MDNSUpdater.update_mdns when P2P is enabled ought to call mdns.update to increment version numbers
        for devices. Device version numbers should be 8-bit integers which roll over to 0 when incremented beyond
        the limits of 1 byte."""

        self.UUT.P2P_enable()

        # Don't check the mDNS update calls here as it'll take forever and it's handled by other tests already
        for i in range(1, 256):
            self.UUT.mdns.update.reset_mock()
            self.UUT.update_mdns("device", "register")
            self.assertEqual(self.UUT.service_versions[self.mappings["device"]], i)
            self.txt_recs.update(self.UUT.service_versions)

        self.UUT.mdns.update.reset_mock()
        while not self.UUT._mdns_update_queue.empty():
            self.UUT._mdns_update_queue.get()
        self.UUT.update_mdns("device", "register")
        self.assertEqual(self.UUT.service_versions[self.mappings["device"]], 0)
        self.txt_recs.update(self.UUT.service_versions)
        counter = 0
        while not self.UUT._mdns_update_queue.empty() and counter < MAX_ITERATIONS:
            time.sleep(0.1)
            counter += 1
        self.UUT.mdns.update.assert_called_once_with(self.mdnsname, self.mdnstype, self.txt_recs)
