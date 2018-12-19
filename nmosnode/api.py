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

from __future__ import print_function

from nmoscommon.webapi import *
from nmoscommon.webapi import WebAPI, route, resource_route, abort
from six.moves.urllib.parse import urljoin
import requests
from socket import gethostname

from nmoscommon.nmoscommonconfig import config as _config

NODE_APIVERSIONS = ["v1.0", "v1.1", "v1.2", "v1.3"]
if _config.get("https_mode", "disabled") == "enabled":
    NODE_APIVERSIONS.remove("v1.0")
NODE_REGVERSION = _config.get('nodefacade', {}).get('NODE_REGVERSION', 'v1.2')
NODE_APINAMESPACE = "x-nmos"
NODE_APINAME = "node"
HOSTNAME = gethostname().split(".", 1)[0]
RESOURCE_TYPES = ["sources", "flows", "devices", "senders", "receivers"]


class FacadeAPI(WebAPI):
    def __init__(self, registry):
        self.registry = registry
        self.node_id = registry.node_id
        super(FacadeAPI, self).__init__()

    @route('/')
    def root(self):
        return [NODE_APINAMESPACE+"/"]

    @route('/'+NODE_APINAMESPACE+'/')
    def namespaceroot(self):
        return [NODE_APINAME+"/"]

    @route('/'+NODE_APINAMESPACE+'/'+NODE_APINAME+"/")
    def nameroot(self):
        return [api_version + "/" for api_version in NODE_APIVERSIONS]

    @route('/'+NODE_APINAMESPACE+'/'+NODE_APINAME+"/<api_version>/")
    def versionroot(self, api_version):
        if api_version not in NODE_APIVERSIONS:
            abort(404)
        return ["self/", "sources/", "flows/", "devices/", "senders/", "receivers/"]

    @resource_route('/'+NODE_APINAMESPACE+'/'+NODE_APINAME+"/<api_version>/<resource_type>/")
    def resource_list(self, api_version, resource_type):
        if api_version not in NODE_APIVERSIONS:
            abort(404)
        if resource_type == "self":
            return self.registry.list_self(api_version=api_version)
        elif resource_type not in RESOURCE_TYPES:
            abort(404)
        return self.registry.list_resource(resource_type.rstrip("s"), api_version=api_version).values()

    @resource_route('/'+NODE_APINAMESPACE+'/'+NODE_APINAME+"/<api_version>/<resource_type>/<resource_id>/")
    def resource_id(self, api_version, resource_type, resource_id):
        if api_version not in NODE_APIVERSIONS:
            abort(404)
        if resource_type not in RESOURCE_TYPES:
            abort(404)
        resources = self.registry.list_resource(resource_type.rstrip("s"), api_version=api_version)
        if resource_id in resources:
            return resources[resource_id]
        else:
            abort(404)

    @resource_route('/'+NODE_APINAMESPACE+'/'+NODE_APINAME+"/<api_version>/receivers/<receiver_id>/target",
                    methods=['PUT'])
    def receiver_id_subscription(self, api_version, receiver_id):
        if api_version not in NODE_APIVERSIONS:
            abort(404)

        receiver_service = self.registry.find_service("receiver", receiver_id)
        if receiver_service is None:
            abort(404)

        receiver_service_href = self.registry.get_service_href(receiver_service)

        if receiver_service_href is None:
            # Service doesn't specify an href
            return {}
        if str(receiver_service_href).isdigit():
            # Service doesn't exist
            abort(404)
        receiver_subs_href = "receivers/"+receiver_id+"/target"
        href = urljoin(receiver_service_href, receiver_subs_href) + "/"
        # TODO: Handle all request types
        # TODO: Move into proxy class?

        headers = dict(request.headers)
        headers['Accept'] = 'application/json'
        del headers['Host']

        print("Sending {} request to '{}' with headers={} and data='{}'".format(request.method, href,
                                                                                headers, request.data))

        try:
            resp = requests.request(request.method, href, params=request.args, data=request.data, headers=headers,
                                    allow_redirects=True, timeout=30)
        except Exception:
            abort(500)

        if not resp:
            abort(503)

        print(resp)

        if resp.status_code/100 != 2:
            abort(resp.status_code)

        data = {}
        if len(resp.text) > 0:
            data = resp.json()
        else:
            return (204, '')
        if resp.status_code == 200:
            return (data)
        else:
            return (resp.status_code, data)
