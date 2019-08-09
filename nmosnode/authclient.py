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

import os
import json
import requests
from requests.exceptions import HTTPError
from six.moves.urllib.parse import urljoin
from authlib.flask.client import OAuth

from mdnsbridge.mdnsbridgeclient import IppmDNSBridge
from nmoscommon.logger import Logger

CREDENTIALS_PATH = os.path.join('/var/nmosauth/', 'oauth_credentials.json')

MDNS_SERVICE_TYPE = "nmos-auth"

API_NAMESPACE = 'x-nmos/auth/v1.0/'
REGISTRATION_ENDPOINT = urljoin(API_NAMESPACE, 'register_client')
AUTHORIZATION_ENDPOINT = urljoin(API_NAMESPACE, 'authorize')
TOKEN_ENDPOINT = urljoin(API_NAMESPACE, 'token')

TOKEN_KEY = 'token'

logger = Logger("auth_client", None)


def get_credentials_from_file(filename=CREDENTIALS_PATH):
    try:
        with open(filename, 'r') as f:
            credentials = json.load(f)
        client_id = credentials['client_id']
        client_secret = credentials['client_secret']
        return client_id, client_secret
    except OSError as e:
        logger.writeError("Could not read OAuth client credentials from file: {}. {}".format(filename, e))
        raise


class AuthRegistrar(object):
    def __init__(self, client_name, redirect_uri, client_uri=None,
                 allowed_scope=None, allowed_grant="authorization_code",
                 allowed_response="code", auth_method="client_secret_basic"):
        self.client_name = client_name
        self.client_uri = client_uri
        self.redirect_uri = redirect_uri
        self.allowed_scope = allowed_scope
        self.allowed_grant = allowed_grant
        self.allowed_response = allowed_response
        self.auth_method = auth_method

        self.client_id = None
        self.client_secret = None
        self.bridge = IppmDNSBridge()
        self._client_registry = {}
        self.initialised = self.initialise()

    def initialise(self):
        """Check if credentials file already exists, meaning the device is already registered.
        If not, register with Auth Server and write client credentials to file."""
        try:
            if os.path.isfile(CREDENTIALS_PATH):
                logger.writeWarning("Credentials file already exists. Using existing credentials.")
                self.client_id, self.client_secret = get_credentials_from_file(CREDENTIALS_PATH)
            else:
                logger.writeInfo("Registering with Authorization Server...")
                reg_resp_json = self.send_oauth_registration_request()
                self.write_credentials_to_file(reg_resp_json, CREDENTIALS_PATH)
            return True
        except Exception as e:
            logger.writeError(
                "Unable to initialise OAuth Client with client credentials. {}".format(e)
            )
            return False

    def write_credentials_to_file(self, data, file_path):
        try:
            self.client_id = data.get('client_id')
            self.client_secret = data.get('client_secret')
            credentials = {
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
            with open(file_path, 'w') as f:
                json.dump(credentials, f)
            os.chmod(file_path, 0o600)
            return True
        except OSError as e:
            logger.writeError(
                "Could not write OAuth client credentials to file {}. {}".format(file_path, e)
            )
            raise

    def send_oauth_registration_request(self):
        try:
            href = self.bridge.getHref(MDNS_SERVICE_TYPE)
            registration_href = urljoin(href, REGISTRATION_ENDPOINT)
            logger.writeDebug('Registration endpoint href is: {}'.format(registration_href))

            data = {
                "client_name": self.client_name,
                "client_uri": self.client_uri,
                "scope": self.allowed_scope,
                "redirect_uri": self.redirect_uri,
                "grant_type": self.allowed_grant,
                "response_type": self.allowed_response,
                "token_endpoint_auth_method": self.auth_method
            }

            # Decide how Basic Auth details are retrieved - user input? Retrieved from file?
            reg_resp = requests.post(
                registration_href,
                data=data,
                auth=('dannym', 'password'),
                timeout=0.5,
                proxies={'http': ''}
            )
            reg_resp.raise_for_status()  # Raise error if status !=201
            self._client_registry[self.client_name] = reg_resp.json()  # Keep a local record of ergistered clients
            return reg_resp.json()
        except HTTPError as e:
            logger.writeError("Unable to Register Client with Auth Server. {}".format(e))
            logger.writeDebug(e.response.text)
            raise


class AuthRegistry(OAuth):

    def __init__(self):
        super(AuthRegistry, self).__init__()
        self.bridge = IppmDNSBridge()
        self.client_name = None
        self.bearer_token = None
        self.auth_url = self.bridge.getHref(MDNS_SERVICE_TYPE)
        self.token_url = urljoin(self.auth_url, TOKEN_ENDPOINT)
        self.refresh_url = urljoin(self.auth_url, TOKEN_ENDPOINT)
        self.authorize_url = urljoin(self.auth_url, AUTHORIZATION_ENDPOINT)
        self.client_kwargs = {
            "scope": "is-04",
            'token_endpoint_auth_method': 'client_secret_basic'
        }

    def fetch_local_token(self):
        return self.bearer_token

    # def update_local_token(self, token):
    #     self.bearer_token = token

    def register_client(self, client_name, client_uri):
        client_id, client_secret = get_credentials_from_file(CREDENTIALS_PATH)
        self.client_name = client_name
        return self.register(
            name=client_name,
            client_id=client_id,
            client_secret=client_secret,
            access_token_url=self.token_url,
            refresh_token_url=self.refresh_url,
            authorize_url=self.authorize_url,
            api_base_url=client_uri,
            client_kwargs=self.client_kwargs,
            fetch_token=self.fetch_local_token,
            # update_token=self.update_local_token
        )


if __name__ == "__main__":  # pragma: no cover

    client_name = "test_oauth_client"
    client_uri = "www.example.com"

    auth_reg = AuthRegistrar(
        client_name=client_name,
        client_uri=client_uri,
        allowed_scope="is-04",
        redirect_uri="www.app.example.com",
        allowed_grant="password\nauthorization_code",  # Authlib only accepts grants seperated with newline chars
        allowed_response="code",
        auth_method="client_secret_basic"
    )
    if auth_reg.initialised is True:
        auth_client = AuthRegistry(name=client_name, uri=client_uri)
        print(auth_client.fetch_token())
