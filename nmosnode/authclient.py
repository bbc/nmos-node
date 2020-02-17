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
try:
    from authlib.integrations.flask_client import OAuth
except ImportError:
    from authlib.flask.client import OAuth
from gevent import sleep

from mdnsbridge.mdnsbridgeclient import IppmDNSBridge
from nmoscommon.logger import Logger

CREDENTIALS_PATH = os.path.join('/var/nmos-node', 'facade.json')
MDNS_SERVICE_TYPE = "nmos-auth"

# ENDPOINTS
AUTH_APIROOT = 'x-nmos/auth/v1.0/'
SERVER_METADATA_ENDPOINT = '.well-known/oauth-authorization-server/'
DEFAULT_REGISTRATION_ENDPOINT = urljoin(AUTH_APIROOT, 'register')
DEFAULT_AUTHORIZATION_ENDPOINT = urljoin(AUTH_APIROOT, 'authorize')
DEFAULT_TOKEN_ENDPOINT = urljoin(AUTH_APIROOT, 'token')
DEFAULT_REVOCATION_ENDPOINT = urljoin(AUTH_APIROOT, 'revoke')
DEFAULT_JWKS_ENDPOINT = urljoin(AUTH_APIROOT, 'jwks')

logger = Logger("auth_client", None)
mdnsbridge = IppmDNSBridge(logger=logger)


def read_from_file(file_path=CREDENTIALS_PATH):
    try:
        with open(file_path, 'r') as f:
            contents = json.load(f)
        return contents
    except (OSError, IOError) as e:
        logger.writeError("Could not read from file '{}'. {}".format(file_path, e))
        raise


def write_to_file(input_data, file_path=CREDENTIALS_PATH):
    try:
        # Load contents if file exists
        if os.path.exists(file_path):
            with open(file_path) as f:
                data = json.load(f)
            data.update(input_data)
        else:
            data = input_data
        # Write credentials to file
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4, sort_keys=True)
        return True
    except (OSError, IOError) as e:
        logger.writeError(
            "Could not write to file {}. {}".format(file_path, e)
        )
        raise


def get_dns_service(service_type):
    wait_time = 2
    retry_count = 3
    for count in range(retry_count):
        href = mdnsbridge.getHref(service_type)
        if href == "":
            logger.writeWarning(
                "Could not locate {} service type, sleeping for {} seconds".format(MDNS_SERVICE_TYPE, wait_time))
            sleep(wait_time)
        else:
            return href
    logger.writeError("Cannot locate service type {} after {} attempts.".format(MDNS_SERVICE_TYPE, retry_count))


# Globally define auth_href so it can be shared between both classes
auth_href = None


class AuthRegistrar(object):
    """Class responsible for registering an OAuth2 client with the Auth Server"""
    def __init__(self, client_name, redirect_uri, client_uri=None,
                 allowed_scope=None, allowed_grant=["authorization_code"],
                 allowed_response="code", auth_method="client_secret_basic"):
        self.client_name = client_name
        self.client_uri = client_uri
        self.redirect_uri = redirect_uri
        self.allowed_scope = allowed_scope
        self.allowed_grant = "\n".join(allowed_grant)
        self.allowed_response = allowed_response
        self.auth_method = auth_method

        self.client_id = None
        self.client_secret = None
        self.server_metadata = {}  # RFC 8414
        self.client_metadata = {}  # The returned data from client registration
        self.registered = False  # Flag to signify Node is registered with Auth Server
        self.initialised = self.initialise()

    def initialise(self, credentials_path=CREDENTIALS_PATH):
        """Check if credentials file already exists, meaning the device is already registered.
        If not, register with Auth Server and write client credentials to file."""
        try:
            global auth_href
            auth_href = get_dns_service(MDNS_SERVICE_TYPE)
            if not auth_href:
                return False
            self._get_server_metadata(auth_href)
            if os.path.isfile(credentials_path):
                data = read_from_file(credentials_path)
                if "client_id" in data and "client_secret" in data:
                    logger.writeWarning("Credentials file already exists. Using existing credentials.")
                    self.client_id, self.client_secret = self.get_credentials_from_file(credentials_path)
                    self.registered = True
                    return True
            logger.writeInfo("Registering with Authorization Server...")
            if self.registered is False:
                self.client_metadata = self.send_oauth_registration_request()
                self.registered = True
            logger.writeInfo("Writing OAuth2 credentials to file...")
            # what if registered but fail to write credentials??
            self.write_credentials_to_file(credentials_path)
            return True
        except Exception as e:
            logger.writeError(
                "Unable to initialise OAuth Client with client credentials. {}".format(e)
            )
            return False

    def write_credentials_to_file(self, credentials_path):
        credentials = {
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        write_to_file(credentials, credentials_path)

    def get_credentials_from_file(self, credentials_path):
        data = read_from_file(credentials_path)
        try:
            client_id = data["client_id"]
            client_secret = data["client_secret"]
            return client_id, client_secret
        except KeyError as e:
            logger.writeError("OAuth2 credentials not found in file: {}. {}".format(credentials_path, e))
            raise

    def remove_credentials_from_file(self, file_path):
        try:
            # Load contents if file exists
            if os.path.exists(file_path):
                with open(file_path) as f:
                    data = json.load(f)
                data.pop("client_id", None)
                data.pop("client_secret", None)
                # Write remaining data back to file
                with open(file_path, 'w') as f:
                    json.dump(data, f)
                return True
            else:
                logger.writeError(
                    "Client credentials file '{}' does not exist".format(file_path)
                )
                return False
        except (OSError, IOError) as e:
            logger.writeError(
                "Could not remove OAuth client credentials from file {}. {}".format(file_path, e)
            )
            raise

    def _make_default_metadata(self, auth_href):
        default_metadata = {
            "authorization_endpoint": urljoin(auth_href, DEFAULT_AUTHORIZATION_ENDPOINT),
            "token_endpoint": urljoin(auth_href, DEFAULT_TOKEN_ENDPOINT),
            "registration_endpoint": urljoin(auth_href, DEFAULT_REGISTRATION_ENDPOINT),
            "jwks_uri": urljoin(auth_href, DEFAULT_JWKS_ENDPOINT),
            "revocation_endpoint": urljoin(auth_href, DEFAULT_REVOCATION_ENDPOINT),
            "issuer": auth_href
        }
        return default_metadata

    def _get_server_metadata(self, auth_href):
        try:
            url = urljoin(auth_href, SERVER_METADATA_ENDPOINT)
            resp = requests.get(url, timeout=0.5, proxies={'http': ''})
            resp.raise_for_status()  # Raise exception if not a 2XX status code
            metadata = resp.json()
        except Exception as e:
            logger.writeWarning("Unable to retrieve server metadata - falling back to defaults. {}".format(e))
            metadata = self._make_default_metadata(auth_href)
        finally:
            self.server_metadata = metadata

    def send_oauth_registration_request(self):
        try:
            oauth_registration_href = self.server_metadata.get('registration_endpoint')
            logger.writeDebug('Registration endpoint href is: {}'.format(oauth_registration_href))

            data = {
                "client_name": self.client_name,
                "client_uri": self.client_uri,
                "scope": self.allowed_scope,
                "redirect_uris": self.redirect_uri,
                "grant_types": self.allowed_grant,
                "response_types": self.allowed_response,
                "token_endpoint_auth_method": self.auth_method
            }

            # Decide how Basic Auth details are retrieved - user input? Retrieved from file?
            reg_resp = requests.post(
                oauth_registration_href,
                data=data,
                timeout=0.5,
                proxies={'http': ''}
            )
            reg_resp.raise_for_status()  # Raise error if status is an error code
            reg_resp_json = reg_resp.json()

            # Store credentials in member variables in case writing to file fails
            self.client_id = reg_resp_json.get('client_id')
            self.client_secret = reg_resp_json.get('client_secret')
            return reg_resp.json()
        except HTTPError as e:
            logger.writeError("Unable to Register Client with Auth Server. {}".format(e))
            logger.writeDebug(e.response.text)
            raise


class AuthRegistry(OAuth):
    """Subclass of top-level Authlib client.
    Registers Auth Server URIs for requesting access tokens for each registered OAuth2 client.
    Also responsible for storing and fetching bearer token"""
    def __init__(self, app=None, scope=None):
        super(AuthRegistry, self).__init__(app)
        self.client_name = None
        self.client_uri = None
        self.bearer_token = None
        self.client_kwargs = {
            "scope": scope,
            'token_endpoint_auth_method': 'client_secret_basic',
            'code_challenge_method': 'S256'
        }

    def fetch_local_token(self):
        try:
            data = read_from_file(CREDENTIALS_PATH)
            if "bearer_token" in data:
                bearer_token = data['bearer_token']
                return bearer_token
            else:
                return None
        except (OSError, IOError) as e:
            logger.writeError(
                "Could not fetch Bearer Token from file {}. {}".format(CREDENTIALS_PATH, e))

    def update_local_token(self, token, refresh_token=None, access_token=None):
        try:
            data = read_from_file(CREDENTIALS_PATH)
            if "bearer_token" not in data:
                data["bearer_token"] = token
            else:
                if "refresh_token" in token:
                    data["bearer_token"]["refresh_token"] = token["refresh_token"]
                data["bearer_token"]["access_token"] = token["access_token"]
                data["bearer_token"]["expires_at"] = token["expires_at"]
            write_to_file(data)
        except (OSError, IOError) as e:
            logger.writeError(
                "Could not write Bearer Token to file {}. {}".format(CREDENTIALS_PATH, e)
            )

    def register_client(self, client_name, client_uri, credentials_path=CREDENTIALS_PATH, **kwargs):
        self.client_name = client_name
        self.client_uri = client_uri

        data = read_from_file(credentials_path)
        client_id = data.get('client_id')
        client_secret = data.get('client_secret')

        # Retrieve server metadata from kwargs, falling back to defaults if not found
        global auth_href
        token_url = getattr(kwargs, 'token_endpoint', urljoin(auth_href, DEFAULT_TOKEN_ENDPOINT))
        authorize_url = getattr(
            kwargs, 'authorization_endpoint', urljoin(auth_href, DEFAULT_AUTHORIZATION_ENDPOINT))

        return self.register(
            name=client_name,
            client_id=client_id,
            client_secret=client_secret,
            access_token_url=token_url,
            refresh_token_url=token_url,
            authorize_url=authorize_url,
            api_base_url=client_uri,
            client_kwargs=self.client_kwargs,
            fetch_token=self.fetch_local_token,
            update_token=self.update_local_token
        )


if __name__ == "__main__":  # pragma: no cover
    from flask import Flask
    from os import environ

    environ["AUTHLIB_INSECURE_TRANSPORT"] = "1"  # Disable Authlib error about insecure transport

    app = Flask(__name__)
    auth_registry = AuthRegistry(app)

    client_name = "test_oauth_client"
    client_uri = "www.example.com"

    auth_registrar = AuthRegistrar(
        client_name=client_name,
        client_uri=client_uri,
        allowed_scope="registration",
        redirect_uri="www.app.example.com",
        allowed_grant=["password", "authorization_code", "client_credentials"],
        allowed_response="code",
        auth_method="client_secret_basic"
    )
    if auth_registrar.initialised is True:
        # Register Client
        auth_registry.register_client(client_name=client_name, client_uri=client_uri, credentials_path=CREDENTIALS_PATH)
        # Extract the 'RemoteApp' class created when registering
        auth_client = getattr(auth_registry, client_name)
        # Fetch Token
        print(auth_client.fetch_access_token())
