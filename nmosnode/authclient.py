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
from werkzeug.exceptions import ServiceUnavailable
try:
    from authlib.integrations.flask_client import OAuth
except ImportError:
    from authlib.flask.client import OAuth

from mdnsbridge.mdnsbridgeclient import IppmDNSBridge
from nmoscommon.logger import Logger
from nmoscommon.auth.auth_middleware import get_auth_server_url, get_auth_server_metadata

CREDENTIALS_PATH = os.path.join('/var/nmos-node', 'facade.json')
MDNS_SERVICE_TYPE = "nmos-auth"

ALLOWED_SCOPE = "registration"  # space-delimited list of scopes as per rfc7591
ALLOWED_GRANTS = ["authorization_code", "refresh_token", "client_credentials"]
ALLOWED_RESPONSE = ["code"]

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


# Globally define auth_href so it can be shared between both classes
auth_href = None


class AuthRegistrar(object):
    """Class responsible for registering an OAuth2 client with the Auth Server"""
    def __init__(self, client_name, redirect_uris=[], client_uri=None,
                 allowed_scope=None, allowed_grants=["authorization_code"],
                 allowed_response=["code"], auth_method="client_secret_basic",
                 credentials_path=CREDENTIALS_PATH):
        self.client_name = client_name
        self.client_uri = client_uri
        self.allowed_scope = allowed_scope
        self.redirect_uris = redirect_uris
        self.allowed_grants = allowed_grants
        self.allowed_response = allowed_response
        self.auth_method = auth_method

        self.client_id = None
        self.client_secret = None
        self.server_metadata = {}  # RFC 8414
        self.client_metadata = {}  # The returned data from client registration
        self.registered = False  # Flag to signify Node is registered with Auth Server
        self.initialised = self.initialise(credentials_path)

    def initialise(self, credentials_path):
        """Check if credentials file already exists, meaning the device is already registered.
        If not, register with Auth Server and write client credentials to file."""
        try:
            self.server_metadata = self._get_metadata(MDNS_SERVICE_TYPE)
            if os.path.isfile(credentials_path):
                data = read_from_file(credentials_path)
                if "client_id" in data and "client_secret" in data:
                    logger.writeWarning("Credentials file already exists. Using existing credentials.")
                    self.client_id, self.client_secret = self.get_credentials_from_file(credentials_path)
                    self.registered = True
                    return True
            logger.writeInfo("Registering with Authorization Server...")
            if self.registered is False:
                self.client_metadata = self.send_dynamic_registration_request()
                self.registered = True
            logger.writeInfo("Registration Successful. Writing OAuth2 credentials to file...")
            # what if registered but fail to write credentials??
            self.write_credentials_to_file(credentials_path)
            return True
        except Exception as e:
            logger.writeError(
                "Unable to initialise OAuth Client with client credentials. {}".format(e)
            )
            return False

    def _get_metadata(self, service_type):
        auth_url = get_auth_server_url(service_type)
        global auth_href
        if auth_url:
            auth_href = auth_url
        elif not auth_url and auth_href:
            logger.writeWarning(
                "Could not find service of type: '{}'. Using previously found metadata endpoint: {}.".format(
                    service_type, auth_href))
        else:
            logger.writeError(
                "No DNS-SD services of type '{}' could be found".format(service_type))
            raise ServiceUnavailable("No DNS-SD services of type '{}' could be found".format(service_type))
        metadata, metadata_url = get_auth_server_metadata(auth_href)
        if metadata is not None:
            return metadata
        else:
            # Construct default URI
            logger.writeError("Could not locate metadata at: {}".format(auth_href))
            raise ServiceUnavailable("Could not locate endpoint at: {}".format(auth_href))

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

    def send_dynamic_registration_request(self, registration_url=None):
        try:
            if not registration_url:
                registration_url = self.server_metadata.get('registration_endpoint')

            logger.writeDebug('Registration endpoint href is: {}'.format(registration_url))

            data = {
                "client_name": self.client_name,
                "client_uri": self.client_uri,
                "scope": self.allowed_scope,
                "redirect_uris": self.redirect_uris,
                "grant_types": self.allowed_grants,
                "response_types": self.allowed_response,
                "token_endpoint_auth_method": self.auth_method
            }

            # Decide how Basic Auth details are retrieved - user input? Retrieved from file?
            reg_resp = requests.post(
                registration_url,
                json=data,
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

    def fetch_local_token(self, credentials_path=CREDENTIALS_PATH):
        try:
            data = read_from_file(credentials_path)
            if "bearer_token" in data:
                bearer_token = data['bearer_token']
                return bearer_token
            else:
                return None
        except (OSError, IOError) as e:
            logger.writeError(
                "Could not fetch Bearer Token from file {}. {}".format(credentials_path, e))

    def update_local_token(self, token, refresh_token=None, access_token=None, credentials_path=CREDENTIALS_PATH):
        try:
            data = read_from_file(credentials_path)
            if "bearer_token" not in data:
                data["bearer_token"] = token
            else:
                if "refresh_token" in token:
                    data["bearer_token"]["refresh_token"] = token["refresh_token"]
                data["bearer_token"]["access_token"] = token["access_token"]
                data["bearer_token"]["expires_at"] = token["expires_at"]
            write_to_file(data)
            logger.writeDebug("Updated local Bearer Token")
        except (OSError, IOError) as e:
            logger.writeError(
                "Could not write Bearer Token to file {}. {}".format(credentials_path, e)
            )

    def register_client(self, client_name, client_uri, credentials_path=CREDENTIALS_PATH, **kwargs):
        self.client_name = client_name
        self.client_uri = client_uri

        node_data = read_from_file(credentials_path)
        client_id = node_data.get('client_id')
        client_secret = node_data.get('client_secret')

        global auth_href
        metadata, metadata_url = get_auth_server_metadata(auth_href)
        server_metadata = metadata if metadata is not None else kwargs

        # Retrieve server metadata from kwargs
        token_url = server_metadata.get('token_endpoint', None)
        authorize_url = server_metadata.get('authorization_endpoint', None)
        jwks_url = server_metadata.get("jwks_uri", None)

        return self.register(
            name=client_name,
            client_id=client_id,
            client_secret=client_secret,
            server_metadata_url=metadata_url,
            access_token_url=token_url,
            refresh_token_url=token_url,
            authorize_url=authorize_url,
            jwks_uri=jwks_url,
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
    client_uri = "https://example.com"

    auth_registrar = AuthRegistrar(
        client_name=client_name,
        client_uri=client_uri,
        allowed_scope="registration",
        redirect_uris=["https://www.app.example.com"],
        allowed_grants=["authorization_code", "client_credentials"],
        allowed_response=["code"],
        auth_method="client_secret_basic"
    )

    if auth_registrar.initialised is True:
        # Register Client
        auth_registry.register_client(
            client_name=client_name,
            client_uri=client_uri,
            credentials_path=CREDENTIALS_PATH,
            **auth_registrar.server_metadata
        )

        # Extract the 'RemoteApp' class created when registering
        auth_client = getattr(auth_registry, client_name)
        # Fetch Token
        token = auth_client.fetch_access_token()
        print("BEARER TOKEN: ", token)
        auth_registry.update_local_token(token)
