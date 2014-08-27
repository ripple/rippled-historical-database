__author__ = 'mtravis'

import urllib.parse
import urllib.request
import websocket
import enum
import sys
import time
import json
import ssl
import http.client
import socket

class RippleError(Exception):
    def __init__(self, value, error=str()):
        self.value = value
        self.error = error

    def __str__(self):
        return repr(self.value)

class Ripple:
    class ConnectionType(enum.Enum):
        none = 0
        rpc = 1
        websocket = 2

    _connectionType = ConnectionType.none
    _connectionString = str()
    _timeout = 0
    _no_ssl_verify = False
    _parsedUrl = None
    _ws = websocket.WebSocket
    _rpc = http.client
    _timeout = None
    _isConnected = False
    # log keys: connectionString, activity, remoteIP, connectTime,
    # disconnectTime, activityStartTime, activityFinishTime, exception
    _log = dict()

    def __init__(self, connectionString, timeout=None, no_ssl_verify=False):
        self._connectionString = connectionString
        self._timeout = timeout
        self._no_ssl_verify = no_ssl_verify
        self._log = dict()
        self._log["connectionString"] = connectionString

        self._parsedUrl = urllib.parse.urlparse(self._connectionString)

        if self._parsedUrl.scheme == 'http' or self._parsedUrl.scheme ==\
                'https':
            self._connectionType = Ripple.ConnectionType.rpc
            socket.setdefaulttimeout(self._timeout)
        elif self._parsedUrl.scheme == 'ws' or self._parsedUrl.scheme == 'wss':
            self._connectionType = Ripple.ConnectionType.websocket
        else:
            raise Exception("Bad URL Scheme", self._parsedUrl.scheme)

    def connect(self):
        if self._isConnected:
            self.disconnect()

        # http.client does not actually connect the socket until a request is
        # sent
        if self._connectionType == Ripple.ConnectionType.rpc:
            if self._parsedUrl.scheme == "https":
                context = ssl.create_default_context()
                self._rpc = http.client.HTTPSConnection(self._parsedUrl.netloc,
                                                        timeout=self._timeout,
                                                        context=context)
            elif self._parsedUrl.scheme == "http":
                self._rpc = http.client.HTTPConnection(self._parsedUrl.netloc,
                                                       timeout=self._timeout)

            self._isConnected = True

        return self._isConnected

    def getRemoteIP(self):
        return self._log["remoteIP"]

    def getIsConnected(self):
        return self._isConnected

    def disconnect(self):
        if self._connectionType == Ripple.ConnectionType.rpc:
            try:
                self._rpc.close()
            except:
                pass

        self._isConnected = False
        self._log["activity"] = "disconnect"
        self._log["disconnectTime"] = time.time()

    def command(self, command, activity=None, params=None, id=None):
        input = { "method" : str(command) }
        if id is not None:
            input["id"] = id
        if params is not None:
            input["params"] = params

        if self._connectionType == Ripple.ConnectionType.rpc:
            try:
                self._log["connectTime"] = time.time()
                reply = urllib.request.urlopen(self._connectionString,
                                               json.dumps(input).encode(),
                                               timeout=self._timeout)
                output = json.loads(reply.read().decode())
                if "exception" in self._log:
                    del self._log["exception"]
            except:
                self._log["exception"] = sys.exc_info()
                output = None
                raise

            self._log["disconnectTime"] = time.time()
            self._log["activity"] = activity

        if "result" not in output:
            raise RippleError(output)
        if "error" in output["result"]:
            raise RippleError(output, output["result"]["error"])

        return output

    def cmd_server_info(self):
        return self.command("server_info", activity="server_info")

    def cmd_ledger(self, ledger, full=False, accounts=False, transactions=False,
                   expand=False):
        params = list()
        params.append({"full" : full, "accounts" : accounts,
                       "transactions" : transactions, "expand" : expand})
        params[0]["ledger"] = ledger

        return self.command("ledger", activity="ledger," + str(ledger),
                            params=params)
