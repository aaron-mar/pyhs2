import sys
from thrift.TTornado import TTornadoStreamTransport

from thrift.protocol.TBinaryProtocol import TBinaryProtocol, TBinaryProtocolFactory
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
import sasl
from tornado import gen
from cloudera.thrift_sasl import TSaslClientTransport

from TCLIService import TCLIService
from TCLIServiceTornado import TCLIService as TCLIServiceTornado

from cursor import Cursor
from TCLIService.ttypes import TCloseSessionReq, TOpenSessionReq
from TCLIServiceTornado.ttypes import TCloseSessionReq as TCloseSessionReqTornado, \
    TOpenSessionReq as TOpenSessionReqTornado

class BaseConnection(object):
    DEFAULT_KRB_SERVICE = 'hive'
    AUTH_MECHANISMS = {'NOSASL', 'PLAIN', 'KERBEROS', 'LDAP'}
    client = None
    session = None

    def __init__(self, authMechanism):
        if authMechanism not in self.AUTH_MECHANISMS:
            raise NotImplementedError('authMechanism is either not supported or not implemented')

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def _get_krb_settings(self, default_host, config):
        host = default_host
        service = self.DEFAULT_KRB_SERVICE

        if config is not None:
            if 'krb_host' in config:
                host = config['krb_host']

            if 'krb_service' in config:
                service = config['krb_service']

        return host, service

    @staticmethod
    def _check_password(authMechanism, password):
        if authMechanism == 'PLAIN' and (password is None or len(password) == 0):
            password = 'password'
        return password

    def _get_sasl_client(self, host, authMechanism, user, password, configuration):
        sasl_mech = 'PLAIN'
        saslc = sasl.Client()
        saslc.setAttr("username", user)
        saslc.setAttr("password", password)
        if authMechanism == 'KERBEROS':
            krb_host,krb_service = self._get_krb_settings(host, configuration)
            sasl_mech = 'GSSAPI'
            saslc.setAttr("host", krb_host)
            saslc.setAttr("service", krb_service)
        saslc.init()
        return saslc, sasl_mech

    def cursor(self):
        return Cursor(self.client, self.session)

    def close(self):
        req = TCloseSessionReq(sessionHandle=self.session)
        self.client.CloseSession(req)

class Connection(BaseConnection):
    def __init__(self, host=None, port=10000, authMechanism=None, user=None, password=None, database=None, configuration=None):
        super(Connection, self).__init__(authMechanism)
        #Must set a password for thrift, even if it doesn't need one
        #Open issue with python-sasl
        password = self._check_password(authMechanism, password)
        socket = TSocket(host, port)
        if authMechanism == 'NOSASL':
            transport = TBufferedTransport(socket)
        else:
            saslc, sasl_mech = self._get_sasl_client(host, authMechanism, user, password, configuration)
            transport = TSaslClientTransport(saslc, sasl_mech, socket)

        self.client = TCLIService.Client(TBinaryProtocol(transport))
        transport.open()
        res = self.client.OpenSession(TOpenSessionReq(configuration=configuration))
        self.session = res.sessionHandle
        if database is not None:
            with self.cursor() as cur:
                query = "USE {0}".format(database)
                cur.execute(query)

class TornadoConnection(BaseConnection):
    def __init__(self, host=None, port=10000, authMechanism=None, user=None, password=None, database=None, configuration=None):
        super(TornadoConnection, self).__init__(authMechanism)
        #Must set a password for thrift, even if it doesn't need one
        #Open issue with python-sasl
        password = self._check_password(authMechanism, password)
        socket = TSocket(host, port)
        if authMechanism == 'NOSASL':
            transport = TTornadoStreamTransport(socket.host, socket.port)
        else:
            saslc, sasl_mech = self._get_sasl_client(host, authMechanism, user, password, configuration)
            # TODO: I think we have to update this transport so it's non=blocking!!!
            transport = TSaslClientTransport(saslc, sasl_mech, socket)
        pfactory = TBinaryProtocolFactory()
        self.client = TCLIServiceTornado.Client(transport, pfactory)
        yield gen.Task(transport.open)
        req = TOpenSessionReqTornado(configuration=configuration)
        res = yield gen.Task(self.client.OpenSession, req)
        self.session = res.sessionHandle
        if database is not None:
            with self.cursor() as cur:
                query = "USE {0}".format(database)
                cur.execute(query)

    def close(self):
        req = TCloseSessionReqTornado(sessionHandle=self.session)
        yield gen.Task(self.client.CloseSession, req)