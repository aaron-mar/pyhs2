import logging
import unittest
from tornado import ioloop, gen
from pyhs2.connections import TornadoConnection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HIVE_SERVER_SETTINGS = dict(
    host='agile01.lab.phemi.com',
    port=10000,
    authMechanism="PLAIN",
    user="hduser",
    password="hduser"
)

DATABASE = "default"


@gen.engine
def _connect(callback):
    conn = TornadoConnection(**HIVE_SERVER_SETTINGS)
    yield gen.Task(conn.connect, database=DATABASE, configuration=None)
    rows = yield gen.Task(conn.cursor().execute, "select * from csbc_patient")
    print rows
    callback()


class TestTornadoCode(unittest.TestCase):
    def setUp(self):
        self.io_loop = ioloop.IOLoop.instance()

    def test_connection(self):
        def func():
            _connect(callback=self.io_loop.stop)
        self.io_loop.add_callback(func)
        self.io_loop.start()