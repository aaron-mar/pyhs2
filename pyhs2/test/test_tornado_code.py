import logging
import unittest
import datetime
from tornado import ioloop, gen
from pyhs2.connections import TornadoConnection, Connection

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
LONG_QUERY = "select age, gender from csbc_patient"
SHORT_QUERY = "select * from csbc_patient"


@gen.engine
def long_task(callback):
    print "starting long task"
    conn = TornadoConnection(**HIVE_SERVER_SETTINGS)
    yield gen.Task(conn.connect, database=DATABASE, configuration=None)
    cur = conn.cursor()
    yield gen.Task(cur.execute, LONG_QUERY)
    rows = yield gen.Task(cur.fetch)
    for row in rows:
        print row
    yield gen.Task(cur.close)
    yield gen.Task(conn.close)
    print "finished long task"
    callback()

@gen.engine
def short_task(callback):
    print "starting short task"
    conn = TornadoConnection(**HIVE_SERVER_SETTINGS)
    yield gen.Task(conn.connect, database=DATABASE, configuration=None)
    cur = conn.cursor()
    yield gen.Task(cur.execute, SHORT_QUERY)
    rows = yield gen.Task(cur.fetch)
    for row in rows:
        print row
    yield gen.Task(cur.close)
    yield gen.Task(conn.close)
    print "finished short task"
    if callback:
        callback()


@gen.engine
def blocking_task(callback):
    settings = HIVE_SERVER_SETTINGS
    settings["database"] = "default"
    with Connection(**settings) as conn:
        with conn.cursor() as cur:
            cur.execute(LONG_QUERY)
            for row in cur.fetch():
                print row
    callback()


def heartbeat():
    print "Not-blocking, yeah! You should see this message every second or so!"


class TestTornadoCode(unittest.TestCase):
    """
    Not really a test case at this point...I'm working on it, just needed to verify everything works asynchronously!
    """
    def setUp(self):
        self.io_loop = ioloop.IOLoop.instance()

    def test_connection(self):
        def func():
            long_task(callback=self.io_loop.stop)
            #blocking_task(callback=self.io_loop.stop)
        # start with the long task
        self.io_loop.add_callback(func)
        def func_short():
            short_task(callback=None)
        # throw a short task into the mix part way through
        self.io_loop.add_timeout(datetime.timedelta(seconds=3), func_short)
        # create a periodic callback to print something every second to confirm that the query isn't blocking
        p = ioloop.PeriodicCallback(heartbeat, 1000)
        p.start()
        self.io_loop.start()