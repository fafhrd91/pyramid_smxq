import ptah
import uuid
from haigha.message import Message
from gevent.queue import Queue, Empty

from . import settings
from .session import get_connection


def rpc(protocol, registry):
    try:
        data = registry.__smxq_rpc__
    except:
        data = registry.__smxq_rpc__ = {}

    if protocol not in data:
        data[protocol] = Rpc(protocol, registry)

    return data[protocol]


class RpcError(Exception):
    pass


class Rpc(object):

    def __init__(self, protocol, registry):
        self.smxq_id = settings.RPC_QUEUE%uuid.uuid4()
        self._calls = {}
        self._methods = {}
        self._protocol = protocol
        self._registry = registry

        # configure queue
        conn = get_connection(registry)
        self.channel = conn.channel()
        self.channel.exchange.declare(settings.S_EXCHANGE, 'direct')
        self.channel.queue.declare(self.smxq_id, auto_delete=True)
        self.channel.queue.bind(self.smxq_id, settings.S_EXCHANGE, self.smxq_id)

        # consumer
        def consumer(msg):
            c_id = msg.properties['correlation_id']
            if c_id in self._calls:
                q = self._calls.pop(c_id)
                q.put_nowait(msg)

        self.channel.basic.consume(self.smxq_id, consumer, no_ack=True)

    def __del__(self):
        self.channel.close()

    def __getattr__(self, name):
        if name not in self._methods:
            self._methods[name] = Method(self, name)

        return self._methods[name]


class Method(object):

    def __init__(self, rpc, method):
        self.rpc = rpc
        self.proto = rpc._protocol
        self.method = method

    def __call__(self, timeout=5.0, **kw):
        id = str(uuid.uuid4())

        self.rpc.channel.basic.publish(
            Message(ptah.json.dumps(kw),
                    type='%s.%s'%(self.proto, self.method),
                    correlation_id=id, reply_to=self.rpc.smxq_id),
            settings.EXCHANGE_ID, settings.ROUTE%self.proto)

        if timeout:
            queue = Queue()

            self.rpc._calls[id] = queue
            try:
                msg = queue.get(block=True, timeout=timeout)
                c_id = msg.properties['correlation_id']
                if c_id == id:
                    return ptah.json.loads(str(msg.body))
            except Empty:
                pass

            raise RpcError()
