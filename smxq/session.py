import ptah
import uuid
import gevent
import socket
from haigha.message import Message
from haigha.connection import Connection
from pyramid.decorator import reify
from pyramid_sockjs import STATE_OPEN, Session

import smxq
from smxq import settings


def get_connection(registry):
    try:
        return registry.__smxq_session__
    except:
        pass

    cfg = ptah.get_settings(smxq.CFG_ID_SMXQ, registry)
    registry.__smxq_session__ = conn = Connection(
        host=cfg['host'], vhost=cfg['vhost'], transport="gevent",
        sock_opts={(socket.IPPROTO_TCP, socket.TCP_NODELAY) : 1})

    def read():
        while 1:
            conn.read_frames()
            gevent.sleep(0)

    registry.__smxq_process__ = gevent.spawn(read)

    return conn


class SmxqSession(Session):

    smxq_channel = None

    def __init__(self, id, *args, **kw):
        self.smxq_id = settings.S_QUEUE+id
        self.smxq_protocols = set()

        super(SmxqSession, self).__init__(id, *args, **kw)

    @reify
    def properties(self):
        return SessionProperties(self.smxq_id, self.registry)

    def tick(self, timeout=None):
        super(SmxqSession, self).tick(timeout)

        if self.state != STATE_OPEN:
            return

        if timeout is None:
            timeout = self.timeout

        self.properties.tick(int(timeout.total_seconds()))

    def setup_protocol_broadcast(self, proto):
        # setup protocol broadcast exchange
        self.smxq_protocols.add(proto)
        self.smxq_channel.exchange.declare(
            settings.S_PROTO_EXCHANGE, 'topic')
        self.smxq_channel.queue.bind(
            self.smxq_id, settings.S_PROTO_EXCHANGE, settings.S_PROTO%proto)

        # send open protocol message
        self.smxq_channel.basic.publish(
            Message('open', type='sys:%s'%proto, reply_to=self.smxq_id,
                    application_headers=self.properties),
            settings.EXCHANGE_ID, settings.ROUTE%proto)

    def message(self, msg):
        proto, tp, payload = msg.split('|', 2)

        if proto not in self.smxq_protocols:
            self.setup_protocol_broadcast(proto)

        # deliver message
        self.smxq_channel.basic.publish(
            Message(payload, type='%s.%s'%(proto, tp),
                    correlation_id=str(uuid.uuid4()), reply_to=self.smxq_id),
            settings.EXCHANGE_ID, settings.ROUTE%proto)

        super(SmxqSession, self).message(msg)

    def open(self):
        conn = get_connection(self.registry)
        self.smxq_channel = conn.channel()

        # setup direct exchange
        self.smxq_channel.exchange.declare(settings.S_EXCHANGE, 'direct')
        self.smxq_channel.queue.declare(self.smxq_id, auto_delete=True)
        self.smxq_channel.queue.bind(
            self.smxq_id, settings.S_EXCHANGE, self.smxq_id)

        # consumer
        def consumer(msg):
            self.queue.put_nowait(ptah.json.loads(str(msg.body)))
            msg.ack()

        self.smxq_channel.basic.consume(self.smxq_id, consumer, no_ack=False)

        super(SmxqSession, self).open()

    def close(self):
        for proto in self.smxq_protocols:
            # send close protocol
            self.smxq_channel.basic.publish(
                Message('close', type='sys:%s'%proto, reply_to=self.smxq_id,
                        application_headers=self.properties),
                settings.EXCHANGE_ID, settings.ROUTE%proto)

        super(SmxqSession, self).close()

    def closed(self):
        self.smxq_channel.close()

        super(SmxqSession, self).closed()


class SessionProperties(object):

    def __init__(self, smxq_id, registry):
        self.smxq_id = smxq_id
        self.redis = smxq.get_redis(registry)

    def tick(self, timeout=10):
        self.redis.expire(self.smxq_id, timeout)

    def get(self, name):
        return self.redis.hget(self.smxq_id, name)

    def set(self, name, val):
        self.redis.hset(self.smxq_id, name, val)

    def __getitem__(self, name):
        return self.redis.hget(self.smxq_id, name)

    def __setitem__(self, name, val):
        self.redis.hset(self.smxq_id, name, val)

    def update(self, **kw):
        self.redis.hmset(self.smxq_id, kw)

    def dict(self):
        return self.redis.hgetall(self.smxq_id)

    def iteritems(self):
        return self.redis.hgetall(self.smxq_id).iteritems()
