import ptah
import gevent
import logging
import socket
from haigha.message import Message
from haigha.connection import Connection
from pyramid.interfaces import IRequest
from pyramid.decorator import reify
from pyramid.testing import DummyRequest

import smxq
from smxq import settings
from smxq.protocol import ID_PROTOCOL
from smxq.session import SessionProperties

log = logging.getLogger('smxq')


def init_dispatcher(cfg):
    log.info("Initialize smxq backend dispatcher")
    cfg.registry.__smxq_dispatcher__ = Dispatcher(cfg.registry)
    return cfg.registry.__smxq_dispatcher__


def get_connection(registry):
    try:
        return registry.__smxq_dispatcher_conn__
    except:
        pass

    cfg = ptah.get_settings(smxq.CFG_ID_SMXQ, registry)
    registry.__smxq_dispatcher_conn__ = conn = Connection(
        host=cfg['host'], vhost=cfg['vhost'], transport="gevent",
        sock_opts={(socket.IPPROTO_TCP, socket.TCP_NODELAY) : 1})

    def read():
        while 1:
            while conn._connected:
                conn.read_frames()
                gevent.sleep(0)

            gevent.sleep(1)
            conn.read_frames()

    registry.__smxq_dispatcher_process__ = gevent.spawn(read)

    return conn


class Request(DummyRequest):

    def __init__(self, registry, app_url, *args, **kw):
        self.registry = registry
        self.application_url = app_url
        self.request_iface = IRequest
        super(Request, self).__init__(*args, **kw)


class Dispatcher(object):

    def __init__(self, registry):
        self.registry = registry
        self.channels = []
        self.protocols = registry.setdefault(ID_PROTOCOL, {})

    def start(self):
        cfg = ptah.get_settings(smxq.CFG_ID_SMXQ, self.registry)
        self.conn = get_connection(self.registry)
        self.request = Request(self.registry, cfg['url'])

        protocols = [s.strip() for s in cfg['protocols'].split()]
        skip = [s.strip() for s in cfg['skip'].split()]
        if '*' in protocols:
            protocols = self.protocols.keys()

        protocols = [p for p in protocols if p not in skip]

        self.channels.append(self.init_channel(sorted(protocols), cfg))

    def init_channel(self, protocols, cfg):
        ch = self.conn.channel()
        ch.exchange.declare(smxq.EXCHANGE_ID, 'direct')

        for protocol in protocols:
            if protocol not in self.protocols:
                log.error("Protocol not found: %s", protocol)
                continue

            log.info("Bind protocol: %s", protocol)

            P_ID = 'smxq.protocol.%s'%protocol

            ch.queue.declare(P_ID, auto_delete=False)
            ch.queue.bind(P_ID, smxq.EXCHANGE_ID, P_ID)

            def consumer(msg):
                gevent.spawn(self.dispatch, msg)

            ch.basic.consume(P_ID, consumer, no_ack=False)

        ch.basic.qos(prefetch_count = cfg['prefetch-count'])
        return ch

    def stop(self):
        for ch in self.channels:
            ch.stop()

    def dispatch(self, msg):
        tp = msg.properties['type']
        log.debug('Incoming message "%s", client "%s": %s',
                  tp, msg.properties['reply_to'], msg.body[:50])
        if tp.startswith('sys:'):
            ac = meth = msg.body
            proto = tp[4:]
            payload = {}
        else:
            try:
                proto, meth = tp.split('.',1)
            except:
                msg.ack() # replace with nack
                return

            ac = 'dispatch'
            try:
                payload = ptah.json.loads(str(msg.body))
            except:
                msg.ack() # error
                return

        # get protocol
        protocol = self.protocols.get(proto)
        if protocol is None:
            return msg.ack() # replace with nack

        # create context
        context = Context(
            proto, meth, payload, msg.properties['reply_to'], msg, self.request)

        # complete
        if ac == 'dispatch':
            protocol.dispatch(meth, context)
        elif ac == 'open':
            protocol.on_open(context)
        elif ac == 'close':
            protocol.on_close(context)

        msg.ack()


class Context(object):

    def __init__(self, proto, type, payload, reply_to, msg, request):
        self.msg = msg
        self.type = type
        self.proto = proto
        self.payload = payload
        self.props = msg.properties.get('application_headers', {})
        self.request = request
        self.registry = request.registry

        self.reply_to = reply_to
        self.client = reply_to
        self.channel = msg.channel

    @reify
    def properties(self):
        return SessionProperties(self.client, self.request.registry)

    def __call__(self, proto):
        if proto == self.proto:
            return self
        return Context(proto, '', {}, self.reply_to, self.msg, self.request)

    def send(self, type, payload, reply_to=None):
        if reply_to is None:
            reply_to = self.reply_to

        msg = Message(ptah.json.dumps(payload),
                      type='%s.%s'%(self.proto, type),
                      correlation_id=self.msg.properties['correlation_id'])
        self.channel.basic.publish(msg, settings.S_EXCHANGE, reply_to)

        log.debug('Outgoing message "%s.%s", client "%s": %s',
                  self.proto, type, reply_to, msg.body[:50])

    def send_to_protocol(self, type, payload):
        msg = Message(
            ptah.json.dumps(payload), type='%s.%s'%(self.proto, type),
            reply_to=self.reply_to,
            correlation_id=self.msg.properties['correlation_id'])
        self.channel.basic.publish(
            msg, settings.EXCHANGE_ID, settings.ROUTE%self.proto)

        log.debug('Outgoing message to protocol "%s.%s", client "%s": %s',
                  self.proto, type, self.reply_to, msg.body[:50])

    def reply(self, payload):
        msg = Message(ptah.json.dumps(payload),
                      type='%s.%s'%(self.proto, self.type),
                      correlation_id=self.msg.properties['correlation_id'])
        self.channel.basic.publish(msg, settings.S_EXCHANGE, self.reply_to)

        log.debug('Outgoing reply "%s.%s", client "%s": %s',
                  self.proto, self.type, self.reply_to, msg.body[:50])

    def broadcast(self, type, payload):
        route_key = settings.S_PROTO%self.proto

        msg = Message(ptah.json.dumps(payload),
                      type='%s.%s'%(self.proto, type),
                      correlation_id=self.msg.properties['correlation_id'])
        self.channel.basic.publish(msg, settings.S_PROTO_EXCHANGE, route_key)

        log.debug('Outgoing broadcast "%s.%s", key "%s": %s',
                  self.proto, type, route_key, msg.body[:50])
