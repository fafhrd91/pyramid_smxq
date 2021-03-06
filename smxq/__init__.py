import logging

from .rpc import rpc, RpcError
from .settings import CFG_ID_SMXQ, CFG_ID_REDIS
from .settings import QUEUE_ID, EXCHANGE_ID
from .settings import S_PROTO, S_PROTO_EXCHANGE

from .message import SmxqMessage
from .session import SmxqSession
from .session import SessionProperties

from .form import Form
from .protocol import handler, protocol, Protocol


def includeme(cfg):
    cfg.include('ptah')

    # dispatcher
    from . import dispatch
    cfg.add_directive('smxq_init_dispatcher', dispatch.init_dispatcher)

    disp = cfg.registry.settings.get('smxq.dispatcher', 'false')
    if disp == 'true':
        cfg.smxq_init_dispatcher()

    # protocol directive
    from .protocol import protocol
    cfg.add_directive('smxq_protocol', protocol.pyramid)

    # sockjs connection
    from .session import register_smxq
    cfg.add_directive('smxq_init_sockjs', register_smxq)

    cfg.scan()


def get_redis(registry):
    try:
        return registry.__smxq_redis__
    except:
        pass

    import ptah
    import redis

    cfg = ptah.get_settings(CFG_ID_REDIS, registry)

    registry.__smxq_redis__ = redis.Redis(
        host=cfg['host'], port=cfg['port'], db=cfg['db'])
    return registry.__smxq_redis__


class StartDispatcher(object):

    def __init__(self, app, *args, **kw):
        self.app = app

    def __call__(self, environ, start_response):
        try:
            self.app.registry.__smxq_dispatcher__.start()
            logging.getLogger('smxq').info("Starting smxq backend dispatcher")
        except AttributeError:
            pass

        StartDispatcher.__call__ = staticmethod(self.app.__call__)
        return self.app(environ, start_response)
