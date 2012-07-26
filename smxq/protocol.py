import logging
import ptah
from pyramid.registry import Introspectable
from pyramid.exceptions import ConfigurationError

ID_PROTOCOL = 'smxq:protocol'

log = logging.getLogger('smxq')


class Protocol(object):

    storage = None
    __name__ = '--unset--'
    __handlers__ = {}

    def __init__(self, name, registry):
        self.__name__ = name
        self.registry = registry

    def on_open(self, context):
        pass

    def on_close(self, context):
        pass

    def dispatch(self, tp, context):
        handler = getattr(self, 'msg_%s'%tp, None)
        if handler is not None:
            try:
                handler(context)
            except Exception as e:
                log.exception("Exception in handler '%s'"%tp)
        else:
            handler = self.__handlers__.get(tp)

            if isinstance(handler, str):
                handler = getattr(self, handler, None)
                if handler is not None:
                    try:
                        handler(context)
                    except Exception as e:
                        log.exception("Exception in handler '%s'"%tp)

            elif handler is not None:
                try:
                    res = handler(tp, context, self)
                    if callable(res):
                        res()
                except Exception as e:
                    log.exception("Exception in handler '%s'"%tp)

            else:
                log.warning(
                    "Can't find handler for %s event, protocol %s",
                    tp, self.__name__)


class handler(object):

    def __init__(self, name, context=None):
        if '.' in name:
            raise ConfigurationError("Symbol '.' is not allowed: %s"%name)
        self.name = name
        self.context = context

        # method decorator
        if context is None:
            info = ptah.config.DirectiveInfo()
            handlers = info.locals.get('__handlers__')
            if handlers is None:
                info.locals['__handlers__'] = handlers = {}
            self.handlers = handlers

    def __call__(self, handler):
        if self.context is None:
            self.handlers[self.name] = handler.__name__
            return handler

        if '__handlers__' not in self.context.__dict__:
            self.context.__handlers__ = {}

        self.context.__handlers__[self.name] = handler

        return handler


class protocol(object):

    def __init__(self, name, __depth=1):
        if '.' in name:
            raise ConfigurationError("Symbol '.' is not allowed: %s"%name)
        self.name = name
        self.depth = __depth

        self.info = ptah.config.DirectiveInfo()
        self.discr = (ID_PROTOCOL, name)

        self.intr = Introspectable(ID_PROTOCOL, self.discr, name, ID_PROTOCOL)
        self.intr['name'] = name
        self.intr['codeinfo'] = self.info.codeinfo

    @classmethod
    def pyramid(cls, cfg, name, proto):
        return cls(name, 3)(proto, cfg)

    def _register(self, cfg):
        data = cfg.registry.setdefault(ID_PROTOCOL, {})
        data[self.name] = self.intr['handler'](self.name, cfg.registry)

    def __call__(self, handler, cfg=None):
        intr = self.intr
        intr['handler'] = handler

        self.info.attach(
            ptah.config.Action(
                self._register,
                discriminator=self.discr,
                introspectables=(intr,), order=999999+1),
            cfg, self.depth)
        return handler
