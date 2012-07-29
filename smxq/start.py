""" smxq-backend command """
import ptah
import gevent
import argparse
from collections import OrderedDict
from pyramid import paster
from pyramid.compat import configparser
from pyramid.config import Configurator, global_registries
from pyramid.threadlocal import manager as threadlocal_manager


def main(init=True):
    import gevent.monkey
    gevent.monkey.patch_all()

    args = BackendCommand.parser.parse_args()

    # logging config
    def fileConfig(f, defaults):
        from logging.config import fileConfig
        return fileConfig(f, defaults, disable_existing_loggers = False)

    paster.setup_logging(args.config.split('#', 1)[0], fileConfig)

    # read settings
    parser = configparser.SafeConfigParser()
    parser.read((args.config,))
    settings = parser.items(configparser.DEFAULTSECT, vars={'here': './'})

    # run configuration
    config = Configurator(settings=settings)
    config.include('ptah')
    config.include('smxq')

    # ptah
    config.ptah_init_sql()
    config.ptah_init_settings()

    config.commit()

    # run command
    cmd = BackendCommand(args, config.registry, config)
    cmd.run()

    ptah.shutdown()


class BackendCommand(object):
    """ 'smxq-backend' command"""

    parser = argparse.ArgumentParser(description="smxq-backend command")
    parser.add_argument('config', metavar='config', help='Config file')

    def __init__(self, args, registry, config):
        self.config = config
        self.options = args
        self.registry = registry
        self.settings = ptah.get_settings('smxq', registry)

    def run(self):
        # start dispatcher
        self.registry.__smxq_dispatcher__.start()

        # thread locals
        threadlocals = {'registry': self.registry,
                        'request': self.registry.__smxq_dispatcher__.request}
        threadlocal_manager.push(threadlocals)

        print 'Smxq dispatcher is initialized... workers are started...'

        try:
            while 1:
                gevent.sleep(1.0)
        except KeyboardInterrupt:
            pass
