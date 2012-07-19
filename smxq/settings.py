import ptah

CFG_ID_SMXQ = 'smxq'
CFG_ID_REDIS = 'redis'

ROUTE = 'smxq.protocol.%s'
QUEUE_ID = 'smxq-protocol'
EXCHANGE_ID = 'smxq-protocol-exch'

S_QUEUE = 'smxq-session.'
S_EXCHANGE = 'smxq-session-expr'

S_PROTO = 'smxq.protocol.broadcast.%s'
S_PROTO_EXCHANGE = 'smxq-session-proto-expr'

RPC_QUEUE = 'smxq-rpc.%s'


ptah.register_settings(
    CFG_ID_SMXQ,

    ptah.form.TextField(
        'host',
        title = 'AMPQ Broker host',
        default = 'localhost'),

    ptah.form.TextField(
        'vhost',
        title = 'vhost',
        default = '/'),

    ptah.form.BoolField(
        'dispatcher',
        title = 'Dispatcher',
        default = False),

    ptah.form.IntegerField(
        'workers',
        title = 'Workers',
        default = 1),

    ptah.form.TextField(
        'protocols',
        title = 'Protocols',
        default = '*'),

    ptah.form.TextField(
        'url',
        title = 'Application url',
        default = 'http://localhost'),

    )


ptah.register_settings(
    CFG_ID_REDIS,

    ptah.form.TextField(
        'host',
        title = 'Redis server host',
        default = 'localhost'),

    ptah.form.IntegerField(
        'port',
        title = 'Redis server port',
        default = 6379),

    ptah.form.IntegerField(
        'db',
        title = 'Redis db',
        default = 0),
    )
