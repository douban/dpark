import logging


def spawn_rconsole(env):
    rfoo_logger = logging.getLogger('rfoo')
    rfoo_logger.disabled = 1
    try:
        from rfoo.utils import rconsole
        import rfoo._rfoo
        rfoo._rfoo.logging = rconsole.logging = rfoo_logger
        rconsole.spawn_server(env, 0)
    except ImportError:
        pass
