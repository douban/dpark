import logging


def spawn_rconsole(env):
    rfoo_logger = logging.getLogger('rfoo')
    rfoo_logger.disabled = 1
    try:
        import rfoo.utils.rconsole
        import rfoo._rfoo
        rfoo._rfoo.logging = rfoo.utils.rconsole.logging = rfoo_logger

        from rfoo.utils import rconsole
        rconsole.spawn_server(env, 0)
    except ImportError:
        pass
