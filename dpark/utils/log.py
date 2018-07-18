import os.path
import sys
import logging
import re
from datetime import datetime

LOG_FORMAT = '{GREEN}%(asctime)-15s{RESET}' \
             ' [%(levelname)s] [%(threadName)s] [%(name)-9s:%(lineno)d] %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

RESET = "\033[0m"
BOLD = "\033[1m"
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = [
    "\033[1;%dm" % i for i in range(30, 38)
]

PALLETE = {
    'RESET': RESET,
    'BOLD': BOLD,
    'BLACK': BLACK,
    'RED': RED,
    'GREEN': GREEN,
    'YELLOW': YELLOW,
    'BLUE': BLUE,
    'MAGENTA': MAGENTA,
    'CYAN': CYAN,
    'WHITE': WHITE,
}

COLORS = {
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': YELLOW,
    'ERROR': RED
}

FORMAT_PATTERN = re.compile('|'.join('{%s}' % k for k in PALLETE))


def formatter_message(message, use_color=True):
    if use_color:
        return FORMAT_PATTERN.sub(
            lambda m: PALLETE[m.group(0)[1:-1]],
            message
        )

    return FORMAT_PATTERN.sub('', message)


class ColoredFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, use_color=True):
        if fmt:
            fmt = formatter_message(fmt, use_color)

        logging.Formatter.__init__(self, fmt=fmt, datefmt=datefmt)
        self.use_color = use_color

    def format(self, record):
        record = logging.makeLogRecord(record.__dict__)
        levelname = record.levelname
        if self.use_color and levelname in COLORS:
            levelname_color = COLORS[levelname] + levelname + RESET
            record.levelname = levelname_color

        record.msg = formatter_message(record.msg, self.use_color)
        return logging.Formatter.format(self, record)


USE_UTF8 = getattr(sys.stderr, 'encoding', None) == 'UTF-8'

ASCII_BAR = ('[ ', ' ]', '#', '-', '-\\|/-\\|')
UNICODE_BAR = (u'[ ', u' ]', u'\u2589', u'-',
               u'-\u258F\u258E\u258D\u258C\u258B\u258A')


def make_progress_bar(ratio, size=14):
    if USE_UTF8:
        L, R, B, E, F = UNICODE_BAR
    else:
        L, R, B, E, F = ASCII_BAR

    if size > 4:
        n = size - 4
        with_border = True
    else:
        n = size
        with_border = False

    p = n * ratio
    blocks = int(p)
    if p > blocks:
        frac = int((p - blocks) * 7)
        blanks = n - blocks - 1
        C = F[frac]
    else:
        blanks = n - blocks
        C = ''

    if with_border:
        return ''.join([L, B * blocks, C, E * blanks, R])
    else:
        return ''.join([B * blocks, C, E * blanks])


def init_dpark_logger(log_level, use_color=None):
    logger = get_logger('dpark')
    logger.propagate = False

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(ColoredFormatter(LOG_FORMAT, DATE_FORMAT, use_color))

    handler.setLevel(max(log_level, logger.level))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


def get_logger(name):
    """ Always use logging.Logger class.

    The user code may change the loggerClass (e.g. pyinotify),
    and will cause exception when format log message.
    """
    old_class = logging.getLoggerClass()
    logging.setLoggerClass(logging.Logger)
    logger = logging.getLogger(name)
    logging.setLoggerClass(old_class)
    return logger


def add_loghub(framework_id):
    logger = get_logger('dpark')
    try:
        import dpark
        from dpark.conf import LOGHUB
        from dpark.utils import getuser
        date_str = datetime.now().strftime("%Y/%m/%d/%H")
        dir_path = os.path.join(LOGHUB, date_str)
        if not os.path.exists(dir_path):
            logger.error("loghub dir not ready: %s", dir_path)
            return

        dpark_mtime = datetime.fromtimestamp(os.stat(dpark.__file__).st_mtime).strftime('%Y-%m-%dT%H:%M:%S')

        infos = [
            ("CMD", ' '.join(sys.argv)),
            ("USER", getuser()),
            ("DPARK", dpark.__file__),
            ("DPARK_MTIME", dpark_mtime),
            ("PYTHONPATH", os.environ.get("PYTHONPATH", ""))
        ]

        log_path = os.path.join(dir_path, framework_id + ".log")
        try:
            with open(log_path, "a") as f:
                for i in infos:
                    f.write("DPARK_{} = {}\n".format(i[0], i[1]))
                f.write("\n")
        except:
            logger.exception("fail to write loghub: %s", log_path)
            return

        file_handler = logging.FileHandler(filename=log_path)
        file_handler.setFormatter(ColoredFormatter(LOG_FORMAT, DATE_FORMAT, True))
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    except:
        logger.exception("add_loghub fail")
