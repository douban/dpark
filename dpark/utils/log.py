import os.path
import sys
import logging
from datetime import datetime
from dpark.util import get_logger, getuser, ColoredFormatter


def add_loghub(framework_id):
    logger = get_logger('dpark')
    try:
        import dpark
        from dpark.conf import LOGHUB
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
                    f.write("DPARK_{} = {}\n".format(i[0],i[1]))
                f.write("\n")
        except Exception as e:
            logger.exception("fail to write loghub: %s", log_path)
            return

        log_format = '{GREEN}%(asctime)-15s{RESET}' \
                    ' [%(levelname)s] [%(threadName)s] [%(name)-9s:%(lineno)d] %(message)s'
        datefmt = '%Y-%m-%d %H:%M:%S'

        file_handler = logging.FileHandler(filename=log_path)
        file_handler.setFormatter(ColoredFormatter(log_format, datefmt, True))
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.exception("add_loghub fail")

