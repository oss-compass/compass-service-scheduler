import os
import logging
import colorlog

DEBUG_LOG_FORMAT = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"
INFO_LOG_FORMAT = "%(asctime)s %(message)s"
COLOR_LOG_FORMAT_SUFFIX = "\033[1m %(log_color)s "
LOG_COLORS = {'DEBUG': 'white', 'INFO': 'cyan', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'red,bg_white'}

def config_logging(debug, logs_dir, append=True):
    """Config logging level output output"""

    if debug:
        fmt = DEBUG_LOG_FORMAT
        logging_mode = logging.DEBUG
    else:
        fmt = INFO_LOG_FORMAT
        logging_mode = logging.INFO

    # Setting the color scheme and level into the root logger
    logging.basicConfig(level=logging_mode)
    stream = logging.root.handlers[0]
    formatter = colorlog.ColoredFormatter(fmt=COLOR_LOG_FORMAT_SUFFIX + fmt,
                                          log_colors=LOG_COLORS)
    stream.setFormatter(formatter)

    # Creating a file handler and adding it to root
    if logs_dir:
        fh_filepath = os.path.join(logs_dir, 'all.log')
        fh = logging.handlers.RotatingFileHandler(fh_filepath, mode=('a' if append else 'w'), maxBytes=(50 * 1024), backupCount=5)
        fh.setLevel(logging_mode)
        formatter = logging.Formatter(fmt)
        fh.setFormatter(formatter)
        logging.getLogger().handlers.clear()
        logging.getLogger().addHandler(stream)
        logging.getLogger().addHandler(fh)

    # ES logger is set to INFO since, it produces a really verbose output if set to DEBUG
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    # Show if debug mode is activated
    if debug:
        logging.debug("Debug mode activated")
