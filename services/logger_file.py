import coloredlogs
import logging
from logging.handlers import RotatingFileHandler



def logging_handler(log_file_path):
    logger = logging.getLogger(__name__)
    coloredlogs.install(
        level='DEBUG',
        logger=logger,
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        milliseconds=True,  # Include milliseconds in asctime
        level_styles={
            'debug': {'color': 'blue'},
            'info': {'color': 'white', 'intensity': 'bold'},
            'warning': {'color': 'green'},
            'error': {'color': 'red'},
            'critical': {'color': 'red', 'bold': True}
        },
        field_styles={
            'asctime': {'color': 'cyan'},
            'hostname': {'color': 'cyan'},
            'levelname': {'color': 'blue', 'bold': True}
        }
    )

    file_handler = RotatingFileHandler(log_file_path, maxBytes=5*1024*1024, backupCount=3)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    return logger