import logging
import os
from datetime import datetime
import ctypes

def logger(logging_level, message):
    """
    Configures and returns a logger that displays and writes logs to the 'logs/' folder.

    Parameters:
    logging_level (str): Level of logging (DEBUG, INFO, ...)
    message (str): Message
    """
    # Create logs folder if it not exists
    if not os.path.exists('logs'):
        os.makedirs('logs')

    if logging_level == "DEBUG":
        log_l = logging.DEBUG
    elif logging_level == "INFO":
        log_l = logging.INFO
    elif logging_level == "WARNING":
        log_l = logging.WARNING
    elif logging_level == "ERROR":
        log_l = logging.ERROR
    elif logging_level == "CRITICAL":
        log_l = logging.CRITICAL
    else:
        log_l = logging.INFO

    # Intialization
    logger = logging.getLogger('analytics_logs')
    logger.setLevel(log_l)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    if not logger.handlers:
        # Handler for print log in terminal
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_l) 
        console_handler.setFormatter(formatter)

        # Handler for print into file
        log_filename = datetime.now().strftime('logs/app_%Y%m%d.log')
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(log_l)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    if log_l == logging.DEBUG:
        logger.debug(message)
    elif log_l == logging.INFO:
        logger.info(message)
    elif log_l == logging.WARNING:
        logger.warning(message)
    elif log_l == logging.ERROR:
        logger.error(message)
    elif log_l == logging.CRITICAL:
        logger.critical(message)


def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False
