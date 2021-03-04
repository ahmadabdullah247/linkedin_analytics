import logging
import sys
from logging.handlers import TimedRotatingFileHandler

# DEBUG     :: Detailed information, typically of interest only when diagnosing problems.
# INFO      :: Confirmation that things are working as expected.
# WARNING   :: An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
# ERROR     :: Due to a more serious problem, the software has not been able to perform some function.
# CRITICAL  :: A serious error, indicating that the program itself may be unable to continue running.
# Configuring logs


FORMATTER = logging.Formatter(fmt="%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s")
LOG_FILE = "src/LIB/DATA/linkedin_scraper.log"

def get_console_handler():
	console_handler = logging.StreamHandler(sys.stdout)
	console_handler.setFormatter(FORMATTER)
	return console_handler

def get_file_handler():
	file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
	file_handler.setFormatter(FORMATTER)
	return file_handler

def get_logger(logger_name):
	"""
    Create and initialize two loggers, one for console and other for log file
        :param logger_name: unique name that will help identify module.
        :return: logger object
    """
	logger = logging.getLogger(logger_name)

	logger.setLevel(logging.DEBUG) # better to have too much log than not enough

	logger.addHandler(get_console_handler())
	logger.addHandler(get_file_handler())

	# with this pattern, it's rarely necessary to propagate the error up to parent
	logger.propagate = False

	return logger