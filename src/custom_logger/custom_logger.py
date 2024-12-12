import logging
import sys


class DefaultLogger():
    @staticmethod
    def configure_logger():
        DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.INFO,
                            stream=sys.stdout,
                            format=DEFAULT_FORMAT,
                            force=True)

        logger = logging.getLogger(__name__)

        logger.info("Default logger initialized")


class CustomAdapterCustomSparkSchemaFirstRow(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have the
    'person_name_1st_row' and 'hobby_1st_row' keys, whose value in brackets is prepended to the log message.
    """

    def process(self, msg, kwargs):
        custom_msg = f" [person_name_1st_row: {self.extra['person_name_1st_row']}, hobby_1st_row: {self.extra['hobby_1st_row']}] - {msg}"

        return custom_msg, kwargs


class CustomAdapterCustomSparkSchemaSecondRow(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have the
    'person_name_2nd_row' and 'hobby_2nd_row' keys, whose value in brackets is prepended to the log message.
    """

    def process(self, msg, kwargs):
        custom_msg = f" [person_name_2nd_row: {self.extra['person_name_2nd_row']}, hobby_2nd_row: {self.extra['hobby_2nd_row']}] - {msg}"

        return custom_msg, kwargs
