import logging

_logger = logging.getLogger(__name__)


def analyze_message(message):
    pulse_id = message.data.pulse_id

    for name, value in message.data.data.items():
        if value.value is None:
            _logger.debug("Channel '%s' data missing in pulse_id %d.", name, pulse_id)
