from logging import getLogger

from bsread import dispatcher, source, SUB
from bsread.handlers import extended

_logger = getLogger(__name__)

channels = [
    "S10BC01-DBAM070:BAM_CH1_NORM",
    "S10BC01-DBAM070:BAM_CH2_NORM",
    "S10BC01-DBAM070:EOM1_RESOLUTION1",
    "S10BC01-DBAM070:EOM1_T1",
    "S10BC01-DBAM070:EOM1_T1-VALID",
    "S10BC01-DBAM070:EOM1_T2",
    "S10BC01-DBAM070:EOM1_T2-VALID",
    "S10BC01-DBAM070:EOM2_T1",
    "S10BC01-DBAM070:EOM2_T1-VALID",
    "S10BC01-DBAM070:EOM2_T2",
    "S10BC01-DBAM070:EOM2_T2-VALID"
]

_logger.info("Requesting channels: %s", channels)

stream_address = dispatcher.request_stream(channels)
source_host, source_port = stream_address.rsplit(":", maxsplit=1)

source_host = source_host.split("//")[1]
source_port = int(source_port)

_logger.info("Input stream host '%s' and port '%s'.", source_host, source_port)

handler = extended.Handler()

with source(host=source_host, port=source_port, mode=SUB, receive_timeout=1000) as stream:
    while True:
        message = stream.receive(handler=handler.receive)

        # In case you set a receive timeout, the returned message can be None.
        if message is None:
            continue

        _logger.debug("Received message with pulse_id '%s'.", message.data.pulse_id)

        break
