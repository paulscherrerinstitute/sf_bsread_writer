import logging
from logging import getLogger

from bsread import dispatcher, SUB, Source
from bsread.handlers import extended

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')

_logger = getLogger(__name__)

n_connections = 10
n_messages_per_connection = 50

_logger.info("Doing %d connection, each with %d messages.", n_connections, n_messages_per_connection)

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

stream = Source(host=source_host, port=source_port, mode=SUB, receive_timeout=1000)
stream.connect()

channels_definitions = {}

for n_connection in range(n_connections):
    _logger.info("Starting connection number %d." % n_connections)

    n_received_messages = 0

    while n_received_messages < n_messages_per_connection:
        message = stream.receive(handler=handler.receive)

        # In case you set a receive timeout, the returned message can be None.
        if message is None:
            _logger.debug("Empty message.")
            continue

        n_received_messages += 1

        pulse_id = message.data["header"]["pulse_id"]

        _logger.debug("Received message with pulse_id '%s'.", pulse_id)

        message_channels = []

        for index, channel in enumerate(message.data["data_header"]["channels"]):
            name = channel["name"]
            dtype = channel.get("type", "float64")
            shape = channel.get("shape", [1])
            encoding = channel.get("encoding", "<")

            message_channels.append(name)

            message_channel_definition = {"type": dtype,
                                          "shape": shape,
                                          "encoding": encoding}

            if name not in channels_definitions:
                _logger.info("Channel '%s' seen for the first time: type=%s, shape=%s, encoding=%s",
                             name, dtype, shape, encoding)

                channels_definitions[name] = message_channel_definition

            # Check if the channel definition has not changed.
            if channels_definitions[name] != message_channel_definition:
                _logger.error("Previous channel '%s' definition %s does not match the current message one %s.",
                              name, channels_definitions[name], message_channel_definition)

                channels_definitions[name] = message_channel_definition
                _logger.warning("Channel '%s' definition updated: type=%s, shape=%s, encoding=%s",
                                name, dtype, shape, encoding)

        # Check if all the requested channels are present in each message.
        if sorted(channels) != sorted(message_channels):
            _logger.error("Requested channels and received channels are not the same.\n"
                          "Requested: %s\n",
                          "Received: %s", sorted(channels), sorted(message_channels))
