import argparse
import logging
from collections import deque
from threading import Thread

from bsread import source, dispatcher
from time import sleep
import json

from bsread.sender import sender, PUSH, SUB

_logger = logging.getLogger(__name__)


def buffer_bsread_messages(stream_address, buffer, receive_timeout=1000, mode=SUB):
    _logger.info("Connecting to stream '%s'.", stream_address)

    with source(host=stream_address, mode=mode, receive_timeout=receive_timeout) as stream:

        while True:
            message = stream.receive()

            # In case you set a receive timeout, the returned message can be None.
            if message is None:
                continue

            buffer.append(message)
            _logger.debug('Message with pulse_id %d added to the buffer.', message.data.pulse_id)


def send_bsread_message(output_port, buffer, mode=PUSH, buffer_timeout=10):
    with sender(port=output_port, mode=mode, queue_size=1) as output_stream:

        while True:

            if len(buffer) == 0:
                sleep(buffer_timeout)
                continue

            message = buffer.popleft()

            _logger.debug("Sending message with pulse_id '%s'.", message.data.pulse_id)

            output_stream.send(timestamp=(message.data.global_timestamp, message.data.global_timestamp_offset),
                               pulse_id=message.data.pulse_id,
                               data=message.data.data)

            _logger.debug("Message with pulse_id '%s' forwarded.", message.data.pulse_id)


def start_server(channels, output_port, ring_buffer_length):
    _logger.info("Requesting stream with channels: %s", channels)

    stream_address = dispatcher.request_stream(channels)

    _logger.debug("Received stream address '%s'", stream_address)

    buffer = deque(maxlen=ring_buffer_length)

    buffer_thread = Thread(target=buffer_bsread_messages, args=(stream_address, buffer))
    send_thread = Thread(target=send_bsread_message, args=(output_port, buffer))

    buffer_thread.start()
    send_thread.start()

    _logger.info("Started listening to the stream.")

    # We wait indefinitely.
    buffer_thread.join()
    send_thread.join()


def run():
    parser = argparse.ArgumentParser(description='bsread buffer')

    parser.add_argument("-c", "--channels_file", help="JSON file with channels to buffer.")

    parser.add_argument('-o', '--output_port', default=8082,
                        help="Port to bind the output stream to.")
    parser.add_argument("-b", "--buffer_length", default=100,
                        help="Length of the ring buffer.")

    parser.add_argument("--log_level", default="INFO",
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        help="Log level to use.")

    arguments = parser.parse_args()

    # Setup the logging level.
    logging.basicConfig(level=arguments.log_level, format='[%(levelname)s] %(message)s')

    data = json.load(open(arguments.channels_file))
    channels = data["channels"]

    start_server(channels=channels, output_port=arguments.output_port, ring_buffer_length=arguments.buffer_length)


if __name__ == "__main__":
    run()
