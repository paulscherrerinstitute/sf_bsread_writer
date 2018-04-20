import argparse
import logging
from collections import deque
from threading import Thread, Event
from time import sleep

from bsread import source, dispatcher
from bsread.sender import sender, PUSH, SUB

from sf_bsread_writer.buffer_analyzer import analyze_message

_logger = logging.getLogger(__name__)


def buffer_bsread_messages(stream_address, buffer, running_event, use_analyzer=False, receive_timeout=1000, mode=SUB):

    _logger.info("Input stream connecting to '%s'.", stream_address)

    try:

        source_host, source_port = stream_address.rsplit(":", maxsplit=1)

        source_host = source_host.split("//")[1]
        source_port = int(source_port)

        _logger.info("Input stream host '%s' and port '%s'.", source_host, source_port)

        with source(host=source_host, port=source_port, mode=mode, receive_timeout=receive_timeout) as stream:

            while running_event.is_set():
                message = stream.receive()

                # In case you set a receive timeout, the returned message can be None.
                if message is None:
                    continue

                if use_analyzer:
                    analyze_message(message)

                buffer.append(message)
                _logger.debug('Message with pulse_id %d added to the buffer.', message.data.pulse_id)

    except Exception as e:
        running_event.clear()
        _logger.error("Exception happened in buffer thread. Stopping buffer.", e)


def send_bsread_message(output_port, buffer, running_event, mode=PUSH, buffer_timeout=0.01):

    _logger.info("Output stream binding to port '%s'.", output_port)

    try:

        with sender(port=output_port, mode=mode, queue_size=1) as output_stream:

            while running_event.is_set():

                if len(buffer) == 0:
                    sleep(buffer_timeout)
                    continue

                message = buffer.popleft()

                data = {}
                for value_name, bsread_value in message.data.data.items():
                    data[value_name] = bsread_value.value

                _logger.debug("Sending message with pulse_id '%s'.", message.data.pulse_id)

                output_stream.send(timestamp=(message.data.global_timestamp, message.data.global_timestamp_offset),
                                   pulse_id=message.data.pulse_id,
                                   data=data,
                                   check_data=True)

                _logger.debug("Message with pulse_id '%s' forwarded.", message.data.pulse_id)

    except Exception as e:
        running_event.clear()
        _logger.error("Exception happened in sending thread. Stopping buffer.", e)


def start_server(channels, output_port, ring_buffer_length, use_analyzer=False):
    _logger.info("Requesting stream with channels: %s", channels)

    stream_address = dispatcher.request_stream(channels)

    _logger.debug("Received stream address '%s'", stream_address)

    buffer = deque(maxlen=ring_buffer_length)

    running_event = Event()
    running_event.set()

    buffer_thread = Thread(target=buffer_bsread_messages, args=(stream_address, buffer, running_event, use_analyzer))
    send_thread = Thread(target=send_bsread_message, args=(output_port, buffer, running_event))

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

    parser.add_argument("--analyzer", action="store_true", help="Analyze the incoming stream for anomalies.")

    arguments = parser.parse_args()

    # Setup the logging level.
    logging.basicConfig(level=arguments.log_level, format='[%(levelname)s] %(message)s')

    _logger.info("Loading channels list file '%s'.", arguments.channels_file)

    with open(arguments.channels_file) as input_file:
        file_lines = input_file.readlines()
        channels = [channel.strip() for channel in file_lines if not channel.strip().startswith("#") and channel.strip()]

    start_server(channels=channels, output_port=arguments.output_port, ring_buffer_length=arguments.buffer_length,
                 use_analyzer=arguments.analyzer)


if __name__ == "__main__":
    run()
