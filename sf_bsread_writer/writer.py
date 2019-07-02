import argparse
import logging
from threading import Event, Thread
from time import time

import bottle
import os

from bsread import PULL, source
from bsread.handlers import extended

from sf_bsread_writer.writer_format import BsreadH5Writer
from sf_bsread_writer.writer_rest import register_rest_interface

_logger = logging.getLogger(__name__)


class BsreadWriterManager(object):
    REQUIRED_PARAMETERS = ["general/created", "general/user", "general/process", "general/instrument"]

    def __init__(self, stream_address, output_file, receive_timeout=1000, mode=PULL):

        self.stream_address = stream_address
        self.output_file = output_file
        self.receive_timeout = receive_timeout
        self.mode = mode
        self.parameters = {}

        _logger.info("Starting writer manager with stream_address %s, output_file %s.",
                     self.stream_address, self.output_file)

        self._running_event = Event()
        self._running_event.clear()

        self._writing_thread = None

        self.start_pulse_id = None
        self.start_timestamp = None

        self.stop_pulse_id = None
        self.stop_timestamp = None

        self.last_pulse_id = -1
        self.last_timestamp = None

    def _is_last_message_too_early(self):
        if self.start_pulse_id is not None and self.last_pulse_id < self.start_pulse_id:
            return True

        elif self.stop_timestamp is not None and self.last_timestamp < self.start_timestamp:
            return True

        return False

    def _is_last_message_too_late(self):
        if self.stop_pulse_id is not None and self.last_pulse_id > self.stop_pulse_id:
            return True

        elif self.stop_timestamp is not None and self.last_timestamp > self.stop_timestamp:
            return True

        return False

    def _stop_writing(self, writer):
        writer.prune_and_close(self.stop_pulse_id)

        _logger.info("Stopping bsread writer at pulse_id: %s" % self.stop_pulse_id)
        self._running_event.clear()

    def write_stream(self, start_pulse_id, start_timestamp):

        source_host, source_port = self.stream_address.rsplit(":", maxsplit=1)

        source_host = source_host.split("//")[1]
        source_port = int(source_port)

        _logger.info("Input stream host '%s' and port '%s'.", source_host, source_port)

        if start_pulse_id is not None:
            _logger.info("First pulse_id to write: %d.", start_pulse_id)

        if start_timestamp is not None:
            _logger.info("First message to write after timestamp %s.", start_timestamp)

        writer = BsreadH5Writer(self.output_file, self.parameters)
        handler = extended.Handler()

        with source(host=source_host, port=source_port,
                    mode=self.mode, receive_timeout=self.receive_timeout,
                    queue_size=1) as stream:

            self._running_event.set()

            while self._running_event.is_set():

                message = stream.receive(handler=handler.receive)

                # In case you set a receive timeout, the returned message can be None.
                if message is None:

                    # In case the stop_pulse_id was set after the camera stream has ended.
                    if self._is_last_message_too_late():
                        self._stop_writing(writer)

                    continue

                self.last_pulse_id = message.data["header"]["pulse_id"]

                self.last_timestamp = message.data["header"]["global_timestamp"]["sec"]
                self.last_timestamp += 1e-9 * message.data["header"]["global_timestamp"]["ns"]

                _logger.debug('Received message with pulse_id %d and timestamp %s.',
                              self.last_pulse_id, self.last_timestamp)

                if self._is_last_message_too_early():
                    _logger.debug("Discarding early messages with pulse_id=%s (start_pulse_id=%s) "
                                  "and timestamp=%s (start_timestamp=%s)",
                                  self.last_pulse_id, self.start_pulse_id,
                                  self.last_timestamp, self.start_timestamp)
                    continue

                if self._is_last_message_too_late():
                    self._stop_writing(writer)
                    continue

                writer.write_message(message)

        if self.start_pulse_id is not None:
            _logger.info("Writing completed. Pulse_id range from %s to %s written to file.",
                         self.start_pulse_id, self.stop_pulse_id)

        elif self.start_timestamp is not None:
            _logger.info("Writing completed. Timestamp range from %s to %s written to file.",
                         self.start_timestamp, self.stop_timestamp)

        else:
            _logger.warning("This should not be possible, but the file is probably still written fine.")

        os._exit(0)

    def set_parameters(self, parameters):

        _logger.debug("Setting parameters %s." % parameters)

        if not all(x in parameters for x in self.REQUIRED_PARAMETERS):
            raise ValueError("Missing mandatory parameters. Mandatory parameters '%s' but received '%s'." %
                             (self.REQUIRED_PARAMETERS, list(parameters.keys())))

        self.parameters = parameters

    def get_parameters(self):
        return self.parameters

    def get_status(self):
        if self._writing_thread is None:
            return "waiting"

        if self._writing_thread is not None and self._writing_thread.is_alive():
            return "writing"

        return "error"

    def stop(self):
        _logger.info("Stopping bsread writer.")

        self._running_event.clear()

        if self._writing_thread is not None:
            self._writing_thread.join()
            self._writing_thread = None

        os._exit(0)

    def start_writer(self, pulse_id):

        _logger.info("Starting to write with pulse_id %s." % pulse_id)

        if pulse_id is None:
            current_timestamp = time()

            self.start_pulse_id = None
            self.start_timestamp = current_timestamp

        else:
            self.start_pulse_id = pulse_id
            self.start_timestamp = None

        self._writing_thread = Thread(target=self.write_stream, args=(self.start_pulse_id, self.start_timestamp))

        self._running_event.clear()

        self._writing_thread.start()

        if not self._running_event.wait(2):
            _logger.error("Bsread writer did not start in time. Killing.")
            os._exit(-1)

    def stop_writer(self, pulse_id):
        _logger.info("Set stop_pulse_id=%s", pulse_id)

        if pulse_id is None:
            current_timestamp = time()

            self.stop_pulse_id = None
            self.stop_timestamp = current_timestamp

        else:
            self.stop_pulse_id = pulse_id
            self.stop_timestamp = None

    def get_statistics(self):
        return {"start_pulse_id": self.start_pulse_id,
                "stop_pulse_id": self.stop_pulse_id,
                "start_timestamp": self.start_timestamp,
                "stop_timestamp": self.stop_timestamp,
                "last_pulse_id": self.last_pulse_id,
                "last_timestamp": self.last_timestamp}


def start_server(stream_address, output_file, user_id, rest_port):
    app = bottle.Bottle()

    manager = BsreadWriterManager(stream_address, output_file)

    register_rest_interface(app, manager)

    if user_id != -1:
        _logger.info("Setting bsread writer uid and gid to %s.", user_id)
        os.setgid(user_id)
        os.setuid(user_id)

    else:
        _logger.info("Not changing process uid and gid.")

    filename_folder = os.path.dirname(output_file)

    _logger.info("Writing output file to folder '%s'." % filename_folder)

    # Create a folder if it does not exist.
    if filename_folder and not os.path.exists(filename_folder):
        _logger.info("Creating folder '%s'.", filename_folder)
        os.makedirs(filename_folder, exist_ok=True)
    else:
        _logger.info("Folder '%s' already exists.", filename_folder)

    try:
        _logger.info("Starting rest API on port %s." % rest_port)
        bottle.run(app=app, host="127.0.0.1", port=rest_port)
    finally:
        pass


def run():
    parser = argparse.ArgumentParser(description='bsread writer')

    parser.add_argument("stream_address", help="Address of the stream to connect to.")
    parser.add_argument("output_file", help="File where to write the bsread stream.")
    parser.add_argument("user_id", type=int, help="user_id under which to run the writer process."
                                                  "Use -1 for current user.")
    parser.add_argument("rest_port", type=int, help="Port for REST api.")

    parser.add_argument("--log_level", default="INFO",
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        help="Log level to use.")

    arguments = parser.parse_args()

    # Setup the logging level.
    logging.basicConfig(level=arguments.log_level, format='[%(levelname)s] %(message)s')

    start_server(stream_address=arguments.stream_address,
                 output_file=arguments.output_file,
                 user_id=arguments.user_id,
                 rest_port=arguments.rest_port)


if __name__ == "__main__":
    run()
