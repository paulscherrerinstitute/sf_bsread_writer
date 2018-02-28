import logging
from collections import deque
from threading import Thread, Event

from bsread import source
from bsread.handlers.compact import Handler as CompactHandler
from time import sleep
from mflow import mflow, PUSH, SUB

_logger = logging.getLogger(__name__)


def buffer_bsread_messages(stream_address, buffer, receive_timeout=1000, mode=SUB):
    _logger.info("Connecting to stream '%s'.", stream_address)

    with source(host=stream_address, mode=mode, receive_timeout=receive_timeout) as stream:
        while True:

            message_data = receiver.receive(handler=handler.receive)

            # In case you set a receive timeout, the returned message can be None.
            if message_data is None:
                continue

            buffer.append(message_data)
            _logger.debug('Message with pulse_id %d added to the buffer.', message_data.data.pulse_id)


def send_bsread_message(bind_address, buffer, send_timeout=100, mode=PUSH):

    while True:

        if len(buffer) == 0:
            sleep(send_timeout)
            continue

        message_data = buffer.pop_left()




def write_bsread_messages(buffer, )


class Manager(object):
    def __init__(self, channels, buffer_size=100):

        self._buffer = deque(maxlen=buffer_size)
        self._buffer_thread = None
        self._writer_thread = None

        dispatcher_address = dispatcher.request_stream(channels)

        self._buffer_thread = Thread(target=buffer_bsread_messages, args=(dispatcher_address, self._buffer))

        self._writer_thread = None

    def get_status(self):
        # Buffering should be always running.
        if not self.is_running():
            return "error"

        # We are only buffering.
        if self._writing_thread is None or not self._writer_thread.is_alive():
            return "buffering"

        return "writing"

    def is_running(self):
        return self._buffer_thread.is_alive()

    def start_writing(self, output_file, user_id):
        status = self.get_status()

        if status != "buffering":
            raise ValueError("Cannot start writing in status '%s'." % status)

        self._writer_thread = Thread(target=write_bsread_messages, args=(self._buffer, ))

    def stop_writing(self, pulse_id):
        pass
