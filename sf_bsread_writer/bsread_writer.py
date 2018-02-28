import argparse
import logging
from threading import Event, Thread

import bottle
from bsread import PULL, source

_logger = logging.getLogger(__name__)


def write_messages(self, start_pulse_id):
        self._logger.info("Writing channels to output_file '%s'.", self.output_file)

        try:
            first_iteration = True

            if start_pulse_id < self._buffer[0].data.pulse_id:
                self._logger.warning("start_pulse_id < oldest buffered message pulse_id")

            with h5py.File(self.output_file, 'w') as h5_file:
                while self._running_event.is_set():
                    if len(self._buffer) == 0:
                        sleep(0.1)  # wait for more messages being buffered
                        continue

                    # process the oldest buffered message
                    next_msg = self._buffer.popleft()
                    msg_pulse_id = next_msg.data.pulse_id

                    if self.end_pulse_id and self.end_pulse_id < msg_pulse_id:
                        # no more messages to write
                        end_pulse_id = self.end_pulse_id
                        self.end_pulse_id = None

                        # finilize hdf5 file
                        if end_pulse_id < msg_pulse_id:
                            self.prune_messages_in_hdf5(h5_file, end_pulse_id)

                        break

                    if msg_pulse_id < start_pulse_id:
                        self._logger.debug('Discard %d', msg_pulse_id)
                        continue  # discard the message

                    self._logger.debug('Write to hdf5 %d', msg_pulse_id)
                    self.write_message_to_hdf5(h5_file, next_msg, first_iteration)
                    first_iteration = False

        except:
            self._logger.exception("Error while writing bsread stream.")

    @staticmethod
    def write_message_to_hdf5(h5_file, message, first_iteration):
        """ Placeholder for a function to write message's content into hdf5 file.

        Intended for debugging. Saves only pulse_ids of messages!
        """
        if first_iteration:
            dset_pulse_id = h5_file.create_dataset('pulse_id', dtype='i8', shape=(1, 1), maxshape=(None, 1))
        else:
            dset_pulse_id = h5_file['pulse_id']
            dset_pulse_id.resize(dset_pulse_id.shape[0] + 1, axis=0)

        dset_pulse_id[-1] = message.data.pulse_id

    @staticmethod
    def prune_messages_in_hdf5(h5_file, end_pulse_id):
        """ Placeholder for a function to prune hdf5 file content down to end_pulse_id.

        Intended for debugging.
        """
        dset_pulse_id = h5_file['pulse_id']
        while dset_pulse_id[-1] > end_pulse_id:
            # this will also discard the data
            # see the Note at http://docs.h5py.org/en/latest/high/dataset.html#resizable-datasets
            dset_pulse_id.resize(dset_pulse_id.shape[0] - 1, axis=0)

class BsreadWriter(object):
    def __init__(self, output_file):
        self.output_file = output_file

    def prune_and_close(self, stop_pulse_id):
        pass

    def write_message(self, message):
        pass

class BsreadWriterManager(object):
    def __init__(self, stream_address, output_file, user_id, receive_timeout=1000, mode=PULL):

        self.stream_address = stream_address
        self.output_file = output_file
        self.user_id = user_id
        self.receive_timeout = receive_timeout
        self.mode = mode

        self._running_event = Event()
        self._running_event.clear()

        self._writing_thread = None

        self.stop_pulse_id = None

    def write_stream(self, start_pulse_id):

        source_host, source_port = self.stream_address.rsplit(":", maxsplit=1)

        source_host = source_host.split("//")[1]
        source_port = int(source_port)

        _logger.info("Input stream host '%s' and port '%s'.", source_host, source_port)

        _logger.info("First pulse_id to write: %d.", start_pulse_id)

        last_pulse_id = -1

        writer = BsreadWriter(self.output_file)

        with source(host=source_host, port=source_port,
                    mode=self.mode, receive_timeout=self.receive_timeout) as stream:

            nonlocal last_pulse_id

            self._running_event.set()

            while self._running_event.is_set():

                message = stream.receive()

                # In case you set a receive timeout, the returned message can be None.
                if message is None:
                    continue

                last_pulse_id = message.data.pulse_id
                _logger.debug('Received message with pulse_id %d.', last_pulse_id)

                # If this pulse_id was generated before the first detector image, discard it.
                if last_pulse_id < start_pulse_id:
                    continue

                # If the current pulse_id is above the stop_pulse_id, stop the recording.
                if self.stop_pulse_id is not None and last_pulse_id > self.stop_pulse_id:
                    writer.prune_and_close(self.stop_pulse_id)

                    _logger.info("Stopping bsread writer at pulse_id: %d" % self.stop_pulse_id)
                    self._running_event.clear()
                    continue

                writer.write_message(message)

        _logger.info("Writing completed. Pulse_id range from %d to %d written to file.",
                     start_pulse_id, self.stop_pulse_id)



    def get_status(self):
        if self._writing_thread is None:
            return "waiting"

        if self._writing_thread is not None and self._writing_thread.is_alive():
            return "writing"

        return "error"

    def stop(self):
        self._running_event.clear()

        if self._writing_thread is not None:
            self._writing_thread.join()
            self._writing_thread = None

        exit()

    def start_writer(self, pulse_id):

        self._writing_thread = Thread(target=self.write_stream, args=(pulse_id,))

        self._running_event.clear()

        self._writing_thread.start()

        if not self._running_event.wait(2):
            _logger.error("Bsread writer did not start in time. Killing.")
            exit()

    def stop_writer(self, pulse_id):

        self.stop_pulse_id = pulse_id


def register_rest_interface(app, manager):

    @app.get("/status")
    def get_status():
        return {"state": "ok",
                "status": manager.get_status()}

    @app.get("/stop")
    def stop():

        manager.stop()

        return {"state": "ok",
                "status": manager.get_status()}

    @app.get("/kill")
    def kill():
        exit()

    @app.put("/start_pulse_id/<pulse_id>")
    def start_pulse_id(pulse_id):

        manager.start_writer(pulse_id)

    @app.put("/stop_pulse_id/<pulse_id>")
    def stop_pulse_id(pulse_id):

        manager.stop_writer(pulse_id)

def start_server(stream_address, output_file, user_id, rest_port):
    app = bottle.Bottle()

    manager = BsreadWriterManager(stream_address, output_file, user_id)

    register_rest_interface(app, manager)

    try:
        bottle.run(app=app, host="127.0.0.1", port=rest_port)
    finally:
        pass

def run():
    parser = argparse.ArgumentParser(description='bsread buffer')

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
                 output_file=arguments.output_port,
                 user_id=arguments.user_id,
                 rest_port=arguments.rest_port)


if __name__ == "__main__":
    run()