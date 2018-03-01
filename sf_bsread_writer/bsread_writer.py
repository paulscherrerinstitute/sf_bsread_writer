import argparse
import logging
from threading import Event, Thread

import bottle
import h5py
import os

import numpy
from bsread import PULL, source, json
from bsread.data.serialization import channel_type_deserializer_mapping
from bsread.handlers import extended
from bsread.writer import Writer

_logger = logging.getLogger(__name__)


class BsreadWriter(object):
    def __init__(self, output_file, parameters):
        self.output_file = output_file
        self.parameters = parameters

        self.h5_writer = Writer()
        self.h5_writer.open_file(self.output_file)

        self.first_iteration = True

    def prune_and_close(self, stop_pulse_id):
        # TODO: Prune.
        # dset_pulse_id = h5_file['pulse_id']
        # while dset_pulse_id[-1] > end_pulse_id:
        #     # this will also discard the data
        #     # see the Note at http://docs.h5py.org/en/latest/high/dataset.html#resizable-datasets
        #     dset_pulse_id.resize(dset_pulse_id.shape[0] - 1, axis=0)

        self.close()

    def write_message(self, message):
        message_data = message.data

        if self.first_iteration and "data_header" in message_data:
            self.prepare_datasets(message_data)
            self.first_iteration = False

        data = message_data['data']

        self.h5_writer.write(data, dataset_group_name='data')
        self.h5_writer.write(message_data['pulse_ids'], dataset_group_name='pulse_id')

    def prepare_datasets(self, message_data):

        data_header = message_data['data_header']
        _logger.debug("Data Header: ", data_header)

        # Interpret the data header and add required datasets
        for channel in data_header['channels']:
            channel_type = channel.get('type')

            group_name = '/data/' + channel['name'] + "/"

            self.h5_writer.add_dataset(group_name + 'pulse_id', dataset_group_name='pulse_id', dtype='i8')

            if channel_type and channel_type.lower() == "string":
                shape = [1]
                maxshape = [None]
                dtype = h5py.special_dtype(vlen=str)

                self.h5_writer.add_dataset(group_name + "data", dataset_group_name='data', shape=shape,
                                           maxshape=maxshape, dtype=dtype)

            else:

                dtype = channel_type_deserializer_mapping[channel_type][0]

                if 'shape' in channel:
                    # H5 is slowest dimension first, but bsread is fastest dimension first.
                    shape = [1] + channel['shape'][::-1]
                    maxshape = [None] + channel['shape'][::-1]

                    self.h5_writer.add_dataset(group_name + "data", dataset_group_name='data', shape=shape,
                                               maxshape=maxshape, dtype=dtype)
                else:
                    self.h5_writer.add_dataset(group_name + "data", dataset_group_name='data', dtype=dtype)

        self.h5_writer.file.create_dataset("/general/created",
                                           data=numpy.string_(self.parameters["general/created"]))

        self.h5_writer.file.create_dataset("/general/instrument",
                                           data=numpy.string_(self.parameters["general/instrument"]))

        self.h5_writer.file.create_dataset("/general/process",
                                           data=numpy.string_(self.parameters["general/process"]))

        self.h5_writer.file.create_dataset("/general/user",
                                           data=numpy.string_(self.parameters["general/user"]))

    def close(self):
        self.h5_writer.close_file()


class BsreadWriterManager(object):

    REQUIRED_PARAMETERS = ["general/created", "general/user", "general/process", "general/instrument"]

    def __init__(self, stream_address, output_file, receive_timeout=1000, mode=PULL):

        self.stream_address = stream_address
        self.output_file = output_file
        self.receive_timeout = receive_timeout
        self.mode = mode
        self.parameters = {}

        self._running_event = Event()
        self._running_event.clear()

        self._writing_thread = None

        self.start_pulse_id = None
        self.stop_pulse_id = None
        self.last_pulse_id = -1

    def write_stream(self, start_pulse_id):

        source_host, source_port = self.stream_address.rsplit(":", maxsplit=1)

        source_host = source_host.split("//")[1]
        source_port = int(source_port)

        _logger.info("Input stream host '%s' and port '%s'.", source_host, source_port)

        _logger.info("First pulse_id to write: %d.", start_pulse_id)

        writer = BsreadWriter(self.output_file, self.parameters)
        handler = extended.Handler()

        with source(host=source_host, port=source_port,
                    mode=self.mode, receive_timeout=self.receive_timeout,
                    queue_size=1) as stream:

            self._running_event.set()

            while self._running_event.is_set():

                message = stream.receive(handler=handler.receive)

                # In case you set a receive timeout, the returned message can be None.
                if message is None:

                    # If the current pulse_id is above the stop_pulse_id, stop the recording.
                    if self.stop_pulse_id is not None and self.last_pulse_id > self.stop_pulse_id:
                        writer.prune_and_close(self.stop_pulse_id)

                        _logger.info("Stopping bsread writer at pulse_id: %d" % self.stop_pulse_id)
                        self._running_event.clear()

                    continue

                self.last_pulse_id = message.data["header"]["pulse_id"]
                _logger.debug('Received message with pulse_id %d.', self.last_pulse_id)

                # If this pulse_id was generated before the first detector image, discard it.
                if self.last_pulse_id < start_pulse_id:
                    _logger.debug("Discarding message with pulse_id %d (before start_pulse_id).", self.last_pulse_id)
                    continue

                # If the current pulse_id is above the stop_pulse_id, stop the recording.
                if self.stop_pulse_id is not None and self.last_pulse_id > self.stop_pulse_id:
                    writer.prune_and_close(self.stop_pulse_id)

                    _logger.info("Stopping bsread writer at pulse_id: %d" % self.stop_pulse_id)
                    self._running_event.clear()
                    continue

                writer.write_message(message)

        _logger.info("Writing completed. Pulse_id range from %d to %d written to file.",
                     start_pulse_id, self.stop_pulse_id)

        os._exit(0)

    def set_parameters(self, parameters):

        _logger.debug("Setting parameters " % parameters)

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

        _logger.info("Starting to write with pulse_id %d." % pulse_id)

        self.start_pulse_id = pulse_id

        self._writing_thread = Thread(target=self.write_stream, args=(pulse_id,))

        self._running_event.clear()

        self._writing_thread.start()

        if not self._running_event.wait(2):
            _logger.error("Bsread writer did not start in time. Killing.")
            os._exit(-1)

    def stop_writer(self, pulse_id):
        _logger.info("Set stop_pulse_id=%d", pulse_id)
        self.stop_pulse_id = pulse_id

    def get_statistics(self):
        return {"start_pulse_id": self.start_pulse_id,
                "stop_pulse_id": self.stop_pulse_id,
                "last_pulse_id": self.last_pulse_id}


def register_rest_interface(app, manager):

    @app.get("/status")
    def get_status():
        return {"state": "ok",
                "status": manager.get_status()}

    @app.post("/parameters")
    def set_parameters():
        manager.set_parameters(bottle.request.json)

        return {"state": "ok",
                "status": manager.get_status(),
                "parameters": manager.get_parameters()}

    @app.get("/stop")
    def stop():

        manager.stop()

        return {"state": "ok",
                "status": manager.get_status()}

    @app.get("/kill")
    def kill():
        os._exit(0)

    @app.get("/statistics")
    def get_statistics():
        return {"state": "ok",
                "status": manager.get_status(),
                "statistics": manager.get_statistics()}

    @app.put("/start_pulse_id/<pulse_id:int>")
    def start_pulse_id(pulse_id):

        manager.start_writer(pulse_id)

    @app.put("/stop_pulse_id/<pulse_id:int>")
    def stop_pulse_id(pulse_id):

        manager.stop_writer(pulse_id)

    @app.error(500)
    def error_handler_500(error):
        bottle.response.content_type = 'application/json'
        bottle.response.status = 200

        error_text = str(error.exception)

        _logger.error(error_text)

        return json.dumps({"state": "error",
                           "status": error_text})


def start_server(stream_address, output_file, user_id, rest_port):
    app = bottle.Bottle()

    manager = BsreadWriterManager(stream_address, output_file)

    register_rest_interface(app, manager)

    _logger.info("Setting bsread writer uid and gid to %s.", user_id)

    os.setgid(user_id)
    os.setuid(user_id)

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
                 output_file=arguments.output_file,
                 user_id=arguments.user_id,
                 rest_port=arguments.rest_port)


if __name__ == "__main__":
    run()
