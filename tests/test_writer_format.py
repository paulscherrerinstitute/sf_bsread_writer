import unittest

import os

import h5py
from bsread import source
from bsread.handlers import extended
from bsread.sender import sender

from sf_bsread_writer.writer_format import BsreadH5Writer


class TestWriterFormat(unittest.TestCase):

    OUTPUT_FILE = "ignore_writer_format.h5"
    STREAM_PORT = 12000
    WRITER_PARAMETERS = {"general/created": "now",
                         "general/instrument": "guitar",
                         "general/process": "deterministic",
                         "general/user": "no, thank you"}

    def setUp(self):
        pass

    def tearDown(self):
        try:
            os.remove(self.OUTPUT_FILE)
        except:
            pass

    def test_start_missing_header(self):

        handler = extended.Handler()
        writer = BsreadH5Writer(self.OUTPUT_FILE, self.WRITER_PARAMETERS)

        with sender(port=self.STREAM_PORT) as output_stream:
            with source(host="localhost", port=self.STREAM_PORT) as input_stream:

                for index in range(10):
                    data = {"fast_source": index,
                            "slow_source": None}

                    output_stream.send(data=data)
                    writer.write_message(input_stream.receive(handler=handler.receive))

                for index in range(10, 20):
                    data = {"fast_source": index,
                            "slow_source": index}

                    output_stream.send(data=data)
                    writer.write_message(input_stream.receive(handler=handler.receive))

        writer.close()

        file = h5py.File(self.OUTPUT_FILE)

        fast_source = file["/data/fast_source/data"]
        slow_source = file["/data/slow_source/data"]

        self.assertIsNotNone(fast_source)
        self.assertIsNotNone(slow_source)

        self.assertListEqual(list(fast_source), list(range(20)))

        self.assertListEqual(list(slow_source[:10]), [0] * 10)
        self.assertListEqual(list(slow_source[10:]), list(range(10, 20)))

        file.close()