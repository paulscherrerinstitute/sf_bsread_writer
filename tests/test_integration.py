import signal
import unittest

from multiprocessing import Process
from time import sleep

import os
from unittest.mock import Mock

import h5py
import requests
from bsread import dispatcher, PUB
from bsread.sender import sender

from sf_bsread_writer import writer, buffer


class TestBsreadWriter(unittest.TestCase):
    def setUp(self):

        self.writer_rest_port = 10002
        self.writer_stream_port = 12345
        self.writer_output_file = "ignore_integration.h5"
        self.writer_rest_url = "http://localhost:%d/" % self.writer_rest_port

        self.buffer_stream_port = 9999
        self.buffer_stream_address = "tcp://localhost:%d" % self.buffer_stream_port

        mock_request_stream = Mock()
        mock_request_stream.return_value = self.buffer_stream_address
        dispatcher.request_stream = mock_request_stream

        self.buffer_process = Process(target=buffer.start_server, args=([], self.writer_stream_port, 100))

        self.writer_process = Process(target=writer.start_server, args=("tcp://127.0.0.1:%d" % self.writer_stream_port,
                                                                        self.writer_output_file,
                                                                        -1,
                                                                        self.writer_rest_port))

    def tearDown(self):
        self.buffer_process.terminate()
        self.writer_process.terminate()
        sleep(0.5)

        try:
            os.kill(self.buffer_process.pid, signal.SIGINT)
        except:
            pass

        try:
            os.kill(self.writer_process.pid, signal.SIGINT)
        except:
            pass

        try:
            os.remove(self.writer_output_file)
        except:
            pass

        # Wait for the server to die.
        sleep(0.5)

    def test_buffer_writer_integration(self):

        with sender(port=self.buffer_stream_port, queue_size=100, mode=PUB) as source_stream:

            self.buffer_process.start()
            sleep(0.5)

            for index in range(0, 10):
                data = {"normal_source": index,
                        "slow_source": None,
                        "changing_source": index}

                source_stream.send(data=data)

            for index in range(10, 20):
                data = {"normal_source": index,
                        "slow_source": index,
                        "changing_source": [index] * 3}

                source_stream.send(data=data)

            # 31, because the writer stops AFTER receiving the first message it should not write.
            for index in range(20, 31):
                data = {"normal_source": index,
                        "slow_source": None,
                        "changing_source": [index] * 3}

                source_stream.send(data=data)

        self.writer_process.start()
        sleep(0.5)

        response = requests.get(self.writer_rest_url + "status").json()
        self.assertEqual(response["state"], "ok")
        self.assertEqual(response["status"], "waiting")

        parameters = {"general/created": "today",
                      "general/user": "p11057",
                      "general/process": "dia",
                      "general/instrument": "jungfrau"}

        response = requests.post(self.writer_rest_url + "parameters", json=parameters).json()
        self.assertEqual(response["state"], "ok")
        self.assertEqual(response["status"], "waiting")
        self.assertEqual(response["parameters"], parameters)

        requests.put(self.writer_rest_url + "start_pulse_id/0")

        response = requests.get(self.writer_rest_url + "statistics").json()
        self.assertEqual(response["state"], "ok")
        self.assertEqual(response["statistics"]["start_pulse_id"], 0)
        self.assertEqual(response["statistics"]["stop_pulse_id"], None)

        requests.put(self.writer_rest_url + "stop_pulse_id/29")

        # Wait for the writer to terminate.
        sleep(1.5)

        self.assertFalse(self.writer_process.is_alive(), "Writer process should be dead by now.")
        self.assertTrue(self.buffer_process.is_alive(), "Buffer process should be still alive.")

        file = h5py.File(self.writer_output_file)

        normal_source = file["/data/normal_source/data"]
        slow_source = file["/data/slow_source/data"]
        changing_source_1 = file["/data/changing_source/data(1)"]
        changing_source_2 = file["/data/changing_source/data"]

        self.assertEqual(len(normal_source), 30)
        self.assertEqual(len(slow_source), 30)
        self.assertEqual(len(changing_source_1), 10)
        self.assertEqual(len(changing_source_2), 30)
