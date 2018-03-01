import signal
import unittest

from multiprocessing import Process
from time import sleep

import os

import requests
from bsread.sender import sender, PUSH

from sf_bsread_writer import bsread_writer


class TestBsreadWriter(unittest.TestCase):
    def setUp(self):
        self.rest_port = 10002
        self.stream_port = 12345
        self.output_file = "ignore_bsread.h5"

        self.writer_process = Process(target=bsread_writer.start_server, args=("tcp://127.0.0.1:%d" % self.stream_port,
                                                                               self.output_file,
                                                                               "-1",
                                                                               self.rest_port))

        self.rest_url = "http://localhost:%d/" % self.rest_port
        self.writer_process.start()

        # Give it some time to start.
        sleep(1)

    def tearDown(self):
        self.writer_process.terminate()
        sleep(0.5)

        try:
            os.kill(self.writer_process.pid, signal.SIGINT)
        except:
            pass

        # Wait for the server to die.
        sleep(1)

    def test_classic_interaction(self):
        response = requests.get(self.rest_url + "status").json()
        self.assertEqual(response["state"], "ok")
        self.assertEqual(response["status"], "waiting")

        response = requests.post(self.rest_url + "parameters", json={}).json()
        self.assertEqual(response["state"], 'error')
        self.assertTrue("Missing mandatory" in response["status"])

        parameters = {"general/created": "today",
                      "general/user": "p11057",
                      "general/process": "dia",
                      "general/instrument": "jungfrau"}

        response = requests.post(self.rest_url + "parameters", json=parameters).json()
        self.assertEqual(response["state"], "ok")
        self.assertEqual(response["status"], "waiting")
        self.assertEqual(response["parameters"], parameters)

        requests.put(self.rest_url + "start_pulse_id/3")

        response = requests.get(self.rest_url + "statistics").json()
        self.assertEqual(response["state"], "ok")
        self.assertEqual(response["statistics"]["start_pulse_id"], 3)
        self.assertEqual(response["statistics"]["stop_pulse_id"], None)

        timestamp = 0
        timestamp_offset = 0

        data = {"mock_data": 0}

        with sender(port=self.stream_port, mode=PUSH, queue_size=1) as output_stream:
            for index in range(10):
                pulse_id = index

                output_stream.send(timestamp=(timestamp, timestamp_offset),
                                   pulse_id=pulse_id,
                                   data=data)

                # Somewhere before the end of the acquisition.
                if pulse_id == 6:
                    requests.put(self.rest_url + "stop_pulse_id/8")

                    response = requests.get(self.rest_url + "statistics").json()
                    self.assertEqual(response["state"], "ok")
                    self.assertEqual(response["statistics"]["start_pulse_id"], 3)
                    self.assertEqual(response["statistics"]["stop_pulse_id"], 8)

        sleep(0.5)
        self.assertFalse(self.writer_process.is_alive())

